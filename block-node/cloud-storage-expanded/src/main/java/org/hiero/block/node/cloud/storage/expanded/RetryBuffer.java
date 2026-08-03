// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.expanded;

import static java.lang.System.Logger.Level.DEBUG;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.hiero.block.node.spi.blockmessaging.BlockSource;

/// In-memory holding area for blocks whose S3 upload failed and are awaiting background retry.
/// Nothing is written to local disk, so a buffered block is lost if the process restarts.
/// Bounded by {@link ExpandedCloudStorageConfig#retryMaxAgeSeconds()} and
/// {@link ExpandedCloudStorageConfig#retryMaxPendingBlocks()}.
///
/// {@link #stage}, {@link #recordFailure}, and {@link #unstage} mutate {@link #buffered} via
/// {@link ConcurrentHashMap#computeIfAbsent} / {@link ConcurrentHashMap#compute} so that concurrent
/// calls for the *same* block number (a duplicate `VerificationNotification` is possible upstream) are
/// serialized; different block numbers may still be manipulated fully concurrently.
class RetryBuffer {

    private static final System.Logger LOGGER = System.getLogger(RetryBuffer.class.getName());

    /// Outcome of {@link #recordFailure(long)}.
    enum RetryOutcome {
        /// The block remains buffered and will be retried again later.
        RETRYING,
        /// The block exceeded `retryMaxAgeSeconds` and was dropped from the buffer.
        EXHAUSTED,
        /// The block was not buffered; likely already resolved by a concurrent {@link #unstage}. Not the
        /// same as {@link #EXHAUSTED} — the block may already be reported successful.
        NOT_STAGED
    }

    /// In-memory record of a block awaiting background retry.
    ///
    /// @param blockNumber        the block number
    /// @param compressedBytes    the already-compressed (ZSTD) block bytes
    /// @param objectKey          the S3 object key this block should be uploaded to
    /// @param storageClass       the S3 storage class to use for the upload
    /// @param blockSource        origin of the block, forwarded to `PersistedNotification`
    /// @param attempts           attempts made so far; kept for logging only, doesn't affect exhaustion
    /// @param firstBufferedEpochMs wall-clock time the block was first buffered
    /// @param nextEligibleEpochMs earliest wall-clock time this block may be retried again
    record BufferedEntry(
            long blockNumber,
            byte[] compressedBytes,
            String objectKey,
            String storageClass,
            BlockSource blockSource,
            int attempts,
            long firstBufferedEpochMs,
            long nextEligibleEpochMs) {}

    private final ExpandedCloudStorageConfig config;
    private final ConcurrentHashMap<Long, BufferedEntry> buffered = new ConcurrentHashMap<>();

    /// Set by {@link #close()}; makes {@link #stage} a permanent no-op.
    private volatile boolean closed;

    RetryBuffer(@NonNull final ExpandedCloudStorageConfig config) {
        this.config = config;
    }

    /// Buffers the given compressed block bytes in memory for background retry.
    ///
    /// @param blockNumber     the block number
    /// @param compressedBytes the already-compressed (ZSTD) block bytes
    /// @param objectKey       the S3 object key this block should be uploaded to
    /// @param storageClass    the S3 storage class to use for the upload
    /// @param blockSource     origin of the block
    /// @return `true` if buffered (now or already), `false` if retry is disabled, the buffer is
    ///         full, or {@link #close}d
    boolean stage(
            final long blockNumber,
            @NonNull final byte[] compressedBytes,
            @NonNull final String objectKey,
            @NonNull final String storageClass,
            @NonNull final BlockSource blockSource) {
        if (!config.retryEnabled() || closed) {
            return false;
        }
        // A duplicate for an already-buffered block must not count against the cap.
        if (!buffered.containsKey(blockNumber) && buffered.size() >= config.retryMaxPendingBlocks()) {
            LOGGER.log(DEBUG, "Retry buffer full; not buffering block {0}.", blockNumber);
            return false;
        }
        // computeIfAbsent leaves an already-buffered block untouched and serializes this write
        // against a concurrent recordFailure()/unstage() for the same block number.
        final BufferedEntry result = buffered.computeIfAbsent(blockNumber, key -> {
            final long now = System.currentTimeMillis();
            return new BufferedEntry(key, compressedBytes, objectKey, storageClass, blockSource, 1, now, now);
        });
        return result != null;
    }

    /// @param now the current time
    /// @return buffered entries whose backoff has elapsed as of `now`
    @NonNull
    List<BufferedEntry> dueForRetry(@NonNull final Instant now) {
        final long nowMs = now.toEpochMilli();
        final List<BufferedEntry> due = new ArrayList<>();
        for (final BufferedEntry entry : buffered.values()) {
            if (entry.nextEligibleEpochMs() <= nowMs) {
                due.add(entry);
            }
        }
        return due;
    }

    /// Removes a block from the buffer, e.g. once a retry succeeds.
    ///
    /// @param blockNumber the block to remove
    /// @return `true` if an entry was actually removed, `false` if it was already gone (e.g. drained)
    boolean unstage(final long blockNumber) {
        return buffered.remove(blockNumber) != null;
    }

    /// Records another failed retry attempt. Drops the block once buffered longer than
    /// `retryMaxAgeSeconds`; otherwise pushes its next eligible retry time out by the fixed
    /// `retryIntervalSeconds`.
    ///
    /// @param blockNumber the block that failed another retry attempt
    /// @return the resulting {@link RetryOutcome}
    @NonNull
    RetryOutcome recordFailure(final long blockNumber) {
        // compute() serializes against a concurrent stage()/unstage() for the same block.
        final AtomicBoolean wasStaged = new AtomicBoolean(true);
        final BufferedEntry result = buffered.compute(blockNumber, (key, previous) -> {
            if (previous == null) {
                // A concurrent unstage() (e.g. duplicate notification succeeding live) can remove
                // the entry while a retry attempt for it is in flight.
                LOGGER.log(DEBUG, "recordFailure() called for block {0}, which is no longer buffered.", key);
                wasStaged.set(false);
                return null;
            }
            final int attempts = previous.attempts() + 1;
            final long now = System.currentTimeMillis();
            final long ageMs = now - previous.firstBufferedEpochMs();
            if (ageMs >= config.retryMaxAgeSeconds() * 1_000L) {
                LOGGER.log(DEBUG, "Block {0} exceeded retryMaxAgeSeconds after {1} attempts.", key, attempts);
                return null;
            }
            return new BufferedEntry(
                    key,
                    previous.compressedBytes(),
                    previous.objectKey(),
                    previous.storageClass(),
                    previous.blockSource(),
                    attempts,
                    previous.firstBufferedEpochMs(),
                    now + config.retryIntervalSeconds() * 1_000L);
        });
        return result != null
                ? RetryOutcome.RETRYING
                : (wasStaged.get() ? RetryOutcome.EXHAUSTED : RetryOutcome.NOT_STAGED);
    }

    /// @return the number of blocks currently buffered and awaiting retry
    int pendingCount() {
        return buffered.size();
    }

    /// Removes and returns every buffered entry. Call {@link #close()} first so a task racing
    /// this can't add an entry that gets silently wiped by `clear()` without ever being returned.
    ///
    /// @return all entries that were buffered at the moment of the call
    @NonNull
    List<BufferedEntry> drainAll() {
        final List<BufferedEntry> drained = new ArrayList<>(buffered.values());
        buffered.clear();
        return drained;
    }

    /// Permanently stops accepting new entries; every subsequent {@link #stage} call returns
    /// `false`. Called at the start of {@link ExpandedCloudStoragePlugin#stop()} so a block that
    /// fails for the first time during shutdown can't be orphaned in the buffer after
    /// {@link #drainAll()} has already run.
    void close() {
        closed = true;
    }
}
