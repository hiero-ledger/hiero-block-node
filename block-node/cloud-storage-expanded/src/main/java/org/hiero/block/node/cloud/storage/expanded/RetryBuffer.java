// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.expanded;

import static java.lang.System.Logger.Level.DEBUG;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.hiero.block.node.spi.blockmessaging.BlockSource;

/// In-memory holding area for blocks whose S3 upload failed and are awaiting background retry.
/// Nothing is written to local disk, so a buffered block is lost if the process restarts.
/// Bounded by {@link ExpandedCloudStorageConfig#retryMaxAgeSeconds()} and
/// {@link ExpandedCloudStorageConfig#retryMaxPendingBlocks()} — once at capacity, {@link #stage}
/// evicts the longest-buffered entry to make room for the newest failure and hands it back via
/// {@link StageOutcome#evicted()} so the caller can report a terminal failure for it.
///
/// {@link #stage} and {@link #recordFailure} mutate {@link #buffered} via
/// {@link ConcurrentHashMap#compute} so that concurrent calls for the *same* block number (a duplicate
/// `VerificationNotification` is possible upstream) are serialized; different block numbers may still
/// be manipulated fully concurrently.
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

    /// Outcome of {@link #stage(long, byte[], String, String, BlockSource)}.
    ///
    /// @param staged  `true` if the block is now buffered (either freshly or already), `false` if
    ///                retry is disabled or the buffer is {@link #close}d
    /// @param evicted the entry displaced to make room for this call, or `null` if none was
    ///                evicted; never the block just staged. The caller must report a terminal
    ///                `succeeded=false` for it, since it will never be retried.
    record StageOutcome(boolean staged, @Nullable BufferedEntry evicted) {}

    /// Outcome of {@link #recordFailure(long)}, pairing the {@link RetryOutcome} with the
    /// `blockSource` of the entry acted on (`null` for {@link RetryOutcome#NOT_STAGED}, since no
    /// entry was found).
    record FailureResult(
            @NonNull RetryOutcome outcome, @Nullable BlockSource blockSource) {}

    private final ExpandedCloudStorageConfig config;
    private final ConcurrentHashMap<Long, BufferedEntry> buffered = new ConcurrentHashMap<>();

    /// Set by {@link #close()}; makes {@link #stage} a permanent no-op.
    private volatile boolean closed;

    RetryBuffer(@NonNull final ExpandedCloudStorageConfig config) {
        this.config = config;
    }

    /// Buffers the given compressed block bytes in memory for background retry. A duplicate call
    /// for an already-buffered block replaces its bytes/objectKey/storageClass/blockSource with
    /// this latest offering (the newest arrival may carry a correction) while preserving its
    /// existing attempt count and backoff timing.
    ///
    /// @param blockNumber     the block number
    /// @param compressedBytes the already-compressed (ZSTD) block bytes
    /// @param objectKey       the S3 object key this block should be uploaded to
    /// @param storageClass    the S3 storage class to use for the upload
    /// @param blockSource     origin of the block
    /// @return the {@link StageOutcome} of this call
    @NonNull
    StageOutcome stage(
            final long blockNumber,
            @NonNull final byte[] compressedBytes,
            @NonNull final String objectKey,
            @NonNull final String storageClass,
            @NonNull final BlockSource blockSource) {
        if (!config.retryEnabled() || closed) {
            return new StageOutcome(false, null);
        }
        // A duplicate for an already-buffered block doesn't grow the buffer, so it must never
        // trigger an eviction.
        final BufferedEntry evicted =
                !buffered.containsKey(blockNumber) && buffered.size() >= config.retryMaxPendingBlocks()
                        ? evictOldestToMakeRoom(blockNumber)
                        : null;
        // compute() serializes this write against a concurrent recordFailure()/unstage() for the
        // same block number.
        buffered.compute(blockNumber, (key, previous) -> {
            final long now = System.currentTimeMillis();
            return previous == null
                    ? new BufferedEntry(key, compressedBytes, objectKey, storageClass, blockSource, 1, now, now)
                    : new BufferedEntry(
                            key,
                            compressedBytes,
                            objectKey,
                            storageClass,
                            blockSource,
                            previous.attempts(),
                            previous.firstBufferedEpochMs(),
                            previous.nextEligibleEpochMs());
        });
        return new StageOutcome(true, evicted);
    }

    /// Evicts the longest-buffered entry so `incomingBlockNumber` can be admitted despite the
    /// buffer being at `retryMaxPendingBlocks` capacity.
    ///
    /// @return the evicted entry, or `null` if nothing was evicted (a concurrent
    ///         recordFailure()/unstage() removed the chosen entry first)
    @Nullable
    private BufferedEntry evictOldestToMakeRoom(final long incomingBlockNumber) {
        BufferedEntry oldest = null;
        for (final BufferedEntry entry : buffered.values()) {
            if (oldest == null || entry.firstBufferedEpochMs() < oldest.firstBufferedEpochMs()) {
                oldest = entry;
            }
        }
        // Conditional remove: only evicts if nothing else (recordFailure/unstage) touched this
        // entry since the scan above.
        if (oldest != null && buffered.remove(oldest.blockNumber(), oldest)) {
            LOGGER.log(
                    DEBUG,
                    "Retry buffer full; evicted block {0} to make room for block {1}.",
                    oldest.blockNumber(),
                    incomingBlockNumber);
            return oldest;
        }
        return null;
    }

    /// @param nowEpochMs the current wall-clock time, in epoch milliseconds
    /// @return buffered entries whose backoff has elapsed as of `nowEpochMs`
    @NonNull
    List<BufferedEntry> dueForRetry(final long nowEpochMs) {
        final List<BufferedEntry> due = new ArrayList<>();
        for (final BufferedEntry entry : buffered.values()) {
            if (entry.nextEligibleEpochMs() <= nowEpochMs) {
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
    /// @return the resulting {@link FailureResult}
    @NonNull
    FailureResult recordFailure(final long blockNumber) {
        // compute() serializes against a concurrent stage()/unstage() for the same block.
        final AtomicBoolean wasStaged = new AtomicBoolean(true);
        final AtomicReference<BlockSource> blockSource = new AtomicReference<>();
        final BufferedEntry result = buffered.compute(blockNumber, (key, previous) -> {
            if (previous == null) {
                // A concurrent unstage() (e.g. duplicate notification succeeding live) can remove
                // the entry while a retry attempt for it is in flight.
                LOGGER.log(DEBUG, "recordFailure() called for block {0}, which is no longer buffered.", key);
                wasStaged.set(false);
                return null;
            }
            blockSource.set(previous.blockSource());
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
        final RetryOutcome outcome = result != null
                ? RetryOutcome.RETRYING
                : (wasStaged.get() ? RetryOutcome.EXHAUSTED : RetryOutcome.NOT_STAGED);
        return new FailureResult(outcome, blockSource.get());
    }

    /// @return the number of blocks currently buffered and awaiting retry
    int pendingCount() {
        return buffered.size();
    }

    /// Removes and returns every buffered entry. Call {@link #close()} first so a task racing
    /// this can't add an entry that gets silently wiped by `clear()` without ever being returned.
    ///
    /// @return all entries that were buffered at the moment of the call, or `null` if called before
    ///         {@link #close()}
    List<BufferedEntry> drainAll() {
        if (!closed) {
            return null;
        }
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
