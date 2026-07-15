// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.expanded;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import org.hiero.block.node.spi.blockmessaging.BlockSource;

/// On-disk staging area for blocks whose S3 upload failed and are awaiting background retry.
/// Each staged block is written as a `{blockNumber}.blk.zstd` blob plus a
/// `{blockNumber}.meta.properties` sidecar under
/// {@link ExpandedCloudStorageConfig#retryStagingDirectoryPath()}; {@link #loadExisting()} rebuilds
/// the in-memory index from these files on startup so retries survive a process restart.
///
/// {@link #stage}, {@link #recordFailure}, and {@link #unstage} mutate {@link #staged} via
/// {@link ConcurrentHashMap#computeIfAbsent} / {@link ConcurrentHashMap#compute} so that concurrent
/// calls for the *same* block number (a duplicate `VerificationNotification` is possible upstream) are
/// serialized; different block numbers may still be manipulated fully concurrently.
class RetryStagingManager {

    private static final System.Logger LOGGER = System.getLogger(RetryStagingManager.class.getName());
    private static final String BLOB_SUFFIX = ".blk.zstd";
    private static final String META_SUFFIX = ".meta.properties";

    /// Outcome of {@link #recordFailure(long)}.
    enum RetryOutcome {
        /// The block remains staged; its backoff has been extended and it will be retried again later.
        RETRYING,
        /// The block exceeded `retryMaxAttempts` or `retryMaxAgeHours` and has been dropped from staging.
        EXHAUSTED
    }

    /// In-memory mirror of a staged block's sidecar.
    ///
    /// @param blockNumber        the block number
    /// @param objectKey          the S3 object key this block should be uploaded to
    /// @param storageClass       the S3 storage class to use for the upload
    /// @param blockSource        origin of the block, forwarded to `PersistedNotification` on eventual outcome
    /// @param attempts           number of upload attempts made so far (including the original failed one)
    /// @param firstStagedEpochMs wall-clock time the block was first staged
    /// @param nextEligibleEpochMs earliest wall-clock time this block may be retried again
    record StagedEntry(
            long blockNumber,
            String objectKey,
            String storageClass,
            BlockSource blockSource,
            int attempts,
            long firstStagedEpochMs,
            long nextEligibleEpochMs) {

        private static final String KEY_OBJECT_KEY = "objectKey";
        private static final String KEY_STORAGE_CLASS = "storageClass";
        private static final String KEY_BLOCK_SOURCE = "blockSource";
        private static final String KEY_ATTEMPTS = "attempts";
        private static final String KEY_FIRST_STAGED_EPOCH_MS = "firstStagedEpochMs";
        private static final String KEY_NEXT_ELIGIBLE_EPOCH_MS = "nextEligibleEpochMs";

        /// Serializes this entry to a {@link Properties} bag suitable for writing to a sidecar file.
        @NonNull
        Properties toProperties() {
            final Properties props = new Properties();
            props.setProperty(KEY_OBJECT_KEY, objectKey);
            props.setProperty(KEY_STORAGE_CLASS, storageClass);
            props.setProperty(KEY_BLOCK_SOURCE, blockSource.name());
            props.setProperty(KEY_ATTEMPTS, Integer.toString(attempts));
            props.setProperty(KEY_FIRST_STAGED_EPOCH_MS, Long.toString(firstStagedEpochMs));
            props.setProperty(KEY_NEXT_ELIGIBLE_EPOCH_MS, Long.toString(nextEligibleEpochMs));
            return props;
        }

        /// Deserializes a sidecar's {@link Properties} back into a {@link StagedEntry}.
        ///
        /// @param props       the loaded sidecar properties
        /// @param blockNumber the block number, taken from the sidecar's file name rather than its
        ///                    contents
        /// @param source      the sidecar path, used only for error messages
        /// @throws IOException if a required field is missing or malformed
        @NonNull
        static StagedEntry fromProperties(
                @NonNull final Properties props, final long blockNumber, @NonNull final Path source)
                throws IOException {
            final String objectKey = requireProperty(props, KEY_OBJECT_KEY, source);
            final String storageClass = requireProperty(props, KEY_STORAGE_CLASS, source);
            final BlockSource blockSource = parseBlockSource(requireProperty(props, KEY_BLOCK_SOURCE, source), source);
            final int attempts = parseIntProperty(props, KEY_ATTEMPTS, source);
            final long firstStagedEpochMs = parseLongProperty(props, KEY_FIRST_STAGED_EPOCH_MS, source);
            final long nextEligibleEpochMs = parseLongProperty(props, KEY_NEXT_ELIGIBLE_EPOCH_MS, source);
            return new StagedEntry(
                    blockNumber,
                    objectKey,
                    storageClass,
                    blockSource,
                    attempts,
                    firstStagedEpochMs,
                    nextEligibleEpochMs);
        }

        private static String requireProperty(
                @NonNull final Properties props, @NonNull final String key, @NonNull final Path source)
                throws IOException {
            final String value = props.getProperty(key);
            if (value == null) {
                throw new IOException("Missing field '" + key + "' in " + source);
            }
            return value;
        }

        private static int parseIntProperty(
                @NonNull final Properties props, @NonNull final String key, @NonNull final Path source)
                throws IOException {
            try {
                return Integer.parseInt(requireProperty(props, key, source));
            } catch (final NumberFormatException e) {
                throw new IOException("Invalid numeric field '" + key + "' in " + source, e);
            }
        }

        private static long parseLongProperty(
                @NonNull final Properties props, @NonNull final String key, @NonNull final Path source)
                throws IOException {
            try {
                return Long.parseLong(requireProperty(props, key, source));
            } catch (final NumberFormatException e) {
                throw new IOException("Invalid numeric field '" + key + "' in " + source, e);
            }
        }

        private static BlockSource parseBlockSource(@NonNull final String value, @NonNull final Path source)
                throws IOException {
            try {
                return BlockSource.valueOf(value);
            } catch (final IllegalArgumentException e) {
                throw new IOException("Invalid blockSource '" + value + "' in " + source, e);
            }
        }
    }

    private final ExpandedCloudStorageConfig config;
    private final ConcurrentHashMap<Long, StagedEntry> staged = new ConcurrentHashMap<>();

    RetryStagingManager(@NonNull final ExpandedCloudStorageConfig config) {
        this.config = config;
    }

    /// Stages the given compressed block bytes for background retry: writes the blob and its
    /// properties sidecar to disk and adds an entry to the in-memory index.
    ///
    /// @param blockNumber     the block number
    /// @param compressedBytes the already-compressed (ZSTD) block bytes
    /// @param objectKey       the S3 object key this block should be uploaded to
    /// @param storageClass    the S3 storage class to use for the upload
    /// @param blockSource     origin of the block
    /// @return `true` if the block is staged (either by this call or already staged from an earlier
    ///         call), `false` if retry is disabled or writing to disk failed
    boolean stage(
            final long blockNumber,
            @NonNull final byte[] compressedBytes,
            @NonNull final String objectKey,
            @NonNull final String storageClass,
            @NonNull final BlockSource blockSource) {
        if (!config.retryEnabled()) {
            return false;
        }
        // computeIfAbsent leaves an already-staged block untouched (a duplicate VerificationNotification
        // for the same block must not reset its attempts/backoff) and serializes this write against a
        // concurrent recordFailure()/unstage() for the same block number.
        staged.computeIfAbsent(blockNumber, key -> {
            final long now = System.currentTimeMillis();
            final StagedEntry entry = new StagedEntry(key, objectKey, storageClass, blockSource, 1, now, now);
            final Path blobPath = blobPath(key);
            final Path metaPath = metaPath(key);
            try {
                Files.write(blobPath, compressedBytes);
                writeMetaFile(metaPath, entry);
                LOGGER.log(DEBUG, "Wrote retry staging files for block {0}.", key);
                return entry;
            } catch (final IOException e) {
                LOGGER.log(INFO, "Failed to stage block {0} for retry; dropping.", key, e);
                silentDelete(blobPath);
                silentDelete(metaPath);
                return null;
            }
        });
        return staged.containsKey(blockNumber);
    }

    /// Scans {@link ExpandedCloudStorageConfig#retryStagingDirectoryPath()} and rebuilds the in-memory
    /// index from any pre-existing sidecar/blob pairs. Called once from `start()` so blocks staged
    /// before a restart are not lost. Any sidecar or blob file whose counterpart is missing is deleted
    /// as an orphan.
    void loadExisting() {
        final Path dir = config.retryStagingDirectoryPath();
        if (!Files.isDirectory(dir)) {
            return;
        }
        final List<Path> allFiles;
        try (final Stream<Path> stream = Files.list(dir)) {
            allFiles = stream.toList();
            for (final Path metaPath : allFiles) {
                if (!metaPath.getFileName().toString().endsWith(META_SUFFIX)) {
                    continue;
                }
                final long blockNumber;
                try {
                    blockNumber = blockNumberFromFileName(metaPath, META_SUFFIX);
                } catch (final NumberFormatException e) {
                    LOGGER.log(
                            DEBUG,
                            "Retry sidecar {0} has a non-numeric block number in its filename; deleting.",
                            metaPath);
                    silentDelete(metaPath);
                    continue;
                }
                final Path blobPath = blobPath(blockNumber);
                if (!Files.isRegularFile(blobPath)) {
                    LOGGER.log(DEBUG, "Orphaned retry sidecar {0} has no matching blob; deleting.", metaPath);
                    silentDelete(metaPath);
                    continue;
                }
                try {
                    final StagedEntry entry = readMetaFile(metaPath, blockNumber);
                    staged.put(blockNumber, entry);
                } catch (final IOException e) {
                    LOGGER.log(WARNING, "Failed to parse retry sidecar {0}; discarding staged block.", metaPath, e);
                    silentDelete(metaPath);
                    silentDelete(blobPath);
                }
            }
        } catch (final IOException e) {
            LOGGER.log(INFO, "Failed to scan retry staging directory {0}.", dir, e);
            return;
        }
        // Clean up any blob file whose sidecar is missing or unparsable (the inverse orphan case).
        for (final Path blobPath : allFiles) {
            if (!blobPath.getFileName().toString().endsWith(BLOB_SUFFIX)) {
                continue;
            }
            final long blockNumber;
            try {
                blockNumber = blockNumberFromFileName(blobPath, BLOB_SUFFIX);
            } catch (final NumberFormatException e) {
                LOGGER.log(
                        WARNING, "Retry blob {0} has a non-numeric block number in its filename; deleting.", blobPath);
                silentDelete(blobPath);
                continue;
            }
            if (!staged.containsKey(blockNumber)) {
                silentDelete(blobPath);
            }
        }
        if (!staged.isEmpty()) {
            LOGGER.log(INFO, "Recovered {0} block(s) awaiting retry from a previous run.", staged.size());
        }
    }

    /// Returns all staged entries whose backoff has elapsed as of `now`.
    @NonNull
    List<StagedEntry> dueForRetry(@NonNull final Instant now) {
        final long nowMs = now.toEpochMilli();
        final List<StagedEntry> due = new ArrayList<>();
        for (final StagedEntry entry : staged.values()) {
            if (entry.nextEligibleEpochMs() <= nowMs) {
                due.add(entry);
            }
        }
        return due;
    }

    /// Reads the staged compressed bytes for a retry attempt.
    ///
    /// @param entry the staged entry to read bytes for
    /// @return the compressed block bytes
    /// @throws IOException if the blob file cannot be read
    byte[] readBytes(@NonNull final StagedEntry entry) throws IOException {
        return Files.readAllBytes(blobPath(entry.blockNumber()));
    }

    /// Removes a block from staging: deletes both files and the in-memory entry. Called both when a
    /// retry succeeds and, from {@link #recordFailure(long)}, when a block's retries are exhausted.
    void unstage(final long blockNumber) {
        staged.compute(blockNumber, (key, previous) -> {
            deleteStagedFiles(key);
            return null;
        });
    }

    /// Records another failed retry attempt for the given block, growing its backoff. If the block has
    /// exceeded `retryMaxAttempts` or `retryMaxAgeHours` it is dropped from staging entirely.
    ///
    /// @param blockNumber the block that failed another retry attempt
    /// @return {@link RetryOutcome#RETRYING} if the block remains staged, {@link RetryOutcome#EXHAUSTED}
    ///         if it was dropped
    @NonNull
    RetryOutcome recordFailure(final long blockNumber) {
        // Computed atomically per block number (via ConcurrentHashMap#compute) so a concurrent stage()
        // or unstage() for the same block cannot interleave with this read-modify-write. compute()
        // returns the new mapping (or null once removed), which already tells us the outcome: null means
        // the block was dropped (unknown block or retry limits exceeded), so no separate outcome variable
        // is needed.
        final StagedEntry result = staged.compute(blockNumber, (key, previous) -> {
            if (previous == null) {
                // Should not happen in practice, but treat an unknown block as already exhausted.
                return null;
            }
            final int attempts = previous.attempts() + 1;
            final long now = System.currentTimeMillis();
            final long ageHours = (now - previous.firstStagedEpochMs()) / 3_600_000L;
            if (attempts > config.retryMaxAttempts() || ageHours >= config.retryMaxAgeHours()) {
                LOGGER.log(
                        DEBUG,
                        "Block {0} exceeded retry limits (attempts={1}, ageHours={2}); removing from staging index.",
                        key,
                        attempts,
                        ageHours);
                deleteStagedFiles(key);
                return null;
            }
            final long baseDelayMs = config.retryBaseBackoffSeconds() * 1_000L;
            final long maxDelayMs = config.retryMaxBackoffSeconds() * 1_000L;
            final int shift = Math.min(attempts - 1, 32);
            final long delayMs = Math.min(baseDelayMs << shift, maxDelayMs);
            final StagedEntry updated = new StagedEntry(
                    key,
                    previous.objectKey(),
                    previous.storageClass(),
                    previous.blockSource(),
                    attempts,
                    previous.firstStagedEpochMs(),
                    now + delayMs);
            try {
                writeMetaFile(metaPath(key), updated);
            } catch (final IOException e) {
                LOGGER.log(WARNING, "Failed to persist retry backoff for block {0}; keeping in-memory state.", key, e);
            }
            return updated;
        });
        return result == null ? RetryOutcome.EXHAUSTED : RetryOutcome.RETRYING;
    }

    /// @return the number of blocks currently staged and awaiting retry
    int pendingCount() {
        return staged.size();
    }

    // ---- Disk I/O helpers -----------------------------------------------------

    private Path blobPath(final long blockNumber) {
        return config.retryStagingDirectoryPath().resolve(blockNumber + BLOB_SUFFIX);
    }

    private Path metaPath(final long blockNumber) {
        return config.retryStagingDirectoryPath().resolve(blockNumber + META_SUFFIX);
    }

    private void deleteStagedFiles(final long blockNumber) {
        silentDelete(blobPath(blockNumber));
        silentDelete(metaPath(blockNumber));
    }

    private static void writeMetaFile(@NonNull final Path path, @NonNull final StagedEntry entry) throws IOException {
        try (final Writer writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            entry.toProperties().store(writer, null);
        }
    }

    private static StagedEntry readMetaFile(@NonNull final Path path, final long blockNumber) throws IOException {
        final Properties props = new Properties();
        try (final Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            props.load(reader);
        }
        return StagedEntry.fromProperties(props, blockNumber, path);
    }

    private static long blockNumberFromFileName(@NonNull final Path path, @NonNull final String suffix) {
        final String name = path.getFileName().toString();
        return Long.parseLong(name.substring(0, name.length() - suffix.length()));
    }

    private static void silentDelete(@NonNull final Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (final IOException ignored) {
            // best-effort cleanup
        }
    }
}
