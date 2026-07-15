// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.expanded;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/// Unit tests for {@link RetryStagingManager}. All tests use a real filesystem directory via
/// {@link TempDir} — no threads or plugin wiring is involved.
class RetryStagingManagerTest {

    @TempDir
    private Path tempDir;

    // ---- Helpers ------------------------------------------------------------

    private ExpandedCloudStorageConfig newConfig(
            final int retryMaxAttempts,
            final int retryMaxAgeHours,
            final int retryBaseBackoffSeconds,
            final int retryMaxBackoffSeconds) {
        return new ExpandedCloudStorageConfig(
                "http://fake:9000",
                "bucket",
                "blocks",
                ExpandedCloudStorageConfig.StorageClass.STANDARD,
                "us-east-1",
                "",
                "",
                60,
                true,
                tempDir,
                30,
                retryBaseBackoffSeconds,
                retryMaxBackoffSeconds,
                retryMaxAttempts,
                retryMaxAgeHours);
    }

    private RetryStagingManager newManager() {
        return new RetryStagingManager(newConfig(20, 24, 30, 900));
    }

    // ---- Tests --------------------------------------------------------------

    @Test
    @DisplayName("stage() writes both files to disk and adds the block to the pending count")
    void stageWritesBothFilesAndUpdatesPendingCount() {
        final RetryStagingManager manager = newManager();
        final byte[] bytes = {1, 2, 3, 4};

        final boolean staged = manager.stage(42L, bytes, "blocks/key42", "STANDARD", BlockSource.PUBLISHER);

        assertTrue(staged, "stage() must succeed when the staging directory is writable");
        assertEquals(1, manager.pendingCount());
        assertTrue(Files.exists(tempDir.resolve("42.blk.zstd")), "blob file must exist");
        assertTrue(Files.exists(tempDir.resolve("42.meta.properties")), "sidecar file must exist");
    }

    @Test
    @DisplayName("stage() returns false and stores no in-memory entry when the staging directory does not exist")
    void stageReturnsFalseWhenStagingDirectoryMissing() {
        // stage() itself never creates the staging directory (that is ExpandedCloudStoragePlugin.start()'s
        // job); pointing at a directory that doesn't exist reliably triggers the IOException catch branch
        // without relying on platform-specific file permission behavior (which root/CI often bypasses).
        final Path missingDir = tempDir.resolve("does-not-exist");
        final RetryStagingManager manager = new RetryStagingManager(new ExpandedCloudStorageConfig(
                "http://fake:9000",
                "bucket",
                "blocks",
                ExpandedCloudStorageConfig.StorageClass.STANDARD,
                "us-east-1",
                "",
                "",
                60,
                true,
                missingDir,
                30,
                30,
                900,
                20,
                6));

        final boolean staged = manager.stage(1L, new byte[] {1, 2}, "blocks/key1", "STANDARD", BlockSource.PUBLISHER);

        assertFalse(staged, "stage() must return false when the staging directory does not exist");
        assertEquals(0, manager.pendingCount(), "a failed stage() must not add an in-memory entry");
    }

    @Test
    @DisplayName(
            "stage()/loadExisting() round-trips an objectKey containing characters that require Properties escaping")
    void stageRoundTripsSpecialCharactersInObjectKey() {
        final RetryStagingManager manager = newManager();
        final String trickyObjectKey = "blocks/weird=key:with#special!chars\\and\"quotes and spaces.blk.zstd";

        final boolean staged = manager.stage(8L, new byte[] {9, 9}, trickyObjectKey, "STANDARD", BlockSource.PUBLISHER);
        assertTrue(staged);

        // Force a real disk round-trip (not just the in-memory entry) via a fresh instance, the same way
        // a process restart would recover it.
        final RetryStagingManager restarted = newManager();
        restarted.loadExisting();

        final RetryStagingManager.StagedEntry entry =
                restarted.dueForRetry(Instant.now()).getFirst();
        assertEquals(trickyObjectKey, entry.objectKey(), "objectKey with special characters must round-trip exactly");
    }

    @Test
    @DisplayName("stage() is a no-op that returns false when retryEnabled is false")
    void stageIsNoOpWhenRetryDisabled() {
        final RetryStagingManager manager = new RetryStagingManager(new ExpandedCloudStorageConfig(
                "http://fake:9000",
                "bucket",
                "blocks",
                ExpandedCloudStorageConfig.StorageClass.STANDARD,
                "us-east-1",
                "",
                "",
                60,
                false,
                tempDir,
                30,
                30,
                900,
                20,
                6));

        final boolean staged = manager.stage(1L, new byte[] {1}, "blocks/key1", "STANDARD", BlockSource.PUBLISHER);

        assertFalse(staged, "stage() must return false when retryEnabled is false");
        assertEquals(0, manager.pendingCount());
        assertFalse(Files.exists(tempDir.resolve("1.blk.zstd")), "no blob file must be written when retry is disabled");
        assertFalse(
                Files.exists(tempDir.resolve("1.meta.properties")),
                "no sidecar file must be written when retry is disabled");
    }

    @Test
    @DisplayName("readBytes() returns exactly the bytes passed to stage()")
    void readBytesReturnsStagedBytes() throws IOException {
        final RetryStagingManager manager = newManager();
        final byte[] bytes = {5, 6, 7, 8, 9};
        manager.stage(7L, bytes, "blocks/key7", "STANDARD", BlockSource.PUBLISHER);

        final RetryStagingManager.StagedEntry entry =
                manager.dueForRetry(Instant.now()).getFirst();
        assertArrayEquals(bytes, manager.readBytes(entry));
    }

    @Test
    @DisplayName("dueForRetry() only returns entries whose backoff has elapsed")
    void dueForRetryOnlyReturnsElapsedEntries() {
        // base backoff = 1s, so after one recordFailure() (attempts 1 -> 2, shift=1) the delay is
        // min(1s << 1, maxBackoff) = 2s — small enough to assert against without flaking on timing.
        final RetryStagingManager manager = new RetryStagingManager(newConfig(20, 24, 1, 100));
        manager.stage(1L, new byte[] {1}, "blocks/key1", "STANDARD", BlockSource.PUBLISHER);
        // Freshly staged entries are immediately due.
        assertEquals(1, manager.dueForRetry(Instant.now()).size(), "freshly staged block must be immediately due");

        manager.recordFailure(1L);
        assertTrue(
                manager.dueForRetry(Instant.now()).isEmpty(),
                "block must not be due immediately after a failed attempt extends its backoff");
        assertEquals(
                1,
                manager.dueForRetry(Instant.now().plusSeconds(3)).size(),
                "block must become due once its backoff delay has elapsed");
    }

    @Test
    @DisplayName("recordFailure() grows backoff exponentially and returns EXHAUSTED past retryMaxAttempts")
    void recordFailureGrowsBackoffAndExhaustsPastMaxAttempts() {
        final RetryStagingManager manager = new RetryStagingManager(newConfig(2, 24, 1, 100));
        manager.stage(9L, new byte[] {1}, "blocks/key9", "STANDARD", BlockSource.PUBLISHER);

        assertEquals(
                RetryStagingManager.RetryOutcome.RETRYING,
                manager.recordFailure(9L),
                "first recorded failure (attempt 2 of 2) must still be retrying");
        assertEquals(1, manager.pendingCount(), "block must remain staged while retrying");

        assertEquals(
                RetryStagingManager.RetryOutcome.EXHAUSTED,
                manager.recordFailure(9L),
                "second recorded failure exceeds retryMaxAttempts=2 and must exhaust");
        assertEquals(0, manager.pendingCount(), "exhausted block must be removed from staging");
        assertFalse(Files.exists(tempDir.resolve("9.blk.zstd")), "blob file must be deleted on exhaustion");
        assertFalse(Files.exists(tempDir.resolve("9.meta.properties")), "sidecar file must be deleted on exhaustion");
    }

    @Test
    @DisplayName("recordFailure() exhausts a block whose age exceeds retryMaxAgeHours even under retryMaxAttempts")
    void recordFailureExhaustsPastMaxAgeHours() throws IOException {
        // retryMaxAttempts is generous (20) so only the age bound can trigger exhaustion here.
        final RetryStagingManager manager = new RetryStagingManager(newConfig(20, 1, 30, 900));
        // Hand-write a sidecar whose firstStagedEpochMs is far enough in the past to already exceed
        // the 1-hour retryMaxAgeHours bound, then load it the same way a restart would.
        final long ancientEpochMs =
                System.currentTimeMillis() - java.time.Duration.ofHours(2).toMillis();
        Files.write(tempDir.resolve("5.blk.zstd"), new byte[] {1});
        Files.writeString(
                tempDir.resolve("5.meta.properties"),
                """
                objectKey=blocks/key5
                storageClass=STANDARD
                blockSource=PUBLISHER
                attempts=1
                firstStagedEpochMs=%d
                nextEligibleEpochMs=%d
                """.formatted(ancientEpochMs, ancientEpochMs),
                StandardCharsets.UTF_8);
        manager.loadExisting();
        assertEquals(1, manager.pendingCount(), "hand-written entry must be picked up by loadExisting()");

        assertEquals(
                RetryStagingManager.RetryOutcome.EXHAUSTED,
                manager.recordFailure(5L),
                "a block older than retryMaxAgeHours must exhaust regardless of attempt count");
        assertEquals(0, manager.pendingCount());
    }

    @Test
    @DisplayName("stage() is idempotent: a duplicate call for an already-staged block does not reset its attempts")
    void stageIsIdempotentForAlreadyStagedBlock() throws IOException {
        final RetryStagingManager manager = newManager();
        manager.stage(15L, new byte[] {1, 2, 3}, "blocks/key15", "STANDARD", BlockSource.PUBLISHER);
        manager.recordFailure(15L);
        final RetryStagingManager.StagedEntry entryAfterFirstFailure =
                manager.dueForRetry(Instant.now().plusSeconds(120)).getFirst();

        final boolean staged =
                manager.stage(15L, new byte[] {9, 9, 9}, "blocks/different-key", "STANDARD", BlockSource.BACKFILL);

        assertTrue(staged, "a duplicate stage() call for an already-staged block must still report staged=true");
        assertEquals(1, manager.pendingCount(), "duplicate stage() must not create a second entry");
        final RetryStagingManager.StagedEntry entry =
                manager.dueForRetry(Instant.now().plusSeconds(120)).getFirst();
        assertEquals(entryAfterFirstFailure.attempts(), entry.attempts(), "duplicate stage() must not reset attempts");
        assertEquals("blocks/key15", entry.objectKey(), "duplicate stage() must not overwrite the original objectKey");
        assertArrayEquals(
                new byte[] {1, 2, 3},
                manager.readBytes(entry),
                "duplicate stage() must not overwrite the original blob bytes");
    }

    @Test
    @DisplayName("concurrent stage() calls for the same block number produce exactly one staged entry")
    void concurrentStageCallsForSameBlockProduceOneEntry() throws InterruptedException {
        final RetryStagingManager manager = newManager();
        final int threadCount = 8;
        final ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        final CountDownLatch ready = new CountDownLatch(threadCount);
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(threadCount);
        for (int i = 0; i < threadCount; i++) {
            final byte[] bytes = {(byte) i};
            executor.submit(() -> {
                ready.countDown();
                try {
                    start.await();
                    manager.stage(50L, bytes, "blocks/key50-" + bytes[0], "STANDARD", BlockSource.PUBLISHER);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        ready.await();
        start.countDown();
        assertTrue(done.await(10, TimeUnit.SECONDS), "all concurrent stage() calls must complete");
        executor.shutdown();

        assertEquals(
                1,
                manager.pendingCount(),
                "concurrent stage() calls for the same block must produce exactly one entry");
        assertTrue(Files.exists(tempDir.resolve("50.blk.zstd")));
        assertTrue(Files.exists(tempDir.resolve("50.meta.properties")));
    }

    @Test
    @DisplayName("unstage() removes both files and the in-memory entry")
    void unstageRemovesFiles() {
        final RetryStagingManager manager = newManager();
        manager.stage(3L, new byte[] {1, 2}, "blocks/key3", "STANDARD", BlockSource.PUBLISHER);
        assertEquals(1, manager.pendingCount());

        manager.unstage(3L);

        assertEquals(0, manager.pendingCount());
        assertFalse(Files.exists(tempDir.resolve("3.blk.zstd")));
        assertFalse(Files.exists(tempDir.resolve("3.meta.properties")));
    }

    @Test
    @DisplayName("loadExisting() rebuilds the in-memory index from sidecar/blob pairs written by a prior instance")
    void loadExistingRebuildsIndexFromPreExistingFiles() throws IOException {
        final RetryStagingManager first = newManager();
        first.stage(10L, new byte[] {1, 1}, "blocks/key10", "STANDARD", BlockSource.PUBLISHER);
        first.stage(11L, new byte[] {2, 2, 2}, "blocks/key11", "STANDARD", BlockSource.BACKFILL);

        // Simulate a process restart: a brand-new manager instance pointed at the same directory.
        final RetryStagingManager restarted = newManager();
        assertEquals(0, restarted.pendingCount(), "a fresh instance must not see staged blocks before loadExisting()");

        restarted.loadExisting();

        assertEquals(2, restarted.pendingCount(), "both previously staged blocks must be recovered");
        final List<RetryStagingManager.StagedEntry> due = restarted.dueForRetry(Instant.now());
        assertEquals(2, due.size());
        final RetryStagingManager.StagedEntry entry10 =
                due.stream().filter(e -> e.blockNumber() == 10L).findFirst().orElseThrow();
        assertEquals("blocks/key10", entry10.objectKey());
        assertArrayEquals(new byte[] {1, 1}, restarted.readBytes(entry10));
    }

    @Test
    @DisplayName("loadExisting() deletes an orphaned sidecar whose blob file is missing")
    void loadExistingDeletesOrphanedSidecar() throws IOException {
        Files.writeString(tempDir.resolve("20.meta.properties"), """
                objectKey=blocks/key20
                storageClass=STANDARD
                blockSource=PUBLISHER
                attempts=1
                firstStagedEpochMs=0
                nextEligibleEpochMs=0
                """, StandardCharsets.UTF_8);

        final RetryStagingManager manager = newManager();
        manager.loadExisting();

        assertEquals(0, manager.pendingCount(), "an orphaned sidecar must not be recovered");
        assertFalse(Files.exists(tempDir.resolve("20.meta.properties")), "orphaned sidecar must be deleted");
    }

    @Test
    @DisplayName("loadExisting() deletes an orphaned blob whose sidecar is missing")
    void loadExistingDeletesOrphanedBlob() throws IOException {
        Files.write(tempDir.resolve("21.blk.zstd"), new byte[] {1});

        final RetryStagingManager manager = newManager();
        manager.loadExisting();

        assertEquals(0, manager.pendingCount());
        assertFalse(Files.exists(tempDir.resolve("21.blk.zstd")), "orphaned blob must be deleted");
    }

    @Test
    @DisplayName("loadExisting() tolerates a non-numeric sidecar filename instead of throwing")
    void loadExistingTreatsNonNumericSidecarFilenameAsMalformed() throws IOException {
        Files.writeString(tempDir.resolve("not-a-number.meta.properties"), "objectKey=x", StandardCharsets.UTF_8);

        final RetryStagingManager manager = newManager();

        assertDoesNotThrow(manager::loadExisting, "a malformed sidecar filename must not abort loadExisting()");
        assertEquals(0, manager.pendingCount());
        assertFalse(
                Files.exists(tempDir.resolve("not-a-number.meta.properties")),
                "malformed sidecar file must be deleted");
    }

    @Test
    @DisplayName("loadExisting() tolerates a non-numeric blob filename instead of throwing")
    void loadExistingTreatsNonNumericBlobFilenameAsMalformed() throws IOException {
        Files.write(tempDir.resolve("not-a-number.blk.zstd"), new byte[] {1});

        final RetryStagingManager manager = newManager();

        assertDoesNotThrow(manager::loadExisting, "a malformed blob filename must not abort loadExisting()");
        assertEquals(0, manager.pendingCount());
        assertFalse(Files.exists(tempDir.resolve("not-a-number.blk.zstd")), "malformed blob file must be deleted");
    }

    @Test
    @DisplayName("loadExisting() still recovers a legitimately staged block alongside a malformed stray file")
    void loadExistingRecoversValidEntryDespiteMalformedStrayFile() throws IOException {
        Files.writeString(tempDir.resolve("garbage.meta.properties"), "garbage content", StandardCharsets.UTF_8);
        final RetryStagingManager priorRunStaging = newManager();
        priorRunStaging.stage(30L, new byte[] {1, 2}, "blocks/key30", "STANDARD", BlockSource.PUBLISHER);

        final RetryStagingManager manager = newManager();
        assertDoesNotThrow(manager::loadExisting, "a malformed stray file must not abort loadExisting()");

        assertEquals(1, manager.pendingCount(), "the legitimately staged block must still be recovered");
    }
}
