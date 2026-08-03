// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.expanded;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.hiero.block.node.cloud.storage.expanded.RetryBuffer.BufferedEntry;
import org.hiero.block.node.cloud.storage.expanded.RetryBuffer.RetryOutcome;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Unit tests for {@link RetryBuffer}. Purely in-memory — no threads or plugin wiring involved.
class RetryBufferTest {

    // ---- Helpers ------------------------------------------------------------

    private ExpandedCloudStorageConfig newConfig(
            final boolean retryEnabled,
            final int retryIntervalSeconds,
            final int retryMaxAgeSeconds,
            final int retryMaxPendingBlocks) {
        return new ExpandedCloudStorageConfig(
                "http://fake:9000",
                "bucket",
                "blocks",
                ExpandedCloudStorageConfig.StorageClass.STANDARD,
                "us-east-1",
                "",
                "",
                60,
                retryEnabled,
                retryIntervalSeconds,
                retryMaxAgeSeconds,
                retryMaxPendingBlocks);
    }

    private RetryBuffer newBuffer() {
        return new RetryBuffer(newConfig(true, 30, 3_600, 200));
    }

    // ---- Tests --------------------------------------------------------------

    @Test
    @DisplayName("stage() buffers the block in memory and updates the pending count")
    void stageBuffersBlockAndUpdatesPendingCount() {
        final RetryBuffer buffer = newBuffer();
        final byte[] bytes = {1, 2, 3, 4};

        final boolean staged = buffer.stage(42L, bytes, "blocks/key42", "STANDARD", BlockSource.PUBLISHER);

        assertTrue(staged);
        assertEquals(1, buffer.pendingCount());
        final BufferedEntry entry = buffer.dueForRetry(Instant.now()).getFirst();
        assertArrayEquals(bytes, entry.compressedBytes());
    }

    @Test
    @DisplayName("stage() is a no-op that returns false when retryEnabled is false")
    void stageIsNoOpWhenRetryDisabled() {
        final RetryBuffer buffer = new RetryBuffer(newConfig(false, 30, 3_600, 200));

        final boolean staged = buffer.stage(1L, new byte[] {1}, "blocks/key1", "STANDARD", BlockSource.PUBLISHER);

        assertFalse(staged, "stage() must return false when retryEnabled is false");
        assertEquals(0, buffer.pendingCount());
    }

    @Test
    @DisplayName("stage() rejects a new block once retryMaxPendingBlocks is reached")
    void stageRejectsOnceCapacityReached() {
        final RetryBuffer buffer = new RetryBuffer(newConfig(true, 30, 3_600, 1));
        assertTrue(buffer.stage(1L, new byte[] {1}, "blocks/key1", "STANDARD", BlockSource.PUBLISHER));

        final boolean secondStaged = buffer.stage(2L, new byte[] {2}, "blocks/key2", "STANDARD", BlockSource.PUBLISHER);

        assertFalse(secondStaged, "a new block must be rejected once the buffer is at capacity");
        assertEquals(1, buffer.pendingCount());
    }

    @Test
    @DisplayName("stage() capacity check does not count a duplicate call for an already-buffered block")
    void stageCapacityCheckIgnoresDuplicateForSameBlock() {
        final RetryBuffer buffer = new RetryBuffer(newConfig(true, 30, 3_600, 1));
        buffer.stage(1L, new byte[] {1}, "blocks/key1", "STANDARD", BlockSource.PUBLISHER);

        final boolean staged = buffer.stage(1L, new byte[] {9}, "blocks/key1-dup", "STANDARD", BlockSource.PUBLISHER);

        assertTrue(staged, "a duplicate stage() for an already-buffered block must not be rejected by the cap");
        assertEquals(1, buffer.pendingCount());
    }

    @Test
    @DisplayName("dueForRetry() only returns entries whose backoff has elapsed")
    void dueForRetryOnlyReturnsElapsedEntries() {
        final RetryBuffer buffer = new RetryBuffer(newConfig(true, 5, 3_600, 200));
        buffer.stage(1L, new byte[] {1}, "blocks/key1", "STANDARD", BlockSource.PUBLISHER);
        assertEquals(1, buffer.dueForRetry(Instant.now()).size(), "freshly buffered block must be immediately due");

        buffer.recordFailure(1L);
        assertTrue(
                buffer.dueForRetry(Instant.now()).isEmpty(),
                "block must not be due immediately after a failed attempt");
        assertEquals(
                1,
                buffer.dueForRetry(Instant.now().plusSeconds(6)).size(),
                "block must become due once retryIntervalSeconds has elapsed");
    }

    @Test
    @DisplayName("recordFailure() keeps retrying under retryMaxAgeSeconds")
    void recordFailureKeepsRetryingUnderMaxAge() {
        final RetryBuffer buffer = new RetryBuffer(newConfig(true, 30, 3_600, 200));
        buffer.stage(9L, new byte[] {1}, "blocks/key9", "STANDARD", BlockSource.PUBLISHER);

        assertEquals(RetryOutcome.RETRYING, buffer.recordFailure(9L));
        assertEquals(1, buffer.pendingCount(), "block must remain buffered while retrying");
    }

    @Test
    @DisplayName("recordFailure() exhausts a block once it has been buffered longer than retryMaxAgeSeconds")
    void recordFailureExhaustsPastMaxAge() throws InterruptedException {
        final RetryBuffer buffer = new RetryBuffer(newConfig(true, 30, 1, 200));
        buffer.stage(5L, new byte[] {1}, "blocks/key5", "STANDARD", BlockSource.PUBLISHER);
        Thread.sleep(1_100);

        assertEquals(
                RetryOutcome.EXHAUSTED, buffer.recordFailure(5L), "a block older than retryMaxAgeSeconds must exhaust");
        assertEquals(0, buffer.pendingCount());
    }

    @Test
    @DisplayName("recordFailure() returns NOT_STAGED, not EXHAUSTED, for a block that was already unstaged")
    void recordFailureReturnsNotStagedForAlreadyUnstagedBlock() {
        final RetryBuffer buffer = newBuffer();
        buffer.stage(20L, new byte[] {1}, "blocks/key20", "STANDARD", BlockSource.PUBLISHER);

        // Simulates a concurrent live-path success (a duplicate VerificationNotification) resolving
        // this block while a background retry attempt for it is still in flight.
        buffer.unstage(20L);

        assertEquals(
                RetryOutcome.NOT_STAGED,
                buffer.recordFailure(20L),
                "a block removed by a concurrent unstage() must not be reported as EXHAUSTED");
    }

    @Test
    @DisplayName("stage() is idempotent: a duplicate call for an already-buffered block does not reset its attempts")
    void stageIsIdempotentForAlreadyStagedBlock() {
        final RetryBuffer buffer = newBuffer();
        buffer.stage(15L, new byte[] {1, 2, 3}, "blocks/key15", "STANDARD", BlockSource.PUBLISHER);
        buffer.recordFailure(15L);
        final BufferedEntry entryAfterFirstFailure =
                buffer.dueForRetry(Instant.now().plusSeconds(120)).getFirst();

        final boolean staged =
                buffer.stage(15L, new byte[] {9, 9, 9}, "blocks/different-key", "STANDARD", BlockSource.BACKFILL);

        assertTrue(staged, "a duplicate stage() call for an already-buffered block must still report staged=true");
        assertEquals(1, buffer.pendingCount(), "duplicate stage() must not create a second entry");
        final BufferedEntry entry =
                buffer.dueForRetry(Instant.now().plusSeconds(120)).getFirst();
        assertEquals(entryAfterFirstFailure.attempts(), entry.attempts(), "duplicate stage() must not reset attempts");
        assertEquals("blocks/key15", entry.objectKey(), "duplicate stage() must not overwrite the original objectKey");
        assertArrayEquals(
                new byte[] {1, 2, 3},
                entry.compressedBytes(),
                "duplicate stage() must not overwrite the original buffered bytes");
    }

    @Test
    @DisplayName("concurrent stage() calls for the same block number produce exactly one buffered entry")
    void concurrentStageCallsForSameBlockProduceOneEntry() throws InterruptedException {
        final RetryBuffer buffer = newBuffer();
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
                    buffer.stage(50L, bytes, "blocks/key50-" + bytes[0], "STANDARD", BlockSource.PUBLISHER);
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
                1, buffer.pendingCount(), "concurrent stage() calls for the same block must produce exactly one entry");
    }

    @Test
    @DisplayName("unstage() removes the buffered entry and reports whether it found one")
    void unstageRemovesEntry() {
        final RetryBuffer buffer = newBuffer();
        buffer.stage(3L, new byte[] {1, 2}, "blocks/key3", "STANDARD", BlockSource.PUBLISHER);
        assertEquals(1, buffer.pendingCount());

        assertTrue(buffer.unstage(3L), "unstage() must report true when it actually removed an entry");
        assertEquals(0, buffer.pendingCount());
        assertFalse(buffer.unstage(3L), "a second unstage() for the same block must report false");
    }

    @Test
    @DisplayName("close() makes stage() a permanent no-op")
    void closeMakesStagePermanentNoOp() {
        final RetryBuffer buffer = newBuffer();
        buffer.stage(1L, new byte[] {1}, "blocks/key1", "STANDARD", BlockSource.PUBLISHER);

        buffer.close();
        final boolean stagedAfterClose =
                buffer.stage(2L, new byte[] {2}, "blocks/key2", "STANDARD", BlockSource.PUBLISHER);

        assertFalse(stagedAfterClose, "stage() must return false for any new block once the buffer is closed");
        assertEquals(1, buffer.pendingCount(), "close() must not remove entries already buffered before it was called");
    }

    @Test
    @DisplayName("drainAll() removes and returns every buffered entry")
    void drainAllReturnsAndClearsEntries() {
        final RetryBuffer buffer = newBuffer();
        buffer.stage(1L, new byte[] {1}, "blocks/key1", "STANDARD", BlockSource.PUBLISHER);
        buffer.stage(2L, new byte[] {2}, "blocks/key2", "STANDARD", BlockSource.BACKFILL);

        final List<BufferedEntry> drained = buffer.drainAll();

        assertEquals(2, drained.size());
        assertEquals(0, buffer.pendingCount(), "drainAll() must clear the buffer");
        assertTrue(buffer.dueForRetry(Instant.now()).isEmpty());
    }
}
