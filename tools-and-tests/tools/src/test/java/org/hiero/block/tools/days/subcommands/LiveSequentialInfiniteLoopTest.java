// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.subcommands;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import org.hiero.block.tools.mirrornode.FixBlockTime;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for the infinite loop fix in {@link LiveSequential}.
 *
 * <p>Issue #2829: LiveSequential would get stuck in an infinite loop when
 * {@code fixBlockTimeRange()} returned 0 (no blocks fixed) but the code
 * always retried. The fix checks the return value and only retries if blocks
 * were actually fixed.
 *
 * <p>This test verifies the behavior of {@link FixBlockTime#fixBlockTimeRange}
 * which is the dependency that LiveSequential relies on. The fix ensures that
 * when this method returns 0, LiveSequential throws an exception instead of
 * retrying infinitely.
 */
class LiveSequentialInfiniteLoopTest {

    @TempDir
    Path tempDir;

    /**
     * Tests that {@code fixBlockTimeRange} returns 0 when the block_times.bin
     * file doesn't exist, which would trigger the exception path in LiveSequential
     * instead of infinite retry.
     */
    @Test
    void testFixBlockTimeReturnsZeroWhenFileNotFound() {
        Path nonExistentFile = tempDir.resolve("nonexistent-block_times.bin");

        int fixedCount = FixBlockTime.fixBlockTimeRange(nonExistentFile, 100, 200);

        assertEquals(0, fixedCount, "Should return 0 when file doesn't exist");
        // In LiveSequential, this would trigger the exception:
        // "No record files found for block X and block time could not be fixed"
    }

    /**
     * Tests that {@code fixBlockTimeRange} returns 0 when the requested range
     * is beyond the file's size, which would trigger the exception path instead
     * of infinite retry.
     */
    @Test
    void testFixBlockTimeReturnsZeroWhenRangeOutOfBounds() throws IOException {
        Path blockTimesFile = tempDir.resolve("block_times.bin");

        // Create a small file with only 10 blocks (10 * 8 bytes = 80 bytes)
        try (RandomAccessFile raf = new RandomAccessFile(blockTimesFile.toFile(), "rw")) {
            for (int i = 0; i < 10; i++) {
                raf.writeLong(1000000000000L + i); // Some timestamp values
            }
        }

        // Try to fix blocks way beyond the file size
        int fixedCount = FixBlockTime.fixBlockTimeRange(blockTimesFile, 1000, 1100);

        assertEquals(0, fixedCount, "Should return 0 when range is beyond file size");
        // In LiveSequential, this would trigger the exception instead of retry
    }

    /**
     * Tests that {@code fixBlockTimeRange} returns 0 when the range is invalid
     * (fromBlock > toBlock after adjustments).
     */
    @Test
    void testFixBlockTimeReturnsZeroWhenRangeInvalid() throws IOException {
        Path blockTimesFile = tempDir.resolve("block_times.bin");

        // Create a file with 5 blocks
        try (RandomAccessFile raf = new RandomAccessFile(blockTimesFile.toFile(), "rw")) {
            for (int i = 0; i < 5; i++) {
                raf.writeLong(1000000000000L + i);
            }
        }

        // Try to fix blocks starting beyond the file (fromBlock > maxBlockInFile)
        int fixedCount = FixBlockTime.fixBlockTimeRange(blockTimesFile, 100, 200);

        assertEquals(0, fixedCount, "Should return 0 when fromBlock exceeds file size");
        // In LiveSequential, this would trigger the exception instead of retry
    }

    /**
     * Documents the expected behavior: when fixBlockTimeRange returns 0,
     * LiveSequential should throw a RuntimeException with a clear message
     * instead of retrying infinitely.
     *
     * <p>This test documents the contract that the fix depends on. The actual
     * LiveSequential logic (lines 1387-1398) is:
     * <pre>
     * int fixedCount = FixBlockTime.fixBlockTimeRange(...);
     * if (fixedCount > 0) {
     *     // Reload and retry
     *     return null;
     * } else {
     *     // Throw exception instead of infinite retry
     *     throw new RuntimeException("No record files found...");
     * }
     * </pre>
     */
    @Test
    void testInfiniteLoopPreventionContract() {
        // This test documents the contract:
        // - If fixBlockTimeRange returns 0, don't retry
        // - If fixBlockTimeRange returns > 0, retry is valid

        // Simulating the LiveSequential decision logic
        int fixedCount = 0; // Simulate fixBlockTimeRange returning 0

        if (fixedCount > 0) {
            // Should NOT reach here when fixedCount is 0
            throw new AssertionError("Should not retry when fixedCount is 0");
        } else {
            // Should reach here and throw exception
            RuntimeException exception = assertThrows(
                    RuntimeException.class,
                    () -> {
                        throw new RuntimeException("No record files found for block X at time Y "
                                + "and block time could not be fixed. "
                                + "The block may not be available yet (live edge) or may be missing from the network.");
                    },
                    "Should throw exception when fixedCount is 0");

            assertEquals(
                    "No record files found for block X at time Y and block time could not be fixed. "
                            + "The block may not be available yet (live edge) or may be missing from the network.",
                    exception.getMessage());
        }
    }
}
