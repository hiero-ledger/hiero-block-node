// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.mirrornode;

import static org.hiero.block.tools.records.RecordFileDates.FIRST_BLOCK_TIME_INSTANT;
import static org.hiero.block.tools.records.RecordFileDates.instantToBlockTimeLong;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Tests for {@link BlockTimeReader}. */
class BlockTimeReaderTest {

    @TempDir
    Path tempDir;

    /**
     * Create a block_times.bin file with the given number of entries.
     * Each entry is a long representing nanoseconds since the first block time.
     */
    private Path createBlockTimesFile(int entryCount) throws IOException {
        final Path file = tempDir.resolve("block_times.bin");
        final ByteBuffer buf = ByteBuffer.allocate(entryCount * Long.BYTES);
        for (int i = 0; i < entryCount; i++) {
            // Each block is 1 second apart from FIRST_BLOCK_TIME_INSTANT
            buf.putLong(instantToBlockTimeLong(FIRST_BLOCK_TIME_INSTANT.plusSeconds(i)));
        }
        Files.write(file, buf.array());
        return file;
    }

    @Test
    @DisplayName("getBlockInstant returns correct Instant for valid index")
    void getBlockInstant_validIndex_returnsCorrectInstant() throws IOException {
        final Path file = createBlockTimesFile(5);
        try (BlockTimeReader reader = new BlockTimeReader(file)) {
            final Instant result = reader.getBlockInstant(0);
            assertEquals(FIRST_BLOCK_TIME_INSTANT, result);

            final Instant result3 = reader.getBlockInstant(3);
            assertEquals(FIRST_BLOCK_TIME_INSTANT.plusSeconds(3), result3);
        }
    }

    @Test
    @DisplayName("getBlockInstant throws IndexOutOfBoundsException with helpful message for out-of-range block")
    void getBlockInstant_outOfBounds_throwsWithMessage() throws IOException {
        final Path file = createBlockTimesFile(5);
        try (BlockTimeReader reader = new BlockTimeReader(file)) {
            final IndexOutOfBoundsException ex =
                    assertThrows(IndexOutOfBoundsException.class, () -> reader.getBlockInstant(10));
            assertTrue(ex.getMessage().contains("Block 10 is out of bounds"), "Message should contain block number");
            assertTrue(ex.getMessage().contains("blocks 0-4"), "Message should contain valid range");
        }
    }

    @Test
    @DisplayName("getBlockInstant throws for negative block number")
    void getBlockInstant_negativeBlock_throws() throws IOException {
        final Path file = createBlockTimesFile(5);
        try (BlockTimeReader reader = new BlockTimeReader(file)) {
            assertThrows(IndexOutOfBoundsException.class, () -> reader.getBlockInstant(-1));
        }
    }

    @Test
    @DisplayName("getMaxBlockNumber returns correct value")
    void getMaxBlockNumber_returnsCorrectValue() throws IOException {
        final Path file = createBlockTimesFile(5);
        try (BlockTimeReader reader = new BlockTimeReader(file)) {
            assertEquals(4, reader.getMaxBlockNumber());
        }
    }
}
