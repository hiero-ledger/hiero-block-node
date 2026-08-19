// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.mirrornode;

import static org.hiero.block.tools.records.RecordFileDates.FIRST_BLOCK_TIME_INSTANT;
import static org.hiero.block.tools.records.RecordFileDates.blockTimeLongToInstant;
import static org.hiero.block.tools.records.RecordFileDates.instantToBlockTimeLong;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.hiero.block.tools.config.NetworkConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Tests for {@link BlockTimeReader}. */
class BlockTimeReaderTest {

    @TempDir
    Path tempDir;

    @AfterEach
    void resetNetworkConfig() {
        // Reset to mainnet after each test to avoid test interference
        NetworkConfig.setCurrent(NetworkConfig.mainnet());
    }

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

    @Test
    @DisplayName("Mainnet genesis uses RecordFileDates helper methods")
    void mainnetGenesis_usesHelperMethods() throws IOException {
        final Path file = createBlockTimesFile(3);
        try (BlockTimeReader reader = new BlockTimeReader(file, FIRST_BLOCK_TIME_INSTANT)) {
            // Block 0 should be at genesis
            final Instant block0 = reader.getBlockInstant(0);
            assertEquals(FIRST_BLOCK_TIME_INSTANT, block0);

            // Block 1 should be 1 second after genesis
            final Instant block1 = reader.getBlockInstant(1);
            final Instant expected1 =
                    blockTimeLongToInstant(instantToBlockTimeLong(FIRST_BLOCK_TIME_INSTANT.plusSeconds(1)));
            assertEquals(expected1, block1);

            // Verify it matches what RecordFileDates helper would produce
            assertEquals(FIRST_BLOCK_TIME_INSTANT.plusSeconds(1), block1);
        }
    }

    @Test
    @DisplayName("getBlockLocalDateTime works with mainnet genesis")
    void getBlockLocalDateTime_mainnetGenesis() throws IOException {
        final Path file = createBlockTimesFile(2);
        try (BlockTimeReader reader = new BlockTimeReader(file, FIRST_BLOCK_TIME_INSTANT)) {
            final LocalDateTime result = reader.getBlockLocalDateTime(0);
            assertEquals(FIRST_BLOCK_TIME_INSTANT.atZone(ZoneOffset.UTC).toLocalDateTime(), result);
        }
    }

    @Test
    @DisplayName("getNearestBlockAfterTime with mainnet genesis uses helper methods")
    void getNearestBlockAfterTime_mainnetGenesis() throws IOException {
        final Path file = createBlockTimesFile(5);
        try (BlockTimeReader reader = new BlockTimeReader(file, FIRST_BLOCK_TIME_INSTANT)) {
            // Search for time 2.5 seconds after genesis - should return block 3
            final LocalDateTime targetTime = FIRST_BLOCK_TIME_INSTANT
                    .plusMillis(2500)
                    .atZone(ZoneOffset.UTC)
                    .toLocalDateTime();
            final long blockIndex = reader.getNearestBlockAfterTime(targetTime);
            assertEquals(3, blockIndex);
        }
    }

    @Test
    @DisplayName("Default constructor uses mainnet genesis")
    void defaultConstructor_usesMainnetGenesis() throws IOException {
        final Path file = createBlockTimesFile(2);
        // Copy to default location for this test
        try (BlockTimeReader reader = new BlockTimeReader(file)) {
            final Instant block0 = reader.getBlockInstant(0);
            // Should use mainnet genesis
            assertEquals(FIRST_BLOCK_TIME_INSTANT, block0);
        }
    }

    @Test
    @DisplayName("Two-arg constructor defaults to mainnet genesis")
    void twoArgConstructor_defaultsToMainnet() throws IOException {
        final Path file = createBlockTimesFile(2);
        try (BlockTimeReader reader = new BlockTimeReader(file)) {
            final Instant block0 = reader.getBlockInstant(0);
            assertEquals(FIRST_BLOCK_TIME_INSTANT, block0);
        }
    }

    @Test
    @DisplayName("Constructor with mainnet NetworkConfig uses mainnet helpers")
    void constructorWithMainnetConfig_usesHelpers() throws IOException {
        final Path file = createBlockTimesFile(2);

        // Pass mainnet config explicitly
        try (BlockTimeReader reader = new BlockTimeReader(file, NetworkConfig.mainnet())) {
            final Instant block0 = reader.getBlockInstant(0);
            // Should use mainnet genesis and RecordFileDates helpers
            assertEquals(FIRST_BLOCK_TIME_INSTANT, block0);

            final Instant block1 = reader.getBlockInstant(1);
            assertEquals(FIRST_BLOCK_TIME_INSTANT.plusSeconds(1), block1);
        }
    }
}
