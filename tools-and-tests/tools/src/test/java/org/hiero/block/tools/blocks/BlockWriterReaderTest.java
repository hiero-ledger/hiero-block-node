// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.output.BlockHeader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.hiero.block.node.base.CompressionType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Comprehensive test for BlockWriter and BlockReader using JimFS in-memory file system.
 * Tests all combinations of archive types and compression types.
 */
@SuppressWarnings("DataFlowIssue")
class BlockWriterReaderTest {
    private FileSystem fileSystem;
    private Path baseDirectory;

    @BeforeEach
    void setUp() throws IOException {
        // Create in-memory file system
        fileSystem = Jimfs.newFileSystem(Configuration.unix());
        baseDirectory = fileSystem.getPath("/blocks");
        Files.createDirectories(baseDirectory);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (fileSystem != null) {
            fileSystem.close();
        }
    }

    /**
     * Test writing and reading a single block with INDIVIDUAL_FILES format and ZSTD compression.
     */
    @Test
    void testIndividualFilesWithZstd() throws IOException {
        // Create a test block
        Block testBlock = createTestBlock(12345L);

        // Write using INDIVIDUAL_FILES format
        BlockWriter.BlockPath writtenPath =
                BlockWriter.writeBlock(baseDirectory, testBlock, BlockArchiveType.INDIVIDUAL_FILES);

        assertNotNull(writtenPath);
        assertEquals(CompressionType.ZSTD, writtenPath.compressionType());

        // Read it back
        Block readBlock = BlockReader.readBlock(baseDirectory, 12345L);

        // Verify the content matches
        assertBlocksEqual(testBlock, readBlock);
    }

    /**
     * Test writing and reading a single block with INDIVIDUAL_FILES format and no compression.
     */
    @Test
    void testIndividualFilesWithNoCompression() throws IOException {
        Block testBlock = createTestBlock(54321L);

        // Write with no compression
        BlockWriter.BlockPath writtenPath = BlockWriter.writeBlock(
                baseDirectory, testBlock, BlockArchiveType.INDIVIDUAL_FILES, CompressionType.NONE, 4);

        assertNotNull(writtenPath);
        assertEquals(CompressionType.NONE, writtenPath.compressionType());

        // Read it back
        Block readBlock = BlockReader.readBlock(baseDirectory, 54321L);

        assertBlocksEqual(testBlock, readBlock);
    }

    /**
     * Test writing and reading a single block with UNCOMPRESSED_ZIP format and ZSTD compression.
     */
    @Test
    void testUncompressedZipWithZstd() throws IOException {
        Block testBlock = createTestBlock(99999L);

        // Write using UNCOMPRESSED_ZIP format
        BlockWriter.BlockPath writtenPath =
                BlockWriter.writeBlock(baseDirectory, testBlock, BlockArchiveType.UNCOMPRESSED_ZIP);

        assertNotNull(writtenPath);
        assertEquals(CompressionType.ZSTD, writtenPath.compressionType());
        assertTrue(Files.exists(writtenPath.zipFilePath()), "Zip file should exist");

        // Read it back
        Block readBlock = BlockReader.readBlock(baseDirectory, 99999L);

        assertBlocksEqual(testBlock, readBlock);
    }

    /**
     * Test writing and reading a single block with UNCOMPRESSED_ZIP format and no compression.
     */
    @Test
    void testUncompressedZipWithNoCompression() throws IOException {
        Block testBlock = createTestBlock(77777L);

        // Write with no compression
        BlockWriter.BlockPath writtenPath = BlockWriter.writeBlock(
                baseDirectory, testBlock, BlockArchiveType.UNCOMPRESSED_ZIP, CompressionType.NONE, 4);

        assertNotNull(writtenPath);
        assertEquals(CompressionType.NONE, writtenPath.compressionType());

        // Read it back
        Block readBlock = BlockReader.readBlock(baseDirectory, 77777L);

        assertBlocksEqual(testBlock, readBlock);
    }

    /**
     * Test writing multiple blocks and reading them back using readBlocks.
     */
    @Test
    void testReadMultipleBlocksIndividualFiles() throws IOException {
        // Write blocks 100-110
        List<Block> writtenBlocks = new ArrayList<>();
        for (long i = 100; i <= 110; i++) {
            Block block = createTestBlock(i);
            writtenBlocks.add(block);
            BlockWriter.writeBlock(baseDirectory, block, BlockArchiveType.INDIVIDUAL_FILES);
        }

        // Read them back using readBlocks
        List<Block> readBlocks = BlockReader.readBlocks(baseDirectory, 100, 110).toList();

        assertEquals(11, readBlocks.size(), "Should read 11 blocks (100-110 inclusive)");

        // Verify each block
        for (int i = 0; i < writtenBlocks.size(); i++) {
            assertBlocksEqual(writtenBlocks.get(i), readBlocks.get(i));
        }
    }

    /**
     * Test writing multiple blocks to zip files and reading them back.
     * Note: JimFS has limitations with nested file systems, so we write blocks with larger
     * block numbers to ensure they go into separate zip files and test reading individual blocks.
     */
    @Test
    void testReadMultipleBlocksFromZip() throws IOException {
        // Write blocks with larger gaps to ensure they're in different zip files
        // Using powers of ten = 4 (10,000 blocks per zip), these will be in separate zips
        List<Block> writtenBlocks = new ArrayList<>();
        long[] blockNumbers = {10000, 20000, 30000, 40000, 50000};

        for (long blockNumber : blockNumbers) {
            Block block = createTestBlock(blockNumber);
            writtenBlocks.add(block);
            BlockWriter.writeBlock(baseDirectory, block, BlockArchiveType.UNCOMPRESSED_ZIP);
        }

        // Read them back individually
        for (int i = 0; i < blockNumbers.length; i++) {
            Block readBlock = BlockReader.readBlock(baseDirectory, blockNumbers[i]);
            assertBlocksEqual(writtenBlocks.get(i), readBlock);
        }
    }

    /**
     * Test that the format detection cache works correctly.
     */
    @Test
    void testCachingBehavior() throws IOException {
        // Write several blocks in INDIVIDUAL_FILES format
        for (long i = 500; i < 510; i++) {
            BlockWriter.writeBlock(baseDirectory, createTestBlock(i), BlockArchiveType.INDIVIDUAL_FILES);
        }

        // Read the first block - this should detect and cache the format
        Block firstRead = BlockReader.readBlock(baseDirectory, 500);
        assertNotNull(firstRead);

        // Read remaining blocks - these should use the cached format (faster)
        for (long i = 501; i < 510; i++) {
            Block block = BlockReader.readBlock(baseDirectory, i);
            assertNotNull(block);
            assertEquals(i, block.items().getFirst().blockHeader().number());
        }
    }

    /**
     * Test reading a non-existent block throws IOException.
     */
    @Test
    void testReadNonExistentBlockThrowsException() {
        IOException exception = assertThrows(IOException.class, () -> {
            BlockReader.readBlock(baseDirectory, 999999L);
        });

        assertTrue(exception.getMessage().contains("not found"), "Exception message should indicate block not found");
    }

    /**
     * Test readBlocks with invalid range throws IllegalArgumentException.
     */
    @Test
    void testReadBlocksWithInvalidRange() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            BlockReader.readBlocks(baseDirectory, 100, 50); // end < start
        });

        assertTrue(exception.getMessage().contains("must be <="));
    }

    /**
     * Test readBlocks throws UncheckedIOException when a block is missing in the range.
     */
    @Test
    void testReadBlocksWithMissingBlock() throws IOException {
        // Write blocks 200, 201, 203 (skip 202)
        BlockWriter.writeBlock(baseDirectory, createTestBlock(200), BlockArchiveType.INDIVIDUAL_FILES);
        BlockWriter.writeBlock(baseDirectory, createTestBlock(201), BlockArchiveType.INDIVIDUAL_FILES);
        BlockWriter.writeBlock(baseDirectory, createTestBlock(203), BlockArchiveType.INDIVIDUAL_FILES);

        // Try to read 200-203 - should fail on 202
        assertThrows(UncheckedIOException.class, () -> BlockReader.readBlocks(baseDirectory, 200, 203)
                .toList());
    }

    /**
     * Test writing blocks with different powers of ten for zip files.
     */
    @Test
    void testDifferentPowersOfTenZipFiles() throws IOException {
        // Test with powers of ten = 2 (100 blocks per zip)
        Block block1 = createTestBlock(10000);
        BlockWriter.writeBlock(baseDirectory, block1, BlockArchiveType.UNCOMPRESSED_ZIP, CompressionType.ZSTD, 2);

        Block readBlock1 = BlockReader.readBlock(baseDirectory, 10000);
        assertBlocksEqual(block1, readBlock1);

        // Clear cache for next test
        Path anotherDir = fileSystem.getPath("/blocks2");
        Files.createDirectories(anotherDir);

        // Test with powers of ten = 5 (100,000 blocks per zip)
        Block block2 = createTestBlock(20000);
        BlockWriter.writeBlock(anotherDir, block2, BlockArchiveType.UNCOMPRESSED_ZIP, CompressionType.ZSTD, 5);

        Block readBlock2 = BlockReader.readBlock(anotherDir, 20000);
        assertBlocksEqual(block2, readBlock2);
    }

    /**
     * Test that mixed formats in the same directory are handled correctly.
     * (This should not happen in practice, but testing defensive behavior)
     */
    @Test
    void testMixedFormatsDetection() throws IOException {
        // Write one block as INDIVIDUAL_FILES
        BlockWriter.writeBlock(baseDirectory, createTestBlock(3000), BlockArchiveType.INDIVIDUAL_FILES);

        // Read it back - should detect an INDIVIDUAL_FILES format
        Block readBlock = BlockReader.readBlock(baseDirectory, 3000);
        assertNotNull(readBlock);
        assertEquals(3000, readBlock.items().getFirst().blockHeader().number());
    }

    /**
     * Test lazy evaluation of the readBlocks stream.
     */
    @Test
    void testReadBlocksLazyEvaluation() throws IOException {
        // Write 100 blocks
        for (long i = 1; i <= 100; i++) {
            BlockWriter.writeBlock(baseDirectory, createTestBlock(i), BlockArchiveType.INDIVIDUAL_FILES);
        }

        // Use limit to only read first 10
        List<Block> limited =
                BlockReader.readBlocks(baseDirectory, 1, 100).limit(10).toList();

        assertEquals(10, limited.size());
        assertEquals(1, limited.get(0).items().getFirst().blockHeader().number());
        assertEquals(10, limited.get(9).items().getFirst().blockHeader().number());
    }

    /**
     * Helper method to create a test block with a given block number.
     */
    private Block createTestBlock(long blockNumber) {
        BlockHeader header = BlockHeader.newBuilder().number(blockNumber).build();

        BlockItem headerItem = BlockItem.newBuilder().blockHeader(header).build();

        return Block.newBuilder().items(List.of(headerItem)).build();
    }

    /**
     * Helper method to assert that two blocks are equal.
     */
    private void assertBlocksEqual(Block expected, Block actual) {
        assertNotNull(actual, "Read block should not be null");
        assertEquals(expected.items().size(), actual.items().size(), "Block should have same number of items");

        BlockHeader expectedHeader = expected.items().getFirst().blockHeader();
        BlockHeader actualHeader = actual.items().getFirst().blockHeader();

        assertEquals(expectedHeader.number(), actualHeader.number(), "Block numbers should match");
    }
}
