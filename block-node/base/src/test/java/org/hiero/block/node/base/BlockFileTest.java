// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.base;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for the BlockFile class.
 */
class BlockFileTest {
    @Test
    void testBlockNumberFormatted() {
        assertEquals("0000000000000000123", BlockFile.blockNumberFormated(123));
    }

    @Test
    void testBlockFileName() {
        assertEquals("0000000000000000123.blk", BlockFile.blockFileName(123));
        assertEquals("0000000000000000123.blk.zstd", BlockFile.blockFileName(123, CompressionType.ZSTD));
    }

    @Test
    void testStandaloneBlockFilePath() {
        Path basePath = Paths.get("/base/path");
        Path expectedPath = Paths.get("/base/path/0000000000000000123.blk.zstd");
        assertEquals(expectedPath, BlockFile.standaloneBlockFilePath(basePath, 123, CompressionType.ZSTD));
    }

    @Test
    void testBlockNumberFromFile() {
        Path filePath = Paths.get("0000000000000000123.blk.zstd");
        assertEquals(123, BlockFile.blockNumberFromFile(filePath));
    }

    /**
     * In a deep directory structure, create a block file and test the nestedDirectoriesBlockFilePath method produces
     * same path.
     *
     * @param tempDir the temporary directory to use for the test
     */
    @Test
    void testNestedDirectoriesBlockFilePath(@TempDir Path tempDir) {
        // 0000000000000000123.blk.zstd
        // 1234567890123456789 nineteen digits
        // create a sample file in correct dir structure
        // dirs should be 000/000/000/000/000/0/123
        Path blockDirPath = tempDir.resolve("000/000/000/000/000/0");
        Path blockFilePath = blockDirPath.resolve("0000000000000000123.blk.zstd");
        try {
            Files.createDirectories(blockDirPath);
            Files.createFile(blockFilePath);
        } catch (Exception e) {
            fail("Failed to create directories: " + e.getMessage());
        }
        // check if the nestedDirectoriesBlockFilePath method returns the correct path
        assertEquals(blockFilePath, BlockFile.nestedDirectoriesBlockFilePath(tempDir, 123, CompressionType.ZSTD, 3));
    }

    /**
     * In a deep directory structure, create some block files and test the nestedDirectoriesBlockFilePath method finds
     * them all.
     *
     * @param tempDir the temporary directory to use for the test
     */
    @Test
    void testNestedDirectoriesAllBlockNumbers(@TempDir Path tempDir) {
        long[] blockNumbers = {1, 2, 3, 50992902L, 492290709837901L, 1234567890123456789L};
        // create some block files in the temp directory
        for (long blockNumber : blockNumbers) {
            Path blockFilePath =
                    BlockFile.nestedDirectoriesBlockFilePath(tempDir, blockNumber, CompressionType.ZSTD, 3);
            try {
                // Create the parent directories
                Files.createDirectories(blockFilePath.getParent());
                // Create the file
                Files.createFile(blockFilePath);
            } catch (Exception e) {
                fail("Failed to create block file: " + e.getMessage());
            }
        }
        // Now test the nestedDirectoriesAllBlockNumbers method
        Set<Long> blockNumberFound = BlockFile.nestedDirectoriesAllBlockNumbers(tempDir, CompressionType.ZSTD);
        // compare results
        assertNotNull(blockNumberFound);
        assertEquals(blockNumbers.length, blockNumberFound.size());
        assertArrayEquals(
                blockNumbers,
                blockNumberFound.stream().mapToLong(Long::longValue).sorted().toArray());
        // test min and max as well
        assertEquals(
                Arrays.stream(blockNumbers).min().orElse(-1),
                BlockFile.nestedDirectoriesMinBlockNumber(tempDir, CompressionType.ZSTD));
        assertEquals(
                Arrays.stream(blockNumbers).max().orElse(-1),
                BlockFile.nestedDirectoriesMaxBlockNumber(tempDir, CompressionType.ZSTD));

        // test exceptions
        assertEquals(-1, BlockFile.nestedDirectoriesMinBlockNumber(Path.of("fake"), CompressionType.NONE));
        assertEquals(-1, BlockFile.nestedDirectoriesMaxBlockNumber(Path.of("fake"), CompressionType.NONE));
        assertThrows(RuntimeException.class, () ->
                BlockFile.nestedDirectoriesAllBlockNumbers(Path.of("fake"), CompressionType.NONE));
    }
}
