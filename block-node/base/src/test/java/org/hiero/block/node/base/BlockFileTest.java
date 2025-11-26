// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.base;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.aggregator.ArgumentsAccessor;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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

    @ParameterizedTest(name = "{0}")
    @MethodSource("org.hiero.block.node.base.BlockFileTest#blockNumbersAndFilePaths")
    @DisplayName("Block Number From File")
    void testBlockNumberFromFile(final ArgumentsAccessor args) {
        final long expectedNumber = args.getLong(1);
        final Path file = args.get(2, Path.class);
        assertEquals(expectedNumber, BlockFile.blockNumberFromFile(file));
    }

    /**
     * Method source for test cases to verify `blockNumberFromFile`.
     */
    private static Stream<Arguments> blockNumbersAndFilePaths() {
        final String thirtySixZeros = "000000000000000000000000000000000000";
        return Stream.of(
                Arguments.of("Simple 'happy' test", 123L, Path.of("0000000000000000123.blk.zstd")),
                Arguments.of("a large block number", 1827329783712391L, Path.of("0001827329783712391.blk.zstd")),
                Arguments.of("max-long block number", Long.MAX_VALUE, Path.of(Long.MAX_VALUE + ".blk.zstd")),
                Arguments.of("number 0", 0L, Path.of("0.blk.zstd")),
                Arguments.of("number 0 with extra 0's prefix", 0L, Path.of(thirtySixZeros + "0000.blk.zstd")),
                Arguments.of("number 1984 with extra 0's prefix", 1984L, Path.of(thirtySixZeros + "1984.blk.zstd")),
                Arguments.of(
                        "directory that looks like a block file",
                        10L,
                        Path.of("/000/000/000/010.blk.zip/nothing").getParent()),
                Arguments.of("Wrong extension returns -1", -1L, Path.of("marker.file")),
                Arguments.of("No extension returns -1", -1L, Path.of("file-without-extension")),
                Arguments.of("Filename is not a number returns -1", -1L, Path.of("copy-of-error.blk.zstd")),
                Arguments.of("Filename is out of range returns -1", -1L, Path.of(Long.MAX_VALUE + "9182.blk.zstd")),
                Arguments.of("Directory without an extension returns -1", -1L, Path.of("/000/000/000/010")),
                Arguments.of(
                        "root path (getFileName is null) returns -1",
                        -1L,
                        Path.of("/000/000/000/010/").getRoot()),
                Arguments.of("null path returns -1", -1L, null));
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
        assertThrows(
                RuntimeException.class,
                () -> BlockFile.nestedDirectoriesAllBlockNumbers(Path.of("fake"), CompressionType.NONE));
    }
}
