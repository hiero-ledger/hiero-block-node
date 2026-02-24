// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.recent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import org.hiero.block.node.base.CompressionType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Unit tests for the {@link RecentBlockPath} class.
 */
@DisplayName("RecentBlockPath Tests")
class RecentBlockPathTest {

    private FileSystem fileSystem;
    private Path blocksRootPath;
    private FilesRecentConfig config;

    @BeforeEach
    void setup() {
        fileSystem = Jimfs.newFileSystem(Configuration.unix());
        blocksRootPath = fileSystem.getPath("/blocks");
        config = new FilesRecentConfig(blocksRootPath, CompressionType.ZSTD, 3, 100);
    }

    @AfterEach
    void cleanup() throws IOException {
        fileSystem.close();
    }

    /**
     * Test that the record constructor creates a valid RecentBlockPath instance.
     */
    @Test
    @DisplayName("Test RecentBlockPath record construction")
    void testRecordConstruction() {
        Path testPath = blocksRootPath.resolve("123.blk.zstd");
        long blockNumber = 123;
        CompressionType compressionType = CompressionType.ZSTD;

        RecentBlockPath recentBlockPath = new RecentBlockPath(testPath, blockNumber, compressionType);

        assertEquals(testPath, recentBlockPath.path());
        assertEquals(blockNumber, recentBlockPath.blockNumber());
        assertEquals(compressionType, recentBlockPath.compressionType());
    }

    /**
     * Test that computeBlockPath returns the expected path structure.
     */
    @Test
    @DisplayName("Test computeBlockPath with default configuration")
    void testComputeBlockPath() {
        long blockNumber = 123;

        RecentBlockPath result = RecentBlockPath.computeBlockPath(config, blockNumber);

        assertNotNull(result);
        assertEquals(blockNumber, result.blockNumber());
        assertEquals(CompressionType.ZSTD, result.compressionType());
        assertNotNull(result.path());

        // Verify the path contains the expected nested directory structure
        String pathString = result.path().toString();
        assertThat(pathString).contains("000").endsWith("0000000000000000123.blk.zstd");
    }

    /**
     * Test computeBlockPath with different block numbers.
     */
    @Test
    @DisplayName("Test computeBlockPath with various block numbers")
    void testComputeBlockPathVariousNumbers() {
        // Test with block number 0
        RecentBlockPath result0 = RecentBlockPath.computeBlockPath(config, 0);
        assertNotNull(result0);
        assertEquals(0, result0.blockNumber());
        assertThat(result0.path().toString()).endsWith("0000000000000000000.blk.zstd");

        // Test with large block number
        long largeBlockNumber = 1234567890123456789L;
        RecentBlockPath resultLarge = RecentBlockPath.computeBlockPath(config, largeBlockNumber);
        assertNotNull(resultLarge);
        assertEquals(largeBlockNumber, resultLarge.blockNumber());
        assertThat(resultLarge.path().toString()).endsWith("1234567890123456789.blk.zstd");

        // Verify nested directory structure for large number
        String pathString = resultLarge.path().toString();
        assertThat(pathString).contains("123").contains("456").contains("789");
    }

    /**
     * Test computeBlockPath with different compression types.
     */
    @ParameterizedTest
    @EnumSource(CompressionType.class)
    @DisplayName("Test computeBlockPath with different compression types")
    void testComputeBlockPathDifferentCompression(CompressionType compressionType) {
        FilesRecentConfig testConfig = new FilesRecentConfig(blocksRootPath, compressionType, 3, 100);
        RecentBlockPath blockPath = RecentBlockPath.computeBlockPath(testConfig, 123);
        assertThat(blockPath).isNotNull();
        assertThat(compressionType).isEqualTo(blockPath.compressionType());
        assertThat(blockPath.path().toString()).endsWith("0000000000000000123.blk" + compressionType.extension());
    }

    /**
     * Test computeBlockPath with different filesPerDir settings.
     */
    @Test
    @DisplayName("Test computeBlockPath with different filesPerDir settings")
    void testComputeBlockPathDifferentFilesPerDir() {
        // Test with filesPerDir=2
        FilesRecentConfig config2 = new FilesRecentConfig(blocksRootPath, CompressionType.ZSTD, 2, 100);
        RecentBlockPath result2 = RecentBlockPath.computeBlockPath(config2, 123);

        assertNotNull(result2);
        String pathString2 = result2.path().toString();
        // With filesPerDir=2, we should have more directories
        assertThat(pathString2).contains("00").endsWith("0000000000000000123.blk.zstd");

        // Test with filesPerDir=3 (default)
        RecentBlockPath result3 = RecentBlockPath.computeBlockPath(config, 123);

        assertThat(result3).isNotNull();
        String pathString3 = result3.path().toString();
        assertThat(pathString3).contains("000");

        // The paths should be different
        assertThat(pathString2).isNotEqualTo(pathString3);
    }

    /**
     * Test computeExistingBlockPath when the file exists with configured compression.
     */
    @Test
    @DisplayName("Test computeExistingBlockPath with existing file")
    void testComputeExistingBlockPathFileExists() throws IOException {
        long blockNumber = 123;

        // Create the expected file
        RecentBlockPath expectedPath = RecentBlockPath.computeBlockPath(config, blockNumber);
        Files.createDirectories(expectedPath.path().getParent());
        Files.createFile(expectedPath.path());

        // Test finding the existing file
        RecentBlockPath result = RecentBlockPath.computeExistingBlockPath(config, blockNumber);

        assertNotNull(result);
        assertEquals(blockNumber, result.blockNumber());
        assertEquals(CompressionType.ZSTD, result.compressionType());
        assertEquals(expectedPath.path(), result.path());
        assertThat(result.path()).exists();
    }

    /**
     * Test computeExistingBlockPath when the file doesn't exist at all.
     */
    @Test
    @DisplayName("Test computeExistingBlockPath with no existing file")
    void testComputeExistingBlockPathNoFile() {
        // Don't create any file
        RecentBlockPath result = RecentBlockPath.computeExistingBlockPath(config, 123);

        assertThat(result).isNull();
    }

    /**
     * Test computeExistingBlockPath when file exists with different compression type.
     */
    @Test
    @DisplayName("Test computeExistingBlockPath finds file with different compression")
    void testComputeExistingBlockPathDifferentCompression() throws IOException {
        long blockNumber = 456;

        // Create file with NONE compression instead of configured ZSTD
        RecentBlockPath expectedPath = RecentBlockPath.computeBlockPath(config, blockNumber);
        Files.createDirectories(expectedPath.path().getParent());

        // Create file with NONE compression in the same directory
        Path noneCompressionPath = expectedPath.path().getParent().resolve("0000000000000000456.blk");
        Files.createFile(noneCompressionPath);

        // Test finding the file with different compression
        RecentBlockPath result = RecentBlockPath.computeExistingBlockPath(config, blockNumber);

        assertThat(result).isNotNull();
        assertThat(blockNumber).isEqualTo(result.blockNumber());
        assertThat(result.compressionType()).isEqualTo(CompressionType.NONE);
        assertThat(noneCompressionPath).isEqualTo(result.path());
        assertThat(result.path()).exists();
    }

    /**
     * Test that computed paths follow the expected nested directory pattern.
     */
    @Test
    @DisplayName("Test nested directory structure pattern")
    void testNestedDirectoryPattern() {
        long blockNumber = 1234567890123456789L;

        RecentBlockPath result = RecentBlockPath.computeBlockPath(config, blockNumber);

        String pathString = result.path().toString();

        // With filesPerDir=3, block number 1234567890123456789 should create:
        // 123/456/789/012/345/6/1234567890123456789.blk.zstd
        assertThat(pathString).matches(".*123.*456.*789.*012.*345.*6.*1234567890123456789\\.blk\\.zstd$");
    }
}
