// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.historic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.from;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.swirlds.config.api.ConfigurationBuilder;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.aggregator.ArgumentsAccessor;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test for {@link BlockPath}.
 */
class BlockPathTest {
    /** The testing in-memory file system. */
    private FileSystem jimfs;

    /** Set up the test environment before each test. */
    @BeforeEach
    void setup() {
        // Initialize the in-memory file system
        jimfs = Jimfs.newFileSystem(Configuration.unix());
    }

    /** Tear down the test environment after each test. */
    @AfterEach
    void tearDown() throws IOException {
        // Close the Jimfs file system
        if (jimfs != null) {
            jimfs.close();
        }
    }

    /**
     * Constructor tests for {@link BlockPath}.
     */
    @Nested
    @DisplayName("Constructor Tests")
    final class ConstructorTests {
        /**
         * This test aims to assert that the constructor of the
         * {@link BlockPath} class does not throw any exceptions when given
         * valid inputs.
         */
        @ParameterizedTest
        @MethodSource("org.hiero.block.node.blocks.files.historic.BlockPathTest#validBlockPathsDefaultConfig")
        @DisplayName("Test constructor throws no exceptions with valid inputs")
        void testConstructorValidInput(final String blockNumStr, final String blockFileName, final String zipFilePath) {
            final Path resolvedZipFilePath = jimfs.getPath(zipFilePath);
            final Path resolvedDirPath = resolvedZipFilePath.getParent();
            assertThatNoException()
                    .isThrownBy(() -> new BlockPath(resolvedDirPath, resolvedZipFilePath, blockNumStr, blockFileName));
        }

        /**
         * This test aims to assert that the constructor of the
         * {@link BlockPath} class does not throw any exceptions when given
         * valid inputs.
         */
        @ParameterizedTest
        @MethodSource("org.hiero.block.node.blocks.files.historic.BlockPathTest#validBlockPathsDefaultConfig")
        @DisplayName("Test constructor does not create any paths with valid inputs")
        void testConstructorValidInputNoCreatePaths(
                final String blockNumStr, final String blockFileName, final String zipFilePath) {
            final Path resolvedZipFilePath = jimfs.getPath(zipFilePath);
            final Path resolvedDirPath = resolvedZipFilePath.getParent();
            // Check that the directory and zip file paths do not exist pre call
            assertThat(resolvedDirPath).doesNotExist();
            assertThat(resolvedZipFilePath).doesNotExist();
            // call
            new BlockPath(resolvedDirPath, resolvedZipFilePath, blockNumStr, blockFileName);
            // Check that the directory and zip file paths are not created post call
            assertThat(resolvedDirPath).doesNotExist();
            assertThat(resolvedZipFilePath).doesNotExist();
        }

        /**
         * This test aims to assert that the constructor of the
         * {@link BlockPath} class throws an {@link NullPointerException} if
         * the directory path is null.
         */
        @ParameterizedTest
        @MethodSource("org.hiero.block.node.blocks.files.historic.BlockPathTest#validBlockPathsDefaultConfig")
        @DisplayName("Test constructor throws NullPointerException when dirPath is null")
        void testConstructorDirPathNull(
                final String blockNumStr, final String blockFileName, final String zipFilePath) {
            final Path resolvedZipFilePath = jimfs.getPath(zipFilePath);
            assertThatNullPointerException()
                    .isThrownBy(() -> new BlockPath(null, resolvedZipFilePath, blockNumStr, blockFileName));
        }

        /**
         * This test aims to assert that the constructor of the
         * {@link BlockPath} class throws an {@link NullPointerException} if
         * the zip file path is null.
         */
        @ParameterizedTest
        @MethodSource("org.hiero.block.node.blocks.files.historic.BlockPathTest#validBlockPathsDefaultConfig")
        @DisplayName("Test constructor throws NullPointerException when zipFilePath is null")
        void testConstructorZipFilePathNull(
                final String blockNumStr, final String blockFileName, final String zipFilePath) {
            final Path resolvedDirPath = jimfs.getPath(zipFilePath).getParent();
            assertThatNullPointerException()
                    .isThrownBy(() -> new BlockPath(resolvedDirPath, null, blockNumStr, blockFileName));
        }

        /**
         * This test aims to assert that the constructor of the
         * {@link BlockPath} class throws an {@link IllegalArgumentException} if
         * the block number string is blank.
         */
        @ParameterizedTest
        @MethodSource("org.hiero.block.node.blocks.files.historic.BlockPathTest#validBlockPathsDefaultConfig")
        @DisplayName("Test constructor throws IllegalArgumentException when blockNumStr is blank")
        void testConstructorBlockNumStrBlank(final ArgumentsAccessor argAccessor) {
            final String blockFileName = argAccessor.getString(1);
            final String zipFilePath = argAccessor.getString(2);
            final Path resolvedZipFilePath = jimfs.getPath(zipFilePath);
            final Path resolvedDirPath = resolvedZipFilePath.getParent();
            assertThatIllegalArgumentException()
                    .isThrownBy(() -> new BlockPath(resolvedDirPath, resolvedZipFilePath, "", blockFileName));
        }

        /**
         * This test aims to assert that the constructor of the
         * {@link BlockPath} class throws an {@link IllegalArgumentException} if
         * the block file name is blank.
         */
        @ParameterizedTest
        @MethodSource("org.hiero.block.node.blocks.files.historic.BlockPathTest#validBlockPathsDefaultConfig")
        @DisplayName("Test constructor throws IllegalArgumentException when blockFileName is blank")
        void testConstructorBlockFileNameBlank(final ArgumentsAccessor argAccessor) {
            final String blockNumStr = argAccessor.getString(0);
            final String zipFilePath = argAccessor.getString(2);
            final Path resolvedZipFilePath = jimfs.getPath(zipFilePath);
            final Path resolvedDirPath = resolvedZipFilePath.getParent();
            assertThatIllegalArgumentException()
                    .isThrownBy(() -> new BlockPath(resolvedDirPath, resolvedZipFilePath, blockNumStr, ""));
        }
    }

    /**
     * Functionality tests for {@link BlockPath}.
     */
    @Nested
    @DisplayName("Functionality Tests")
    final class FunctionalityTests {
        /**
         * This test aims to verify that the {@link BlockPath#computeBlockPath}
         * method correctly computes the block path based on the given block
         * number and default configuration.
         */
        @ParameterizedTest
        @MethodSource("org.hiero.block.node.blocks.files.historic.BlockPathTest#validBlockPathsDefaultConfig")
        @DisplayName("Test computeBlockPath with valid inputs")
        void testComputeBlockPath(
                final String expectedBlockNumStr,
                final String expectedBlockFileName,
                final String zipFilePath,
                final long blockNumber,
                final FilesHistoricConfig baseConfig) {
            final Path expectedZipFilePath = jimfs.getPath(zipFilePath);
            final Path expectedDirPath = expectedZipFilePath.getParent();
            // create the config to use for the test, resolve paths with jimfs
            final FilesHistoricConfig testConfig = new FilesHistoricConfig(
                    jimfs.getPath(baseConfig.rootPath().toString()),
                    baseConfig.compression(),
                    baseConfig.digitsPerDir(),
                    baseConfig.digitsPerZipFileName(),
                    baseConfig.digitsPerZipFileContents());
            final BlockPath actual = BlockPath.computeBlockPath(testConfig, blockNumber);
            assertThat(actual)
                    .isNotNull()
                    .returns(expectedBlockNumStr, from(BlockPath::blockNumStr))
                    .returns(expectedBlockFileName, from(BlockPath::blockFileName))
                    .returns(expectedZipFilePath, from(BlockPath::zipFilePath))
                    .returns(expectedDirPath, from(BlockPath::dirPath));
        }
    }

    /**
     * Stream of arguments of valid block paths.
     */
    private static Stream<Arguments> validBlockPathsDefaultConfig() {
        // default configuration
        final FilesHistoricConfig baseConfig = ConfigurationBuilder.create()
                .withConfigDataType(FilesHistoricConfig.class)
                .build()
                .getConfigData(FilesHistoricConfig.class);
        // root path from config, to use below to create the zip file path (concat)
        final String configRootPath = baseConfig.rootPath().toString();
        return Stream.of(
                Arguments.of(
                        "0000000000123456789",
                        "0000000000123456789.blk.zstd",
                        configRootPath.concat("/000/000/000/012/345/6000s.zip"),
                        123_456_789L,
                        baseConfig),
                Arguments.of(
                        "1234567890123456789",
                        "1234567890123456789.blk.zstd",
                        configRootPath.concat("/123/456/789/012/345/6000s.zip"),
                        1_234_567_890_123_456_789L,
                        baseConfig),
                Arguments.of(
                        "0000000000000000000",
                        "0000000000000000000.blk.zstd",
                        configRootPath.concat("/000/000/000/000/000/0000s.zip"),
                        0L,
                        baseConfig),
                Arguments.of(
                        "0000000000000000010",
                        "0000000000000000010.blk.zstd",
                        configRootPath.concat("/000/000/000/000/000/0000s.zip"),
                        10L,
                        baseConfig),
                Arguments.of(
                        "0000000000000000100",
                        "0000000000000000100.blk.zstd",
                        configRootPath.concat("/000/000/000/000/000/0000s.zip"),
                        100L,
                        baseConfig),
                Arguments.of(
                        "0000000000000001000",
                        "0000000000000001000.blk.zstd",
                        configRootPath.concat("/000/000/000/000/000/1000s.zip"),
                        1_000L,
                        baseConfig),
                Arguments.of(
                        "0000000000000010000",
                        "0000000000000010000.blk.zstd",
                        configRootPath.concat("/000/000/000/000/001/0000s.zip"),
                        10_000L,
                        baseConfig),
                Arguments.of(
                        "0000000000000100000",
                        "0000000000000100000.blk.zstd",
                        configRootPath.concat("/000/000/000/000/010/0000s.zip"),
                        100_000L,
                        baseConfig),
                Arguments.of(
                        "0000000000001000000",
                        "0000000000001000000.blk.zstd",
                        configRootPath.concat("/000/000/000/000/100/0000s.zip"),
                        1_000_000L,
                        baseConfig),
                Arguments.of(
                        "0000000000010000000",
                        "0000000000010000000.blk.zstd",
                        configRootPath.concat("/000/000/000/001/000/0000s.zip"),
                        10_000_000L,
                        baseConfig),
                Arguments.of(
                        "0000000000100000000",
                        "0000000000100000000.blk.zstd",
                        configRootPath.concat("/000/000/000/010/000/0000s.zip"),
                        100_000_000L,
                        baseConfig),
                Arguments.of(
                        "0000000001000000000",
                        "0000000001000000000.blk.zstd",
                        configRootPath.concat("/000/000/000/100/000/0000s.zip"),
                        1_000_000_000L,
                        baseConfig),
                Arguments.of(
                        "0000000010000000000",
                        "0000000010000000000.blk.zstd",
                        configRootPath.concat("/000/000/001/000/000/0000s.zip"),
                        10_000_000_000L,
                        baseConfig),
                Arguments.of(
                        "0000000100000000000",
                        "0000000100000000000.blk.zstd",
                        configRootPath.concat("/000/000/010/000/000/0000s.zip"),
                        100_000_000_000L,
                        baseConfig),
                Arguments.of(
                        "0000001000000000000",
                        "0000001000000000000.blk.zstd",
                        configRootPath.concat("/000/000/100/000/000/0000s.zip"),
                        1_000_000_000_000L,
                        baseConfig),
                Arguments.of(
                        "0000010000000000000",
                        "0000010000000000000.blk.zstd",
                        configRootPath.concat("/000/001/000/000/000/0000s.zip"),
                        10_000_000_000_000L,
                        baseConfig),
                Arguments.of(
                        "0000100000000000000",
                        "0000100000000000000.blk.zstd",
                        configRootPath.concat("/000/010/000/000/000/0000s.zip"),
                        100_000_000_000_000L,
                        baseConfig),
                Arguments.of(
                        "0001000000000000000",
                        "0001000000000000000.blk.zstd",
                        configRootPath.concat("/000/100/000/000/000/0000s.zip"),
                        1_000_000_000_000_000L,
                        baseConfig),
                Arguments.of(
                        "0010000000000000000",
                        "0010000000000000000.blk.zstd",
                        configRootPath.concat("/001/000/000/000/000/0000s.zip"),
                        10_000_000_000_000_000L,
                        baseConfig),
                Arguments.of(
                        "0100000000000000000",
                        "0100000000000000000.blk.zstd",
                        configRootPath.concat("/010/000/000/000/000/0000s.zip"),
                        100_000_000_000_000_000L,
                        baseConfig),
                Arguments.of(
                        "9223372036854775807",
                        "9223372036854775807.blk.zstd",
                        configRootPath.concat("/922/337/203/685/477/5000s.zip"),
                        Long.MAX_VALUE,
                        baseConfig));
    }
}
