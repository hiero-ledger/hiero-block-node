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
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.hiero.block.node.base.CompressionType;
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
    private static final String ROOT_PATH = "/foo/bar";
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
        @MethodSource({
            "org.hiero.block.node.blocks.files.historic.BlockPathTest#validBlockPathsDefaultConfig",
            "org.hiero.block.node.blocks.files.historic.BlockPathTest#validBlockPathsConfigVariation1"
        })
        @DisplayName("Test constructor throws no exceptions with valid inputs")
        void testConstructorValidInput(final ArgumentsAccessor argAccessor) {
            final String blockNumStr = argAccessor.getString(0);
            final String blockFileName = argAccessor.getString(1);
            final String zipFilePath = argAccessor.getString(2);
            final CompressionType compressionType = argAccessor.get(4, CompressionType.class);
            final Path resolvedZipFilePath = jimfs.getPath(zipFilePath);
            final Path resolvedDirPath = resolvedZipFilePath.getParent();
            assertThatNoException()
                    .isThrownBy(() -> new BlockPath(
                            resolvedDirPath, resolvedZipFilePath, blockNumStr, blockFileName, compressionType));
        }

        /**
         * This test aims to assert that the constructor of the
         * {@link BlockPath} class does not throw any exceptions when given
         * valid inputs.
         */
        @ParameterizedTest
        @MethodSource({
            "org.hiero.block.node.blocks.files.historic.BlockPathTest#validBlockPathsDefaultConfig",
            "org.hiero.block.node.blocks.files.historic.BlockPathTest#validBlockPathsConfigVariation1"
        })
        @DisplayName("Test constructor does not create any paths with valid inputs")
        void testConstructorValidInputNoCreatePaths(final ArgumentsAccessor argAccessor) {
            final String blockNumStr = argAccessor.getString(0);
            final String blockFileName = argAccessor.getString(1);
            final String zipFilePath = argAccessor.getString(2);
            final CompressionType compressionType = argAccessor.get(4, CompressionType.class);
            final Path resolvedZipFilePath = jimfs.getPath(zipFilePath);
            final Path resolvedDirPath = resolvedZipFilePath.getParent();
            // Check that the directory and zip file paths do not exist pre call
            assertThat(resolvedDirPath).doesNotExist();
            assertThat(resolvedZipFilePath).doesNotExist();
            // call
            new BlockPath(resolvedDirPath, resolvedZipFilePath, blockNumStr, blockFileName, compressionType);
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
        @MethodSource({
            "org.hiero.block.node.blocks.files.historic.BlockPathTest#validBlockPathsDefaultConfig",
            "org.hiero.block.node.blocks.files.historic.BlockPathTest#validBlockPathsConfigVariation1"
        })
        @DisplayName("Test constructor throws NullPointerException when dirPath is null")
        void testConstructorDirPathNull(final ArgumentsAccessor argAccessor) {
            final String blockNumStr = argAccessor.getString(0);
            final String blockFileName = argAccessor.getString(1);
            final String zipFilePath = argAccessor.getString(2);
            final CompressionType compressionType = argAccessor.get(4, CompressionType.class);
            final Path resolvedZipFilePath = jimfs.getPath(zipFilePath);
            assertThatNullPointerException()
                    .isThrownBy(() ->
                            new BlockPath(null, resolvedZipFilePath, blockNumStr, blockFileName, compressionType));
        }

        /**
         * This test aims to assert that the constructor of the
         * {@link BlockPath} class throws an {@link NullPointerException} if
         * the zip file path is null.
         */
        @ParameterizedTest
        @MethodSource({
            "org.hiero.block.node.blocks.files.historic.BlockPathTest#validBlockPathsDefaultConfig",
            "org.hiero.block.node.blocks.files.historic.BlockPathTest#validBlockPathsConfigVariation1"
        })
        @DisplayName("Test constructor throws NullPointerException when zipFilePath is null")
        void testConstructorZipFilePathNull(final ArgumentsAccessor argAccessor) {
            final String blockNumStr = argAccessor.getString(0);
            final String blockFileName = argAccessor.getString(1);
            final String zipFilePath = argAccessor.getString(2);
            final CompressionType compressionType = argAccessor.get(4, CompressionType.class);
            final Path resolvedDirPath = jimfs.getPath(zipFilePath).getParent();
            assertThatNullPointerException()
                    .isThrownBy(
                            () -> new BlockPath(resolvedDirPath, null, blockNumStr, blockFileName, compressionType));
        }

        /**
         * This test aims to assert that the constructor of the
         * {@link BlockPath} class throws an {@link IllegalArgumentException} if
         * the block number string is blank.
         */
        @ParameterizedTest
        @MethodSource({
            "org.hiero.block.node.blocks.files.historic.BlockPathTest#validBlockPathsDefaultConfig",
            "org.hiero.block.node.blocks.files.historic.BlockPathTest#validBlockPathsConfigVariation1"
        })
        @DisplayName("Test constructor throws IllegalArgumentException when blockNumStr is blank")
        void testConstructorBlockNumStrBlank(final ArgumentsAccessor argAccessor) {
            final String blockFileName = argAccessor.getString(1);
            final String zipFilePath = argAccessor.getString(2);
            final CompressionType compressionType = argAccessor.get(4, CompressionType.class);
            final Path resolvedZipFilePath = jimfs.getPath(zipFilePath);
            final Path resolvedDirPath = resolvedZipFilePath.getParent();
            assertThatIllegalArgumentException()
                    .isThrownBy(() ->
                            new BlockPath(resolvedDirPath, resolvedZipFilePath, "", blockFileName, compressionType));
        }

        /**
         * This test aims to assert that the constructor of the
         * {@link BlockPath} class throws an {@link IllegalArgumentException} if
         * the block file name is blank.
         */
        @ParameterizedTest
        @MethodSource({
            "org.hiero.block.node.blocks.files.historic.BlockPathTest#validBlockPathsDefaultConfig",
            "org.hiero.block.node.blocks.files.historic.BlockPathTest#validBlockPathsConfigVariation1"
        })
        @DisplayName("Test constructor throws IllegalArgumentException when blockFileName is blank")
        void testConstructorBlockFileNameBlank(final ArgumentsAccessor argAccessor) {
            final String blockNumStr = argAccessor.getString(0);
            final String zipFilePath = argAccessor.getString(2);
            final CompressionType compressionType = argAccessor.get(4, CompressionType.class);
            final Path resolvedZipFilePath = jimfs.getPath(zipFilePath);
            final Path resolvedDirPath = resolvedZipFilePath.getParent();
            assertThatIllegalArgumentException()
                    .isThrownBy(() ->
                            new BlockPath(resolvedDirPath, resolvedZipFilePath, blockNumStr, "", compressionType));
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
        @MethodSource({
            "org.hiero.block.node.blocks.files.historic.BlockPathTest#validBlockPathsDefaultConfig",
            "org.hiero.block.node.blocks.files.historic.BlockPathTest#validBlockPathsConfigVariation1"
        })
        @DisplayName("Test computeBlockPath with valid inputs")
        void testComputeBlockPath(
                final String expectedBlockNumStr,
                final String expectedBlockFileName,
                final String expectedRelativeZipFilePathStr,
                final long blockNumber,
                final CompressionType expectedCompressionType,
                final int digitsPerZipFileContents) {
            final Path expectedZipFilePath = jimfs.getPath(ROOT_PATH + expectedRelativeZipFilePathStr);
            final Path expectedDirPath = expectedZipFilePath.getParent();
            // create the config to use for the test, resolve paths with jimfs
            final FilesHistoricConfig testConfig = new FilesHistoricConfig(
                    jimfs.getPath(ROOT_PATH), expectedCompressionType, digitsPerZipFileContents);
            final BlockPath actual = BlockPath.computeBlockPath(testConfig, blockNumber);
            assertThat(actual)
                    .isNotNull()
                    .returns(expectedBlockNumStr, from(BlockPath::blockNumStr))
                    .returns(expectedBlockFileName, from(BlockPath::blockFileName))
                    .returns(expectedZipFilePath, from(BlockPath::zipFilePath))
                    .returns(expectedDirPath, from(BlockPath::dirPath))
                    .returns(expectedCompressionType, from(BlockPath::compressionType));
        }
    }

    /**
     * Stream of arguments of valid block paths with default config.
     */
    private static Stream<Arguments> validBlockPathsDefaultConfig() {
        // default configuration
        final FilesHistoricConfig baseConfig = ConfigurationBuilder.create()
                .withConfigDataType(FilesHistoricConfig.class)
                .build()
                .getConfigData(FilesHistoricConfig.class);
        return Stream.of(
                Arguments.of(
                        "0000000000123456789",
                        "0000000000123456789.blk.zstd",
                        "/000/000/000/012/34/50000s.zip",
                        123_456_789L,
                        baseConfig.compression(),
                        baseConfig.powersOfTenPerZipFileContents()),
                Arguments.of(
                        "1234567890123456789",
                        "1234567890123456789.blk.zstd",
                        "/123/456/789/012/34/50000s.zip",
                        1_234_567_890_123_456_789L,
                        baseConfig.compression(),
                        baseConfig.powersOfTenPerZipFileContents()),
                Arguments.of(
                        "0000000000000000000",
                        "0000000000000000000.blk.zstd",
                        "/000/000/000/000/00/00000s.zip",
                        0L,
                        baseConfig.compression(),
                        baseConfig.powersOfTenPerZipFileContents()),
                Arguments.of(
                        "0000000000000000010",
                        "0000000000000000010.blk.zstd",
                        "/000/000/000/000/00/00000s.zip",
                        10L,
                        baseConfig.compression(),
                        baseConfig.powersOfTenPerZipFileContents()),
                Arguments.of(
                        "0000000000000000100",
                        "0000000000000000100.blk.zstd",
                        "/000/000/000/000/00/00000s.zip",
                        100L,
                        baseConfig.compression(),
                        baseConfig.powersOfTenPerZipFileContents()),
                Arguments.of(
                        "0000000000000001000",
                        "0000000000000001000.blk.zstd",
                        "/000/000/000/000/00/00000s.zip",
                        1_000L,
                        baseConfig.compression(),
                        baseConfig.powersOfTenPerZipFileContents()),
                Arguments.of(
                        "0000000000000010000",
                        "0000000000000010000.blk.zstd",
                        "/000/000/000/000/00/10000s.zip",
                        10_000L,
                        baseConfig.compression(),
                        baseConfig.powersOfTenPerZipFileContents()),
                Arguments.of(
                        "0000000000000100000",
                        "0000000000000100000.blk.zstd",
                        "/000/000/000/000/01/00000s.zip",
                        100_000L,
                        baseConfig.compression(),
                        baseConfig.powersOfTenPerZipFileContents()),
                Arguments.of(
                        "0000000000001000000",
                        "0000000000001000000.blk.zstd",
                        "/000/000/000/000/10/00000s.zip",
                        1_000_000L,
                        baseConfig.compression(),
                        baseConfig.powersOfTenPerZipFileContents()),
                Arguments.of(
                        "0000000000010000000",
                        "0000000000010000000.blk.zstd",
                        "/000/000/000/001/00/00000s.zip",
                        10_000_000L,
                        baseConfig.compression(),
                        baseConfig.powersOfTenPerZipFileContents()),
                Arguments.of(
                        "0000000000100000000",
                        "0000000000100000000.blk.zstd",
                        "/000/000/000/010/00/00000s.zip",
                        100_000_000L,
                        baseConfig.compression(),
                        baseConfig.powersOfTenPerZipFileContents()),
                Arguments.of(
                        "0000000001000000000",
                        "0000000001000000000.blk.zstd",
                        "/000/000/000/100/00/00000s.zip",
                        1_000_000_000L,
                        baseConfig.compression(),
                        baseConfig.powersOfTenPerZipFileContents()),
                Arguments.of(
                        "0000000010000000000",
                        "0000000010000000000.blk.zstd",
                        "/000/000/001/000/00/00000s.zip",
                        10_000_000_000L,
                        baseConfig.compression(),
                        baseConfig.powersOfTenPerZipFileContents()),
                Arguments.of(
                        "0000000100000000000",
                        "0000000100000000000.blk.zstd",
                        "/000/000/010/000/00/00000s.zip",
                        100_000_000_000L,
                        baseConfig.compression(),
                        baseConfig.powersOfTenPerZipFileContents()),
                Arguments.of(
                        "0000001000000000000",
                        "0000001000000000000.blk.zstd",
                        "/000/000/100/000/00/00000s.zip",
                        1_000_000_000_000L,
                        baseConfig.compression(),
                        baseConfig.powersOfTenPerZipFileContents()),
                Arguments.of(
                        "0000010000000000000",
                        "0000010000000000000.blk.zstd",
                        "/000/001/000/000/00/00000s.zip",
                        10_000_000_000_000L,
                        baseConfig.compression(),
                        baseConfig.powersOfTenPerZipFileContents()),
                Arguments.of(
                        "0000100000000000000",
                        "0000100000000000000.blk.zstd",
                        "/000/010/000/000/00/00000s.zip",
                        100_000_000_000_000L,
                        baseConfig.compression(),
                        baseConfig.powersOfTenPerZipFileContents()),
                Arguments.of(
                        "0001000000000000000",
                        "0001000000000000000.blk.zstd",
                        "/000/100/000/000/00/00000s.zip",
                        1_000_000_000_000_000L,
                        baseConfig.compression(),
                        baseConfig.powersOfTenPerZipFileContents()),
                Arguments.of(
                        "0010000000000000000",
                        "0010000000000000000.blk.zstd",
                        "/001/000/000/000/00/00000s.zip",
                        10_000_000_000_000_000L,
                        baseConfig.compression(),
                        baseConfig.powersOfTenPerZipFileContents()),
                Arguments.of(
                        "0100000000000000000",
                        "0100000000000000000.blk.zstd",
                        "/010/000/000/000/00/00000s.zip",
                        100_000_000_000_000_000L,
                        baseConfig.compression(),
                        baseConfig.powersOfTenPerZipFileContents()),
                Arguments.of(
                        "9223372036854775807",
                        "9223372036854775807.blk.zstd",
                        "/922/337/203/685/47/70000s.zip",
                        Long.MAX_VALUE,
                        baseConfig.compression(),
                        baseConfig.powersOfTenPerZipFileContents()));
    }

    /**
     * Stream of arguments of valid block paths with config variation 1.
     */
    private static Stream<Arguments> validBlockPathsConfigVariation1() {
        final List<Arguments> argumentsList = new ArrayList<>();
        for (final CompressionType compressionType : CompressionType.values()) {
            argumentsList.addAll(List.of(
                    Arguments.of(
                            "0000000000123456789",
                            "0000000000123456789.blk" + compressionType.extension(),
                            "/000/000/000/012/345/67/80s.zip",
                            123_456_789L,
                            compressionType,
                            1),
                    Arguments.of(
                            "0000000000123456789",
                            "0000000000123456789.blk" + compressionType.extension(),
                            "/000/000/000/012/345/6/700s.zip",
                            123_456_789L,
                            compressionType,
                            2),
                    Arguments.of(
                            "0000000000123456789",
                            "0000000000123456789.blk" + compressionType.extension(),
                            "/000/000/000/012/345/6000s.zip",
                            123_456_789L,
                            compressionType,
                            3),
                    Arguments.of(
                            "0000000000123456789",
                            "0000000000123456789.blk" + compressionType.extension(),
                            "/000/000/000/012/34/50000s.zip",
                            123_456_789L,
                            compressionType,
                            4),
                    Arguments.of(
                            "0000000000123456789",
                            "0000000000123456789.blk" + compressionType.extension(),
                            "/000/000/000/012/3/400000s.zip",
                            123_456_789L,
                            compressionType,
                            5),
                    Arguments.of(
                            "0000000000000000000",
                            "0000000000000000000.blk" + compressionType.extension(),
                            "/000/000/000/000/0000000s.zip",
                            0L,
                            compressionType,
                            6),
                    Arguments.of(
                            "0000000000000000010",
                            "0000000000000000010.blk" + compressionType.extension(),
                            "/000/000/000/000/000/0000s.zip",
                            10L,
                            compressionType,
                            3),
                    Arguments.of(
                            "0000000000000000100",
                            "0000000000000000100.blk" + compressionType.extension(),
                            "/000/000/000/000/000/0000s.zip",
                            100L,
                            compressionType,
                            3),
                    Arguments.of(
                            "0000000000000001000",
                            "0000000000000001000.blk" + compressionType.extension(),
                            "/000/000/000/000/000/1000s.zip",
                            1_000L,
                            compressionType,
                            3),
                    Arguments.of(
                            "0000000000000010000",
                            "0000000000000010000.blk" + compressionType.extension(),
                            "/000/000/000/000/001/0000s.zip",
                            10_000L,
                            compressionType,
                            3),
                    Arguments.of(
                            "0000000000000100000",
                            "0000000000000100000.blk" + compressionType.extension(),
                            "/000/000/000/000/010/0000s.zip",
                            100_000L,
                            compressionType,
                            3),
                    Arguments.of(
                            "0000000000001000000",
                            "0000000000001000000.blk" + compressionType.extension(),
                            "/000/000/000/000/100/0000s.zip",
                            1_000_000L,
                            compressionType,
                            3),
                    Arguments.of(
                            "0000000000010000000",
                            "0000000000010000000.blk" + compressionType.extension(),
                            "/000/000/000/001/000/0000s.zip",
                            10_000_000L,
                            compressionType,
                            3),
                    Arguments.of(
                            "0000000000100000000",
                            "0000000000100000000.blk" + compressionType.extension(),
                            "/000/000/000/010/000/0000s.zip",
                            100_000_000L,
                            compressionType,
                            3),
                    Arguments.of(
                            "0000000001000000000",
                            "0000000001000000000.blk" + compressionType.extension(),
                            "/000/000/000/100/000/0000s.zip",
                            1_000_000_000L,
                            compressionType,
                            3),
                    Arguments.of(
                            "0000000010000000000",
                            "0000000010000000000.blk" + compressionType.extension(),
                            "/000/000/001/000/000/0000s.zip",
                            10_000_000_000L,
                            compressionType,
                            3),
                    Arguments.of(
                            "0000000100000000000",
                            "0000000100000000000.blk" + compressionType.extension(),
                            "/000/000/010/000/000/0000s.zip",
                            100_000_000_000L,
                            compressionType,
                            3),
                    Arguments.of(
                            "0000001000000000000",
                            "0000001000000000000.blk" + compressionType.extension(),
                            "/000/000/100/000/000/0000s.zip",
                            1_000_000_000_000L,
                            compressionType,
                            3),
                    Arguments.of(
                            "0000010000000000000",
                            "0000010000000000000.blk" + compressionType.extension(),
                            "/000/001/000/000/000/0000s.zip",
                            10_000_000_000_000L,
                            compressionType,
                            3),
                    Arguments.of(
                            "0000100000000000000",
                            "0000100000000000000.blk" + compressionType.extension(),
                            "/000/010/000/000/000/0000s.zip",
                            100_000_000_000_000L,
                            compressionType,
                            3),
                    Arguments.of(
                            "0001000000000000000",
                            "0001000000000000000.blk" + compressionType.extension(),
                            "/000/100/000/000/000/0000s.zip",
                            1_000_000_000_000_000L,
                            compressionType,
                            3),
                    Arguments.of(
                            "0010000000000000000",
                            "0010000000000000000.blk" + compressionType.extension(),
                            "/001/000/000/000/000/0000s.zip",
                            10_000_000_000_000_000L,
                            compressionType,
                            3),
                    Arguments.of(
                            "0100000000000000000",
                            "0100000000000000000.blk" + compressionType.extension(),
                            "/010/000/000/000/000/0000s.zip",
                            100_000_000_000_000_000L,
                            compressionType,
                            3),
                    Arguments.of(
                            "9223372036854775807",
                            "9223372036854775807.blk" + compressionType.extension(),
                            "/922/337/203/685/477/5000s.zip",
                            Long.MAX_VALUE,
                            compressionType,
                            3)));
        }
        return argumentsList.stream();
    }
}
