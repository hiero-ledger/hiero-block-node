// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.historic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.hiero.block.node.base.CompressionType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test for {@link FilesHistoricConfig}.
 */
@DisplayName("FilesHistoricConfig Tests")
class FilesHistoricConfigTest {
    private FileSystem jimfs;
    private Path defaultRootPath;
    private CompressionType defaultCompression;
    private int defaultDigitsPerDir;
    private int defaultDigitsPerZipFileName;
    private int defaultDigitsPerZipFileContents;

    /**
     * Environment setup before each test.
     */
    @BeforeEach
    void setup() {
        jimfs = Jimfs.newFileSystem(Configuration.unix());
        defaultRootPath = jimfs.getPath("/opt/hashgraph/blocknode/data/historic");
        defaultCompression = CompressionType.ZSTD;
        defaultDigitsPerDir = 3;
        defaultDigitsPerZipFileName = 1;
        defaultDigitsPerZipFileContents = 4;
    }

    /**
     * Teardown method after each test.
     */
    @AfterEach
    void tearDown() throws IOException {
        if (jimfs != null) {
            jimfs.close();
        }
    }

    /**
     * Constructor tests.
     */
    @Nested
    @DisplayName("Constructor Tests")
    final class ConstructorTests {
        /**
         * This test aims to verify that the constructor of
         * {@link FilesHistoricConfig} throws a {@link NullPointerException} if
         * the rootPath is null.
         */
        @Test
        @DisplayName("Test that NullPointerException is thrown when rootPath is null")
        void testNullRootPath() {
            assertThatNullPointerException()
                    .isThrownBy(() -> new FilesHistoricConfig(
                            null,
                            defaultCompression,
                            defaultDigitsPerDir,
                            defaultDigitsPerZipFileName,
                            defaultDigitsPerZipFileContents));
        }

        /**
         * This test aims to verify that the constructor of
         * {@link FilesHistoricConfig} throws a {@link NullPointerException} if
         * the compression is null.
         */
        @Test
        @DisplayName("Test that NullPointerException is thrown when compression is null")
        void testNullCompression() {
            assertThatNullPointerException()
                    .isThrownBy(() -> new FilesHistoricConfig(
                            defaultRootPath,
                            null,
                            defaultDigitsPerDir,
                            defaultDigitsPerZipFileName,
                            defaultDigitsPerZipFileContents));
        }

        /**
         * This test aims to verify that the constructor of
         * {@link FilesHistoricConfig} does not throw an exception when the
         * digitsPerDir is in range.
         */
        @ParameterizedTest
        @MethodSource("org.hiero.block.node.blocks.files.historic.FilesHistoricConfigTest#validDigitsPerDir")
        @DisplayName("Test that no exception is thrown when digitsPerDir is in range")
        void testValidDigitsPerDir(final int validDigitsPerDir) {
            assertThatNoException()
                    .isThrownBy(() -> new FilesHistoricConfig(
                            defaultRootPath,
                            defaultCompression,
                            validDigitsPerDir,
                            defaultDigitsPerZipFileName,
                            defaultDigitsPerZipFileContents));
        }

        /**
         * This test aims to verify that the constructor of
         * {@link FilesHistoricConfig} throws a {@link IllegalArgumentException}
         * if the digitsPerDir is out of range.
         */
        @ParameterizedTest
        @MethodSource("org.hiero.block.node.blocks.files.historic.FilesHistoricConfigTest#invalidDigitsPerDir")
        @DisplayName("Test that IllegalArgumentException is thrown when digitsPerDir is out of range")
        void testInvalidDigitsPerDir(final int invalidDigitsPerDir) {
            assertThatIllegalArgumentException()
                    .isThrownBy(() -> new FilesHistoricConfig(
                            defaultRootPath,
                            defaultCompression,
                            invalidDigitsPerDir,
                            defaultDigitsPerZipFileName,
                            defaultDigitsPerZipFileContents));
        }

        /**
         * This test aims to verify that the constructor of
         * {@link FilesHistoricConfig} does not throw an exception when the
         * digitsPerZipFileName is in range.
         */
        @ParameterizedTest
        @MethodSource("org.hiero.block.node.blocks.files.historic.FilesHistoricConfigTest#validDigitsPerZipFileName")
        @DisplayName("Test that no exception is thrown when digitsPerZipFileName is in range")
        void testValidDigitsPerZipFileName(final int validDigitsPerZipFileName) {
            assertThatNoException()
                    .isThrownBy(() -> new FilesHistoricConfig(
                            defaultRootPath,
                            defaultCompression,
                            defaultDigitsPerDir,
                            validDigitsPerZipFileName,
                            defaultDigitsPerZipFileContents));
        }

        /**
         * This test aims to verify that the constructor of
         * {@link FilesHistoricConfig} throws a {@link IllegalArgumentException}
         * if the digitsPerZipFileName is out of range.
         */
        @ParameterizedTest
        @MethodSource("org.hiero.block.node.blocks.files.historic.FilesHistoricConfigTest#invalidDigitsPerZipFileName")
        @DisplayName("Test that IllegalArgumentException is thrown when digitsPerZipFileName is out of range")
        void testInvalidDigitsPerZipFileName(final int invalidDigitsPerZipFileName) {
            assertThatIllegalArgumentException()
                    .isThrownBy(() -> new FilesHistoricConfig(
                            defaultRootPath,
                            defaultCompression,
                            defaultDigitsPerDir,
                            invalidDigitsPerZipFileName,
                            defaultDigitsPerZipFileContents));
        }

        /**
         * This test aims to verify that the constructor of
         * {@link FilesHistoricConfig} does not throw an exception when the
         * digitsPerZipFileContents is in range.
         */
        @ParameterizedTest
        @MethodSource(
                "org.hiero.block.node.blocks.files.historic.FilesHistoricConfigTest#validDigitsPerZipFileContents")
        @DisplayName("Test that no exception is thrown when digitsPerZipFileContents is in range")
        void testValidDigitsPerZipFileContents(final int validDigitsPerZipFileContents) {
            assertThatNoException()
                    .isThrownBy(() -> new FilesHistoricConfig(
                            defaultRootPath,
                            defaultCompression,
                            defaultDigitsPerDir,
                            defaultDigitsPerZipFileName,
                            validDigitsPerZipFileContents));
        }

        /**
         * This test aims to verify that the constructor of
         * {@link FilesHistoricConfig} throws a {@link IllegalArgumentException}
         * if the digitsPerZipFileContents is out of range.
         */
        @ParameterizedTest
        @MethodSource(
                "org.hiero.block.node.blocks.files.historic.FilesHistoricConfigTest#invalidDigitsPerZipFileContents")
        @DisplayName("Test that IllegalArgumentException is thrown when digitsPerZipFileContents is out of range")
        void testInvalidDigitsPerZipFileContents(final int invalidDigitsPerZipFileContents) {
            assertThatIllegalArgumentException()
                    .isThrownBy(() -> new FilesHistoricConfig(
                            defaultRootPath,
                            defaultCompression,
                            defaultDigitsPerDir,
                            defaultDigitsPerZipFileName,
                            invalidDigitsPerZipFileContents));
        }

        /**
         * This test aims to verify that the constructor of
         * {@link FilesHistoricConfig} does not throw an exception when all
         * inputs are valid.
         */
        @Test
        @DisplayName("Test that constructor does not throw an exception when all inputs are valid")
        void testValidConstructor() {
            assertThatNoException()
                    .isThrownBy(() -> new FilesHistoricConfig(
                            defaultRootPath.resolve("valid"),
                            CompressionType.NONE,
                            defaultDigitsPerDir + 1,
                            defaultDigitsPerZipFileName + 1,
                            defaultDigitsPerZipFileContents + 1));
        }

        /**
         * This test aims to verify that the constructor of
         * {@link FilesHistoricConfig} does not throw an exception when all
         * inputs are valid and default values are used.
         */
        @Test
        @DisplayName("Test that constructor does not throw an exception with default values")
        void testValidConstructorWithDefaults() {
            assertThatNoException()
                    .isThrownBy(() -> new FilesHistoricConfig(
                            defaultRootPath,
                            defaultCompression,
                            defaultDigitsPerDir,
                            defaultDigitsPerZipFileName,
                            defaultDigitsPerZipFileContents));
        }

        /**
         * This test aims to verify that the constructor of
         * {@link FilesHistoricConfig} does not create any paths or directories.
         */
        @Test
        @DisplayName("Test that constructor does not create any paths or directories")
        void testNoPathCreation() {
            assertThat(defaultRootPath).doesNotExist();
            new FilesHistoricConfig(
                    defaultRootPath,
                    defaultCompression,
                    defaultDigitsPerDir,
                    defaultDigitsPerZipFileName,
                    defaultDigitsPerZipFileContents);
            assertThat(defaultRootPath).doesNotExist();
        }
    }

    /**
     * Stream of valid digitsPerDir values.
     */
    private static Stream<Arguments> validDigitsPerDir() {
        return Stream.of(
                Arguments.of(1), Arguments.of(2), Arguments.of(3), Arguments.of(4), Arguments.of(5), Arguments.of(6));
    }

    /**
     * Stream of invalid digitsPerDir values.
     */
    private static Stream<Arguments> invalidDigitsPerDir() {
        return Stream.of(
                Arguments.of(Integer.MAX_VALUE),
                Arguments.of(7),
                Arguments.of(0),
                Arguments.of(-1),
                Arguments.of(Integer.MIN_VALUE));
    }

    /**
     * Stream of valid digitsPerZipFileName values.
     */
    private static Stream<Arguments> invalidDigitsPerZipFileName() {
        return Stream.of(
                Arguments.of(Integer.MAX_VALUE),
                Arguments.of(11),
                Arguments.of(0),
                Arguments.of(-1),
                Arguments.of(Integer.MIN_VALUE));
    }

    /**
     * Stream of valid digitsPerZipFileName values.
     */
    private static Stream<Arguments> validDigitsPerZipFileName() {
        return Stream.of(
                Arguments.of(1),
                Arguments.of(2),
                Arguments.of(3),
                Arguments.of(4),
                Arguments.of(5),
                Arguments.of(6),
                Arguments.of(7),
                Arguments.of(8),
                Arguments.of(9),
                Arguments.of(10));
    }

    /**
     * Stream of valid digitsPerZipFileContents values.
     */
    private static Stream<Arguments> invalidDigitsPerZipFileContents() {
        return Stream.of(
                Arguments.of(Integer.MAX_VALUE),
                Arguments.of(11),
                Arguments.of(0),
                Arguments.of(-1),
                Arguments.of(Integer.MIN_VALUE));
    }

    /**
     * Stream of valid digitsPerZipFileContents values.
     */
    private static Stream<Arguments> validDigitsPerZipFileContents() {
        return Stream.of(
                Arguments.of(1),
                Arguments.of(2),
                Arguments.of(3),
                Arguments.of(4),
                Arguments.of(5),
                Arguments.of(6),
                Arguments.of(7),
                Arguments.of(8),
                Arguments.of(9),
                Arguments.of(10));
    }
}
