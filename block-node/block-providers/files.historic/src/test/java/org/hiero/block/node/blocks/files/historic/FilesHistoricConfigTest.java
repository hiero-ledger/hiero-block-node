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
import java.util.stream.IntStream;
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
    private int powersOfTenPerZipFileContents;

    /**
     * Environment setup before each test.
     */
    @BeforeEach
    void setup() {
        jimfs = Jimfs.newFileSystem(Configuration.unix());
        defaultRootPath = jimfs.getPath("/opt/hashgraph/blocknode/data/historic");
        defaultCompression = CompressionType.ZSTD;
        powersOfTenPerZipFileContents = 4;
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
                    .isThrownBy(() -> new FilesHistoricConfig(null, defaultCompression, powersOfTenPerZipFileContents));
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
                    .isThrownBy(() -> new FilesHistoricConfig(defaultRootPath, null, powersOfTenPerZipFileContents));
        }

        /**
         * This test aims to verify that the constructor of
         * {@link FilesHistoricConfig} does not throw an exception when the
         * powersOfTenPerZipFileContents is in range.
         */
        @ParameterizedTest
        @MethodSource(
                "org.hiero.block.node.blocks.files.historic.FilesHistoricConfigTest#validDigitsPerZipFileContents")
        @DisplayName("Test that no exception is thrown when powersOfTenPerZipFileContents is in range")
        void testValidDigitsPerZipFileContents(final int validDigitsPerZipFileContents) {
            assertThatNoException()
                    .isThrownBy(() -> new FilesHistoricConfig(
                            defaultRootPath, defaultCompression, validDigitsPerZipFileContents));
        }

        /**
         * This test aims to verify that the constructor of
         * {@link FilesHistoricConfig} throws a {@link IllegalArgumentException}
         * if the powersOfTenPerZipFileContents is out of range.
         */
        @ParameterizedTest
        @MethodSource(
                "org.hiero.block.node.blocks.files.historic.FilesHistoricConfigTest#powersOfTenPerZipFileContents")
        @DisplayName("Test that IllegalArgumentException is thrown when powersOfTenPerZipFileContents is out of range")
        void testInvalidPowersOfTenPerZipFileContents(final int invalidPowersOfTenPerZipFileContents) {
            assertThatIllegalArgumentException()
                    .isThrownBy(() -> new FilesHistoricConfig(
                            defaultRootPath, defaultCompression, invalidPowersOfTenPerZipFileContents));
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
                            defaultRootPath.resolve("valid"), CompressionType.NONE, powersOfTenPerZipFileContents + 1));
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
                            defaultRootPath, defaultCompression, powersOfTenPerZipFileContents));
        }

        /**
         * This test aims to verify that the constructor of
         * {@link FilesHistoricConfig} does not create any paths or directories.
         */
        @Test
        @DisplayName("Test that constructor does not create any paths or directories")
        void testNoPathCreation() {
            assertThat(defaultRootPath).doesNotExist();
            new FilesHistoricConfig(defaultRootPath, defaultCompression, powersOfTenPerZipFileContents);
            assertThat(defaultRootPath).doesNotExist();
        }
    }

    /**
     * Stream of valid powersOfTenPerZipFileContents values.
     */
    private static Stream<Arguments> powersOfTenPerZipFileContents() {
        return Stream.of(
                Arguments.of(Integer.MAX_VALUE),
                Arguments.of(11),
                Arguments.of(0),
                Arguments.of(-1),
                Arguments.of(Integer.MIN_VALUE));
    }

    /**
     * Stream of valid powersOfTenPerZipFileContents values.
     */
    private static Stream<Arguments> validDigitsPerZipFileContents() {
        return IntStream.range(1, 6).mapToObj(Arguments::of);
    }
}
