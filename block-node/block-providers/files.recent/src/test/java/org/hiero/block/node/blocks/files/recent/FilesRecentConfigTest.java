// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.recent;

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
 * Test for {@link FilesRecentConfig}.
 */
@DisplayName("FilesRecentConfig Tests")
class FilesRecentConfigTest {
    private FileSystem jimfs;
    /** Default live root path value. */
    private Path defaultLiveRootPath;
    /** Default compression value. */
    private CompressionType defaultCompression;
    /** Default max files per dir value. */
    private int defaultMaxFilesPerDir;
    /** Default retention policy threshold value. */
    private long defaultRetentionPolicyThreshold;

    /**
     * Set up the test environment before each test.
     */
    @BeforeEach
    void setup() {
        // initialize expected defaults
        // do not create any directories, config classes are only value holders
        // and should be void of any logic
        jimfs = Jimfs.newFileSystem(Configuration.unix());
        defaultLiveRootPath = jimfs.getPath("/opt/hiero/blocknode/data/live");
        defaultCompression = CompressionType.ZSTD;
        defaultMaxFilesPerDir = 3;
        defaultRetentionPolicyThreshold = 96_000L;
    }

    /**
     * Tear down the test environment after each test.
     */
    @AfterEach
    void tearDown() throws IOException {
        // Close the Jimfs file system
        if (jimfs != null) {
            jimfs.close();
        }
    }

    /**
     * Tests for the {@link FilesRecentConfig} constructor.
     */
    @Nested
    @DisplayName("Constructor Tests")
    final class ConstructorTests {
        /**
         * This test asserts that a {@link NullPointerException} is thrown when
         * the input liveRootPath is null.
         */
        @Test
        @DisplayName("Test that NullPointerException is thrown when liveRootPath is null")
        void testNullLiveRootPath() {
            // call && assert
            assertThatNullPointerException()
                    .isThrownBy(() -> new FilesRecentConfig(
                            null, defaultCompression, defaultMaxFilesPerDir, defaultRetentionPolicyThreshold));
        }

        /**
         * This test asserts that a {@link NullPointerException} is thrown when
         * the input compression is null.
         */
        @Test
        @DisplayName("Test that NullPointerException is thrown when compression is null")
        void testNullCompression() {
            // call && assert
            assertThatNullPointerException()
                    .isThrownBy(() -> new FilesRecentConfig(
                            defaultLiveRootPath, null, defaultMaxFilesPerDir, defaultRetentionPolicyThreshold));
        }

        /**
         * This test asserts that a {@link IllegalArgumentException} is thrown
         * when the input maxFilesPerDir is negative.
         */
        @ParameterizedTest
        @MethodSource("org.hiero.block.node.blocks.files.recent.FilesRecentConfigTest#invalidMaxFilesPerDir")
        @DisplayName("Test that IllegalArgumentException is thrown when maxFilesPerDir is negative")
        void testNegativeMaxFilesPerDir(final int invalidMaxFilesPerDir) {
            // call && assert
            assertThatIllegalArgumentException()
                    .isThrownBy(() -> new FilesRecentConfig(
                            defaultLiveRootPath,
                            defaultCompression,
                            invalidMaxFilesPerDir,
                            defaultRetentionPolicyThreshold));
        }

        /**
         * This test asserts that the constructor does not throw an exception
         * when all inputs are valid.
         */
        @Test
        @DisplayName("Test that constructor does not throw an exception when all inputs are valid")
        void testValidConstructor() {
            // call && assert
            assertThatNoException()
                    .isThrownBy(() -> new FilesRecentConfig(
                            defaultLiveRootPath.resolve("valid"),
                            CompressionType.NONE,
                            defaultMaxFilesPerDir + 1,
                            defaultRetentionPolicyThreshold));
        }

        /**
         * This test asserts that the constructor does not throw an exception
         * with default values.
         */
        @Test
        @DisplayName("Test that constructor does not throw an exception with default values")
        void testValidConstructorWithDefaults() {
            // call && assert
            assertThatNoException()
                    .isThrownBy(() -> new FilesRecentConfig(
                            defaultLiveRootPath,
                            defaultCompression,
                            defaultMaxFilesPerDir,
                            defaultRetentionPolicyThreshold));
        }

        /**
         * This test asserts that the constructor will not create any paths
         * or directories.
         */
        @Test
        @DisplayName("Test that constructor does not create any paths or directories")
        void testNoPathCreation() {
            // assert that no paths exist before the constructor is called
            assertThat(defaultLiveRootPath).doesNotExist();
            // call
            new FilesRecentConfig(
                    defaultLiveRootPath, defaultCompression, defaultMaxFilesPerDir, defaultRetentionPolicyThreshold);
            // assert that no paths exist after the constructor is called
            assertThat(defaultLiveRootPath).doesNotExist();
        }
    }

    /**
     * A stream of invalid maxFilesPerDir values.
     */
    private static Stream<Arguments> invalidMaxFilesPerDir() {
        return Stream.of(
                Arguments.of(-1),
                Arguments.of(-2),
                Arguments.of(-3),
                Arguments.of(-4),
                Arguments.of(-5),
                Arguments.of(-10),
                Arguments.of(-20),
                Arguments.of(-50),
                Arguments.of(-100),
                Arguments.of(-1_000),
                Arguments.of(-10_000),
                Arguments.of(-100_000),
                Arguments.of(-1_000_000),
                Arguments.of(-10_000_000),
                Arguments.of(-100_000_000),
                Arguments.of(-1_000_000_000),
                Arguments.of(Integer.MIN_VALUE));
    }
}
