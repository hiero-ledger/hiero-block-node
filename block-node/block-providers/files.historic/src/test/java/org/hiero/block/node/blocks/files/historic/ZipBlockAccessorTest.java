// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.historic;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.swirlds.config.api.ConfigurationBuilder;
import java.io.IOException;
import java.nio.file.FileSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Test class for {@link ZipBlockAccessor}.
 */
@DisplayName("ZipBlockAccessor Tests")
class ZipBlockAccessorTest {
    /** The testing in-memory file system. */
    private FileSystem jimfs;
    /** The configuration for the test. */
    private FilesHistoricConfig defaultConfig;

    /** Set up the test environment before each test. */
    @BeforeEach
    void setup() {
        // Initialize the in-memory file system
        jimfs = Jimfs.newFileSystem(Configuration.unix());
        final FilesHistoricConfig localDefaultConfig = ConfigurationBuilder.create()
                .withConfigDataType(FilesHistoricConfig.class)
                .build()
                .getConfigData(FilesHistoricConfig.class);
        // Set the default configuration for the test, use jimfs for paths
        defaultConfig = new FilesHistoricConfig(
                jimfs.getPath("/opt/hashgraph/blocknode/data/historic"),
                localDefaultConfig.compression(),
                localDefaultConfig.digitsPerDir(),
                localDefaultConfig.digitsPerZipFileName(),
                localDefaultConfig.digitsPerZipFileContents());
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
     * Tests for the {@link ZipBlockAccessor} constructor.
     */
    @Nested
    @DisplayName("Constructor Tests")
    final class ConstructorTests {
        /**
         * This test aims to assert that the constructor of
         * {@link ZipBlockAccessor} does not throw any exceptions when the input
         * is valid.
         */
        @Test
        @DisplayName("Test constructor throws no exception when input is valid")
        void testValidConstructor() {
            final BlockPath blockPath = BlockPath.computeBlockPath(defaultConfig, 1L);
            assertThatNoException().isThrownBy(() -> new ZipBlockAccessor(blockPath));
        }

        /**
         * This test aims to assert that the constructor of
         * {@link ZipBlockAccessor} throws a {@link NullPointerException} when
         * the input blockPath is null.
         */
        @Test
        @DisplayName("Test constructor throws NullPointerException when blockPath is null")
        @SuppressWarnings("all")
        void testNullBlockPath() {
            assertThatNullPointerException().isThrownBy(() -> new ZipBlockAccessor(null));
        }
    }

    /**
     * Tests for the {@link ZipBlockAccessor} functionality.
     */
    @Nested
    @DisplayName("Functionality Tests")
    final class FunctionalityTests {
        /**
         * This test aims to assert that the
         * {@link ZipBlockAccessor#delete()} method throws an
         * {@link UnsupportedOperationException} when called.
         */
        @Test
        @DisplayName("Test delete throws UnsupportedOperationException")
        void testZipBlockAccessorFunctionality() {
            // construct a valid ZipBlockAccessor
            final BlockPath blockPath = BlockPath.computeBlockPath(defaultConfig, 1L);
            final ZipBlockAccessor actual = new ZipBlockAccessor(blockPath);
            assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(actual::delete);
        }
    }
}
