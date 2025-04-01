// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.recent;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.LongConsumer;
import org.hiero.block.node.base.CompressionType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Test class for {@link UnverifiedHandler}.
 */
class UnverifiedHandlerTest {
    /**
     * A no-op long consumer for testing purposes.
     */
    private static final LongConsumer NOOP_LONG_CONSUMER = blockNumber -> {
        // This is a no-op for the test
    };
    /**
     * Test configuration for the files recent plugin.
     */
    private FilesRecentConfig config;

    /**
     * Environment setup before each test.
     */
    @BeforeEach
    void setUp() throws Exception {
        // Create a temporary directory using Jimfs
        final Path tempDir = Jimfs.newFileSystem(Configuration.unix()).getPath("/tmp");
        // Create the directory if it doesn't exist
        if (Files.notExists(tempDir)) {
            Files.createDirectories(tempDir);
        }
        // Create the live and unverified directories
        final Path livePath = tempDir.resolve("live");
        final Path unverifiedPath = tempDir.resolve("unverified");
        if (Files.notExists(livePath)) {
            Files.createDirectories(livePath);
        }
        if (Files.notExists(unverifiedPath)) {
            Files.createDirectories(unverifiedPath);
        }
        // Initialize the configuration with the temporary paths
        config = new FilesRecentConfig(livePath, unverifiedPath, CompressionType.NONE, 3);
    }

    /**
     * Tests for the constructor of {@link UnverifiedHandler}.
     */
    @Nested
    final class ConstructorTests {
        /**
         * This test aims to
         */
        @Test
        @DisplayName("Test constructor no exception when non-null")
        void testNonNull() {
            assertThatNoException().isThrownBy(() -> new UnverifiedHandler(config, NOOP_LONG_CONSUMER));
        }

        /**
         * This test aims to check that the constructor throws a
         * {@link NullPointerException} when the config is null.
         */
        @Test
        @DisplayName("Test constructor throws NullPointerException when config is null")
        void testNullConfig() {
            assertThatNullPointerException().isThrownBy(() -> new UnverifiedHandler(null, NOOP_LONG_CONSUMER));
        }

        /**
         * This test aims to check that the constructor throws a
         * {@link NullPointerException} when the consumer is null.
         */
        @Test
        @DisplayName("Test constructor throws NullPointerException when consumer is null")
        void testNullConsumer() {
            assertThatNullPointerException().isThrownBy(() -> new UnverifiedHandler(config, null));
        }
    }
}
