// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.recent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DecimalFormat;
import java.util.List;
import java.util.function.LongConsumer;
import java.util.stream.Stream;
import org.hiero.block.node.base.CompressionType;
import org.hiero.hapi.block.node.BlockItemUnparsed;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Test class for {@link UnverifiedHandler}.
 */
@DisplayName("UnverifiedHandler Tests")
@ExtendWith(MockitoExtension.class)
class UnverifiedHandlerTest {
    /**
     * Format for long leading digits.
     */
    private static final DecimalFormat LONG_LEADING_DIGITS_FORMAT = new DecimalFormat("0000000000000000000");
    /**
     * Mocked long consumer for testing purposes.
     */
    @Mock
    private LongConsumer longConsumerMock;
    /**
     * Test configuration for the files recent plugin.
     */
    private FilesRecentConfig config;

    /**
     * Environment setup before each test.
     */
    @SuppressWarnings("resource")
    @BeforeEach
    void setUp() throws Exception {
        // Create a temporary directory using Jimfs
        final Path tempDir = Jimfs.newFileSystem(Configuration.unix()).getPath("/tmp");
        Files.createDirectories(tempDir);
        // Create the live and unverified directories
        final Path livePath = tempDir.resolve("live");
        final Path unverifiedPath = tempDir.resolve("unverified");
        Files.createDirectories(livePath);
        Files.createDirectories(unverifiedPath);
        // Initialize the configuration with the temporary paths
        config = new FilesRecentConfig(livePath, unverifiedPath, CompressionType.NONE, 3);
    }

    /**
     * Tests for the constructor of {@link UnverifiedHandler}.
     */
    @Nested
    @DisplayName("Constructor Tests")
    final class ConstructorTests {
        /**
         * This test aims to
         */
        @Test
        @DisplayName("Test constructor no exception when non-null")
        void testNonNull() {
            assertThatNoException().isThrownBy(() -> new UnverifiedHandler(config, longConsumerMock));
        }

        /**
         * This test aims to check that the constructor throws a
         * {@link NullPointerException} when the config is null.
         */
        @Test
        @DisplayName("Test constructor throws NullPointerException when config is null")
        void testNullConfig() {
            assertThatNullPointerException().isThrownBy(() -> new UnverifiedHandler(null, longConsumerMock));
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

    /**
     * Tests for the functionality of {@link UnverifiedHandler}.
     */
    @Nested
    @DisplayName("Test Functionality")
    final class FunctionalityTests {
        private UnverifiedHandler toTest;

        /**
         * Environment setup before each functionality test.
         */
        @BeforeEach
        void setUp() {
            toTest = new UnverifiedHandler(config, longConsumerMock);
        }

        /**
         * This test aims to check that the blockVerified method throws an
         * {@link IllegalStateException} when block is verified, but not found
         * under the unverified root.
         */
        @Test
        @DisplayName("Test blockVerified throws when block is verified but not found under unverified storage")
        void testThrowIllegalState() {
            final long targetBlockNumber = 1L;
            final String expectedErrorMessage =
                    "Block %d is verified but not found in unverified storage".formatted(targetBlockNumber);
            assertThatIllegalStateException()
                    .isThrownBy(() -> toTest.blockVerified(targetBlockNumber))
                    .withMessageContaining(expectedErrorMessage);
        }

        /**
         * This test aims to check that the storeIfUnverifiedBlock correctly
         * creates the target block file when invoked.
         */
        @ParameterizedTest
        @MethodSource("org.hiero.block.node.blocks.files.recent.UnverifiedHandlerTest#validBlockNumbers")
        @DisplayName("Test storeIfUnverifiedBlock correctly creates the target block file")
        void test(final long blockNumber) {
            final Path targetBlockPath = config.unverifiedRootPath()
                    .resolve(LONG_LEADING_DIGITS_FORMAT.format(blockNumber).concat(".blk"));
            final List<BlockItemUnparsed> targetBlockItems = List.of();
            assertThat(targetBlockPath).doesNotExist();
            toTest.storeIfUnverifiedBlock(targetBlockItems, blockNumber);
            assertThat(targetBlockPath).exists().isReadable().isWritable().isRegularFile();
        }
    }

    /**
     * A stream of valid block number arguments for parameterized tests.
     */
    private static Stream<Arguments> validBlockNumbers() {
        return Stream.of(
                Arguments.of(0L),
                Arguments.of(1L),
                Arguments.of(2L),
                Arguments.of(3L),
                Arguments.of(4L),
                Arguments.of(5L),
                Arguments.of(10L),
                Arguments.of(20L),
                Arguments.of(50L),
                Arguments.of(100L),
                Arguments.of(1_000L),
                Arguments.of(10_000L),
                Arguments.of(100_000L),
                Arguments.of(1_000_000L),
                Arguments.of(10_000_000L),
                Arguments.of(100_000_000L),
                Arguments.of(1_000_000_000L),
                Arguments.of(10_000_000_000L),
                Arguments.of(100_000_000_000L),
                Arguments.of(1_000_000_000_000L),
                Arguments.of(10_000_000_000_000L),
                Arguments.of(100_000_000_000_000L),
                Arguments.of(1_000_000_000_000_000L),
                Arguments.of(10_000_000_000_000_000L),
                Arguments.of(100_000_000_000_000_000L),
                Arguments.of(1_000_000_000_000_000_000L),
                Arguments.of(Long.MAX_VALUE));
    }
}
