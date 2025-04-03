// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.recent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.LongConsumer;
import java.util.stream.Stream;
import org.hiero.block.node.base.CompressionType;
import org.hiero.hapi.block.node.BlockItemUnparsed;
import org.junit.jupiter.api.AfterEach;
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
    /** Temp directory root, to be resolved using jimfs. */
    private static final String TMP_ROOT = "/tmp";
    /** Live directory, to be resolved under the temp root. */
    private static final String LIVE_DIR = "live";
    /** Unverified directory, to be resolved under the temp root. */
    private static final String UNVERIFIED_DIR = "unverified";
    /** Unverified root, to be resolved using jimfs. */
    private static final String UNVERIFIED_ROOT = TMP_ROOT.concat("/").concat(UNVERIFIED_DIR);
    /** Mocked long consumer for testing purposes. */
    @Mock
    private LongConsumer longConsumerMock;
    /** Jimfs file system for creating temporary directories during tests. */
    private FileSystem jimfs;
    /** Test configuration for the files recent plugin. */
    private FilesRecentConfig config;

    /**
     * Environment setup before each test.
     */
    @BeforeEach
    void setUp() throws Exception {
        // Create a temporary directory using Jimfs
        jimfs = Jimfs.newFileSystem(Configuration.unix());
        final Path tempDir = jimfs.getPath(TMP_ROOT);
        Files.createDirectories(tempDir);
        // Create the live and unverified directories
        final Path livePath = tempDir.resolve(LIVE_DIR);
        final Path unverifiedPath = tempDir.resolve(UNVERIFIED_ROOT);
        Files.createDirectories(livePath);
        Files.createDirectories(unverifiedPath);
        // Initialize the configuration with the temporary paths
        config = new FilesRecentConfig(livePath, unverifiedPath, CompressionType.NONE, 3);
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
     * Tests for the constructor of {@link UnverifiedHandler}.
     */
    @SuppressWarnings("all")
    @Nested
    @DisplayName("Constructor Tests")
    final class ConstructorTests {
        /**
         * This test aims to
         */
        @Test
        @DisplayName("Test constructor no exception when non-null")
        void testNonNull() {
            // call && assert
            assertThatNoException().isThrownBy(() -> new UnverifiedHandler(config, longConsumerMock));
        }

        /**
         * This test aims to check that the constructor throws a
         * {@link NullPointerException} when the config is null.
         */
        @Test
        @DisplayName("Test constructor throws NullPointerException when config is null")
        void testNullConfig() {
            // call && assert
            assertThatNullPointerException().isThrownBy(() -> new UnverifiedHandler(null, longConsumerMock));
        }

        /**
         * This test aims to check that the constructor throws a
         * {@link NullPointerException} when the consumer is null.
         */
        @Test
        @DisplayName("Test constructor throws NullPointerException when consumer is null")
        void testNullConsumer() {
            // call && assert
            assertThatNullPointerException().isThrownBy(() -> new UnverifiedHandler(config, null));
        }
    }

    /**
     * Tests for the functionality of {@link UnverifiedHandler}.
     */
    @Nested
    @DisplayName("Test Functionality")
    final class FunctionalityTests {
        /** The {@link UnverifiedHandler} instance to be tested. */
        private UnverifiedHandler toTest;

        /**
         * Environment setup before each functionality test.
         */
        @BeforeEach
        void setUp() {
            // create a new UnverifiedHandler instance for each test
            toTest = new UnverifiedHandler(config, longConsumerMock);
        }

        /**
         * This test aims to check that the blockVerified method throws an
         * {@link IllegalStateException} when block is verified, but not found
         * under the unverified root.
         */
        @ParameterizedTest
        @MethodSource("org.hiero.block.node.blocks.files.recent.UnverifiedHandlerTest#validBlockNumbersAndLocations")
        @DisplayName("Test blockVerified throws when block is verified but not found under unverified storage")
        void testThrowIllegalState(final long blockNumber) {
            // resolve the expected error message
            final String expectedErrorMessage =
                    "Block %d is verified but not found in unverified storage".formatted(blockNumber);
            // call && assert
            assertThatIllegalStateException()
                    .isThrownBy(() -> toTest.blockVerified(blockNumber))
                    .withMessageContaining(expectedErrorMessage);
        }

        /**
         * This test aims to verify that the blockVerified method correctly
         * moves a block to live storage when the block is verified. For now,
         * we utilize a consumer which is called back, so we can only correctly
         * verify that it is called. Depending on future changes to the
         * implementation under test, we may need to assert that the block is
         * actually moved (assert actual IO operations) or if the handler will
         * no longer do any IO, we need to assert that it correctly follows the
         * state of persisted/verified pings.
         */
        @ParameterizedTest
        @MethodSource("org.hiero.block.node.blocks.files.recent.UnverifiedHandlerTest#validBlockNumbersAndLocations")
        @DisplayName("Test blockVerified correctly moves the block to live storage")
        void testMoveToLive(final long blockNumber, final String unverifiedPath) {
            // resolve the target block path
            final Path targetUnverifiedBlockPath = jimfs.getPath(unverifiedPath);
            // create the target block items, for this test, we need no items
            final List<BlockItemUnparsed> targetBlockItems = List.of();
            // store the block
            toTest.storeIfUnverifiedBlock(targetBlockItems, blockNumber);
            // assert that the target block path exists before call
            assertThat(targetUnverifiedBlockPath)
                    .exists()
                    .isReadable()
                    .isWritable()
                    .isRegularFile();
            // call
            toTest.blockVerified(blockNumber);
            // assert that the target block path does not exist after call
            // currently, we have a mocked consumer which we only need to verify
            // that it is called once, but if implementation changes we need to
            // assert either actual IO operations happen or if the handler will
            // no longer do any IO, then we need to assert that it correctly
            // follows the state of persisted/verified pings
            verify(longConsumerMock, times(1)).accept(blockNumber);
        }

        /**
         * This test aims to check that the storeIfUnverifiedBlock returns true
         * when the block is stored successfully.
         */
        @ParameterizedTest
        @MethodSource("org.hiero.block.node.blocks.files.recent.UnverifiedHandlerTest#validBlockNumbersAndLocations")
        @DisplayName("Test storeIfUnverifiedBlock correctly returns true when block is stored")
        void testReturnTrue(final long blockNumber, final String unverifiedPath) {
            // resolve the target block path
            final Path targetBlockPath = jimfs.getPath(unverifiedPath);
            // create the target block items, for this test, we need no items
            final List<BlockItemUnparsed> targetBlockItems = List.of();
            // assert that the target block path does not exist before call
            assertThat(targetBlockPath).doesNotExist();
            // call
            final boolean actual = toTest.storeIfUnverifiedBlock(targetBlockItems, blockNumber);
            // assert true
            assertThat(actual).isTrue();
        }

        /**
         * This test aims to check that the storeIfUnverifiedBlock correctly
         * overwrites the target block file when it already exists.
         */
        @ParameterizedTest
        @MethodSource("org.hiero.block.node.blocks.files.recent.UnverifiedHandlerTest#validBlockNumbersAndLocations")
        @DisplayName("Test storeIfUnverifiedBlock correctly overwrites a stored unverified block file")
        void testBlockFileOverwrite(final long blockNumber, final String unverifiedPath) throws IOException {
            // resolve the target block path
            final Path targetBlockPath = jimfs.getPath(unverifiedPath);
            // create the target block items, for this test, we need no items
            final List<BlockItemUnparsed> targetBlockItems = List.of();
            // write some bytes to the target block path to be overwritten
            final byte[] bytesToOverwrite = "bytes to overwrite".getBytes();
            Files.write(targetBlockPath, bytesToOverwrite);
            // assert target path exists and contains the bytes to be overwritten
            assertThat(targetBlockPath)
                    .exists()
                    .isReadable()
                    .isWritable()
                    .isRegularFile()
                    .binaryContent()
                    .isEqualTo(bytesToOverwrite);
            // call
            toTest.storeIfUnverifiedBlock(targetBlockItems, blockNumber);
            // assert that the target block path's content is overwritten
            assertThat(targetBlockPath)
                    .exists()
                    .isReadable()
                    .isWritable()
                    .isRegularFile()
                    .binaryContent()
                    .isNotEqualTo(bytesToOverwrite);
        }

        /**
         * This test aims to check that the storeIfUnverifiedBlock correctly
         * creates the target block file when invoked.
         */
        @ParameterizedTest
        @MethodSource("org.hiero.block.node.blocks.files.recent.UnverifiedHandlerTest#validBlockNumbersAndLocations")
        @DisplayName("Test storeIfUnverifiedBlock correctly creates the target block file")
        void testBlockFileCreate(final long blockNumber, final String unverifiedPath) {
            // resolve the target block path
            final Path targetBlockPath = jimfs.getPath(unverifiedPath);
            // create the target block items, for this test, we need no items
            final List<BlockItemUnparsed> targetBlockItems = List.of();
            // assert that the target block path does not exist before call
            assertThat(targetBlockPath).doesNotExist();
            // call
            toTest.storeIfUnverifiedBlock(targetBlockItems, blockNumber);
            // assert that the target block path exists after call
            assertThat(targetBlockPath).exists().isReadable().isWritable().isRegularFile();
        }
    }

    /**
     * A stream of valid block number arguments for parameterized tests. Also
     * are provided paths to where the blocks would be stored under the
     * unverified root.
     */
    private static Stream<Arguments> validBlockNumbersAndLocations() {
        return Stream.of(
                Arguments.of(0L, UNVERIFIED_ROOT.concat("/0000000000000000000.blk")),
                Arguments.of(1L, UNVERIFIED_ROOT.concat("/0000000000000000001.blk")),
                Arguments.of(2L, UNVERIFIED_ROOT.concat("/0000000000000000002.blk")),
                Arguments.of(3L, UNVERIFIED_ROOT.concat("/0000000000000000003.blk")),
                Arguments.of(4L, UNVERIFIED_ROOT.concat("/0000000000000000004.blk")),
                Arguments.of(5L, UNVERIFIED_ROOT.concat("/0000000000000000005.blk")),
                Arguments.of(10L, UNVERIFIED_ROOT.concat("/0000000000000000010.blk")),
                Arguments.of(20L, UNVERIFIED_ROOT.concat("/0000000000000000020.blk")),
                Arguments.of(50L, UNVERIFIED_ROOT.concat("/0000000000000000050.blk")),
                Arguments.of(100L, UNVERIFIED_ROOT.concat("/0000000000000000100.blk")),
                Arguments.of(1_000L, UNVERIFIED_ROOT.concat("/0000000000000001000.blk")),
                Arguments.of(10_000L, UNVERIFIED_ROOT.concat("/0000000000000010000.blk")),
                Arguments.of(100_000L, UNVERIFIED_ROOT.concat("/0000000000000100000.blk")),
                Arguments.of(1_000_000L, UNVERIFIED_ROOT.concat("/0000000000001000000.blk")),
                Arguments.of(10_000_000L, UNVERIFIED_ROOT.concat("/0000000000010000000.blk")),
                Arguments.of(100_000_000L, UNVERIFIED_ROOT.concat("/0000000000100000000.blk")),
                Arguments.of(1_000_000_000L, UNVERIFIED_ROOT.concat("/0000000001000000000.blk")),
                Arguments.of(10_000_000_000L, UNVERIFIED_ROOT.concat("/0000000010000000000.blk")),
                Arguments.of(100_000_000_000L, UNVERIFIED_ROOT.concat("/0000000100000000000.blk")),
                Arguments.of(1_000_000_000_000L, UNVERIFIED_ROOT.concat("/0000001000000000000.blk")),
                Arguments.of(10_000_000_000_000L, UNVERIFIED_ROOT.concat("/0000010000000000000.blk")),
                Arguments.of(100_000_000_000_000L, UNVERIFIED_ROOT.concat("/0000100000000000000.blk")),
                Arguments.of(1_000_000_000_000_000L, UNVERIFIED_ROOT.concat("/0001000000000000000.blk")),
                Arguments.of(10_000_000_000_000_000L, UNVERIFIED_ROOT.concat("/0010000000000000000.blk")),
                Arguments.of(100_000_000_000_000_000L, UNVERIFIED_ROOT.concat("/0100000000000000000.blk")),
                Arguments.of(1_000_000_000_000_000_000L, UNVERIFIED_ROOT.concat("/1000000000000000000.blk")),
                Arguments.of(Long.MAX_VALUE, UNVERIFIED_ROOT.concat("/9223372036854775807.blk")));
    }
}
