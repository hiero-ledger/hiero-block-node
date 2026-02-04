// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.recent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hiero.block.node.spi.BlockNodePlugin.UNKNOWN_BLOCK_NUMBER;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.hedera.hapi.block.stream.Block;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.blocks.TestBlock;
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
import org.hiero.block.node.app.fixtures.plugintest.PluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.base.BlockFile;
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Complete plugin test for the {@link BlockFileRecentPlugin} plugin.
 */
class BlockFileRecentPluginTest {
    /** The testing file system. */
    private final FileSystem fileSystem;
    /** The path to the live root directory in the testing file system. */
    private final Path blocksRootPath;
    /** The path to the links root directory in the testing file system. */
    private final Path linksRootPath;
    /** The plugin configuration, customized with testing file system. */
    private final FilesRecentConfig filesRecentConfig;
    /** The plugin under test. */
    private final BlockFileRecentPlugin blockFileRecentPlugin;
    /** The historical block facility. */
    private final SimpleInMemoryHistoricalBlockFacility historicalBlockFacility;

    /**
     * Construct test environment.
     */
    BlockFileRecentPluginTest() {
        this.fileSystem = Jimfs.newFileSystem(Configuration.unix());
        this.blocksRootPath = fileSystem.getPath("/live");
        this.linksRootPath = blocksRootPath.resolve("links");
        this.filesRecentConfig = new FilesRecentConfig(blocksRootPath, CompressionType.ZSTD, 3, 100);
        this.blockFileRecentPlugin = new BlockFileRecentPlugin(this.filesRecentConfig);
        this.historicalBlockFacility = new SimpleInMemoryHistoricalBlockFacility();
    }

    /**
     * Nested class for plugin startup tests.
     */
    @Nested
    @DisplayName("Startup Tests")
    final class StartupTest extends PluginTestBase<BlockFileRecentPlugin, BlockingExecutor, ScheduledExecutorService> {
        /**
         * Test Constructor.
         */
        StartupTest() {
            super(
                    new BlockingExecutor(new LinkedBlockingQueue<>()),
                    new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
        }

        /**
         * This test aims to assert that the links and data root directories are
         * created on plugin startup.
         */
        @Test
        @DisplayName("Links and data roots are created on startup")
        void testLinksAndDataRootsCreated() throws IOException {
            // Assert that the roots do not exist
            assertThat(linksRootPath).doesNotExist();
            assertThat(blocksRootPath).doesNotExist();
            // Now start the plugin
            start(blockFileRecentPlugin, historicalBlockFacility);
            // Assert that the roots are created
            assertThat(linksRootPath).exists().isDirectory().isEmptyDirectory();
            assertThat(blocksRootPath).exists().isDirectory().isNotEmptyDirectory();
            assertThat(Files.list(blocksRootPath).toList()).hasSize(1).containsExactly(linksRootPath);
        }

        /**
         * This test aims to assert that the links root directory is cleared on
         * plugin startup. The directory exists and is empty.
         */
        @Test
        @DisplayName("Links root is cleared on startup")
        void testLinksRootEmptyOnStartup() throws IOException {
            // First we need to create and populate the links directory
            Files.createDirectories(linksRootPath);
            Files.createFile(linksRootPath.resolve("file1"));
            Files.createFile(linksRootPath.resolve("file2"));
            Files.createFile(linksRootPath.resolve("file3"));
            final Path subDir = linksRootPath.resolve("subdir");
            Files.createDirectories(subDir);
            Files.createFile(subDir.resolve("file4"));
            Files.createFile(subDir.resolve("file5"));
            final Path link1 = subDir.resolve("file6");
            Files.createFile(link1);
            Files.createLink(subDir.resolve("link1"), link1);
            Files.createSymbolicLink(subDir.resolve("symlink1"), link1);
            // Assert that the links root directory is not empty
            assertThat(linksRootPath).isDirectory().isNotEmptyDirectory();
            // Now start the plugin
            start(blockFileRecentPlugin, historicalBlockFacility);
            // Assert that the links root directory exists and is empty
            assertThat(linksRootPath).isDirectory().isEmptyDirectory();
        }
    }

    /**
     * Nested class for testing the plugin, it is a nested class so environment can be built outside.
     */
    @Nested
    @DisplayName("Plugin Tests")
    final class PluginTest extends PluginTestBase<BlockFileRecentPlugin, BlockingExecutor, ScheduledExecutorService> {

        /**
         * Test Constructor.
         */
        PluginTest() {
            super(
                    new BlockingExecutor(new LinkedBlockingQueue<>()),
                    new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
            start(blockFileRecentPlugin, historicalBlockFacility);
        }

        /**
         * Test that the config change to use JimFS works.
         */
        @Test
        @DisplayName("Test that we are using JimFS")
        void testWeAreUsingMockFileSystem() {
            assertEquals(
                    "JimfsFileSystem",
                    filesRecentConfig.liveRootPath().getFileSystem().getClass().getSimpleName());
            assertEquals("JimfsFileSystem", fileSystem.getClass().getSimpleName());
        }

        /**
         * Test that the plugin works to store and retrieve a block.
         */
        @SuppressWarnings("DataFlowIssue")
        @Test
        @DisplayName("Test send/retrieve block by persistence first")
        void testSendingBlockAndReadingBack() {
            // create sample block of block items
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0);
            final long blockNumber = block.number();
            // check the block is not stored yet
            assertNull(plugin.block(blockNumber));
            assertEquals(UNKNOWN_BLOCK_NUMBER, plugin.availableBlocks().max());
            assertEquals(UNKNOWN_BLOCK_NUMBER, plugin.availableBlocks().min());
            // check if we try to read we get null as nothing is verified yet
            assertNull(plugin.block(blockNumber));
            assertEquals(UNKNOWN_BLOCK_NUMBER, plugin.availableBlocks().max());
            assertEquals(UNKNOWN_BLOCK_NUMBER, plugin.availableBlocks().min());
            // send verified block notification
            blockMessaging.sendBlockVerification(new VerificationNotification(
                    true, blockNumber, Bytes.EMPTY, block.blockUnparsed(), BlockSource.PUBLISHER));
            // now try and read it back
            final BlockUnparsed blockFromPlugin = plugin.block(blockNumber).blockUnparsed();
            // check we got the correct block
            assertArrayEquals(
                    block.asBlockItemUnparsedArray(),
                    blockFromPlugin.blockItems().toArray());
            assertEquals(blockNumber, plugin.availableBlocks().max());
            assertEquals(blockNumber, plugin.availableBlocks().min());
        }

        /**
         * Test that the plugin does not store a block if verification has
         * failed.
         */
        @SuppressWarnings("DataFlowIssue")
        @Test
        @DisplayName("Test send/retrieve failed verification")
        void testSendingBlockAndReadingBackFailedVerification() {
            // create sample block of block items
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0);
            final long blockNumber = block.number();
            // check the block is not stored yet
            assertNull(plugin.block(blockNumber));
            assertEquals(UNKNOWN_BLOCK_NUMBER, plugin.availableBlocks().max());
            assertEquals(UNKNOWN_BLOCK_NUMBER, plugin.availableBlocks().min());
            // check if we try to read we get null as nothing is verified yet
            assertNull(plugin.block(blockNumber));
            assertEquals(UNKNOWN_BLOCK_NUMBER, plugin.availableBlocks().max());
            assertEquals(UNKNOWN_BLOCK_NUMBER, plugin.availableBlocks().min());
            // send verified block notification with failure
            blockMessaging.sendBlockVerification(new VerificationNotification(
                    false, blockNumber, Bytes.EMPTY, block.blockUnparsed(), BlockSource.PUBLISHER));
            assertNull(plugin.block(blockNumber));
            assertEquals(UNKNOWN_BLOCK_NUMBER, plugin.availableBlocks().max());
            assertEquals(UNKNOWN_BLOCK_NUMBER, plugin.availableBlocks().min());
        }

        /**
         * Test that the plugin works to store and retrieve a block but receive
         * verification first.
         */
        @SuppressWarnings("DataFlowIssue")
        @Test
        @DisplayName("Test send/retrieve block by verification first")
        void testSendingBlockAndReadingBackVerificationFirst() {
            // create sample block of block items
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0);
            final BlockUnparsed blockOrig = block.blockUnparsed();
            final long blockNumber = block.number();
            // check the block is not stored yet
            assertNull(plugin.block(blockNumber));
            assertEquals(UNKNOWN_BLOCK_NUMBER, plugin.availableBlocks().max());
            assertEquals(UNKNOWN_BLOCK_NUMBER, plugin.availableBlocks().min());
            // check if we try to read we get null as nothing is persisted yet
            assertNull(plugin.block(blockNumber));
            assertEquals(UNKNOWN_BLOCK_NUMBER, plugin.availableBlocks().max());
            assertEquals(UNKNOWN_BLOCK_NUMBER, plugin.availableBlocks().min());
            // send verified block notification
            blockMessaging.sendBlockVerification(
                    new VerificationNotification(true, blockNumber, Bytes.EMPTY, blockOrig, BlockSource.PUBLISHER));
            // now try and read it back
            final BlockUnparsed blockFromPlugin = plugin.block(blockNumber).blockUnparsed();
            // check we got the correct block
            assertArrayEquals(
                    blockOrig.blockItems().toArray(),
                    blockFromPlugin.blockItems().toArray());
            assertEquals(blockNumber, plugin.availableBlocks().max());
            assertEquals(blockNumber, plugin.availableBlocks().min());
        }

        /**
         * Test that the plugin's retention policy works correctly.
         */
        @Test
        @DisplayName("Test the retention policy happy path")
        void testRetentionPolicyHappyPath() {
            // pre-check that there are no blocks stored
            assertThat(plugin.availableBlocks().min()).isEqualTo(UNKNOWN_BLOCK_NUMBER);
            assertThat(plugin.availableBlocks().max()).isEqualTo(UNKNOWN_BLOCK_NUMBER);
            // create 150 blocks and then send a verification notification for them
            for (int i = 0; i < 150; i++) {
                // generate the next block
                final BlockUnparsed block =
                        TestBlockBuilder.generateBlockWithNumber(i).blockUnparsed();
                // send the block items to the plugin
                blockMessaging.sendBlockVerification(new VerificationNotification(
                        true, i, Bytes.EMPTY, new BlockUnparsed(block.blockItems()), BlockSource.PUBLISHER));
                // assert that the block is persisted
                final Path persistedBlock = BlockFile.nestedDirectoriesBlockFilePath(
                        blocksRootPath, i, filesRecentConfig.compression(), filesRecentConfig.maxFilesPerDir());
                assertThat(persistedBlock).exists();
            }
            // assert that the plugin has 100 blocks available (properly updated)
            assertThat(plugin.availableBlocks().min()).isEqualTo(50L);
            assertThat(plugin.availableBlocks().max()).isEqualTo(149L);
            // assert that the first 50 blocks are not persisted anymore
            for (int i = 0; i < 50; i++) {
                final Path persistedBlock = BlockFile.nestedDirectoriesBlockFilePath(
                        blocksRootPath, i, filesRecentConfig.compression(), filesRecentConfig.maxFilesPerDir());
                assertThat(persistedBlock).doesNotExist();
            }
            // assert that the last 100 blocks are persisted
            for (int i = 50; i < 150; i++) {
                final Path persistedBlock = BlockFile.nestedDirectoriesBlockFilePath(
                        blocksRootPath, i, filesRecentConfig.compression(), filesRecentConfig.maxFilesPerDir());
                assertThat(persistedBlock).exists();
            }
        }

        /**
         * Test that the plugin's retention policy does not apply when disabled.
         */
        @Test
        @DisplayName("Test the retention policy does not apply when disabled")
        void testRetentionPolicyDisabled() {
            // override the plugin under test to disable the retention policy
            final FilesRecentConfig filesRecentConfigOverride =
                    new FilesRecentConfig(blocksRootPath, CompressionType.ZSTD, 3, 0L);
            final BlockFileRecentPlugin toTest = new BlockFileRecentPlugin(filesRecentConfigOverride);
            final HistoricalBlockFacility localHistoricalBlockFacility = new SimpleInMemoryHistoricalBlockFacility();
            // unregister the original plugin from the messaging queue
            blockMessaging.unregisterBlockNotificationHandler(blockFileRecentPlugin);
            // start the plugin with the overridden config
            start(toTest, localHistoricalBlockFacility);
            // pre-check that there are no blocks stored
            assertThat(toTest.availableBlocks().min()).isEqualTo(UNKNOWN_BLOCK_NUMBER);
            assertThat(toTest.availableBlocks().max()).isEqualTo(UNKNOWN_BLOCK_NUMBER);
            // create 150 blocks and then send a verification notification for them
            for (int i = 0; i < 150; i++) {
                // generate the next block
                final BlockUnparsed block =
                        TestBlockBuilder.generateBlockWithNumber(i).blockUnparsed();
                // send the block items to the plugin
                blockMessaging.sendBlockVerification(new VerificationNotification(
                        true, i, Bytes.EMPTY, new BlockUnparsed(block.blockItems()), BlockSource.PUBLISHER));
                // assert that the block is persisted
                final Path persistedBlock = BlockFile.nestedDirectoriesBlockFilePath(
                        blocksRootPath,
                        i,
                        filesRecentConfigOverride.compression(),
                        filesRecentConfigOverride.maxFilesPerDir());
                assertThat(persistedBlock).exists();
            }
            // assert that the plugin has all 150 blocks available
            assertThat(toTest.availableBlocks().min()).isEqualTo(0L);
            assertThat(toTest.availableBlocks().max()).isEqualTo(149L);
            // assert that all the blocks are persisted
            for (int i = 0; i < 150; i++) {
                final Path persistedBlock = BlockFile.nestedDirectoriesBlockFilePath(
                        blocksRootPath,
                        i,
                        filesRecentConfigOverride.compression(),
                        filesRecentConfigOverride.maxFilesPerDir());
                assertThat(persistedBlock).exists();
            }
        }

        /**
         * This test verifies that {@link BlockFileBlockAccessor#determineCompressionType} correctly
         * identifies the actual compression type by reading magic bytes from file content, regardless of
         * the file extension. This ensures that if someone adds a new CompressionType with incorrect
         * magic bytes, or if files are misnamed with wrong extensions, the system will detect the mismatch.
         */
        @Test
        @DisplayName("Test compression type is determined regardless of extension and config")
        void testCompressionTypeIsDetermined() throws IOException {
            final CompressionType[] compressionTypes = CompressionType.values();

            // Create sample block content to compress
            final Block block = new Block(List.of(TestBlockBuilder.sampleHeader(0)));
            final Bytes protoBytes = Block.PROTOBUF.toBytes(block);

            // Create blocks with all combinations of actual compression (i) vs file extension (j)
            for (int compressionIdx = 0; compressionIdx < compressionTypes.length; compressionIdx++) {
                for (int extensionIdx = 0; extensionIdx < compressionTypes.length; extensionIdx++) {
                    final int blockNumber = compressionIdx * compressionTypes.length + extensionIdx;
                    // Create file path with extension suggesting compression type extensionIdx
                    final Path blockPath = BlockFile.nestedDirectoriesBlockFilePath(
                            blocksRootPath,
                            blockNumber,
                            compressionTypes[extensionIdx],
                            filesRecentConfig.maxFilesPerDir());
                    // Create parent directories
                    Files.createDirectories(blockPath.getParent());
                    // Write content compressed with compression type compressionIdx
                    final byte[] compressedData = compressionTypes[compressionIdx].compress(protoBytes.toByteArray());
                    Files.write(blockPath, compressedData);
                }
            }

            // Verify that for each combination, the actual compression (compressionIdx) is correctly detected
            // by reading magic bytes, regardless of the file extension (extensionIdx)
            for (int compressionIdx = 0; compressionIdx < compressionTypes.length; compressionIdx++) {
                for (int extensionIdx = 0; extensionIdx < compressionTypes.length; extensionIdx++) {
                    final int blockNumber = compressionIdx * compressionTypes.length + extensionIdx;
                    // Get the block file path (with extension suggesting extensionIdx)
                    final Path blockPath = BlockFile.nestedDirectoriesBlockFilePath(
                            blocksRootPath,
                            blockNumber,
                            compressionTypes[extensionIdx],
                            filesRecentConfig.maxFilesPerDir());
                    // Create accessor which should detect the actual compression by magic bytes
                    try (BlockFileBlockAccessor accessor =
                            new BlockFileBlockAccessor(blockPath, linksRootPath, blockNumber)) {
                        // Verify that the detected compression type matches the actual compression used
                        // (compressionIdx)
                        // not the file extension (extensionIdx)
                        assertThat(accessor.compressionType())
                                .as(
                                        "Block %d: actual compression %s, extension %s",
                                        blockNumber, compressionTypes[compressionIdx], compressionTypes[extensionIdx])
                                .isEqualTo(compressionTypes[compressionIdx]);
                    }
                }
            }
        }

        /**
         * Cleanup after each test.
         */
        @AfterEach
        void close() throws IOException {
            tearDown();
            // close the file system
            fileSystem.close();
        }
    }
}
