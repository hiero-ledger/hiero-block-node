// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.recent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hiero.block.node.app.fixtures.blocks.BlockItemUtils.toBlockItemsUnparsed;
import static org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocks;
import static org.hiero.block.node.spi.BlockNodePlugin.UNKNOWN_BLOCK_NUMBER;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.util.List;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder;
import org.hiero.block.node.app.fixtures.plugintest.PluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.base.BlockFile;
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Complete plugin test for the {@link BlocksFilesRecentPlugin} plugin.
 */
class BlockFileRecentPluginTest {
    /** The testing file system. */
    private final FileSystem fileSystem;
    /** The path to the live root directory in the testing file system. */
    private final Path testPath;
    /** The plugin configuration, customized with testing file system. */
    private final FilesRecentConfig filesRecentConfig;
    /** The plugin under test. */
    private final BlocksFilesRecentPlugin blocksFilesRecentPlugin;
    /** The historical block facility. */
    private final SimpleInMemoryHistoricalBlockFacility historicalBlockFacility;

    /**
     * Construct test environment.
     */
    BlockFileRecentPluginTest() {
        this.fileSystem = Jimfs.newFileSystem(Configuration.unix());
        this.testPath = fileSystem.getPath("/live");
        this.filesRecentConfig = new FilesRecentConfig(testPath, CompressionType.ZSTD, 3, 100);
        this.blocksFilesRecentPlugin = new BlocksFilesRecentPlugin(this.filesRecentConfig);
        this.historicalBlockFacility = new SimpleInMemoryHistoricalBlockFacility();
    }

    /**
     * Nested class for testing the plugin, it is a nested class so environment can be built outside.
     */
    @Nested
    @DisplayName("Plugin Tests")
    final class PluginTest extends PluginTestBase<BlocksFilesRecentPlugin> {

        /**
         * Test Constructor.
         */
        PluginTest() {
            start(blocksFilesRecentPlugin, historicalBlockFacility);
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
            final BlockItem[] blockBlockItems = createNumberOfVerySimpleBlocks(1);
            final long blockNumber = blockBlockItems[0].blockHeader().number();
            // check the block is not stored yet
            assertNull(plugin.block(blockNumber));
            assertEquals(UNKNOWN_BLOCK_NUMBER, plugin.availableBlocks().max());
            assertEquals(UNKNOWN_BLOCK_NUMBER, plugin.availableBlocks().min());
            assertEquals(
                    UNKNOWN_BLOCK_NUMBER,
                    blockNodeContext.historicalBlockProvider().availableBlocks().max());
            assertEquals(
                    UNKNOWN_BLOCK_NUMBER,
                    blockNodeContext.historicalBlockProvider().availableBlocks().min());
            // check if we try to read we get null as nothing is verified yet
            assertNull(plugin.block(blockNumber));
            assertEquals(UNKNOWN_BLOCK_NUMBER, plugin.availableBlocks().max());
            assertEquals(UNKNOWN_BLOCK_NUMBER, plugin.availableBlocks().min());
            assertEquals(
                    UNKNOWN_BLOCK_NUMBER,
                    blockNodeContext.historicalBlockProvider().availableBlocks().max());
            assertEquals(
                    UNKNOWN_BLOCK_NUMBER,
                    blockNodeContext.historicalBlockProvider().availableBlocks().min());
            // send verified block notification
            blockMessaging.sendBlockVerification(new VerificationNotification(
                    true, blockNumber, Bytes.EMPTY, new BlockUnparsed(toBlockItemsUnparsed(blockBlockItems))));
            // now try and read it back
            final Block block = plugin.block(blockNumber).block();
            // check we got the correct block
            assertArrayEquals(blockBlockItems, block.items().toArray());
            assertEquals(blockNumber, plugin.availableBlocks().max());
            assertEquals(blockNumber, plugin.availableBlocks().min());
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
            final BlockItem[] blockBlockItems = createNumberOfVerySimpleBlocks(1);
            final BlockUnparsed blockOrig = new BlockUnparsed(toBlockItemsUnparsed(blockBlockItems));
            final long blockNumber = blockBlockItems[0].blockHeader().number();
            // check the block is not stored yet
            assertNull(plugin.block(blockNumber));
            assertEquals(UNKNOWN_BLOCK_NUMBER, plugin.availableBlocks().max());
            assertEquals(UNKNOWN_BLOCK_NUMBER, plugin.availableBlocks().min());
            assertEquals(
                    UNKNOWN_BLOCK_NUMBER,
                    blockNodeContext.historicalBlockProvider().availableBlocks().max());
            assertEquals(
                    UNKNOWN_BLOCK_NUMBER,
                    blockNodeContext.historicalBlockProvider().availableBlocks().min());
            // check if we try to read we get null as nothing is persisted yet
            assertNull(plugin.block(blockNumber));
            assertEquals(UNKNOWN_BLOCK_NUMBER, plugin.availableBlocks().max());
            assertEquals(UNKNOWN_BLOCK_NUMBER, plugin.availableBlocks().min());
            assertEquals(
                    UNKNOWN_BLOCK_NUMBER,
                    blockNodeContext.historicalBlockProvider().availableBlocks().max());
            assertEquals(
                    UNKNOWN_BLOCK_NUMBER,
                    blockNodeContext.historicalBlockProvider().availableBlocks().min());
            // send verified block notification
            blockMessaging.sendBlockVerification(
                    new VerificationNotification(true, blockNumber, Bytes.EMPTY, blockOrig));
            // now try and read it back
            final Block block = plugin.block(blockNumber).block();
            // check we got the correct block
            assertArrayEquals(blockBlockItems, block.items().toArray());
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
                final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createSimpleBlockUnparsedWithNumber(i);
                // send the block items to the plugin
                blockMessaging.sendBlockVerification(
                        new VerificationNotification(true, i, Bytes.EMPTY, new BlockUnparsed(List.of(block))));
                // assert that the block is persisted
                final Path persistedBlock = BlockFile.nestedDirectoriesBlockFilePath(
                        testPath, i, filesRecentConfig.compression(), filesRecentConfig.maxFilesPerDir());
                assertThat(persistedBlock).exists();
            }
            // assert that the plugin has 100 blocks available (properly updated)
            assertThat(plugin.availableBlocks().min()).isEqualTo(50L);
            assertThat(plugin.availableBlocks().max()).isEqualTo(149L);
            // assert that the first 50 blocks are not persisted anymore
            for (int i = 0; i < 50; i++) {
                final Path persistedBlock = BlockFile.nestedDirectoriesBlockFilePath(
                        testPath, i, filesRecentConfig.compression(), filesRecentConfig.maxFilesPerDir());
                assertThat(persistedBlock).doesNotExist();
            }
            // assert that the last 100 blocks are persisted
            for (int i = 50; i < 150; i++) {
                final Path persistedBlock = BlockFile.nestedDirectoriesBlockFilePath(
                        testPath, i, filesRecentConfig.compression(), filesRecentConfig.maxFilesPerDir());
                assertThat(persistedBlock).exists();
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
