// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.recent;

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
import java.util.List;
import org.hiero.block.node.app.HistoricalBlockFacilityImpl;
import org.hiero.block.node.app.fixtures.plugintest.PluginTestBase;
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockNotification.Type;
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
    /** The plugin configuration, customized with testing file system. */
    private final FilesRecentConfig filesRecentConfig;
    /** The plugin under test. */
    private final BlocksFilesRecentPlugin blocksFilesRecentPlugin;
    /** The historical block facility. */
    private final HistoricalBlockFacilityImpl historicalBlockFacility;

    /**
     * Construct test environment.
     */
    BlockFileRecentPluginTest() {
        this.fileSystem = Jimfs.newFileSystem(Configuration.unix());
        this.filesRecentConfig = new FilesRecentConfig(
                fileSystem.getPath("/live"), fileSystem.getPath("/unverified"), CompressionType.ZSTD, 3);
        this.blocksFilesRecentPlugin = new BlocksFilesRecentPlugin(this.filesRecentConfig);
        this.historicalBlockFacility = new HistoricalBlockFacilityImpl(List.of(blocksFilesRecentPlugin));
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
            super(blocksFilesRecentPlugin, historicalBlockFacility);
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
            assertEquals(UNKNOWN_BLOCK_NUMBER, plugin.latestBlockNumber());
            assertEquals(UNKNOWN_BLOCK_NUMBER, plugin.oldestBlockNumber());
            assertEquals(
                    UNKNOWN_BLOCK_NUMBER,
                    blockNodeContext.historicalBlockProvider().latestBlockNumber());
            assertEquals(
                    UNKNOWN_BLOCK_NUMBER,
                    blockNodeContext.historicalBlockProvider().oldestBlockNumber());
            // send it to messaging
            blockMessaging.sendBlockItems(new BlockItems(toBlockItemsUnparsed(blockBlockItems), blockNumber));
            // check if we try to read we get null as nothing is verified yet
            assertNull(plugin.block(blockNumber));
            assertEquals(UNKNOWN_BLOCK_NUMBER, plugin.latestBlockNumber());
            assertEquals(UNKNOWN_BLOCK_NUMBER, plugin.oldestBlockNumber());
            assertEquals(
                    UNKNOWN_BLOCK_NUMBER,
                    blockNodeContext.historicalBlockProvider().latestBlockNumber());
            assertEquals(
                    UNKNOWN_BLOCK_NUMBER,
                    blockNodeContext.historicalBlockProvider().oldestBlockNumber());
            // send verified block notification
            blockMessaging.sendBlockNotification(new BlockNotification(blockNumber, Type.BLOCK_VERIFIED, Bytes.EMPTY));
            // now try and read it back
            final Block block = plugin.block(blockNumber).block();
            // check we got the correct block
            assertArrayEquals(blockBlockItems, block.items().toArray());
            assertEquals(blockNumber, plugin.latestBlockNumber());
            assertEquals(blockNumber, plugin.oldestBlockNumber());
            assertEquals(blockNumber, blockNodeContext.historicalBlockProvider().latestBlockNumber());
            assertEquals(blockNumber, blockNodeContext.historicalBlockProvider().oldestBlockNumber());
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
            final long blockNumber = blockBlockItems[0].blockHeader().number();
            // check the block is not stored yet
            assertNull(plugin.block(blockNumber));
            assertEquals(UNKNOWN_BLOCK_NUMBER, plugin.latestBlockNumber());
            assertEquals(UNKNOWN_BLOCK_NUMBER, plugin.oldestBlockNumber());
            assertEquals(
                    UNKNOWN_BLOCK_NUMBER,
                    blockNodeContext.historicalBlockProvider().latestBlockNumber());
            assertEquals(
                    UNKNOWN_BLOCK_NUMBER,
                    blockNodeContext.historicalBlockProvider().oldestBlockNumber());
            // send verified block notification
            blockMessaging.sendBlockNotification(new BlockNotification(blockNumber, Type.BLOCK_VERIFIED, Bytes.EMPTY));
            // check if we try to read we get null as nothing is persisted yet
            assertNull(plugin.block(blockNumber));
            assertEquals(UNKNOWN_BLOCK_NUMBER, plugin.latestBlockNumber());
            assertEquals(UNKNOWN_BLOCK_NUMBER, plugin.oldestBlockNumber());
            assertEquals(
                    UNKNOWN_BLOCK_NUMBER,
                    blockNodeContext.historicalBlockProvider().latestBlockNumber());
            assertEquals(
                    UNKNOWN_BLOCK_NUMBER,
                    blockNodeContext.historicalBlockProvider().oldestBlockNumber());
            // send it to messaging
            blockMessaging.sendBlockItems(new BlockItems(toBlockItemsUnparsed(blockBlockItems), blockNumber));
            // now try and read it back
            final Block block = plugin.block(blockNumber).block();
            // check we got the correct block
            assertArrayEquals(blockBlockItems, block.items().toArray());
            assertEquals(blockNumber, plugin.latestBlockNumber());
            assertEquals(blockNumber, plugin.oldestBlockNumber());
            assertEquals(blockNumber, blockNodeContext.historicalBlockProvider().latestBlockNumber());
            assertEquals(blockNumber, blockNodeContext.historicalBlockProvider().oldestBlockNumber());
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
