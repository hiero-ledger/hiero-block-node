// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.hedera.pbj.runtime.OneOf;
import com.hedera.pbj.runtime.ParseException;
import java.io.IOException;
import java.util.List;
import org.hiero.block.node.app.fixtures.blocks.BlockUtils;
import org.hiero.block.node.app.fixtures.plugintest.NoBlocksHistoricalBlockFacility;
import org.hiero.block.node.app.fixtures.plugintest.PluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.TestHealthFacility;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.hapi.block.node.BlockItemUnparsed;
import org.hiero.hapi.block.node.BlockItemUnparsed.ItemOneOfType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit test for {@link VerificationServicePlugin}.
 */
class VerificationServicePluginTest extends PluginTestBase<VerificationServicePlugin> {

    public VerificationServicePluginTest() {
        super(new VerificationServicePlugin(), new NoBlocksHistoricalBlockFacility());
    }

    @Test
    void testVerificationPlugin() throws IOException, ParseException {

        BlockUtils.SampleBlockInfo sampleBlockInfo =
                BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.GENERATED_10);

        List<BlockItemUnparsed> blockItems = sampleBlockInfo.blockUnparsed().blockItems();
        long blockNumber = sampleBlockInfo.blockNumber();

        blockMessaging.sendBlockItems(new BlockItems(blockItems, blockNumber));

        // check we received a block verification
        VerificationNotification blockNotification =
                blockMessaging.getSentVerificationNotifications().getFirst();
        assertNotNull(blockNotification);
        assertEquals(
                blockNumber,
                blockNotification.blockNumber(),
                "The block number should be the same as the one in the block header");
        assertTrue(blockNotification.success(), "The verification should be successful");
        assertEquals(
                sampleBlockInfo.blockRootHash(),
                blockNotification.blockHash(),
                "The block hash should be the same as the one in the block header");
        assertEquals(
                sampleBlockInfo.blockUnparsed(),
                blockNotification.block(),
                "The block should be the same as the one sent");
    }

    @Test
    void testFailedVerification() throws IOException, ParseException {

        BlockUtils.SampleBlockInfo sampleBlockInfo =
                BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.GENERATED_10);

        List<BlockItemUnparsed> blockItems = sampleBlockInfo.blockUnparsed().blockItems();
        // remove one block item, so the hash is no longer valid
        blockItems.remove(3);
        long blockNumber = sampleBlockInfo.blockNumber();

        blockMessaging.sendBlockItems(new BlockItems(blockItems, blockNumber));

        // check we received a block verification
        VerificationNotification blockNotification =
                blockMessaging.getSentVerificationNotifications().getFirst();
        assertNotNull(blockNotification);

        assertEquals(
                blockNumber,
                blockNotification.blockNumber(),
                "The block number should be the same as the one in the block header");
        assertFalse(blockNotification.success(), "The verification should be unsuccessful");
        assertNotEquals(
                sampleBlockInfo.blockRootHash(),
                blockNotification.blockHash(),
                "The block hash should be the same as the one in the block header");
        assertNull(blockNotification.block(), "The block should be null since the verification failed");
    }

    @Test
    @DisplayName("Test handleBlockItemsReceived without a block header")
    void testHandleBlockItemsReceived_NoCurrentSession() throws IOException, ParseException {
        // create sample block data
        BlockUtils.SampleBlockInfo sampleBlockInfo =
                BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.GENERATED_10);
        long blockNumber = sampleBlockInfo.blockNumber();
        List<BlockItemUnparsed> blockItems = sampleBlockInfo.blockUnparsed().blockItems();
        // remove the header to simulate a case where receive items and have never received a header
        blockItems.removeFirst();
        // send some items to the plugin, they should be ignored
        plugin.handleBlockItemsReceived(new BlockItems(blockItems, blockNumber));
        // check we did not receive a block verification
        assertEquals(0, blockMessaging.getSentVerificationNotifications().size());
    }

    @Test
    @DisplayName("Test handleBlockItemsReceived with non-running server")
    void testHandleBlockItemsReceived_NotRunning() {
        // make the server state not running
        ((TestHealthFacility) blockNodeContext.serverHealth()).isRunning.set(false);
        // send some items to the plugin, they should be ignored
        plugin.handleBlockItemsReceived(
                new BlockItems(List.of(new BlockItemUnparsed(new OneOf<>(ItemOneOfType.BLOCK_HEADER, null))), -1));
        // check we did not receive a block verification
        assertEquals(0, blockMessaging.getSentVerificationNotifications().size());
    }

    @Test
    @DisplayName("Test handleBlockItemsReceived with BlockItems that throws an exception")
    void testHandleBlockItemsReceived_ExceptionThrown() {
        // mock a BlockItems object to throw an exception when isStartOfNewBlock is called
        BlockItems blockItems = mock(BlockItems.class);
        when(blockItems.isStartOfNewBlock()).thenThrow(new RuntimeException("Test Exception"));
        // sent the mocked BlockItems to the plugin
        plugin.handleBlockItemsReceived(blockItems);
        // check the exception was thrown and resulted in a shutdown
        assertTrue(
                ((TestHealthFacility) blockNodeContext.serverHealth()).shutdownCalled.get(),
                "The server should be shutdown after an exception is thrown");
    }
}
