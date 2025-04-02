// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.hedera.pbj.runtime.ParseException;
import java.io.IOException;
import java.util.List;
import org.hiero.block.node.app.fixtures.blocks.BlockUtils;
import org.hiero.block.node.app.fixtures.plugintest.NoBlocksHistoricalBlockFacility;
import org.hiero.block.node.app.fixtures.plugintest.PluginTestBase;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockNotification;
import org.hiero.hapi.block.node.BlockItemUnparsed;
import org.junit.jupiter.api.Test;

class VerificationServicePluginTest extends PluginTestBase {

    public VerificationServicePluginTest() {
        super(new VerificationServicePlugin(), new NoBlocksHistoricalBlockFacility());
    }

    @Test
    void testVerificationPlugin() throws IOException, ParseException {

        BlockUtils.SampleBlockInfo sampleBlockInfo =
                BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.PERF_10K_1731);

        List<BlockItemUnparsed> blockItems = sampleBlockInfo.blockUnparsed().blockItems();
        long blockNumber = sampleBlockInfo.blockNumber();

        blockMessaging.sendBlockItems(new BlockItems(blockItems, blockNumber));

        // check we received a block verification
        BlockNotification blockNotification =
                blockMessaging.getSentBlockNotifications().getFirst();
        assertNotNull(blockNotification);
        assertEquals(
                blockNumber,
                blockNotification.blockNumber(),
                "The block number should be the same as the one in the block header");
        assertEquals(
                BlockNotification.Type.BLOCK_VERIFIED,
                blockNotification.type(),
                "The block notification type should be BLOCK_VERIFIED");
        assertEquals(
                sampleBlockInfo.blockRootHash(),
                blockNotification.blockHash(),
                "The block hash should be the same as the one in the block header");
    }

    @Test
    void testFailedVerification() throws IOException, ParseException {

        BlockUtils.SampleBlockInfo sampleBlockInfo =
                BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.PERF_10K_1731);

        List<BlockItemUnparsed> blockItems = sampleBlockInfo.blockUnparsed().blockItems();
        // remove one block item, so the hash is no longer valid
        blockItems.remove(3);
        long blockNumber = sampleBlockInfo.blockNumber();

        blockMessaging.sendBlockItems(new BlockItems(blockItems, blockNumber));

        // check we received a block verification
        BlockNotification blockNotification =
                blockMessaging.getSentBlockNotifications().getFirst();
        assertNotNull(blockNotification);

        assertEquals(
                blockNumber,
                blockNotification.blockNumber(),
                "The block number should be the same as the one in the block header");
        assertEquals(
                BlockNotification.Type.BLOCK_FAILED_VERIFICATION,
                blockNotification.type(),
                "The block notification type should be BLOCK_FAILED_VERIFICATION");
        assertNotEquals(
                sampleBlockInfo.blockRootHash(),
                blockNotification.blockHash(),
                "The block hash should NOT be the same");
    }
}
