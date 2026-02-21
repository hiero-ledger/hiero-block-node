// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification.session.impl;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import java.io.IOException;
import java.util.List;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.node.app.fixtures.blocks.BlockUtils;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ExtendedMerkleTreeSessionTest {

    @Test
    @DisplayName("happy path - HAPI 0.72.0 block")
    void happyPath() throws IOException, ParseException {
        BlockUtils.SampleBlockInfo sampleBlockInfo =
                BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.HAPI_0_72_0_BLOCK_21);
        List<BlockItemUnparsed> blockItems = sampleBlockInfo.blockUnparsed().blockItems();

        BlockHeader blockHeader =
                BlockHeader.PROTOBUF.parse(blockItems.getFirst().blockHeaderOrThrow());
        long blockNumber = blockHeader.number();

        ExtendedMerkleTreeSession session =
                new ExtendedMerkleTreeSession(blockNumber, BlockSource.PUBLISHER, null, null);

        BlockItems blockItemsMessage = new BlockItems(blockItems, blockNumber, true, true);
        VerificationNotification blockNotification = session.processBlockItems(blockItemsMessage);

        assertArrayEquals(
                blockItems.toArray(),
                session.blockItems.toArray(),
                "The internal block items should be the same as ones sent in");

        assertArrayEquals(
                blockItems.toArray(),
                blockNotification.block().blockItems().toArray(),
                "The notification's block items should be the same as ones sent in");

        assertEquals(
                blockNumber,
                blockNotification.blockNumber(),
                "The block number should be the same as the one in the block header");

        assertEquals(
                sampleBlockInfo.blockRootHash(),
                blockNotification.blockHash(),
                "The block hash should be the same as the one in the block header");

        assertTrue(blockNotification.success(), "The block notification should be successful");

        assertEquals(
                sampleBlockInfo.blockUnparsed(),
                blockNotification.block(),
                "The block should be the same as the one sent");
    }
}
