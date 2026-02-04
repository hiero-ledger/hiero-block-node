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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class ExtendedMerkleTreeSessionTest {
    BlockUtils.SampleBlockInfo sampleBlockInfo;
    List<BlockItemUnparsed> blockItems;

    @BeforeEach
    void setUp() throws IOException, ParseException {
        sampleBlockInfo = BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.HAPI_0_69_0_BLOCK_240);
        blockItems = sampleBlockInfo.blockUnparsed().blockItems();
    }

    /**
     * Happy path test for the BlockVerificationSession class.
     */
    // @todo(2002): Re-enable once proper v0.69.0 verification is implemented with updated protobuf
    @Disabled("Disabled until proper v0.69.0 verification is implemented - see @todo(2002)")
    @Test
    void happyPath() throws ParseException {
        BlockHeader blockHeader =
                BlockHeader.PROTOBUF.parse(blockItems.getFirst().blockHeaderOrThrow());

        long blockNumber = blockHeader.number();

        ExtendedMerkleTreeSession session =
                new ExtendedMerkleTreeSession(blockNumber, BlockSource.PUBLISHER, null, null);

        // Create BlockItems with isEndOfBlock=true to trigger finalization
        BlockItems blockItemsMessage = new BlockItems(blockItems, blockNumber);
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
