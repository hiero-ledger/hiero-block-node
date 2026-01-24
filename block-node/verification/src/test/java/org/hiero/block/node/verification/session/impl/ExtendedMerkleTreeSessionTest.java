// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification.session.impl;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import java.util.ArrayList;
import java.util.List;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ExtendedMerkleTreeSessionTest {
    List<BlockItemUnparsed> blockItems;
    long blockNumber;

    @BeforeEach
    void setUp() {
        // Use synthetic block with all required items: header, round header, footer, and proof
        blockNumber = 100;
        blockItems = new ArrayList<>();
        blockItems.add(SimpleTestBlockItemBuilder.sampleBlockHeaderUnparsed(blockNumber));
        blockItems.add(SimpleTestBlockItemBuilder.sampleRoundHeaderUnparsed(blockNumber * 10L));
        blockItems.add(BlockItemUnparsed.newBuilder()
                .blockFooter(SimpleTestBlockItemBuilder.createBlockFooterUnparsed(blockNumber))
                .build());
        blockItems.add(SimpleTestBlockItemBuilder.sampleBlockProofUnparsed(blockNumber));
    }

    /**
     * Happy path test for the BlockVerificationSession class.
     */
    @Test
    void happyPath() throws ParseException {
        BlockHeader blockHeader =
                BlockHeader.PROTOBUF.parse(blockItems.getFirst().blockHeaderOrThrow());

        assertEquals(blockNumber, blockHeader.number(), "Block number should match");

        ExtendedMerkleTreeSession session =
                new ExtendedMerkleTreeSession(blockNumber, BlockSource.PUBLISHER, null, null);

        // Create BlockItems with isEndOfBlock=true to trigger finalization
        BlockItems blockItemsMessage = new BlockItems(blockItems, blockNumber);
        VerificationNotification blockNotification = session.processBlockItems(blockItemsMessage);

        assertNotNull(blockNotification, "Block notification should not be null");

        assertArrayEquals(
                blockItems.toArray(),
                session.blockItems.toArray(),
                "The internal block items should be the same as ones sent in");

        assertEquals(
                blockNumber,
                blockNotification.blockNumber(),
                "The block number should be the same as the one in the block header");

        assertNotNull(blockNotification.blockHash(), "Block hash should not be null");

        // Note: verification may fail due to dummy signature in test block,
        // but we still get a notification with a computed hash
    }
}
