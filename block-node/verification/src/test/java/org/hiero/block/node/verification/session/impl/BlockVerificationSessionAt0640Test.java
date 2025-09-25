// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification.session.impl;

import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import java.io.IOException;
import java.util.List;
import org.hiero.block.common.utils.ChunkUtils;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.node.app.fixtures.blocks.BlockUtils;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.verification.session.BlockVerificationSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class BlockVerificationSessionAt0640Test {

    BlockUtils.SampleBlockInfo sampleBlockInfo;
    List<BlockItemUnparsed> blockItems;

    @BeforeEach
    void setUp() throws IOException, ParseException {
        sampleBlockInfo = BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.HAPI_0_64_0_BLOCK_14);
        blockItems = sampleBlockInfo.blockUnparsed().blockItems();
    }

    /**
     * Happy path test for the BlockVerificationSession class.
     * */
    @Test
    void happyPath() throws ParseException {
        BlockHeader blockHeader =
                BlockHeader.PROTOBUF.parse(blockItems.getFirst().blockHeaderOrThrow());

        long blockNumber = blockHeader.number();

        BlockVerificationSessionAt0640 session = new BlockVerificationSessionAt0640(blockNumber, BlockSource.PUBLISHER);

        VerificationNotification blockNotification = session.processBlockItems(blockItems);

        Assertions.assertArrayEquals(
                blockItems.toArray(),
                session.blockItems.toArray(),
                "The internal block items should be the same as ones sent in");

        Assertions.assertArrayEquals(
                blockItems.toArray(),
                blockNotification.block().blockItems().toArray(),
                "The notification's block items should be the same as ones sent in");

        Assertions.assertEquals(
                blockNumber,
                blockNotification.blockNumber(),
                "The block number should be the same as the one in the block header");

        Assertions.assertEquals(
                sampleBlockInfo.blockRootHash(),
                blockNotification.blockHash(),
                "The block hash should be the same as the one in the block header");

        Assertions.assertTrue(blockNotification.success(), "The block notification should be successful");

        Assertions.assertEquals(
                sampleBlockInfo.blockUnparsed(),
                blockNotification.block(),
                "The block should be the same as the one sent");
    }

    /**
     * Happy path test for the BlockVerificationSession class with chunked list of items, to simulate actual usage.
     * */
    @Test
    void happyPath_chunked() throws ParseException {
        List<List<BlockItemUnparsed>> chunkifiedItems = ChunkUtils.chunkify(blockItems, 2);
        int currentChunk = 0;
        long blockNumber = sampleBlockInfo.blockNumber();

        BlockVerificationSessionAt0640 session = new BlockVerificationSessionAt0640(blockNumber, BlockSource.PUBLISHER);

        VerificationNotification blockNotification = session.processBlockItems(chunkifiedItems.get(currentChunk));

        while (blockNotification == null) {
            currentChunk++;
            blockNotification = session.processBlockItems(chunkifiedItems.get(currentChunk));
        }

        Assertions.assertEquals(
                blockNumber,
                blockNotification.blockNumber(),
                "The block number should be the same as the one in the block header");

        Assertions.assertEquals(
                sampleBlockInfo.blockRootHash(),
                blockNotification.blockHash(),
                "The block hash should be the same as the one in the block header");

        Assertions.assertTrue(blockNotification.success(), "The block notification should be successful");

        Assertions.assertArrayEquals(
                blockItems.toArray(),
                session.blockItems.toArray(),
                "The internal block items should be the same as ones sent in");

        Assertions.assertArrayEquals(
                blockItems.toArray(),
                blockNotification.block().blockItems().toArray(),
                "The notification's block items should be the same as ones sent in");
    }

    /**
     * Non-Happy path test for the BlockVerificationSession class with invalid hash
     * */
    @Test
    void invalidHash() throws ParseException {
        // if we remove a block item, the hash will be different
        blockItems.remove(5);

        long blockNumber = sampleBlockInfo.blockNumber();
        BlockVerificationSession session = new BlockVerificationSessionAt0640(blockNumber, BlockSource.PUBLISHER);
        VerificationNotification blockNotification = session.processBlockItems(blockItems);

        Assertions.assertEquals(
                blockNumber,
                blockNotification.blockNumber(),
                "The block number should be the same as the one in the block header");

        Assertions.assertNotEquals(
                sampleBlockInfo.blockRootHash(),
                blockNotification.blockHash(),
                "The block hash should not be the same as the one in the block header");

        Assertions.assertFalse(blockNotification.success(), "The block notification should be unsuccessful");
    }

    /**
     * Non-Happy path test for the BlockVerificationSession class with invalid hash with chunked list of items, to simulate actual usage.
     * */
    @Test
    void invalidHash_chunked() throws ParseException {
        // remove a block item, the hash will be different
        blockItems.remove(5);
        // chunkify the list of items
        List<List<BlockItemUnparsed>> chunkifiedItems = ChunkUtils.chunkify(blockItems, 2);
        int currentChunk = 0;
        long blockNumber = sampleBlockInfo.blockNumber();

        BlockVerificationSession session = new BlockVerificationSessionAt0640(blockNumber, BlockSource.PUBLISHER);
        VerificationNotification blockNotification = session.processBlockItems(chunkifiedItems.get(currentChunk));

        while (blockNotification == null) {
            currentChunk++;
            blockNotification = session.processBlockItems(chunkifiedItems.get(currentChunk));
        }

        Assertions.assertEquals(
                blockNumber,
                blockNotification.blockNumber(),
                "The block number should be the same as the one in the block header");

        Assertions.assertNotEquals(
                sampleBlockInfo.blockRootHash(),
                blockNotification.blockHash(),
                "The block hash should not be the same as the one in the block header");

        Assertions.assertFalse(blockNotification.success(), "The block notification should be unsuccessful");
    }

    @Test
    @Disabled("BlockHeader number and blockNumber on constructor mismatch, should throw IllegalStateException")
    void blockHeaderAndNumberMismatch() throws ParseException {
        BlockHeader blockHeader =
                BlockHeader.PROTOBUF.parse(blockItems.getFirst().blockHeaderOrThrow());

        long blockNumber = blockHeader.number() + 1;

        BlockVerificationSessionAt0640 session = new BlockVerificationSessionAt0640(blockNumber, BlockSource.PUBLISHER);

        Assertions.assertThrows(IllegalStateException.class, () -> session.processBlockItems(blockItems));
    }
}
