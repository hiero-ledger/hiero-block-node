// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification;

import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import org.hiero.block.common.utils.ChunkUtils;
import org.hiero.block.node.app.fixtures.blocks.BlockUtils;
import org.hiero.block.node.spi.blockmessaging.BlockNotification;
import org.hiero.hapi.block.node.BlockItemUnparsed;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BlockVerificationSessionTest {

    BlockUtils.SampleBlockInfo sampleBlockInfo;
    List<BlockItemUnparsed> blockItems;

    @BeforeEach
    void setUp() throws IOException, ParseException {
        sampleBlockInfo = BlockUtils.getSampleBlockInfo(BlockUtils.SAMPLE_BLOCKS.GENERATED_10);
        blockItems = sampleBlockInfo.blockUnparsed().blockItems();
    }

    /**
     * Happy path test for the BlockVerificationSession class.
     * */
    @Test
    void happyPath() throws ParseException {
        BlockHeader blockHeader = BlockHeader.PROTOBUF.parse(blockItems.getFirst().blockHeaderOrThrow());

        long blockNumber = blockHeader.number();

        BlockVerificationSession session = new BlockVerificationSession(blockNumber);

        BlockNotification blockNotification = session.processBlockItems(blockItems);

        Assertions.assertEquals(
                blockNumber,
                blockNotification.blockNumber(),
                "The block number should be the same as the one in the block header");

        Assertions.assertEquals(
                sampleBlockInfo.blockRootHash(),
                blockNotification.blockHash(),
                "The block hash should be the same as the one in the block header");

        Assertions.assertEquals(
                BlockNotification.Type.BLOCK_VERIFIED,
                blockNotification.type(),
                "The block notification type should be BLOCK_VERIFIED");
    }

    /**
     * Happy path test for the BlockVerificationSession class with chunked list of items, to simulate actual usage.
     * */
    @Test
    void happyPath_chunked() throws IOException, ParseException, URISyntaxException {
        List<List<BlockItemUnparsed>> chunkifiedItems = ChunkUtils.chunkify(blockItems, 2);
        int currentChunk = 0;
        long blockNumber = sampleBlockInfo.blockNumber();

        BlockVerificationSession session = new BlockVerificationSession(blockNumber);

        BlockNotification blockNotification = session.processBlockItems(chunkifiedItems.get(currentChunk));

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

        Assertions.assertEquals(
                BlockNotification.Type.BLOCK_VERIFIED,
                blockNotification.type(),
                "The block notification type should be BLOCK_VERIFIED");
    }

    /**
     * Non-Happy path test for the BlockVerificationSession class with invalid hash
     * */
    @Test
    void invalidHash() throws IOException, ParseException, URISyntaxException {
        // if we remove a block item, the hash will be different
        blockItems.remove(5);

        long blockNumber = sampleBlockInfo.blockNumber();
        BlockVerificationSession session = new BlockVerificationSession(blockNumber);
        BlockNotification blockNotification = session.processBlockItems(blockItems);

        Assertions.assertEquals(
                blockNumber,
                blockNotification.blockNumber(),
                "The block number should be the same as the one in the block header");

        Assertions.assertNotEquals(
                sampleBlockInfo.blockRootHash(),
                blockNotification.blockHash(),
                "The block hash should not be the same as the one in the block header");

        Assertions.assertEquals(
                BlockNotification.Type.BLOCK_FAILED_VERIFICATION,
                blockNotification.type(),
                "The block notification type should be BLOCK_FAILED_VERIFICATION");
    }

    /**
     * Non-Happy path test for the BlockVerificationSession class with invalid hash with chunked list of items, to simulate actual usage.
     * */
    @Test
    void invalidHash_chunked() throws IOException, ParseException, URISyntaxException {
        // remove a block item, the hash will be different
        blockItems.remove(5);
        // chunkify the list of items
        List<List<BlockItemUnparsed>> chunkifiedItems = ChunkUtils.chunkify(blockItems, 2);
        int currentChunk = 0;
        long blockNumber = sampleBlockInfo.blockNumber();

        BlockVerificationSession session = new BlockVerificationSession(blockNumber);
        BlockNotification blockNotification = session.processBlockItems(chunkifiedItems.get(currentChunk));

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

        Assertions.assertEquals(
                BlockNotification.Type.BLOCK_FAILED_VERIFICATION,
                blockNotification.type(),
                "The block notification type should be BLOCK_FAILED_VERIFICATION");
    }
}
