// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification;

import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.zip.GZIPInputStream;
import org.hiero.block.common.utils.ChunkUtils;
import org.hiero.block.node.app.fixtures.TestUtils;
import org.hiero.block.node.spi.blockmessaging.BlockNotification;
import org.hiero.hapi.block.node.BlockItemUnparsed;
import org.hiero.hapi.block.node.BlockUnparsed;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class BlockVerificationSessionTest {

    protected final Bytes EXPECTED_BLOCK_ROOT_HASH = Bytes.fromHex(
            "6d8707da4c8265508ec5a26c8c483036c9bf95615bd6613e15131635a2a33528d0c5d4c1552aba3efb5fedcdff0804f8");

    /**
     * Happy path test for the BlockVerificationSession class.
     * */
    @Test
    void happyPath() throws IOException, ParseException, URISyntaxException {
        List<BlockItemUnparsed> blockItems = getTestBlock1Items();
        BlockHeader blockHeader =
                BlockHeader.PROTOBUF.parse(blockItems.getFirst().blockHeader());
        long blockNumber = blockHeader.number();

        BlockVerificationSession session = new BlockVerificationSession(blockNumber);

        BlockNotification blockNotification = session.processBlockItems(blockItems);

        Assertions.assertEquals(
                blockNumber,
                blockNotification.blockNumber(),
                "The block number should be the same as the one in the block header");

        Assertions.assertEquals(
                EXPECTED_BLOCK_ROOT_HASH,
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
        List<BlockItemUnparsed> blockItems = getTestBlock1Items();
        List<List<BlockItemUnparsed>> chunkifiedItems = ChunkUtils.chunkify(blockItems, 2);
        int currentChunk = 0;

        BlockHeader blockHeader =
                BlockHeader.PROTOBUF.parse(blockItems.getFirst().blockHeader());
        long blockNumber = blockHeader.number();

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
                EXPECTED_BLOCK_ROOT_HASH,
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
        List<BlockItemUnparsed> blockItems = getTestBlock1Items();
        // if we remove a block item, the hash will be different
        blockItems.remove(5);

        BlockHeader blockHeader =
                BlockHeader.PROTOBUF.parse(blockItems.getFirst().blockHeader());
        long blockNumber = blockHeader.number();

        BlockVerificationSession session = new BlockVerificationSession(blockNumber);

        BlockNotification blockNotification = session.processBlockItems(blockItems);

        Assertions.assertEquals(
                blockNumber,
                blockNotification.blockNumber(),
                "The block number should be the same as the one in the block header");

        Assertions.assertNotEquals(
                EXPECTED_BLOCK_ROOT_HASH,
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
        List<BlockItemUnparsed> blockItems = getTestBlock1Items();
        // remove a block item, the hash will be different
        blockItems.remove(5);
        // chunkify the list of items
        List<List<BlockItemUnparsed>> chunkifiedItems = ChunkUtils.chunkify(blockItems, 2);
        int currentChunk = 0;

        BlockHeader blockHeader =
                BlockHeader.PROTOBUF.parse(blockItems.getFirst().blockHeader());
        long blockNumber = blockHeader.number();

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
                EXPECTED_BLOCK_ROOT_HASH,
                blockNotification.blockHash(),
                "The block hash should not be the same as the one in the block header");

        Assertions.assertEquals(
                BlockNotification.Type.BLOCK_FAILED_VERIFICATION,
                blockNotification.type(),
                "The block notification type should be BLOCK_FAILED_VERIFICATION");
    }

    protected List<BlockItemUnparsed> getTestBlock1Items() throws IOException, ParseException, URISyntaxException {

        // Not sure why JSON is not working :( but parses an empty Block.
        //        byte[] jsonBytes =
        // TestUtils.class.getModule().getResourceAsStream("test-blocks/perf-10K-1731.blk.json").readAllBytes();
        //        BlockUnparsed blockUnparsed = BlockUnparsed.JSON.parse(Bytes.wrap(jsonBytes));

        BlockUnparsed blockUnparsed = null;
        var stream = TestUtils.class.getModule().getResourceAsStream("test-blocks/perf-10K-1731.blk.gz");
        try (final var gzipInputStream = new GZIPInputStream(stream)) {
            // Read the bytes from the GZIPInputStream
            byte[] bytes = gzipInputStream.readAllBytes();
            // Parse the bytes into a BlockUnparsed object
            blockUnparsed = BlockUnparsed.PROTOBUF.parse(Bytes.wrap(bytes));
        }

        return blockUnparsed.blockItems();
    }
}
