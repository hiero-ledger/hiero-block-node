// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification;

import static org.hiero.block.common.utils.FileUtilities.readGzipFileUnsafe;

import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.List;
import org.hiero.block.common.utils.ChunkUtils;
import org.hiero.block.node.spi.blockmessaging.BlockNotification;
import org.hiero.hapi.block.node.BlockItemUnparsed;
import org.hiero.hapi.block.node.BlockUnparsed;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class BlockVerificationSessionTest {

    protected final Bytes BLOCKHASH_PERF_10K_1731 = Bytes.fromHex(
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
                BlockNotification.Type.BLOCK_VERIFIED,
                blockNotification.type(),
                "The block notification type should be BLOCK_VERIFIED");

        Assertions.assertEquals(
                BLOCKHASH_PERF_10K_1731,
                blockNotification.blockHash(),
                "The block hash should be the same as the one in the block header");
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
                BlockNotification.Type.BLOCK_VERIFIED,
                blockNotification.type(),
                "The block notification type should be BLOCK_VERIFIED");

        Assertions.assertEquals(
                BLOCKHASH_PERF_10K_1731,
                blockNotification.blockHash(),
                "The block hash should be the same as the one in the block header");
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

        Assertions.assertEquals(
                BlockNotification.Type.BLOCK_FAILED_VERIFICATION,
                blockNotification.type(),
                "The block notification type should be BLOCK_FAILED_VERIFICATION");

        Assertions.assertNotEquals(
                BLOCKHASH_PERF_10K_1731,
                blockNotification.blockHash(),
                "The block hash should not be the same as the one in the block header");
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

        Assertions.assertEquals(
                BlockNotification.Type.BLOCK_FAILED_VERIFICATION,
                blockNotification.type(),
                "The block notification type should be BLOCK_FAILED_VERIFICATION");

        Assertions.assertNotEquals(
                BLOCKHASH_PERF_10K_1731,
                blockNotification.blockHash(),
                "The block hash should not be the same as the one in the block header");
    }

    protected List<BlockItemUnparsed> getTestBlock1Items() throws IOException, ParseException, URISyntaxException {
        Path block01Path = Path.of(
                getClass().getResource("/test-blocks/perf-10K-1731.blk.gz").toURI());
        Bytes block01Bytes = Bytes.wrap(readGzipFileUnsafe(block01Path));
        BlockUnparsed blockUnparsed = BlockUnparsed.PROTOBUF.parse(block01Bytes);
        return blockUnparsed.blockItems();
    }
}
