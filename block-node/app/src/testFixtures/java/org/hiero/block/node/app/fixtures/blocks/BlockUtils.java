// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.blocks;

import com.hedera.hapi.block.stream.Block;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.TestUtils;

/*
 * Utility class for getting test blocks.
 * */
@SuppressWarnings("unused")
public final class BlockUtils {

    /**
     * Converts Block to a List of BlockUnparsed
     *
     * @param block the Block to convert
     * @return BlockUnparsed representation of the BlockItem
     */
    public static BlockUnparsed toBlockUnparsed(Block block) {
        try {
            return BlockUnparsed.PROTOBUF.parse(Block.PROTOBUF.toBytes(block));
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Converts a BlockUnparsed to a Block
     *
     * @param block the BlockUnparsed to convert
     * @return the Block representation of the BlockUnparsed
     */
    public static Block toBlock(BlockUnparsed block) {
        try {
            return Block.PROTOBUF.parse(BlockUnparsed.PROTOBUF.toBytes(block));
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Gets a SampleBlockInfo, out of the defined sample blocks enum.
     */
    public static SampleBlockInfo getSampleBlockInfo(SAMPLE_BLOCKS sampleBlocks) throws IOException, ParseException {
        // JSON Option not working
        // Parsing is coming empty.
        //        BlockUnparsed jsonOrigin = BlockUnparsed.JSON.parse(Bytes.wrap(BlockUtils.class
        //                .getModule()
        //                .getResourceAsStream("test-blocks/" + "perf-10K-1731.blk.json")
        //                .readAllBytes()));

        BlockUnparsed blockUnparsed;
        var stream = TestUtils.class.getModule().getResourceAsStream("test-blocks/" + sampleBlocks.blockName);
        try (final var gzipInputStream = new GZIPInputStream(stream)) {
            // Read the bytes from the GZIPInputStream
            byte[] bytes = gzipInputStream.readAllBytes();
            // Parse the bytes into a BlockUnparsed object
            blockUnparsed = BlockUnparsed.PROTOBUF.parse(Bytes.wrap(bytes));
        }

        // Get the block root hash and block number
        Bytes blockRootHash = sampleBlocks.getBlockHash();
        long blockNumber = sampleBlocks.getBlockNumber();
        // Return a SampleBlockInfo object with the block root hash, block number, and BlockUnparsed object
        return new SampleBlockInfo(blockRootHash, blockNumber, blockUnparsed);
    }

    /**
     * SampleBlockInfo is a simple record that contains the block root hash, block number, and BlockUnparsed object for convenience
     * */
    public record SampleBlockInfo(Bytes blockRootHash, Long blockNumber, BlockUnparsed blockUnparsed) {}

    /**
     * Sample blocks for testing.
     * These blocks are used for testing purposes only.
     */
    public enum SAMPLE_BLOCKS {
        HAPI_0_64_0_BLOCK_14(
                "HAPI-0-64-0/000000000000000000000000000000000014.blk.gz",
                "54dca69665741f13d8c956dffb3327edbe5d56e32d2bc66d6ce145a1e1b62fe61cddb74b19f17a5da9bb01933429d5e0",
                14),
        HAPI_0_66_0_BLOCK_10(
                "HAPI-0-66-0/000000000000000000000000000000000010.blk.gz",
                "8d6d9004594f10f15518a2c6a7c5d473077f2a83a874a9489095adc0d0bf6b7c9f7e49b1e5430203e7234ebfd2880de2",
                10),
        HAPI_0_68_0_BLOCK_14(
            "HAPI-0-68-0/000000000000000000000000000000000014.blk.gz",
            "d4924fe896fd32375ced195f29238f36b50f1a04d7b0e34e00b82758a2e9cabd37c98945181788e309d2c9588830bbcb",
            14);

        // VNymlmV0HxPYyVbf+zMn7b5dVuMtK8ZtbOFFoeG2L+Yc3bdLGfF6Xam7AZM0KdXg
        private final String blockName;
        private final Bytes blockHash;
        private final long blockNumber;

        SAMPLE_BLOCKS(String blockName, String blockHash, long blockNumber) {
            this.blockName = blockName;
            this.blockHash = Bytes.fromHex(blockHash);
            this.blockNumber = blockNumber;
        }

        public String getBlockName() {
            return blockName;
        }

        public Bytes getBlockHash() {
            return blockHash;
        }

        public long getBlockNumber() {
            return blockNumber;
        }
    }
}
