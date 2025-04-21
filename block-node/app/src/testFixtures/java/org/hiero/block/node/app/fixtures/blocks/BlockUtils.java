// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.blocks;

import com.hedera.hapi.block.stream.Block;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import org.hiero.block.api.BlockUnparsed;
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
        GENERATED_10(
                "generated-10.blk.gz",
                "1d315470a3cfe12838d10cda3ad17f93a577a563efb45725f78a58384b8d2ebddf1dd5d849f922ddd72e9e824f565ebc",
                10);

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
