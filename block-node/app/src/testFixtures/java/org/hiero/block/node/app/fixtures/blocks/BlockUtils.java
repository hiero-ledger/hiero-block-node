// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.blocks;

import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import org.hiero.block.node.app.fixtures.TestUtils;
import org.hiero.hapi.block.node.BlockUnparsed;

/*
 * Utility class for getting test blocks.
 * */
public final class BlockUtils {

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

        BlockUnparsed blockUnparsed = null;
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
        PERF_10K_1731(
                "perf-10K-1731.blk.gz",
                "6d8707da4c8265508ec5a26c8c483036c9bf95615bd6613e15131635a2a33528d0c5d4c1552aba3efb5fedcdff0804f8",
                1731);

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
