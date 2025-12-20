// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.fixtures.blocks;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import com.hedera.hapi.block.stream.input.protoc.RoundHeader;
import com.hedera.hapi.block.stream.output.protoc.BlockHeader;
import com.hedera.hapi.block.stream.protoc.Block;
import com.hedera.hapi.block.stream.protoc.BlockItem;
import com.hedera.hapi.block.stream.protoc.BlockProof;
import com.hedera.hapi.block.stream.protoc.TssSignedBlockProof;
import com.hederahashgraph.api.proto.java.BlockHashAlgorithm;
import com.hederahashgraph.api.proto.java.SemanticVersion;
import java.util.Arrays;

public class BlockBuilder {

    public static BlockHeader createBlockHeader(final long blockNumber) {
        return BlockHeader.newBuilder()
                .setHapiProtoVersion(SemanticVersion.newBuilder()
                        .setMajor(1)
                        .setMinor(2)
                        .setPatch(3)
                        .setPre("a")
                        .setBuild("b")
                        .build())
                .setSoftwareVersion(SemanticVersion.newBuilder()
                        .setMajor(4)
                        .setMinor(5)
                        .setPatch(6)
                        .setPre("c")
                        .setBuild("d")
                        .build())
                .setNumber(blockNumber)
                .setBlockTimestamp(com.hederahashgraph.api.proto.java.Timestamp.newBuilder()
                        .setSeconds(123)
                        .setNanos(456)
                        .build())
                .setHashAlgorithm(BlockHashAlgorithm.SHA2_384)
                .build();
    }

    public static Block createBlocks(final long startBlockNumber, final long endBlockNumber) {
        return Block.newBuilder()
                .addAllItems(Arrays.asList(createNumberOfVerySimpleBlocks(startBlockNumber, endBlockNumber)))
                .build();
    }

    /**
     * Creates an array of BlockItem objects representing a very simple block stream of blocks from startBlockNumber to
     * endBlockNumber inclusive.
     *
     * @param startBlockNumber the starting block number
     * @param endBlockNumber the ending block number, inclusive
     * @return an array of BlockItem objects
     */
    public static BlockItem[] createNumberOfVerySimpleBlocks(final long startBlockNumber, final long endBlockNumber) {
        assertTrue(startBlockNumber <= endBlockNumber, "startBlockNumber must be less than or equal to endBlockNumber");
        assertTrue(startBlockNumber >= 0, "startBlockNumber must be greater than or equal to 0");
        final int numberOfBlocks = (int) (endBlockNumber - startBlockNumber + 1);
        final BlockItem[] blockItems = new BlockItem[numberOfBlocks * 3];
        for (int blockNumber = (int) startBlockNumber; blockNumber <= endBlockNumber; blockNumber++) {
            final int i = (blockNumber - (int) startBlockNumber) * 3;
            blockItems[i] = sampleBlockHeader(blockNumber);
            blockItems[i + 1] = sampleRoundHeader(blockNumber * 10L);
            blockItems[i + 2] = sampleBlockProof(blockNumber);
        }
        return blockItems;
    }

    public static BlockItem sampleBlockHeader(final long blockNumber) {
        return BlockItem.newBuilder()
                .setBlockHeader(createBlockHeader(blockNumber))
                .build();
    }

    public static BlockItem sampleRoundHeader(final long roundNumber) {
        return BlockItem.newBuilder()
                .setRoundHeader(createRoundHeader(roundNumber))
                .build();
    }

    public static RoundHeader createRoundHeader(final long roundNumber) {
        return RoundHeader.newBuilder().setRoundNumber(roundNumber).build();
    }

    public static BlockItem sampleBlockProof(final long blockNumber) {
        return BlockItem.newBuilder()
                .setBlockProof(createBlockProof(blockNumber))
                .build();
    }

    public static BlockProof createBlockProof(final long blockNumber) {
        TssSignedBlockProof tssSignedBlockProof = TssSignedBlockProof.newBuilder()
                .setBlockSignature(ByteString.copyFrom("block_signature".getBytes()))
                .build();
        BlockProof blockProof =
                BlockProof.newBuilder().setSignedBlockProof(tssSignedBlockProof).build();
        return blockProof;
    }
}
