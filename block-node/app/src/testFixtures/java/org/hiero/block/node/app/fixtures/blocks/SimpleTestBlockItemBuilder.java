// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.blocks;

import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.BlockItem.ItemOneOfType;
import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.input.RoundHeader;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.BlockHashAlgorithm;
import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.pbj.runtime.OneOf;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.Collections;
import org.hiero.hapi.block.node.BlockItemUnparsed;

/**
 * A utility class to create sample BlockItem objects for testing purposes.
 */
public final class SimpleTestBlockItemBuilder {
    public static BlockHeader createBlockHeader(final long blockNumber) {
        return new BlockHeader(
                new SemanticVersion(1, 2, 3, "a", "b"),
                new SemanticVersion(4, 5, 6, "c", "d"),
                blockNumber,
                Bytes.wrap("hash".getBytes()),
                new Timestamp(123L, 456),
                BlockHashAlgorithm.SHA2_384);
    }

    public static Bytes createBlockHeaderUnparsed(final long blockNumber) {
        return BlockHeader.PROTOBUF.toBytes(createBlockHeader(blockNumber));
    }

    public static RoundHeader createRoundHeader(final long roundNumber) {
        return new RoundHeader(roundNumber);
    }

    public static Bytes createRoundHeaderUnparsed(final long roundNumber) {
        return RoundHeader.PROTOBUF.toBytes(createRoundHeader(roundNumber));
    }

    public static BlockProof createBlockProof(final long blockNumber) {
        return new BlockProof(
                blockNumber,
                Bytes.wrap("previousBlockRootHash".getBytes()),
                Bytes.wrap("startOfBlockStateRootHash".getBytes()),
                Bytes.wrap("signature".getBytes()),
                Collections.emptyList());
    }

    public static Bytes createBlockProofUnparsed(final long blockNumber) {
        return BlockProof.PROTOBUF.toBytes(createBlockProof(blockNumber));
    }

    public static BlockItem sampleBlockHeader(final long blockNumber) {
        return new BlockItem(new OneOf<>(ItemOneOfType.BLOCK_HEADER, createBlockHeader(blockNumber)));
    }

    public static BlockItemUnparsed sampleBlockHeaderUnparsed(final long blockNumber) {
        return BlockItemUnparsed.newBuilder()
                .blockHeader(createBlockHeaderUnparsed(blockNumber))
                .build();
    }

    public static BlockItem sampleRoundHeader(final long roundNumber) {
        return new BlockItem(new OneOf<>(ItemOneOfType.ROUND_HEADER, createRoundHeader(roundNumber)));
    }

    public static BlockItemUnparsed sampleRoundHeaderUnparsed(final long roundNumber) {
        return BlockItemUnparsed.newBuilder()
                .roundHeader(createRoundHeaderUnparsed(roundNumber))
                .build();
    }

    public static BlockItem sampleBlockProof(final long blockNumber) {
        return new BlockItem(new OneOf<>(ItemOneOfType.BLOCK_PROOF, createBlockProof(blockNumber)));
    }

    public static BlockItemUnparsed sampleBlockProofUnparsed(final long blockNumber) {
        return BlockItemUnparsed.newBuilder()
                .blockProof(createBlockProofUnparsed(blockNumber))
                .build();
    }

    /**
     * Creates an array of BlockItem objects representing a very simple block stream of N blocks.
     *
     * @param numberOfBlocks the number of blocks to create
     * @return an array of BlockItem objects
     */
    public static BlockItem[] createNumberOfVerySimpleBlocks(final int numberOfBlocks) {
        final BlockItem[] blockItems = new BlockItem[numberOfBlocks * 3];
        for (int i = 0; i < blockItems.length; i += 3) {
            long blockNumber = i / 3;
            blockItems[i] = sampleBlockHeader(blockNumber);
            blockItems[i + 1] = sampleRoundHeader(blockNumber * 10);
            blockItems[i + 2] = sampleBlockProof(blockNumber);
        }
        return blockItems;
    }

    /**
     * Creates an array of BlockItem objects representing a very simple block stream of blocks from startBlockNumber to
     * but not including endBlockNumber.
     *
     * @param startBlockNumber the starting block number
     * @param endBlockNumber the ending block number, non-inclusive
     * @return an array of BlockItem objects
     */
    public static BlockItem[] createNumberOfVerySimpleBlocks(final long startBlockNumber, final int endBlockNumber) {
        final int numberOfBlocks = (int) (endBlockNumber - startBlockNumber);
        final BlockItem[] blockItems = new BlockItem[numberOfBlocks * 3];
        for (long blockNumber = startBlockNumber; blockNumber < endBlockNumber; blockNumber++) {
            final int i = (int) ((blockNumber - startBlockNumber) * 3);
            blockItems[i] = sampleBlockHeader(blockNumber);
            blockItems[i + 1] = sampleRoundHeader(blockNumber * 10);
            blockItems[i + 2] = sampleBlockProof(blockNumber);
        }
        return blockItems;
    }

    /**
     * Creates an array of BlockItem objects representing a very simple block stream of N blocks.
     *
     * @param numberOfBlocks the number of blocks to create
     * @return an array of BlockItem objects
     */
    public static BlockItemUnparsed[] createNumberOfVerySimpleBlocksUnparsed(final int numberOfBlocks) {
        final BlockItemUnparsed[] blockItems = new BlockItemUnparsed[numberOfBlocks * 3];
        for (int i = 0; i < blockItems.length; i += 3) {
            long blockNumber = i / 3;
            blockItems[i] = sampleBlockHeaderUnparsed(blockNumber);
            blockItems[i + 1] = sampleRoundHeaderUnparsed(blockNumber * 10);
            blockItems[i + 2] = sampleBlockProofUnparsed(blockNumber);
        }
        return blockItems;
    }

    private SimpleTestBlockItemBuilder() {}
}
