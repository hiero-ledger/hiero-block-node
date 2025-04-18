// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.blocks;

import com.hedera.hapi.block.stream.Block;
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
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.LongStream;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.internal.BlockItemUnparsed;

/**
 * A utility class to create sample BlockItem objects for testing purposes.
 */
public final class SimpleTestBlockItemBuilder {
    public static BlockHeader createBlockHeader(final long blockNumber) {
        return new BlockHeader(
                new SemanticVersion(1, 2, 3, "a", "b"),
                new SemanticVersion(4, 5, 6, "c", "d"),
                blockNumber,
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

    /**
     * Creates a sample BlockItem representing a block header with the given block number and consensus time.
     */
    public static BlockItem sampleBlockHeader(final long blockNumber, Instant consensusTime) {
        return new BlockItem(new OneOf<>(
                ItemOneOfType.BLOCK_HEADER,
                new BlockHeader(
                        new SemanticVersion(1, 2, 3, "a", "b"),
                        new SemanticVersion(4, 5, 6, "c", "d"),
                        blockNumber,
                        new Timestamp(consensusTime.getEpochSecond(), consensusTime.getNano()),
                        BlockHashAlgorithm.SHA2_384)));
    }

    /**
     * Creates a sample BlockItemUnparsed representing a block header with the given block number and consensus time.
     */
    public static BlockItemUnparsed sampleBlockHeaderUnparsed(final long blockNumber, Instant consensusTime) {
        //noinspection DataFlowIssue
        return BlockItemUnparsed.newBuilder()
                .blockHeader(BlockHeader.PROTOBUF.toBytes(
                        sampleBlockHeader(blockNumber, consensusTime).blockHeader()))
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
        return createNumberOfVerySimpleBlocks(0, numberOfBlocks - 1);
    }

    /**
     * Creates an array of BlockItem objects representing a very simple block stream of blocks from startBlockNumber to
     * but not including endBlockNumber.
     *
     * @param startBlockNumber the starting block number
     * @param endBlockNumber the ending block number, inclusive
     * @return an array of BlockItem objects
     */
    public static BlockItem[] createNumberOfVerySimpleBlocks(final long startBlockNumber, final long endBlockNumber) {
        assert startBlockNumber <= endBlockNumber;
        assert startBlockNumber >= 0;
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

    /**
     * Creates an array of BlockItem objects representing a very simple block stream of blocks from startBlockNumber to
     * but not including endBlockNumber.
     *
     * @param startBlockNumber the starting block number
     * @param endBlockNumber the ending block number, inclusive
     * @param firstBlockConsensusTime the consensus time of the first block
     * @param consensusTimeBetweenBlocks the time between blocks starts with the first block at 2025-01-01T00:00:00Z
     * @return an array of BlockItem objects
     */
    public static BlockItemUnparsed[] createNumberOfVerySimpleBlocksUnparsed(
            final long startBlockNumber,
            final long endBlockNumber,
            Instant firstBlockConsensusTime,
            Duration consensusTimeBetweenBlocks) {
        assert startBlockNumber <= endBlockNumber;
        assert startBlockNumber >= 0;
        final int numberOfBlocks = (int) (endBlockNumber - startBlockNumber + 1);
        final BlockItemUnparsed[] blockItems = new BlockItemUnparsed[numberOfBlocks * 3];
        Instant blockTime = firstBlockConsensusTime;
        for (int blockNumber = (int) startBlockNumber; blockNumber <= endBlockNumber; blockNumber++) {
            final int i = (blockNumber - (int) startBlockNumber) * 3;
            blockItems[i] = sampleBlockHeaderUnparsed(blockNumber, blockTime);
            blockItems[i + 1] = sampleRoundHeaderUnparsed(blockNumber * 10L);
            blockItems[i + 2] = sampleBlockProofUnparsed(blockNumber);
            // Increment the block time by the consensus time between blocks
            blockTime = blockTime.plus(consensusTimeBetweenBlocks);
        }
        return blockItems;
    }

    /**
     * Creates an array of BlockAccessor objects representing a very simple block stream of blocks from startBlockNumber
     * to but endBlockNumber inclusive.
     *
     * @param startBlockNumber the starting block number
     * @param endBlockNumber the ending block number, inclusive
     * @return an array of BlockAccessor objects
     */
    public static BlockAccessor[] createNumberOfVerySimpleBlockAccessors(
            final int startBlockNumber, final int endBlockNumber) {
        return LongStream.range(startBlockNumber, endBlockNumber + 1)
                .mapToObj(bn -> {
                    BlockItem[] blockItems = createNumberOfVerySimpleBlocks(bn, bn);
                    Block block = new Block(Arrays.asList(blockItems));
                    return new BlockAccessor() {
                        @Override
                        public long blockNumber() {
                            return bn;
                        }

                        @Override
                        public Block block() {
                            return block;
                        }
                    };
                })
                .toArray(BlockAccessor[]::new);
    }

    /**
     * Creates an array of BlockItem objects representing a very simple block stream of N blocks.
     *
     * @param numberOfBlocks the number of blocks to create
     * @return an array of BlockItem objects
     */
    public static BlockItemUnparsed[] createNumberOfVerySimpleBlocksUnparsed(final int numberOfBlocks) {
        return createNumberOfVerySimpleBlocksUnparsed(0, numberOfBlocks);
    }

    public static BlockItemUnparsed[] createNumberOfVerySimpleBlocksUnparsed(
            final int startBlockNumber, final int endBlockNumber) {
        assert startBlockNumber < endBlockNumber;
        assert startBlockNumber >= 0;
        final int numberOfBlocks = endBlockNumber - startBlockNumber;
        final BlockItemUnparsed[] blockItems = new BlockItemUnparsed[numberOfBlocks * 3];
        for (int i = 0; i < blockItems.length; i += 3) {
            long blockNumber = i / 3;
            blockItems[i] = sampleBlockHeaderUnparsed(blockNumber);
            blockItems[i + 1] = sampleRoundHeaderUnparsed(blockNumber * 10);
            blockItems[i + 2] = sampleBlockProofUnparsed(blockNumber);
        }
        return blockItems;
    }

    public static BlockItems[] createNumberOfSimpleBlockBatches(final int numberOfBatches) {
        return createNumberOfSimpleBlockBatches(0, numberOfBatches);
    }

    public static BlockItems[] createNumberOfSimpleBlockBatches(final int startBlockNumber, final int endBlockNumber) {
        assert startBlockNumber < endBlockNumber;
        assert startBlockNumber >= 0;
        final int numberOfBatches = endBlockNumber - startBlockNumber;
        final BlockItems[] batches = new BlockItems[numberOfBatches];
        for (int i = startBlockNumber; i < endBlockNumber; i++) {
            List<BlockItemUnparsed> oneBatch = new ArrayList<>(3);
            oneBatch.add(sampleBlockHeaderUnparsed(i));
            oneBatch.add(sampleRoundHeaderUnparsed(i * 10L));
            oneBatch.add(sampleBlockProofUnparsed(i));
            batches[i - startBlockNumber] = new BlockItems(oneBatch, i);
        }
        return batches;
    }

    public static BlockItem[] createSimpleBlockWithNumber(final long blockNumber) {
        final BlockItem[] blockItems = new BlockItem[3];
        blockItems[0] = sampleBlockHeader(blockNumber);
        blockItems[1] = sampleRoundHeader(blockNumber * 10L);
        blockItems[2] = sampleBlockProof(blockNumber);
        return blockItems;
    }

    public static BlockItemUnparsed[] createSimpleBlockUnparsedWithNumber(final long blockNumber) {
        final BlockItemUnparsed[] blockItems = new BlockItemUnparsed[3];
        blockItems[0] = sampleBlockHeaderUnparsed(blockNumber);
        blockItems[1] = sampleRoundHeaderUnparsed(blockNumber * 10L);
        blockItems[2] = sampleBlockProofUnparsed(blockNumber);
        return blockItems;
    }

    private SimpleTestBlockItemBuilder() {}
}
