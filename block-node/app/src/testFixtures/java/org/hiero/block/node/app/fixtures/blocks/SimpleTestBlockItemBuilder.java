// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.blocks;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.BlockItem.ItemOneOfType;
import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.ChainOfTrustProof;
import com.hedera.hapi.block.stream.MerkleSiblingHash;
import com.hedera.hapi.block.stream.TssSignedBlockProof;
import com.hedera.hapi.block.stream.input.EventHeader;
import com.hedera.hapi.block.stream.input.RoundHeader;
import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.BlockHashAlgorithm;
import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.hapi.platform.event.EventCore;
import com.hedera.pbj.runtime.OneOf;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.LongStream;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;

/**
 * A utility class to create sample BlockItem objects for testing purposes.
 */
public final class SimpleTestBlockItemBuilder {
    private static final Bytes RANDOM_HALF_MB;

    static {
        final Random random = new Random(1435134141854542L);
        final byte[] randomBytes = new byte[512 * 1024];
        random.nextBytes(randomBytes);
        RANDOM_HALF_MB = Bytes.wrap(randomBytes);
    }

    // Required to quiet warnings.
    private SimpleTestBlockItemBuilder() {}

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

        TssSignedBlockProof.Builder tssBuildder =
                TssSignedBlockProof.newBuilder().blockSignature(Bytes.wrap("block_signature".getBytes()));

        return BlockProof.newBuilder()
                .block(blockNumber)
                .siblingHashes(List.of(MerkleSiblingHash.newBuilder()
                        .siblingHash(Bytes.wrap("sibling_hash".getBytes()))
                        .build()))
                .signedBlockProof(tssBuildder)
                .verificationKey(Bytes.wrap("verification_key".getBytes()))
                .verificationKeyProof(ChainOfTrustProof.newBuilder()
                        .wrapsProof(Bytes.wrap("verificationKeyProof".getBytes()))
                        .build())
                .build();
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

    private static BlockItemUnparsed sampleBrokenBlockHeaderUnparsed(final long blockNumber) {
        final Bytes valid = createBlockHeaderUnparsed(blockNumber);
        final byte[] arr = valid.toByteArray();
        arr[0] = (byte) (arr[0] ^ 0xFF); // flip the first byte to break the header
        return BlockItemUnparsed.newBuilder().blockHeader(Bytes.wrap(arr)).build();
    }

    private static BlockItemUnparsed sampleNullBlockHeaderUnparsed() {
        return BlockItemUnparsed.newBuilder().blockHeader(null).build();
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

    public static BlockFooter createBlockFooter(final long blockNumber) {
        return BlockFooter.newBuilder()
                .previousBlockRootHash(Bytes.wrap("previous_block_root_hash".getBytes()))
                .rootHashOfAllBlockHashesTree(Bytes.wrap("root_hash_of_all_blocks".getBytes()))
                .startOfBlockStateRootHash(Bytes.wrap("start_block_state_root_hash".getBytes()))
                .build();
    }

    public static Bytes createBlockFooterUnparsed(final long blockNumber) {
        return BlockFooter.PROTOBUF.toBytes(createBlockFooter(blockNumber));
    }

    public static BlockItem sampleBlockFooter(final long blockNumber) {
        return new BlockItem(new OneOf<>(ItemOneOfType.BLOCK_FOOTER, createBlockFooter(blockNumber)));
    }

    public static BlockItemUnparsed sampleBlockFooterUnparsed(final long blockNumber) {
        return BlockItemUnparsed.newBuilder()
                .blockFooter(Bytes.wrap(("block_footer_" + blockNumber).getBytes()))
                .build();
    }

    public static BlockItem sampleRoundHeader(final long roundNumber) {
        return new BlockItem(new OneOf<>(ItemOneOfType.ROUND_HEADER, createRoundHeader(roundNumber)));
    }

    /**
     * Create an EventHeader with no parents and no signature middle bit.
     */
    public static BlockItem sampleLargeEventHeader() {
        return new BlockItem(
                new OneOf<>(ItemOneOfType.EVENT_HEADER, new EventHeader(EventCore.DEFAULT, Collections.emptyList())));
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

    public static BlockItemUnparsed sampleBrokenBlockProofUnparsed(final long blockNumber) {
        final Bytes valid = createBlockProofUnparsed(blockNumber);
        final byte[] arr = valid.toByteArray();
        arr[0] = (byte) (arr[0] ^ 0xFF); // flip the first byte to break the proof
        return BlockItemUnparsed.newBuilder().blockProof(Bytes.wrap(arr)).build();
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
     * endBlockNumber inclusive.
     *
     * @param startBlockNumber the starting block number
     * @param endBlockNumber the ending block number, inclusive
     * @return an array of BlockItem objects
     */
    public static BlockItem[] createNumberOfVerySimpleBlocks(final long startBlockNumber, final long endBlockNumber) {
        assertTrue(startBlockNumber <= endBlockNumber);
        assertTrue(startBlockNumber >= 0);
        final int numberOfBlocks = (int) (endBlockNumber - startBlockNumber + 1);
        final BlockItem[] blockItems = new BlockItem[numberOfBlocks * 3];
        for (int blockNumber = (int) startBlockNumber; blockNumber <= endBlockNumber; blockNumber++) {
            final int i = (blockNumber - (int) startBlockNumber) * 3;
            blockItems[i] = sampleBlockHeader(blockNumber);
            blockItems[i + 1] = sampleRoundHeader(blockNumber * 10L);
            blockItems[i + 2] = sampleBlockFooter(blockNumber * 10L);
            blockItems[i + 2] = sampleBlockProof(blockNumber); // TODO Improve proof generation
        }
        return blockItems;
    }

    /**
     * Creates a number of block items, batched per block.
     *
     * @param startBlockNumber the starting block number
     * @param endBlockNumber the ending block number, inclusive
     * @return a list of block items that are batched per block
     */
    public static List<Block> createNumberOfVerySimpleBlocksBatched(
            final int startBlockNumber, final int endBlockNumber) {
        final ArrayList<Block> result = new ArrayList<>();
        for (int i = startBlockNumber; i <= endBlockNumber; i++) {
            result.add(Block.newBuilder()
                    .items(createNumberOfVerySimpleBlocks(i, i))
                    .build());
        }
        return result;
    }

    /**
     * Creates an array of BlockItem objects representing a simple block stream of blocks with large 2.5MB data each
     * from startBlockNumber to endBlockNumber inclusive.
     *
     * @param startBlockNumber the starting block number
     * @param endBlockNumber the ending block number, inclusive
     * @return an array of BlockItem objects
     */
    public static BlockItem[] createNumberOfLargeBlocks(final long startBlockNumber, final long endBlockNumber) {
        assertTrue(startBlockNumber <= endBlockNumber);
        assertTrue(startBlockNumber >= 0);
        final int numberOfBlocks = (int) (endBlockNumber - startBlockNumber + 1);
        final BlockItem[] blockItems = new BlockItem[numberOfBlocks * 8];
        for (int blockNumber = (int) startBlockNumber; blockNumber <= endBlockNumber; blockNumber++) {
            final int i = (blockNumber - (int) startBlockNumber) * 8;
            blockItems[i] = sampleBlockHeader(blockNumber);
            blockItems[i + 1] = sampleRoundHeader(blockNumber * 10L);
            blockItems[i + 2] = sampleLargeEventHeader();
            blockItems[i + 3] = sampleLargeEventHeader();
            blockItems[i + 4] = sampleLargeEventHeader();
            blockItems[i + 5] = sampleLargeEventHeader();
            blockItems[i + 6] = sampleLargeEventHeader();
            blockItems[i + 7] = sampleBlockProof(blockNumber);
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
        assertTrue(startBlockNumber <= endBlockNumber);
        assertTrue(startBlockNumber >= 0);
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
                    return new MinimalBlockAccessor(bn, block);
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
        return createNumberOfVerySimpleBlocksUnparsed(0, numberOfBlocks - 1);
    }

    public static BlockItemUnparsed[] createNumberOfVerySimpleBlocksUnparsed(
            final int startBlockNumber, final int endBlockNumber) {
        assertTrue(startBlockNumber <= endBlockNumber);
        assertTrue(startBlockNumber >= 0);
        final int numberOfBlocks = (endBlockNumber - startBlockNumber + 1);
        final BlockItemUnparsed[] blockItems = new BlockItemUnparsed[numberOfBlocks * 3];
        for (int blockNumber = startBlockNumber, i = 0; blockNumber <= endBlockNumber; blockNumber++) {
            blockItems[i++] = sampleBlockHeaderUnparsed(blockNumber);
            blockItems[i++] = sampleRoundHeaderUnparsed(blockNumber * 10L);
            blockItems[i++] = sampleBlockProofUnparsed(blockNumber);
        }
        return blockItems;
    }

    public static BlockItemUnparsed[] createNumberOfVerySimpleBlocksUnparsedWithBrokenHeaders(
            final int startBlockNumber, final int endBlockNumber) {
        assertTrue(startBlockNumber <= endBlockNumber);
        assertTrue(startBlockNumber >= 0);
        final int numberOfBlocks = (endBlockNumber - startBlockNumber + 1);
        final BlockItemUnparsed[] blockItems = new BlockItemUnparsed[numberOfBlocks * 3];
        for (int blockNumber = startBlockNumber, i = 0; blockNumber <= endBlockNumber; blockNumber++) {
            blockItems[i++] = sampleBrokenBlockHeaderUnparsed(blockNumber);
            blockItems[i++] = sampleRoundHeaderUnparsed(blockNumber * 10L);
            blockItems[i++] = sampleBlockProofUnparsed(blockNumber);
        }
        return blockItems;
    }

    public static BlockItemUnparsed[] createNumberOfVerySimpleBlocksUnparsedWithBrokenProofs(
            final int startBlockNumber, final int endBlockNumber) {
        assertTrue(startBlockNumber <= endBlockNumber);
        assertTrue(startBlockNumber >= 0);
        final int numberOfBlocks = (endBlockNumber - startBlockNumber + 1);
        final BlockItemUnparsed[] blockItems = new BlockItemUnparsed[numberOfBlocks * 3];
        for (int blockNumber = startBlockNumber, i = 0; blockNumber <= endBlockNumber; blockNumber++) {
            blockItems[i++] = sampleBlockHeaderUnparsed(blockNumber);
            blockItems[i++] = sampleRoundHeaderUnparsed(blockNumber * 10L);
            blockItems[i++] = sampleBrokenBlockProofUnparsed(blockNumber);
        }
        return blockItems;
    }

    public static BlockItemUnparsed[] createNumberOfVerySimpleBlocksUnparsedWithNullHeaderBytes(
            final int startBlockNumber, final int endBlockNumber) {
        assertTrue(startBlockNumber <= endBlockNumber);
        assertTrue(startBlockNumber >= 0);
        final int numberOfBlocks = (endBlockNumber - startBlockNumber + 1);
        final BlockItemUnparsed[] blockItems = new BlockItemUnparsed[numberOfBlocks * 3];
        for (int blockNumber = startBlockNumber, i = 0; blockNumber <= endBlockNumber; blockNumber++) {
            blockItems[i++] = sampleNullBlockHeaderUnparsed();
            blockItems[i++] = sampleRoundHeaderUnparsed(blockNumber * 10L);
            blockItems[i++] = sampleBlockProofUnparsed(blockNumber);
        }
        return blockItems;
    }

    public static BlockItems[] createNumberOfSimpleBlockBatches(final int numberOfBatches) {
        return createNumberOfSimpleBlockBatches(0, numberOfBatches);
    }

    public static BlockItems[] createNumberOfSimpleBlockBatches(final int startBlockNumber, final int endBlockNumber) {
        assertTrue(startBlockNumber <= endBlockNumber);
        assertTrue(startBlockNumber >= 0);
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

    public static BlockItems toBlockItems(final List<BlockItem> block) {
        return toBlockItems(block, true, block.getFirst().blockHeader().number());
    }

    @SuppressWarnings("all")
    public static BlockItems toBlockItems(
            final List<BlockItem> block, final boolean includeHeader, final long blockNumber) {
        final List<BlockItemUnparsed> result = new ArrayList<>();
        final List<BlockItem> blockItemsToConvert;
        if (!block.getFirst().hasBlockHeader()) {
            blockItemsToConvert = block;
        } else {
            blockItemsToConvert = includeHeader ? block : block.subList(1, block.size());
        }
        for (final BlockItem item : blockItemsToConvert) {
            try {
                result.add(BlockItemUnparsed.PROTOBUF.parse(BlockItem.PROTOBUF.toBytes(item)));
            } catch (final ParseException e) {
                fail("Failed to parse BlockItem to BlockItemUnparsed", e);
            }
        }
        return new BlockItems(result, blockNumber);
    }
}
