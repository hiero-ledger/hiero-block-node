// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.blocks;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.BlockItem.ItemOneOfType;
import com.hedera.hapi.block.stream.BlockProof;
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
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.spi.blockmessaging.BlockItems;

/**
 * A utility class to create sample BlockItem objects for testing purposes.
 */
public final class TestBlockBuilder {
    private static final Bytes RANDOM_HALF_MB;

    static {
        final Random random = new Random(1435134141854542L);
        final byte[] randomBytes = new byte[512 * 1024];
        random.nextBytes(randomBytes);
        RANDOM_HALF_MB = Bytes.wrap(randomBytes);
    }

    // Required to quiet warnings.
    private TestBlockBuilder() {}

    public static BlockHeader createHeader(final long blockNumber) {
        return new BlockHeader(
                new SemanticVersion(1, 2, 3, "a", "b"),
                new SemanticVersion(4, 5, 6, "c", "d"),
                blockNumber,
                new Timestamp(123L, 456),
                BlockHashAlgorithm.SHA2_384);
    }

    public static BlockItem sampleHeader(final long blockNumber) {
        return new BlockItem(new OneOf<>(ItemOneOfType.BLOCK_HEADER, createHeader(blockNumber)));
    }

    /**
     * Creates a sample BlockItem representing a block header with the given block number and consensus time.
     */
    public static BlockItem sampleHeader(final long blockNumber, Instant consensusTime) {
        return new BlockItem(new OneOf<>(
                ItemOneOfType.BLOCK_HEADER,
                new BlockHeader(
                        new SemanticVersion(1, 2, 3, "a", "b"),
                        new SemanticVersion(4, 5, 6, "c", "d"),
                        blockNumber,
                        new Timestamp(consensusTime.getEpochSecond(), consensusTime.getNano()),
                        BlockHashAlgorithm.SHA2_384)));
    }

    public static Bytes createHeaderUnparsed(final long blockNumber) {
        return BlockHeader.PROTOBUF.toBytes(createHeader(blockNumber));
    }

    public static BlockItemUnparsed sampleHeaderUnparsed(final long blockNumber) {
        return BlockItemUnparsed.newBuilder()
                .blockHeader(createHeaderUnparsed(blockNumber))
                .build();
    }

    /**
     * Creates a sample BlockItemUnparsed representing a block header with the given block number and consensus time.
     */
    public static BlockItemUnparsed sampleHeaderUnparsed(final long blockNumber, final Instant consensusTime) {
        //noinspection DataFlowIssue
        return BlockItemUnparsed.newBuilder()
                .blockHeader(BlockHeader.PROTOBUF.toBytes(
                        sampleHeader(blockNumber, consensusTime).blockHeader()))
                .build();
    }

    private static BlockItemUnparsed sampleBrokenBlockHeaderUnparsed(final long blockNumber) {
        final Bytes valid = createHeaderUnparsed(blockNumber);
        final byte[] arr = valid.toByteArray();
        arr[0] = (byte) (arr[0] ^ 0xFF); // flip the first byte to break the header
        return BlockItemUnparsed.newBuilder().blockHeader(Bytes.wrap(arr)).build();
    }

    private static BlockItemUnparsed sampleNullHeaderUnparsed() {
        return BlockItemUnparsed.newBuilder().blockHeader(null).build();
    }

    public static RoundHeader createRoundHeader(final long roundNumber) {
        return new RoundHeader(roundNumber);
    }

    public static BlockItem sampleRoundHeader(final long roundNumber) {
        return new BlockItem(new OneOf<>(ItemOneOfType.ROUND_HEADER, createRoundHeader(roundNumber)));
    }

    public static Bytes createRoundHeaderUnparsed(final long roundNumber) {
        return RoundHeader.PROTOBUF.toBytes(createRoundHeader(roundNumber));
    }

    public static BlockItemUnparsed sampleRoundHeaderUnparsed(final long roundNumber) {
        return BlockItemUnparsed.newBuilder()
                .roundHeader(createRoundHeaderUnparsed(roundNumber))
                .build();
    }

    public static BlockFooter createFooter(final long blockNumber) {
        return BlockFooter.newBuilder()
                .previousBlockRootHash(Bytes.wrap("previous_block_root_hash".getBytes()))
                .rootHashOfAllBlockHashesTree(Bytes.wrap("root_hash_of_all_blocks".getBytes()))
                .startOfBlockStateRootHash(Bytes.wrap("start_block_state_root_hash".getBytes()))
                .build();
    }

    public static BlockItem sampleFooter(final long blockNumber) {
        return new BlockItem(new OneOf<>(ItemOneOfType.BLOCK_FOOTER, createFooter(blockNumber)));
    }

    public static Bytes createFooterUnparsed(final long blockNumber) {
        return BlockFooter.PROTOBUF.toBytes(createFooter(blockNumber));
    }

    public static BlockItemUnparsed sampleFooterUnparsed(final long blockNumber) {
        return BlockItemUnparsed.newBuilder()
                .blockFooter(createFooterUnparsed(blockNumber))
                .build();
    }

    public static BlockProof createProof(final long blockNumber) {
        final TssSignedBlockProof.Builder tssBuilder =
                TssSignedBlockProof.newBuilder().blockSignature(Bytes.wrap("block_signature".getBytes()));
        final BlockProof blockProof = BlockProof.newBuilder()
                .block(blockNumber)
                .signedBlockProof(tssBuilder)
                .build();
        return blockProof;
    }

    public static BlockItem sampleProof(final long blockNumber) {
        return new BlockItem(new OneOf<>(ItemOneOfType.BLOCK_PROOF, createProof(blockNumber)));
    }

    public static Bytes createProofUnparsed(final long blockNumber) {
        return BlockProof.PROTOBUF.toBytes(createProof(blockNumber));
    }

    public static BlockItemUnparsed sampleProofUnparsed(final long blockNumber) {
        return BlockItemUnparsed.newBuilder()
                .blockProof(createProofUnparsed(blockNumber))
                .build();
    }

    public static BlockItemUnparsed sampleBrokenProofUnparsed(final long blockNumber) {
        final Bytes valid = createProofUnparsed(blockNumber);
        final byte[] arr = valid.toByteArray();
        arr[0] = (byte) (arr[0] ^ 0xFF); // flip the first byte to break the proof
        return BlockItemUnparsed.newBuilder().blockProof(Bytes.wrap(arr)).build();
    }

    /**
     * Create an EventHeader with no parents and no signature middle bit.
     */
    public static BlockItem sampleLargeEventHeader() {
        return new BlockItem(
                new OneOf<>(ItemOneOfType.EVENT_HEADER, new EventHeader(EventCore.DEFAULT, Collections.emptyList())));
    }

    /// Creates a single simple test block.
    public static TestBlock generateBlockWithNumber(final long blockNumber) {
        return new TestBlock(blockNumber, createSimpleBlockWithNumber(blockNumber));
    }

    /// Creates a list of simple test blocks.
    public static List<TestBlock> generateBlocksInRange(final long startBlockNumber, final long endBlockNumber) {
        assertTrue(startBlockNumber <= endBlockNumber);
        assertTrue(startBlockNumber >= 0);
        final ArrayList<TestBlock> result = new ArrayList<>();
        for (long i = startBlockNumber; i <= endBlockNumber; i++) {
            result.add(generateBlockWithNumber(i));
        }
        return result;
    }

    /// Creates a single simple test block.
    public static TestBlock generateBlockWithNumber(final long blockNumber, final Instant blockTime) {
        return new TestBlock(blockNumber, createSimpleBlockUnparsedWithNumber(blockNumber, blockTime));
    }

    ///  Creates a list of simple test blocks.
    public static List<TestBlock> generateBlocksInRange(
            final long startBlockNumber,
            final long endBlockNumber,
            final Instant firstBlockConsensusTime,
            final Duration consensusTimeBetweenBlocks) {
        assertTrue(startBlockNumber <= endBlockNumber);
        assertTrue(startBlockNumber >= 0);
        final List<TestBlock> result = new ArrayList<>();
        Instant blockTime = firstBlockConsensusTime;
        for (long blockNumber = startBlockNumber; blockNumber <= endBlockNumber; blockNumber++) {
            result.add(generateBlockWithNumber(blockNumber, blockTime));
            // Increment the block time by the consensus time between blocks
            blockTime = blockTime.plus(consensusTimeBetweenBlocks);
        }
        return result;
    }

    /// Creates a single large 2.5MB data test block.
    public static TestBlock generateLargeBlockWithNumber(final long blockNumber) {
        return new TestBlock(blockNumber, createLargeBlockWithNumber(blockNumber));
    }

    /// Creates a list of large 2.5MB data blocks.
    public static List<TestBlock> generateLargeBlocksInRange(final long startBlockNumber, final long endBlockNumber) {
        assertTrue(startBlockNumber <= endBlockNumber);
        assertTrue(startBlockNumber >= 0);
        final List<TestBlock> result = new ArrayList<>();
        for (long blockNumber = startBlockNumber; blockNumber <= endBlockNumber; blockNumber++) {
            result.add(generateLargeBlockWithNumber(blockNumber));
        }
        return result;
    }

    /// Creates a single simple block with a broken header.
    public static BlockUnparsed generateBlockWithBrokenHeader(final long blockNumber) {
        assertTrue(blockNumber >= 0);
        final BlockItemUnparsed[] blockItems = new BlockItemUnparsed[3];
        blockItems[0] = sampleBrokenBlockHeaderUnparsed(blockNumber);
        blockItems[1] = sampleRoundHeaderUnparsed(blockNumber * 10L);
        blockItems[2] = sampleProofUnparsed(blockNumber);
        return BlockUnparsed.newBuilder().blockItems(blockItems).build();
    }

    /// Creates a single simple block with null header bytes.
    public static BlockUnparsed generateBlockWithNullHeaderBytes(final long blockNumber) {
        assertTrue(blockNumber >= 0);
        final BlockItemUnparsed[] blockItems = new BlockItemUnparsed[3];
        blockItems[0] = sampleNullHeaderUnparsed();
        blockItems[1] = sampleRoundHeaderUnparsed(blockNumber * 10L);
        blockItems[2] = sampleProofUnparsed(blockNumber);
        return BlockUnparsed.newBuilder().blockItems(blockItems).build();
    }

    /// Creates a single simple block with broken proof.
    public static BlockUnparsed generateBlockWithBrokenProof(final long blockNumber) {
        assertTrue(blockNumber >= 0);
        final BlockItemUnparsed[] blockItems = new BlockItemUnparsed[3];
        blockItems[0] = sampleHeaderUnparsed(blockNumber);
        blockItems[1] = sampleRoundHeaderUnparsed(blockNumber * 10L);
        blockItems[2] = sampleBrokenProofUnparsed(blockNumber);
        return BlockUnparsed.newBuilder().blockItems(blockItems).build();
    }

    private static Block createSimpleBlockWithNumber(final long blockNumber) {
        assertTrue(blockNumber >= 0);
        final List<BlockItem> blockItems = new ArrayList<>();
        blockItems.add(sampleHeader(blockNumber));
        blockItems.add(sampleRoundHeader(blockNumber * 10L));
        blockItems.add(sampleFooter(blockNumber * 10L));
        blockItems.add(sampleProof(blockNumber));
        return Block.newBuilder().items(blockItems).build();
    }

    private static BlockUnparsed createSimpleBlockUnparsedWithNumber(final long blockNumber, final Instant blockTime) {
        final List<BlockItemUnparsed> items = new ArrayList<>();
        items.add(sampleHeaderUnparsed(blockNumber, blockTime));
        items.add(sampleRoundHeaderUnparsed(blockNumber * 10L));
        items.add(sampleFooterUnparsed(blockNumber * 10L));
        items.add(sampleProofUnparsed(blockNumber));
        return BlockUnparsed.newBuilder().blockItems(items).build();
    }

    private static Block createLargeBlockWithNumber(final long blockNumber) {
        final List<BlockItem> items = new ArrayList<>();
        items.add(sampleHeader(blockNumber));
        items.add(sampleRoundHeader(blockNumber * 10L));
        items.add(sampleLargeEventHeader());
        items.add(sampleLargeEventHeader());
        items.add(sampleLargeEventHeader());
        items.add(sampleLargeEventHeader());
        items.add(sampleLargeEventHeader());
        items.add(sampleProof(blockNumber));
        return Block.newBuilder().items(items).build();
    }

    @SuppressWarnings("all")
    public static BlockItems toBlockItems(
            final List<BlockItem> itemsToConvert,
            final long blockNumber,
            final boolean isStartOfNewBlock,
            final boolean isEndOfBlock) {
        final List<BlockItemUnparsed> converted = convertToUnparsedItems(itemsToConvert);
        return new BlockItems(converted, blockNumber, isStartOfNewBlock, isEndOfBlock);
    }

    public static List<BlockItemUnparsed> convertToUnparsedItems(final List<BlockItem> itemsToConvert) {
        final List<BlockItemUnparsed> result = new ArrayList<>();
        for (final BlockItem item : itemsToConvert) {
            try {
                result.add(BlockItemUnparsed.PROTOBUF.parse(BlockItem.PROTOBUF.toBytes(item)));
            } catch (final ParseException e) {
                fail("Failed to convert BlockItem to BlockItemUnparsed", e);
            }
        }
        return List.copyOf(result);
    }

    public static List<BlockItem> convertToItems(final List<BlockItemUnparsed> unparsedItemsToConvert) {
        final List<BlockItem> result = new ArrayList<>();
        for (final BlockItemUnparsed item : unparsedItemsToConvert) {
            try {
                result.add(BlockItem.PROTOBUF.parse(BlockItemUnparsed.PROTOBUF.toBytes(item)));
            } catch (ParseException e) {
                fail("Failed to convert BlockItemUnparsed to BlockItem", e);
            }
        }
        return List.copyOf(result);
    }
}
