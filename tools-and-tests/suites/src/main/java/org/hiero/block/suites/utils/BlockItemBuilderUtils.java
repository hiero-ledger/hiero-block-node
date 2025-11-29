// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.utils;

import static org.junit.jupiter.api.Assertions.assertTrue;

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
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.*;

/**
 * A utility class to create sample BlockItem objects for testing purposes.
 * Inspired by BlockItemBuilderUtils in block-node-app testFixtures.
 */
public final class BlockItemBuilderUtils {
    private static final Bytes RANDOM_HALF_MB;

    static {
        final Random random = new Random(1435134141854542L);
        final byte[] randomBytes = new byte[512 * 1024];
        random.nextBytes(randomBytes);
        RANDOM_HALF_MB = Bytes.wrap(randomBytes);
    }

    // Required to quiet warnings.
    private BlockItemBuilderUtils() {}

    public static BlockHeader createBlockHeader(final long blockNumber) {
        return createBlockHeader(blockNumber, 1, 68);
    }

    public static BlockHeader createBlockHeader(final long blockNumber, int hapiMajor, int hapiMinor) {
        return new BlockHeader(
                new SemanticVersion(hapiMajor, hapiMinor, 3, "a", "b"),
                new SemanticVersion(4, 5, 6, "c", "d"),
                blockNumber,
                new Timestamp(123L, 456),
                BlockHashAlgorithm.SHA2_384);
    }

    public static RoundHeader createRoundHeader(final long roundNumber) {
        return new RoundHeader(roundNumber);
    }

    public static BlockProof createBlockProof(final long blockNumber) {
        return BlockProof.newBuilder()
                .signedBlockProof(TssSignedBlockProof.newBuilder().build())
                .block(blockNumber)
                .previousBlockRootHash(Bytes.wrap("previousBlockRootHash"))
                .startOfBlockStateRootHash(Bytes.wrap("startOfBlockStateRootHash"))
                .blockSignature(Bytes.wrap("block_signature"))
                .verificationKey(Bytes.wrap("verification_key"))
                .build();
    }

    public static BlockFooter createBlockFooter(final long blockNumber) {
        return BlockFooter.newBuilder()
                .previousBlockRootHash(Bytes.wrap("previous_block_root_hash".getBytes()))
                .rootHashOfAllBlockHashesTree(Bytes.wrap("root_hash_of_all_blocks".getBytes()))
                .startOfBlockStateRootHash(Bytes.wrap("start_block_state_root_hash".getBytes()))
                .build();
    }

    public static BlockItem sampleBlockFooter(final long blockNumber) {
        return new BlockItem(new OneOf<>(ItemOneOfType.BLOCK_FOOTER, createBlockFooter(blockNumber)));
    }

    public static BlockItem sampleBlockHeader(final long blockNumber) {
        return new BlockItem(new OneOf<>(ItemOneOfType.BLOCK_HEADER, createBlockHeader(blockNumber)));
    }

    public static BlockItem sampleBlockHeader(final long blockNumber, int hapiMajor, int hapiMinor) {
        return new BlockItem(
                new OneOf<>(ItemOneOfType.BLOCK_HEADER, createBlockHeader(blockNumber, hapiMajor, hapiMinor)));
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

    public static BlockItem sampleBlockProof(final long blockNumber) {
        return new BlockItem(new OneOf<>(ItemOneOfType.BLOCK_PROOF, createBlockProof(blockNumber)));
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
        final int blockItemTypesRepresented = 4;
        for (int blockNumber = (int) startBlockNumber; blockNumber <= endBlockNumber; blockNumber++) {
            final int i = (blockNumber - (int) startBlockNumber) * blockItemTypesRepresented;
            blockItems[i] = sampleBlockHeader(blockNumber);
            blockItems[i + 1] = sampleRoundHeader(blockNumber * 10L);
            blockItems[i + 2] = sampleBlockFooter(blockNumber);
            blockItems[i + 3] = sampleBlockProof(blockNumber);
        }
        return blockItems;
    }

    public static BlockItem[] createSimpleBlockWithNumber(final long blockNumber) {
        final int blockItemTypesRepresented = 4;
        final BlockItem[] blockItems = new BlockItem[blockItemTypesRepresented];
        blockItems[0] = sampleBlockHeader(blockNumber);
        blockItems[1] = sampleRoundHeader(blockNumber * 10L);
        blockItems[2] = sampleBlockFooter(blockNumber);
        blockItems[3] = sampleBlockProof(blockNumber);
        return blockItems;
    }
}
