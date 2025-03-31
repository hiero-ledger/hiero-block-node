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

/**
 * A utility class to create sample BlockItem objects for testing purposes.
 */
public class SimpleTestBlockItemBuilder {

    public static BlockItem sampleBlockHeader(long blockNumber) {
        return new BlockItem(new OneOf<>(
                ItemOneOfType.BLOCK_HEADER,
                new BlockHeader(
                        new SemanticVersion(1, 2, 3, "a", "b"),
                        new SemanticVersion(4, 5, 6, "c", "d"),
                        blockNumber,
                        Bytes.wrap("hash".getBytes()),
                        new Timestamp(123L, 456),
                        BlockHashAlgorithm.SHA2_384)));
    }

    public static BlockItem sampleRoundHeader(long roundNumber) {
        return new BlockItem(new OneOf<>(ItemOneOfType.ROUND_HEADER, new RoundHeader(roundNumber)));
    }

    public static BlockItem sampleBlockProof(long blockNumber) {
        return new BlockItem(new OneOf<>(
                ItemOneOfType.BLOCK_PROOF,
                new BlockProof(
                        blockNumber,
                        Bytes.wrap("previousBlockRootHash".getBytes()),
                        Bytes.wrap("startOfBlockStateRootHash".getBytes()),
                        Bytes.wrap("signature".getBytes()),
                        Collections.emptyList())));
    }

    /**
     * Creates an array of BlockItem objects representing a very simple block stream of N blocks.
     *
     * @param numberOfBlocks the number of blocks to create
     * @return an array of BlockItem objects
     */
    public static BlockItem[] createNumberOfVerySimpleBlocks(int numberOfBlocks) {
        BlockItem[] blockItems = new BlockItem[numberOfBlocks * 3];
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
    public static BlockItem[] createNumberOfVerySimpleBlocks(long startBlockNumber, int endBlockNumber) {
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
}
