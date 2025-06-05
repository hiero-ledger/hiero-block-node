// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.blocks;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import java.util.List;
import java.util.Objects;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;

/**
 * A simple in-memory block accessor class. This class is intended to be used
 * for testing purposes only!
 */
public final class InMemoryBlockAccessor implements BlockAccessor {
    /** Internal in-memory reference of the accessible block */
    private final Block block;
    /** The block number of the accessible block */
    private final long blockNumber;

    /**
     * Constructor.
     *
     * @param blockItems of the block, must not be empty, first item must be a
     * {@link com.hedera.hapi.block.stream.output.BlockHeader} and last item
     * must be a {@link com.hedera.hapi.block.stream.BlockProof}.
     * @throws IllegalArgumentException if preconditions are not met
     */
    public InMemoryBlockAccessor(final List<BlockItem> blockItems) {
        if (blockItems.isEmpty()) {
            throw new IllegalArgumentException("Block items list cannot be empty");
        }
        // Ensure the first item is a BlockHeader
        if (!blockItems.getFirst().hasBlockHeader()) {
            throw new IllegalArgumentException("First block item must be a BlockHeader");
        }
        // Ensure the last item is a BlockProof
        if (!blockItems.getLast().hasBlockProof()) {
            throw new IllegalArgumentException("Last block item must be a BlockProof");
        }
        // Create a Block from the provided block items
        block = Block.newBuilder().items(blockItems).build();
        blockNumber =
                Objects.requireNonNull(block.items().getFirst().blockHeader()).number();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long blockNumber() {
        return blockNumber;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Block block() {
        return block;
    }
}
