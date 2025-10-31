// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.blockmessaging;

import static org.hiero.block.node.spi.BlockNodePlugin.UNKNOWN_BLOCK_NUMBER;

import java.util.List;
import java.util.Objects;
import org.hiero.block.internal.BlockItemUnparsed;

/**
 * A record that holds a list of block items and the block number if items start with block header of a new block.
 * This is used to send block items throughout the server.The parsed block number for the start of a new block, is
 * included to avoid every consumer having to parse the block number from the block items.
 *
 * @param blockItems an immutable list of block items to handle.
 * @param blockNumber the number of the block containing these items.
 */
public record BlockItems(List<BlockItemUnparsed> blockItems, long blockNumber) {
    public BlockItems {
        Objects.requireNonNull(blockItems);
        if (blockItems.isEmpty()) {
            throw new IllegalArgumentException("Block items cannot be empty");
        }
        if (blockNumber < 0 && blockNumber != UNKNOWN_BLOCK_NUMBER) {
            throw new IllegalArgumentException("Block number cannot be negative unless it is UNKNOWN_BLOCK_NUMBER");
        }
    }

    /**
     * Helper method to check if these items include the start of a new block.
     *
     * @return true if these items include the start of a new block, false otherwise.
     */
    public boolean isStartOfNewBlock() {
        return blockItems.getFirst().hasBlockHeader();
    }

    /**
     * Helper method to check if this set of items is the end of a block, this is true of last item is a block proof.
     *
     * Note, this will change at some future point, when we fully handle multiple block proofs.
     *
     * @return true if last item is a block proof, false otherwise.
     */
    public boolean isEndOfBlock() {
        // BlockItems constructor does not allow empty lists so there is always at least one item
        return blockItems.getLast().hasBlockProof();
    }
}
