// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.blockmessaging;

import java.util.List;
import java.util.Objects;
import org.hiero.block.internal.BlockItemUnparsed;

/**
 * A record that holds a list of block items and the block number if items start with block header of a new block. This
 * is used to send block items throughout the server.The parsed block number for the start of a new block, is included
 * to avoid every consumer having to parse the block number from the block items.
 *
 * @param blockItems an immutable list of block items to handle.
 * @param blockNumber the number of the block containing these items.
 * @param isStartOfNewBlock does this include the start of the block
 * @param isEndOfBlock does this include the end of the block
 */
public record BlockItems(
        List<BlockItemUnparsed> blockItems, long blockNumber, boolean isStartOfNewBlock, boolean isEndOfBlock) {
    public BlockItems {
        Objects.requireNonNull(blockItems);
        if (blockItems.isEmpty()) {
            throw new IllegalArgumentException("Block items cannot be empty");
        }
        if (blockNumber < 0) {
            throw new IllegalArgumentException("Block number cannot be negative");
        }
    }
}
