// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.historicalblocks;

import org.hiero.block.node.spi.BlockNodePlugin;

/**
 * The HistoricalBlockFacility interface is used to provide access to historical blocks to the different parts of the
 * block node. The range from the oldest block to the newest block is an inclusive range.
 */
@SuppressWarnings("unused")
public interface HistoricalBlockFacility extends BlockNodePlugin {

    /**
     * Use this method to get the block at the specified block number.
     *
     * @param blockNumber the block number
     * @return the block at the specified block number, null if the block is not available
     */
    BlockAccessor block(long blockNumber);

    /**
     * Use this method to get the set of all blocks available in this block node.
     *
     * @return the set of all blocks available in this block node
     */
    BlockRangeSet availableBlocks();
}
