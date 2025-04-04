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
     * Use this method to get the oldest block number available from all providers. This is used to determine the
     * oldest block number they block node has available.
     *
     * @return the oldest block number available from all providers,
     */
    long oldestBlockNumber();

    /**
     * Use this method to get the latest block number available from all providers. This is used to determine the latest
     * block number they block node knows about so it can be used to determine the next block number it needs.
     *
     * @return the latest block number available from all providers,
     * {@link org.hiero.block.node.spi.BlockNodePlugin#UNKNOWN_BLOCK_NUMBER} -1 if no blocks are available
     */
    long latestBlockNumber();
}
