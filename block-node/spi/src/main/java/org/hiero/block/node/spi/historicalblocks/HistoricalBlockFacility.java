// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.historicalblocks;

/**
 * The HistoricalBlockFacility interface is used to provide access to historical blocks to the different parts of the
 * block node.
 */
public interface HistoricalBlockFacility {

    /**
     * Use this method to get the block at the specified block number.
     *
     * @param blockNumber the block number
     * @return the block at the specified block number, null if the block is not available
     */
    BlockAccessor block(long blockNumber);

    /**
     * Use this method to get the block at the specified block number.
     *
     * @param blockNumber the block number
     * @param deleteBlockCallback the callback to be called when you are finished accessing the block, and it should be
     *                            deleted from the provider. This can be null if the provider does not support deletion.
     * @return the block at the specified block number, null if the block is not available
     */
    BlockAccessor block(long blockNumber, Runnable deleteBlockCallback);

    /**
     * Use this method to get the latest block number available from all providers. This is used to determine the latest
     * block number they block node knows about so it can be used to determine the next block number it needs.
     *
     * @return the latest block number available from all providers,
     * {@link org.hiero.block.node.spi.BlockNodePlugin#UNKNOWN_BLOCK_NUMBER} -1 if no blocks are available
     */
    long latestBlockNumber();
}
