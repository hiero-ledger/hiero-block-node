package org.hiero.block.server.plugins.historicalblocks;

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
}
