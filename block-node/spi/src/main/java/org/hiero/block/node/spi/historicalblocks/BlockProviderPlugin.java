// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.historicalblocks;

import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;

/**
 * The BlockProviderPlugin interface is implemented by plugins that provide access to historical blocks. The plugin then
 * exposes the BlockProviderPlugin through the Java ServiceLoader. Each plugin provides a name and a default priority.
 * They are sorted by priority, with the highest priority first. System config can be used to chose list of plugins to
 * use and override their default priorities.
 * <p>
 * Some examples of plugins that could be implemented are:
 * <ul>
 *     <li>RAM cache</li>
 *     <li>File system</li>
 *     <li>S3 style cloud storage</li>
 *     <li>Other block nodes</li>
 * </ul>
 * </p>
 * <p>
 * BlockProviderPlugins are responsible for acquiring the blocks themselves. They can get them either from
 * {@link BlockMessagingFacility} by listening in incoming new blocks or
 * by reading from other BlockProviderPlugins via the {@link HistoricalBlockFacility}. They can collect blocks and
 * store them at their own pace. They may choose to provide some, all or node of the blocks the collect back to
 * requesters.
 * </p>
 */
public interface BlockProviderPlugin {
    /**
     * The name of the plugin. This is used to identify the plugin in the system config.
     *
     * @return the name of the plugin
     */
    String name();

    /**
     * The default priority of the plugin. This is used to sort the plugins by priority. The higher the number, the higher
     * the priority.
     *
     * @return the default priority of the plugin
     */
    int defaultPriority();

    /**
     * Use this method to get the block at the specified block number. Block providers have to be trusted that they have
     * safely stored the block before calling delete callback.
     *
     * @param blockNumber the block number
     * @param deleteBlockCallback the callback to be called when you are finished accessing the block, and it should be
     *                            deleted from the provider. This can be null if the provider does not support deletion.
     * @return the block at the specified block number, null if the block is not available
     */
    BlockAccessor block(long blockNumber, Runnable deleteBlockCallback);

    /**
     * Get the latest block number available from this provider. This is used to determine the latest block number
     * they block node knows about so it can be used to determine the next block number it needs.
     *
     * @return the latest block number available from this provider,
     * {@link org.hiero.block.node.spi.BlockNodePlugin#UNKNOWN_BLOCK_NUMBER} -1 if no blocks are available
     */
    long latestBlockNumber();
}
