// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.historicalblocks;

import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
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
     * Special value for block number that indicates that the block number is unknown.
     */
    long UNKNOWN_BLOCK_NUMBER = BlockNodePlugin.UNKNOWN_BLOCK_NUMBER;

    /**
     * The name of the plugin. This is used to identify the plugin in the system config.
     *
     * @return the name of the plugin
     */
    String name();

    /**
     * Start the plugin. This method is called when the block node is starting up. It provides the block node context to
     * the plugin which can be used to access the different facilities of the block node.
     * <p>
     * The default implementation does nothing. This is to be overridden by the plugin if it needs to do
     * anything on start.
     * </p>
     *
     * @param context the block node context
     */
    default void init(BlockNodeContext context) {}

    /**
     * Start the plugin. This method is called when the block node is starting up after all initialization is complete.
     * At this point all facilities are available and the plugin can use them. Any background threads should be started
     * here. This method is called after the {@link #init(BlockNodeContext)} method.
     * <p>
     * The default implementation does nothing. This is to be overridden by the plugin if it needs to do
     * anything on start.
     * </p>
     */
    default void start() {}

    /**
     * The default priority of the plugin. This is used to sort the plugins by priority. The higher the number, the higher
     * the priority.
     *
     * @return the default priority of the plugin
     */
    int defaultPriority();

    /**
     * Use this method to get the block at the specified block number. Block providers have to be trusted that they have
     * safely stored the block before calling delete on the returned {code BlockAccessor}.
     *
     * @param blockNumber the block number
     * @return the block at the specified block number, null if the block is not available
     */
    BlockAccessor block(long blockNumber);

    /**
     * Get the latest block number available from this provider. This is used to determine the latest block number
     * they block node knows about so it can be used to determine the next block number it needs.
     *
     * @return the latest block number available from this provider,
     * {@link org.hiero.block.node.spi.BlockNodePlugin#UNKNOWN_BLOCK_NUMBER} -1 if no blocks are available
     */
    long latestBlockNumber();
}
