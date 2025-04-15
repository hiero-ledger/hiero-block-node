// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.historicalblocks;

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
 * <p>
 * BlockProviderPlugins are responsible for acquiring the blocks themselves. They can get them either from
 * {@link BlockMessagingFacility} by listening in incoming new blocks or by reading from other BlockProviderPlugins
 * via the {@link HistoricalBlockFacility}. They can collect blocks and store them at their own pace. They may choose
 * to provide some, all or node of the blocks the collect back to requesters.
 * </p>
 */
public interface BlockProviderPlugin extends BlockNodePlugin {
    /**
     * The default priority of the plugin. This is used to sort the plugins by priority. The higher the number, the higher
     * the priority. Blocks are read from the highest priority plugin first.
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
     * Use this method to get the set of all blocks available from this plugin.
     *
     * @return the set of all blocks available in from this plugin
     */
    BlockRangeSet availableBlocks();
}
