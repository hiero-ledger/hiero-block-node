// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

import java.util.Comparator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import java.util.stream.Collectors;
import org.hiero.block.node.base.ranges.CombinedBlockRangeSet;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.BlockProviderPlugin;
import org.hiero.block.node.spi.historicalblocks.BlockRangeSet;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;

/**
 * The HistoricalBlockFacilityImpl class is an implementation of the HistoricalBlockFacility interface. It provides
 * access to historical blocks using a list of block provider plugins, sorted by priority.
 */
public class HistoricalBlockFacilityImpl implements HistoricalBlockFacility {

    /**
     * The list of block providers, sorted by priority. The first provider in the list is the one that will be used to
     * access blocks first.
     */
    private final List<BlockProviderPlugin> providers;

    /**
     * The set of available blocks.
     */
    private final CombinedBlockRangeSet availableBlocks;

    /**
     * Constructor for the HistoricalBlockFacilityImpl class. This constructor loads the block providers using the Java
     * ServiceLoader.
     */
    @SuppressWarnings("unused")
    public HistoricalBlockFacilityImpl() {
        // TODO: Add configuration to the choose block providers and override the priorities
        this(ServiceLoader.load(BlockProviderPlugin.class, HistoricalBlockFacilityImpl.class.getClassLoader()).stream()
                .map(Provider::get)
                .toList());
    }

    /**
     * Constructor for the HistoricalBlockFacilityImpl class. This constructor takes a list of block providers and sorts
     * them by priority.
     *
     * @param providers the list of block providers to use
     */
    public HistoricalBlockFacilityImpl(List<BlockProviderPlugin> providers) {
        this.providers = providers.stream()
                .sorted(Comparator.comparingInt(BlockProviderPlugin::defaultPriority)
                        .reversed())
                .toList();
        this.availableBlocks = new CombinedBlockRangeSet(
                providers.stream().map(BlockProviderPlugin::availableBlocks).toArray(BlockRangeSet[]::new));
    }

    /**
     * Get the list of all block providers. This method is used to get the list of all block providers that are
     * registered with the block node.
     *
     * @return the list of all block providers
     */
    List<BlockProviderPlugin> allBlockProvidersPlugins() {
        return providers;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BlockAccessor block(long blockNumber) {
        for (BlockProviderPlugin provider : providers) {
            BlockAccessor blockAccessor = provider.block(blockNumber);
            if (blockAccessor != null) {
                return blockAccessor;
            }
        }
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BlockRangeSet availableBlocks() {
        return availableBlocks;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "HistoricalBlockFacilityImpl{" + "availableBlocks="
                + availableBlocks() + ", providers=["
                + providers.stream().map(p -> p.getClass().getSimpleName()).collect(Collectors.joining(", ")) + "]"
                + '}';
    }
}
