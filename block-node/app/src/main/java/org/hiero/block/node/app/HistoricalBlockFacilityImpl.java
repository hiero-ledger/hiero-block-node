// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

import java.util.Comparator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.BlockProviderPlugin;
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
     * Constructor for the HistoricalBlockFacilityImpl class. This constructor loads the block providers using the Java
     * ServiceLoader.
     */
    @SuppressWarnings("unused")
    public HistoricalBlockFacilityImpl() {
        // TODO: Add configuration to the choose block providers and override the priorities
        providers = ServiceLoader.load(BlockProviderPlugin.class, getClass().getClassLoader()).stream()
                .map(Provider::get)
                .sorted(Comparator.comparingInt(BlockProviderPlugin::defaultPriority)
                        .reversed())
                .toList();
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
    public long latestBlockNumber() {
        return providers.stream()
                .mapToLong(BlockProviderPlugin::latestBlockNumber)
                .max()
                .orElse(-1);
    }
}
