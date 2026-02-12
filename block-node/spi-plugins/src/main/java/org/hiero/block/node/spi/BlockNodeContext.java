// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi;

import com.swirlds.config.api.Configuration;
import com.swirlds.metrics.api.Metrics;
import org.hiero.block.api.BlockNodeVersions;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.health.HealthFacility;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;
import org.hiero.block.node.spi.threading.ThreadPoolManager;

/**
 * The BlockNodeContext record is used to provide access to the different facilities of the block node. This is a
 * standard global context that provides default facilities to all plugins.
 * <p><b><i>
 * Important this interface will want to grow, that urge should be fought against. The goal is to keep this interface
 * small and simple. If you need to add a new method, you will need to APOLOGIZE to the all other developers first :-)
 * </i></b></p>
 * <p>
 * The reason this exists and is an interface rather than passing each of the facilities into the plugin
 * {@link BlockNodePlugin#init(BlockNodeContext, ServiceBuilder)} method is to allow future additions without breaking all existing
 * plugins by changing the start method signature. Or ending up with multiple versioned start methods with different
 * signatures. It is aiming for the best balance between allowing future expansion, avoiding growth into a mess and
 * keeping the API clean.
 * </p>
 *
 * @param configuration the configuration of the block node
 * @param metrics the metrics of the block node
 * @param serverHealth the health of the block node
 * @param blockMessaging the block messaging service of the block node
 * @param historicalBlockProvider the historical block provider of the block node
 * @param serviceLoader the service loader function to use to load services
 * @param threadPoolManager the thread pool manager for the block node
 * @param blockNodeVersions the version information associated with a block node
 */
public record BlockNodeContext(
        Configuration configuration,
        Metrics metrics,
        HealthFacility serverHealth,
        BlockMessagingFacility blockMessaging,
        HistoricalBlockFacility historicalBlockProvider,
        ServiceLoaderFunction serviceLoader,
        ThreadPoolManager threadPoolManager,
        BlockNodeVersions blockNodeVersions) {}
