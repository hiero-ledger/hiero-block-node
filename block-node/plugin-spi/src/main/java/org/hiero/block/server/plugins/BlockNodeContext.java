package org.hiero.block.server.plugins;

import com.swirlds.config.api.Configuration;
import com.swirlds.metrics.api.Metrics;
import org.hiero.block.server.plugins.historicalblocks.HistoricalBlockFacility;
import org.hiero.block.server.plugins.blockmessaging.BlockMessagingFacility;
import org.hiero.block.server.plugins.health.HealthFacility;

/**
 * The BlockNodeContext interface is used to provide access to the different facilities of the block node. This is a
 * standard global context that provides default facilities to all plugins.
 * <p><b><i>
 * Important this interface will want to grow, that urge should be fought against. The goal is to keep this interface
 * small and simple. If you need to add a new method, you will need to APOLOGIZE to the all other developers first :-)
 * </i></b></p>
 * <p>
 * The reason this exists and is an interface rather than passing each of the facilities into the plugin
 * {@link BlockNodePlugin#start(BlockNodeContext)} method is to allow future additions without breaking all existing
 * plugins by changing the start method signature. Or ending up with multiple versioned start methods with different
 * signatures. It is aiming for the best balance between allowing future expansion, avoiding growth into a mess and
 * keeping the API clean.
 * </p>
 */
public interface BlockNodeContext {
    /**
     * Use this method to get the configuration of the block node.
     *
     * @return the configuration of the block node
     */
    Configuration configuration();

    /**
     * Use this method to get the metrics of the block node.
     *
     * @return the metrics of the block node
     */
    Metrics metrics();

    /**
     * Use this method to get the health of the block node.
     *
     * @return the status service of the block node
     */
    HealthFacility serverHealth();

    /**
     * Use this method to get the block messaging service of the block node.
     *
     * @return the block messaging service of the block node
     */
    BlockMessagingFacility blockMessaging();

    /**
     * Use this method to get the historical block provider of the block node.
     *
     * @return the block provider of the block node
     */
    HistoricalBlockFacility historicalBlockProvider();

    /**
     * Shutdown the block node. This method is called to start shutting down the block node.
     */
    void shutdown();
}
