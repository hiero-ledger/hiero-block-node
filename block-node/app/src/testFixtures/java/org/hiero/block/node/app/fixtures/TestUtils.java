// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures;

import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import org.hiero.block.api.BlockNodeVersions;
import org.hiero.block.api.BlockRange;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.app.fixtures.plugintest.TestApplicationStateFacility;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.app.fixtures.plugintest.TestHealthFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.ServiceLoaderFunction;
import org.hiero.block.node.spi.threading.ThreadPoolManager;
import org.hiero.metrics.core.MetricRegistry;

/**
 * Generic Test Utilities.
 */
public class TestUtils {
    /**
     * Enable debug logging for the test. This is useful for debugging test failures.
     */
    public static void enableDebugLogging() {
        // enable debug System.logger logging
        Logger rootLogger = LogManager.getLogManager().getLogger("");
        rootLogger.setLevel(Level.ALL);
        for (var handler : rootLogger.getHandlers()) {
            handler.setLevel(Level.ALL);
        }
    }

    public static MetricRegistry createMetrics() {
        return MetricRegistry.builder().build();
    }

    public static ConfigurationBuilder createTestConfiguration() {
        ConfigurationBuilder configurationBuilder = ConfigurationBuilder.create()
                .withConfigDataType(org.hiero.block.node.app.config.ServerConfig.class)
                .withConfigDataType(org.hiero.block.node.app.config.node.NodeConfig.class);
        return configurationBuilder;
    }

    public static BlockNodeContext testContext(
            final Configuration configuration, final ThreadPoolManager threadPoolManager) {
        final MetricRegistry metrics = TestUtils.createMetrics();
        final TestHealthFacility healthFacility = new TestHealthFacility();
        final TestBlockMessagingFacility blockMessaging = new TestBlockMessagingFacility();
        final SimpleInMemoryHistoricalBlockFacility historicalBlockProvider =
                new SimpleInMemoryHistoricalBlockFacility();
        final TestApplicationStateFacility applicationStateFacility = new TestApplicationStateFacility();
        final ServiceLoaderFunction serviceLoader = new ServiceLoaderFunction();
        final BlockNodeVersions blockNodeVersions = BlockNodeVersions.DEFAULT;
        final ArrayList<BlockRange> storedBlocks = new ArrayList<>();
        final ArrayList<BlockRange> availableBlocks = new ArrayList<>();
        return new BlockNodeContext(
                configuration,
                metrics,
                healthFacility,
                blockMessaging,
                historicalBlockProvider,
                applicationStateFacility,
                serviceLoader,
                threadPoolManager,
                blockNodeVersions,
                null,
                null,
                storedBlocks,
                availableBlocks);
    }
}
