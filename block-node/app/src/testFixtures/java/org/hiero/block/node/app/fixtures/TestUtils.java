// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures;

import com.swirlds.common.metrics.platform.DefaultMetricsProvider;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.metrics.api.Metrics;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

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

    public static Metrics createMetrics() {
        final var metricsProvider =
                new DefaultMetricsProvider(createTestConfiguration().build());
        final Metrics metrics = metricsProvider.createGlobalMetrics();
        metricsProvider.start();
        return metrics;
    }

    public static ConfigurationBuilder createTestConfiguration() {
        ConfigurationBuilder configurationBuilder = ConfigurationBuilder.create()
                .withConfigDataType(com.swirlds.common.metrics.config.MetricsConfig.class)
                .withConfigDataType(com.swirlds.common.metrics.platform.prometheus.PrometheusConfig.class)
                .withConfigDataType(org.hiero.block.node.app.config.ServerConfig.class)
                .withConfigDataType(org.hiero.block.node.app.config.node.NodeConfig.class)
                .withValue("prometheus.endpointEnabled", "false");
        return configurationBuilder;
    }
}
