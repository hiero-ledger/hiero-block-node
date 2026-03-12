// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures;

import com.swirlds.config.api.ConfigurationBuilder;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
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
}
