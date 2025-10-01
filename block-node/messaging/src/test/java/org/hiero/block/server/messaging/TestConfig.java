// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.messaging;

import com.swirlds.common.metrics.platform.DefaultMetricsProvider;
import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.metrics.api.Metrics;
import java.util.concurrent.LinkedBlockingQueue;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.async.TestThreadPoolManager;
import org.hiero.block.node.messaging.MessagingConfig;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.threading.ThreadPoolManager;

public class TestConfig {

    /**
     * Helper method to get the configuration for the messaging service. For use in tests.
     *
     * @return the configuration for the messaging service
     * @throws IllegalStateException if no ConfigurationBuilderFactory implementation is found
     */
    public static Configuration getConfig() {
        return ConfigurationBuilder.create()
                .withConfigDataType(MessagingConfig.class)
                .build();
    }

    /**
     * A test context for the BlockNodeContext interface. Only provides a configuration object as that is all that is
     * needed by messaging facility so far.
     */
    public static final BlockNodeContext BLOCK_NODE_CONTEXT = new BlockNodeContext(
            getConfig(),
            getMetrics(),
            null,
            null,
            null,
            null,
            new TestThreadPoolManager<>(new BlockingExecutor(new LinkedBlockingQueue<>())));

    public static BlockNodeContext generateContext(final ThreadPoolManager threadPoolManager) {
        return new BlockNodeContext(getConfig(), getMetrics(), null, null, null, null, threadPoolManager);
    }

    /**
     * Helper method to get the metrics for the messaging service. For use in tests.
     *
     * @return the metrics for the messaging service
     */
    public static Metrics getMetrics() {
        ConfigurationBuilder configurationBuilder = ConfigurationBuilder.create()
                .withConfigDataType(com.swirlds.common.metrics.config.MetricsConfig.class)
                .withConfigDataType(com.swirlds.common.metrics.platform.prometheus.PrometheusConfig.class);
        final Configuration configuration = configurationBuilder.build();
        // create metrics provider
        final DefaultMetricsProvider metricsProvider;
        metricsProvider = new DefaultMetricsProvider(configuration);
        final Metrics metrics = metricsProvider.createGlobalMetrics();
        metricsProvider.start();

        return metrics;
    }
}
