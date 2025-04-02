// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.plugintest;

import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.swirlds.common.metrics.platform.DefaultMetricsProvider;
import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.metrics.api.Metrics;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.helidon.webserver.http.HttpService;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.health.HealthFacility;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;
import org.junit.jupiter.api.AfterEach;

/**
 * Base class for testing GRPC apps.
 * <p>
 * This class provides a mock implementation of the BlockNodePlugin interface and sets up a GRPC connection to the
 * plugin. It mocks out all the base functionality of the block node, including the configuration, metrics, health,
 * block messaging, and historical block facilities.
 * <p>
 * The messaging services is mocked out with two concurrent queues, one for block items
 * {@link NonGrpcPluginTestBase :sentBlockBlockItems} and one for block notifications
 * {@link NonGrpcPluginTestBase :sentBlockNotifications}. You can look at these queues to see what was sent to the messaging
 * service by the plugin.
 */
public abstract class NonGrpcPluginTestBase {
    private final DefaultMetricsProvider metricsProvider;
    protected final BlockNodeContext blockNodeContext;
    protected final TestBlockMessagingFacility blockMessaging = new TestBlockMessagingFacility();

    public NonGrpcPluginTestBase(BlockNodePlugin plugin, HistoricalBlockFacility historicalBlockFacility) {
        // Build the configuration
        //noinspection unchecked
        final Configuration configuration = ConfigurationBuilder.create()
                .withConfigDataType(com.swirlds.common.metrics.config.MetricsConfig.class)
                .withConfigDataType(com.swirlds.common.metrics.platform.prometheus.PrometheusConfig.class)
                .withConfigDataTypes(plugin.configDataTypes().toArray(new Class[0]))
                .build();
        // create metrics provider
        metricsProvider = new DefaultMetricsProvider(configuration);
        final Metrics metrics = metricsProvider.createGlobalMetrics();
        metricsProvider.start();
        // mock health facility
        final HealthFacility healthFacility = new AllwaysRunningHealthFacility();
        // create block node context
        blockNodeContext = new BlockNodeContext() {
            @Override
            public Configuration configuration() {
                return configuration;
            }

            @Override
            public Metrics metrics() {
                return metrics;
            }

            @Override
            public HealthFacility serverHealth() {
                return healthFacility;
            }

            @Override
            public BlockMessagingFacility blockMessaging() {
                return blockMessaging;
            }

            @Override
            public HistoricalBlockFacility historicalBlockProvider() {
                return historicalBlockFacility;
            }
        };

        ServiceBuilder mockServiceBuilder = new ServiceBuilder() {
            @Override
            public void registerHttpService(String path, HttpService... service) {}

            @Override
            public void registerGrpcService(@NonNull ServiceInterface service) {}
        };

        // start plugin
        plugin.init(blockNodeContext, mockServiceBuilder);
        plugin.start();
    }

    @AfterEach
    public void tearDown() {
        metricsProvider.stop();
    }
}
