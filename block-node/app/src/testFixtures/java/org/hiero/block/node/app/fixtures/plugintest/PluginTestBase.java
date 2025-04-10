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
import org.hiero.block.node.spi.blockmessaging.BlockItemHandler;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.health.HealthFacility;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;
import org.junit.jupiter.api.AfterEach;

/**
 * Base class for testing block node plugins. If you are testing a GRPC plugin, use {@link GrpcPluginTestBase} instead.
 * <p>
 * It mocks out all the base functionality of the block node, including the configuration, metrics, health,
 * block messaging, and historical block facilities.
 * <p>
 * The messaging services is mocked out with two concurrent queues, one for block items
 * {@link PluginTestBase :sentBlockBlockItems} and one for block notifications
 * {@link PluginTestBase :sentBlockNotifications}. You can look at these queues to see what was sent to the messaging
 * service by the plugin.
 *
 * @param <P> the type of plugin being tested
 */
public abstract class PluginTestBase<P extends BlockNodePlugin> {
    private final DefaultMetricsProvider metricsProvider;
    protected final BlockNodeContext blockNodeContext;
    protected final TestBlockMessagingFacility blockMessaging = new TestBlockMessagingFacility();
    protected final P plugin;

    public PluginTestBase(P plugin, HistoricalBlockFacility historicalBlockFacility) {
        this.plugin = plugin;
        org.hiero.block.node.app.fixtures.logging.CleanColorfulFormatter.makeLoggingColorful();
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

        // if the subclass implements ServiceBuilder, use it otherwise create a mock
        ServiceBuilder mockServiceBuilder = (this instanceof ServiceBuilder)
                ? (ServiceBuilder) this
                : new ServiceBuilder() {
                    @Override
                    public void registerHttpService(String path, HttpService... service) {}

                    @Override
                    public void registerGrpcService(@NonNull ServiceInterface service) {}
                };

        // if HistoricalBlockFacility is a BlockItemHandler, register it with the messaging facility
        if (historicalBlockFacility instanceof BlockItemHandler blockItemHandler) {
            blockMessaging.registerBlockItemHandler(
                    blockItemHandler, false, historicalBlockFacility.getClass().getSimpleName());
        }
        // if HistoricalBlockFacility is a BlockNotificationHandler, register it with the messaging facility
        if (historicalBlockFacility instanceof BlockNotificationHandler blockNotificationHandler) {
            blockMessaging.registerBlockNotificationHandler(
                    blockNotificationHandler,
                    false,
                    historicalBlockFacility.getClass().getSimpleName());
        }

        // start plugin
        plugin.init(blockNodeContext, mockServiceBuilder);
        plugin.start();
    }

    @AfterEach
    public void tearDown() {
        metricsProvider.stop();
    }
}
