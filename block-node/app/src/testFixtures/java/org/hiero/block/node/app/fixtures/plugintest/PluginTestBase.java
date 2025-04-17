// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.plugintest;

import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.swirlds.common.metrics.platform.DefaultMetricsProvider;
import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.metrics.api.Metrics;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.helidon.webserver.http.HttpService;
import java.util.Collections;
import java.util.Map;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.ServiceLoaderFunction;
import org.hiero.block.node.spi.blockmessaging.BlockItemHandler;
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
 * The messaging services are mocked out with two concurrent queues, one for block items
 * {@link PluginTestBase :sentBlockBlockItems} and one for block notifications
 * {@link PluginTestBase :sentBlockNotifications}. You can look at these queues to see what was sent to the messaging
 * service by the plugin.
 * <p>
 * Implementations of this class should call one of the start() methods. This will start the plugin and initialize the
 * test fixture. This fixture uses start() methods vs. a constructor to allow for subclasses to do work before calling
 * start(). Such as computing the configuration or starting other things needed like minio.
 *
 * @param <P> the type of plugin being tested
 */
public abstract class PluginTestBase<P extends BlockNodePlugin> {
    /** The logger for this class. */
    protected final System.Logger LOGGER = System.getLogger(getClass().getName());
    /** The metrics provider for the test. */
    private DefaultMetricsProvider metricsProvider;
    /** The block node context, for access to core facilities. */
    protected BlockNodeContext blockNodeContext;
    /** The test block messaging facility, for mocking out the messaging service. */
    protected TestBlockMessagingFacility blockMessaging = new TestBlockMessagingFacility();
    /** The plugin to be tested */
    protected P plugin;

    /**
     * Start the test fixture with the given plugin and historical block facility.
     *
     * @param plugin the plugin to be tested
     * @param historicalBlockFacility the historical block facility to be used
     */
    public void start(P plugin, HistoricalBlockFacility historicalBlockFacility) {
        start(plugin, historicalBlockFacility, Collections.emptyMap());
    }

    /**
     * Start the test fixture with the given plugin, historical block facility, and configuration overrides.
     *
     * @param plugin the plugin to be tested
     * @param historicalBlockFacility the historical block facility to be used
     * @param configOverrides a map of configuration overrides to be applied to loaded configuration
     */
    public void start(P plugin, HistoricalBlockFacility historicalBlockFacility, Map<String, String> configOverrides) {
        this.plugin = plugin;
        org.hiero.block.node.app.fixtures.logging.CleanColorfulFormatter.makeLoggingColorful();
        // Build the configuration
        //noinspection unchecked
        final ConfigurationBuilder configurationBuilder = ConfigurationBuilder.create()
                .withConfigDataType(com.swirlds.common.metrics.config.MetricsConfig.class)
                .withConfigDataType(com.swirlds.common.metrics.platform.prometheus.PrometheusConfig.class)
                .withConfigDataTypes(plugin.configDataTypes().toArray(new Class[0]));
        if (!configOverrides.isEmpty()) {
            configOverrides.forEach(configurationBuilder::withValue);
        }
        final Configuration configuration = configurationBuilder.build();
        // create metrics provider
        metricsProvider = new DefaultMetricsProvider(configuration);
        final Metrics metrics = metricsProvider.createGlobalMetrics();
        metricsProvider.start();
        // mock health facility
        final HealthFacility healthFacility = new TestHealthFacility();
        // create block node context
        blockNodeContext = new BlockNodeContext(
                configuration,
                metrics,
                healthFacility,
                blockMessaging,
                historicalBlockFacility,
                new ServiceLoaderFunction());
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

    /**
     * Tears down the test fixture by stopping the metrics provider.
     */
    @AfterEach
    public void tearDown() {
        metricsProvider.stop();
    }
}
