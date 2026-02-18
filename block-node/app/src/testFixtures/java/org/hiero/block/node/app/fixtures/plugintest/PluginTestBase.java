// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.plugintest;

import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.swirlds.common.metrics.platform.DefaultMetricsProvider;
import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.metrics.api.Metrics;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.helidon.webserver.http.HttpService;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Stream;
import org.hiero.block.api.BlockNodeVersions;
import org.hiero.block.api.BlockNodeVersions.PluginVersion;
import org.hiero.block.node.app.fixtures.async.TestThreadPoolManager;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.ServiceLoaderFunction;
import org.hiero.block.node.spi.blockmessaging.BlockItemHandler;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.health.HealthFacility;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;
import org.hiero.block.node.spi.module.SemanticVersionUtility;
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
public abstract class PluginTestBase<
        P extends BlockNodePlugin, E extends ExecutorService, S extends ScheduledExecutorService> {
    /** The logger for this class. */
    protected final System.Logger LOGGER = System.getLogger(getClass().getName());
    /** The test thread pool manager */
    protected final TestThreadPoolManager<E, S> testThreadPoolManager;
    /** The metrics provider for the test. */
    private DefaultMetricsProvider metricsProvider;
    /** The block node context, for access to core facilities. */
    protected BlockNodeContext blockNodeContext;
    /** The test block messaging facility, for mocking out the messaging service. */
    protected TestBlockMessagingFacility blockMessaging = new TestBlockMessagingFacility();
    /** The plugin to be tested */
    protected P plugin;

    protected PluginTestBase(@NonNull final E executorService, @NonNull final S scheduledExecutorService) {
        testThreadPoolManager = new TestThreadPoolManager<>(executorService, scheduledExecutorService);
    }

    /**
     * Start the test fixture.<br/>
     * This overload uses the default configuration and the default
     * test thread pool manager.
     *
     * @param plugin the plugin to be tested
     * @param historicalBlockFacility the historical block facility to be used
     */
    public void start(@NonNull final P plugin, @NonNull final HistoricalBlockFacility historicalBlockFacility) {
        start(plugin, historicalBlockFacility, null);
    }

    /**
     * Start the test fixture with the given plugin, historical block facility, and configuration overrides.
     *
     * @param plugin the plugin to be tested
     * @param historicalBlockFacility the historical block facility to be used
     * @param configOverrides a map of configuration overrides to be applied to loaded configuration
     */
    public void start(
            @NonNull final P plugin,
            @NonNull final HistoricalBlockFacility historicalBlockFacility,
            final Map<String, String> configOverrides) {
        this.plugin = plugin;
        org.hiero.block.node.app.fixtures.logging.CleanColorfulFormatter.makeLoggingColorful();
        // Build the configuration
        //noinspection unchecked
        ConfigurationBuilder configurationBuilder = ConfigurationBuilder.create()
                .withConfigDataType(com.swirlds.common.metrics.config.MetricsConfig.class)
                .withConfigDataType(org.hiero.block.node.app.config.node.NodeConfig.class)
                .withConfigDataTypes(plugin.configDataTypes().toArray(new Class[0]))
                .withConfigDataType(com.swirlds.common.metrics.platform.prometheus.PrometheusConfig.class)
                .withConfigDataType(org.hiero.block.node.app.config.ServerConfig.class)
                .withValue("prometheus.endpointEnabled", "false");
        if (configOverrides != null) {
            for (Entry<String, String> override : configOverrides.entrySet()) {
                configurationBuilder = configurationBuilder.withValue(override.getKey(), override.getValue());
            }
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
                new ServiceLoaderFunction(),
                testThreadPoolManager,
                buildBlockNodeVersions());
        // if the subclass implements ServiceBuilder, use it otherwise create a mock
        final ServiceBuilder mockServiceBuilder = (this instanceof ServiceBuilder)
                ? (ServiceBuilder) this
                : new ServiceBuilder() {
                    @Override
                    public void registerHttpService(String path, HttpService... service) {}

                    @Override
                    public void registerGrpcService(@NonNull ServiceInterface service) {}
                };
        // initialize the block messaging facility
        historicalBlockFacility.init(blockNodeContext, mockServiceBuilder);
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
        // init plugin
        plugin.init(blockNodeContext, mockServiceBuilder);
        // start everything
        historicalBlockFacility.start();
        plugin.start();
    }

    /**
     * Teardown after each.
     */
    @AfterEach
    public void tearDown() {
        metricsProvider.stop();
        testThreadPoolManager.shutdownNow();
    }

    /**
     * Build a BlockNodeVersions instance to be used for testing
     */
    private BlockNodeVersions buildBlockNodeVersions() {
        List<PluginVersion> pluginVersions = Stream.of(plugin.version()).toList();
        return BlockNodeVersions.newBuilder()
                .blockNodeVersion(SemanticVersionUtility.from(this.getClass()))
                .installedPluginVersions(pluginVersions)
                .build();
    }
}
