// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

import static java.lang.System.Logger;
import static java.lang.System.Logger.Level.INFO;
import static org.hiero.block.common.constants.StringsConstants.APPLICATION_PROPERTIES;
import static org.hiero.block.server.service.Constants.PBJ_PROTOCOL_PROVIDER_CONFIG_NAME;

import com.hedera.pbj.grpc.helidon.config.PbjConfig;
import com.swirlds.common.metrics.platform.DefaultMetricsProvider;
import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.config.extensions.sources.ClasspathFileConfigSource;
import com.swirlds.config.extensions.sources.SystemPropertiesConfigSource;
import com.swirlds.metrics.api.Metrics;
import io.helidon.common.Builder;
import io.helidon.webserver.ConnectionConfig;
import io.helidon.webserver.Routing;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.WebServerConfig;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicReference;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.health.HealthFacility;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;
import org.hiero.block.server.config.ServerMappedConfigSourceInitializer;
import org.hiero.block.server.config.logging.ConfigurationLoggingImpl;

/** Main class for the block node server */
public class BlockNodeApp implements HealthFacility {

    private static final Logger LOGGER = System.getLogger(BlockNodeApp.class.getName());

    private final BlockNodeContext blockNodeContext;
    private final List<BlockNodePlugin> loadedPlugins;
    private final AtomicReference<State> state = new AtomicReference<>(State.STARTING);
    private final WebServer webServer;

    private BlockNodeApp() throws IOException {
        // Init BlockNode Configuration
        final Configuration configuration = ConfigurationBuilder.create()
                .withSource(ServerMappedConfigSourceInitializer.getMappedConfigSource())
                .withSource(SystemPropertiesConfigSource.getInstance())
                .withSources(new ClasspathFileConfigSource(Path.of(APPLICATION_PROPERTIES)))
                .autoDiscoverExtensions()
                .build();
        final ServerConfig serverConfig = configuration.getConfigData(ServerConfig.class);
        // load logging config and log the configuration
        final ConfigurationLoggingImpl configurationLogging = new ConfigurationLoggingImpl(configuration);
        configurationLogging.log();
        // Init Metrics
        final DefaultMetricsProvider metricsProvider = new DefaultMetricsProvider(configuration);
        final Metrics metrics = metricsProvider.createGlobalMetrics();

        // Create HistoricalBlockFacilityImpl
        final HistoricalBlockFacility blockProvider = new HistoricalBlockFacilityImpl(configuration);

        // Load Block Messaging Service plugin - for now allow nulls
        final BlockMessagingFacility blockMessagingService = ServiceLoader.load(
                        BlockMessagingFacility.class, BlockNodeApp.class.getClassLoader())
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No BlockMessagingFacility provided"));

        // Build block node context
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
                return BlockNodeApp.this;
            }

            @Override
            public BlockMessagingFacility blockMessaging() {
                return blockMessagingService;
            }

            @Override
            public HistoricalBlockFacility historicalBlockProvider() {
                return blockProvider;
            }
        };
        // Load all the plugins
        loadedPlugins = ServiceLoader.load(BlockNodePlugin.class, BlockNodeApp.class.getClassLoader()).stream()
                .map(ServiceLoader.Provider::get)
                .toList();
        // Override the default message size in PBJ
        final PbjConfig pbjConfig = PbjConfig.builder()
                .name(PBJ_PROTOCOL_PROVIDER_CONFIG_NAME)
                .maxMessageSizeBytes(serverConfig.maxMessageSizeBytes())
                .build();
        // Create the web server builder and configure
        final var webServerBuilder = WebServerConfig.builder()
                .port(serverConfig.port())
                .addProtocol(pbjConfig)
                .connectionConfig(ConnectionConfig.builder()
                        .sendBufferSize(serverConfig.socketSendBufferSizeBytes())
                        .receiveBufferSize(serverConfig.socketSendBufferSizeBytes())
                        .build());
        // Initialize all the plugins, adding routing for each plugin
        for (BlockNodePlugin plugin : loadedPlugins) {
            LOGGER.log(INFO, "    Initializing plugin: {0}", plugin.name());
            final Builder<?, ? extends Routing> routingBuilder = plugin.init(blockNodeContext);
            if (routingBuilder != null) {
                webServerBuilder.addRouting(routingBuilder);
            }
        }
        // Build the web server
        webServer = webServerBuilder.build();
    }

    /**
     * Starts the block node server. This method initializes all the plugins, starts the web server,
     * and starts the metrics.
     */
    private void start() {
        LOGGER.log(INFO, "Starting BlockNode Server");
        // Start the web server
        webServer.start();
        // Start metrics
        blockNodeContext.metrics().start();
        // Start all the plugins
        for (BlockNodePlugin plugin : loadedPlugins) {
            LOGGER.log(INFO, "    Starting plugin: {0}", plugin.name());
            plugin.start();
        }
        // mark the server as started
        state.set(State.RUNNING);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public State blockNodeState() {
        return state.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdown(String className, String reason) {
        state.set(State.SHUTTING_DOWN);
        LOGGER.log(INFO, "Shutting down, reason: {0}, class: {1}", reason, className);
        // Stop server
        // Stop all the plugins
        for (BlockNodePlugin plugin : loadedPlugins) {
            LOGGER.log(INFO, "Stopping plugin: {0}", plugin.name());
            plugin.stop();
        }
        LOGGER.log(INFO, "Bye bye");
    }

    /**
     * Main entrypoint for the block node server
     *
     * @param args Command line arguments. Not used at present.
     * @throws IOException if there is an error starting the server
     */
    public static void main(final String[] args) throws IOException {
        BlockNodeApp server = new BlockNodeApp();
        server.start();
    }
}
