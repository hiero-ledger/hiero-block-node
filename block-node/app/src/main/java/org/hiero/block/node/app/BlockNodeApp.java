// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

import static java.lang.System.Logger;
import static java.lang.System.Logger.Level.INFO;
import static org.hiero.block.common.constants.StringsConstants.APPLICATION_PROPERTIES;

import com.hedera.pbj.grpc.helidon.config.PbjConfig;
import com.swirlds.common.metrics.platform.DefaultMetricsProvider;
import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.config.extensions.sources.ClasspathFileConfigSource;
import com.swirlds.metrics.api.Metrics;
import io.helidon.common.Builder;
import io.helidon.webserver.ConnectionConfig;
import io.helidon.webserver.Routing;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.WebServerConfig;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicReference;
import org.hiero.block.node.app.config.AutomaticEnvironmentVariableConfigSource;
import org.hiero.block.node.app.config.ConfigurationLogging;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.health.HealthFacility;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;

/** Main class for the block node server */
public class BlockNodeApp implements HealthFacility {
    /** Constant mapped to PbjProtocolProvider.CONFIG_NAME in the PBJ Helidon Plugin */
    public static final String PBJ_PROTOCOL_PROVIDER_CONFIG_NAME = "pbj";
    /** The logger for this class. */
    private static final Logger LOGGER = System.getLogger(BlockNodeApp.class.getName());
    /** The block node context. */
    private final BlockNodeContext blockNodeContext;
    /** list of all loaded plugins */
    private final List<BlockNodePlugin> loadedPlugins = new ArrayList<>();
    /** The state of the server. */
    private final AtomicReference<State> state = new AtomicReference<>(State.STARTING);
    /** The web server. */
    private final WebServer webServer;
    /** The server configuration. */
    private final ServerConfig serverConfig;
    /** The historical block node facility */
    private final HistoricalBlockFacilityImpl historicalBlockFacility;

    /**
     * Constructor for the BlockNodeApp class. This constructor initializes the server configuration,
     * loads the plugins, and creates the web server.
     *
     * @throws IOException if there is an error starting the server
     */
    private BlockNodeApp() throws IOException {
        // ==== FACILITY & PLUGIN LOADING ==============================================================================
        // Load Block Messaging Service plugin - for now allow nulls
        final BlockMessagingFacility blockMessagingService = ServiceLoader.load(
                        BlockMessagingFacility.class, BlockNodeApp.class.getClassLoader())
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No BlockMessagingFacility provided"));
        loadedPlugins.add(blockMessagingService);
        // Load HistoricalBlockFacilityImpl
        historicalBlockFacility = new HistoricalBlockFacilityImpl();
        loadedPlugins.add(historicalBlockFacility);
        loadedPlugins.addAll(historicalBlockFacility.allBlockProvidersPlugins());
        // Load all the plugins, just the classes are crated at this point, they are not initialized
        ServiceLoader.load(BlockNodePlugin.class, BlockNodeApp.class.getClassLoader()).stream()
                .map(ServiceLoader.Provider::get)
                .forEach(loadedPlugins::add);
        // ==== CONFIGURATION ==========================================================================================
        // Collect all the config data types from the plugins and global server level
        final List<Class<? extends Record>> allConfigDataTypes = new ArrayList<>();
        allConfigDataTypes.add(ServerConfig.class);
        loadedPlugins.forEach(plugin -> allConfigDataTypes.addAll(plugin.configDataTypes()));
        // Init BlockNode Configuration
        //noinspection unchecked
        final ConfigurationBuilder configurationBuilder = ConfigurationBuilder.create()
                .withSource(new AutomaticEnvironmentVariableConfigSource(allConfigDataTypes, System::getenv))
                .withSources(new ClasspathFileConfigSource(Path.of(APPLICATION_PROPERTIES)))
                .withConfigDataType(com.swirlds.common.metrics.config.MetricsConfig.class)
                .withConfigDataType( com.swirlds.common.metrics.platform.prometheus.PrometheusConfig.class)
                .withConfigDataTypes(allConfigDataTypes.toArray(new Class[0]));
        // Build the configuration
        final Configuration configuration = configurationBuilder.build();
        // Log loaded configuration data types
        configuration.getConfigDataTypes().forEach(configDataType ->
                LOGGER.log(INFO, "Loaded config data type: " + configDataType.getName()));
        // Log the configuration
        ConfigurationLogging.log(configuration);
        // now that configuration is loaded we can get config for server
        serverConfig = configuration.getConfigData(ServerConfig.class);
        // ==== METRICS ================================================================================================
        final DefaultMetricsProvider metricsProvider = new DefaultMetricsProvider(configuration);
        final Metrics metrics = metricsProvider.createGlobalMetrics();
        // ==== CONTEXT ================================================================================================
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
                return historicalBlockFacility;
            }
        };
        // ==== LOAD & CONFIGURE WEB SERVER ============================================================================
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
        // ==== INITIALIZE EVERYTHING ==================================================================================
        // Initialize all the facilities & plugins, adding routing for each plugin
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
        // Start all the facilities & plugins
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
        // wait for the shutdown delay
        try {
            Thread.sleep(serverConfig.shutdownDelayMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.log(INFO, "Shutdown interrupted");
        }
        // Stop server
        webServer.stop();
        // Stop all the facilities &  plugins
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
