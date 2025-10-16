// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

import static java.lang.System.Logger;
import static java.lang.System.Logger.Level.INFO;
import static org.hiero.block.common.constants.StringsConstants.APPLICATION_PROPERTIES;
import static org.hiero.block.common.constants.StringsConstants.APPLICATION_TEST_PROPERTIES;
import static org.hiero.block.node.app.logging.CleanColorfulFormatter.GREY;
import static org.hiero.block.node.app.logging.CleanColorfulFormatter.LIGHT_GREEN;
import static org.hiero.block.node.spi.BlockNodePlugin.METRICS_CATEGORY;

import com.hedera.pbj.grpc.helidon.config.PbjConfig;
import com.swirlds.common.metrics.platform.DefaultMetricsProvider;
import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.config.extensions.sources.ClasspathFileConfigSource;
import com.swirlds.metrics.api.LongGauge;
import com.swirlds.metrics.api.Metrics;
import io.helidon.webserver.ConnectionConfig;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.WebServerConfig;
import io.helidon.webserver.http2.Http2Config;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.LogManager;
import java.util.stream.Collectors;
import org.hiero.block.node.app.config.AutomaticEnvironmentVariableConfigSource;
import org.hiero.block.node.app.config.ServerConfig;
import org.hiero.block.node.app.config.WebServerHttp2Config;
import org.hiero.block.node.app.config.node.NodeConfig;
import org.hiero.block.node.app.logging.CleanColorfulFormatter;
import org.hiero.block.node.app.logging.ConfigLogger;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceLoaderFunction;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.health.HealthFacility;
import org.hiero.block.node.spi.historicalblocks.LongRange;
import org.hiero.block.node.spi.threading.ThreadPoolManager;

/** Main class for the block node server */
public class BlockNodeApp implements HealthFacility {
    /** Constant mapped to PbjProtocolProvider.CONFIG_NAME in the PBJ Helidon Plugin */
    public static final String PBJ_PROTOCOL_PROVIDER_CONFIG_NAME = "pbj";
    /** The logger for this class. */
    private static final Logger LOGGER = System.getLogger(BlockNodeApp.class.getName());
    /** The state of the server. */
    private final AtomicReference<State> state = new AtomicReference<>(State.STARTING);
    /** The web server. */
    private final WebServer webServer;
    /** The server configuration. */
    private final ServerConfig serverConfig;
    /** The historical block node facility */
    private final HistoricalBlockFacilityImpl historicalBlockFacility;
    /** Should the shutdown() method exit the JVM. */
    private final boolean shouldExitJvmOnShutdown;
    /** The block node context. Package so accessible for testing. */
    final BlockNodeContext blockNodeContext;
    /** list of all loaded plugins. Package so accessible for testing. */
    final List<BlockNodePlugin> loadedPlugins = new ArrayList<>();
    /** The metrics provider. Package so accessible for testing. */
    final DefaultMetricsProvider metricsProvider;
    /** metric to report the BN App oldest available block **/
    private LongGauge historicalOldestBlockGauge;
    /** metric to report the BN App latest/newest available block **/
    private LongGauge historicalLatestBlockGauge;
    /** metric to report the BN App state (0) Starting, (1) Running and (2) Shutting_Down**/
    private LongGauge appStateStatus;

    /**
     * Constructor for the BlockNodeApp class. This constructor initializes the server configuration,
     * loads the plugins, and creates the web server.
     *
     * @param serviceLoader Optional function to load the service loader, if null then the default will be used
     * @param shouldExitJvmOnShutdown if true, the JVM will exit on shutdown, otherwise it will not
     * @throws IOException if there is an error starting the server
     */
    BlockNodeApp(final ServiceLoaderFunction serviceLoader, final boolean shouldExitJvmOnShutdown) throws IOException {
        this.shouldExitJvmOnShutdown = shouldExitJvmOnShutdown;
        // ==== LOAD LOGGING CONFIG ====================================================================================
        final boolean externalLogging = System.getProperty("java.util.logging.config.file") != null;
        if (externalLogging) {
            LOGGER.log(INFO, "External logging configuration found");
        } else {
            // load the logging configuration from the classpath and make it colorful
            try (var loggingConfigIn = BlockNodeApp.class.getClassLoader().getResourceAsStream("logging.properties")) {
                if (loggingConfigIn != null) {
                    LogManager.getLogManager().readConfiguration(loggingConfigIn);
                } else {
                    LOGGER.log(INFO, "No logging configuration found");
                }
            } catch (IOException e) {
                LOGGER.log(INFO, "Failed to load logging configuration", e);
            }
            CleanColorfulFormatter.makeLoggingColorful();
            LOGGER.log(INFO, "Using default logging configuration");
        }
        // tell helidon to use the same logging configuration
        System.setProperty("io.helidon.logging.config.disabled", "true");
        // ==== LOG HIERO MODULES ======================================================================================
        // this can be useful when debugging issues with modules/plugins not being loaded
        LOGGER.log(INFO, "=".repeat(120));
        LOGGER.log(INFO, "Loaded Hiero Java modules:");
        // log all the modules loaded by the class loader
        final String moduleClassPath = System.getProperty("jdk.module.path");
        if (moduleClassPath != null) {
            final String[] moduleClassPathArray = moduleClassPath.split(":");
            for (String module : moduleClassPathArray) {
                if (module.contains("hiero")) {
                    LOGGER.log(INFO, GREY + "    " + module);
                }
            }
        }
        // ==== FACILITY & PLUGIN LOADING ==============================================================================
        // Load Block Messaging Service plugin - for now allow nulls
        final BlockMessagingFacility blockMessagingService = serviceLoader
                .loadServices(BlockMessagingFacility.class)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No BlockMessagingFacility provided"));
        loadedPlugins.add(blockMessagingService);
        // Load HistoricalBlockFacilityImpl
        historicalBlockFacility = new HistoricalBlockFacilityImpl(serviceLoader);
        loadedPlugins.add(historicalBlockFacility);
        loadedPlugins.addAll(historicalBlockFacility.allBlockProvidersPlugins());
        // Load all the plugins, just the classes are crated at this point, they are not initialized
        serviceLoader.loadServices(BlockNodePlugin.class).forEach(loadedPlugins::add);
        // ==== CONFIGURATION ==========================================================================================
        // Collect all the config data types from the plugins and global server level
        final List<Class<? extends Record>> allConfigDataTypes = new ArrayList<>();
        allConfigDataTypes.add(ServerConfig.class);
        allConfigDataTypes.add(WebServerHttp2Config.class);
        allConfigDataTypes.add(NodeConfig.class);
        loadedPlugins.forEach(plugin -> allConfigDataTypes.addAll(plugin.configDataTypes()));
        // Init BlockNode Configuration
        String appProperties = getClass().getClassLoader().getResource(APPLICATION_TEST_PROPERTIES) != null
                ? APPLICATION_TEST_PROPERTIES
                : APPLICATION_PROPERTIES;
        //noinspection unchecked
        final ConfigurationBuilder configurationBuilder = ConfigurationBuilder.create()
                .withSource(new AutomaticEnvironmentVariableConfigSource(allConfigDataTypes, System::getenv))
                .withSources(new ClasspathFileConfigSource(Path.of(appProperties)))
                .withConfigDataType(com.swirlds.common.metrics.config.MetricsConfig.class)
                .withConfigDataType(com.swirlds.common.metrics.platform.prometheus.PrometheusConfig.class)
                .withConfigDataTypes(allConfigDataTypes.toArray(new Class[0]));
        // Build the configuration
        final Configuration configuration = configurationBuilder.build();
        // Log the configuration
        ConfigLogger.log(configuration);
        // now that configuration is loaded we can get config for server
        serverConfig = configuration.getConfigData(ServerConfig.class);
        WebServerHttp2Config webServerHttp2Config = configuration.getConfigData(WebServerHttp2Config.class);
        // ==== METRICS ================================================================================================
        metricsProvider = new DefaultMetricsProvider(configuration);
        final Metrics metrics = metricsProvider.createGlobalMetrics();
        // ==== THREAD POOL MANAGER ====================================================================================
        final ThreadPoolManager threadPoolManager = new DefaultThreadPoolManager();
        // ==== CONTEXT ================================================================================================
        blockNodeContext = new BlockNodeContext(
                configuration,
                metrics,
                this,
                blockMessagingService,
                historicalBlockFacility,
                serviceLoader,
                threadPoolManager);
        // ==== CREATE ROUTING BUILDERS ================================================================================
        // Create HTTP & GRPC routing builders
        final ServiceBuilderImpl serviceBuilder = new ServiceBuilderImpl();
        // ==== INITIALIZE PLUGINS =====================================================================================
        // Initialize all the facilities & plugins, adding routing for each plugin
        LOGGER.log(INFO, "Initializing plugins:");
        for (BlockNodePlugin plugin : loadedPlugins) {
            LOGGER.log(INFO, GREY + "    " + plugin.name());
            plugin.init(blockNodeContext, serviceBuilder);
        }
        // ==== LOAD & CONFIGURE WEB SERVER ============================================================================
        // Override the default message size in PBJ
        final PbjConfig pbjConfig = PbjConfig.builder()
                .name(PBJ_PROTOCOL_PROVIDER_CONFIG_NAME)
                .maxMessageSizeBytes(serverConfig.maxMessageSizeBytes())
                .build();

        // Http2 Config
        final Http2Config http2Config = Http2Config.builder()
                .flowControlTimeout(Duration.parse(webServerHttp2Config.flowControlTimeout()))
                .initialWindowSize(webServerHttp2Config.initialWindowSize())
                .maxConcurrentStreams(webServerHttp2Config.maxConcurrentStreams())
                .maxEmptyFrames(webServerHttp2Config.maxEmptyFrames())
                .maxFrameSize(webServerHttp2Config.maxFrameSize())
                .maxHeaderListSize(webServerHttp2Config.maxHeaderListSize())
                .maxRapidResets(webServerHttp2Config.maxRapidResets())
                .rapidResetCheckPeriod(Duration.parse(webServerHttp2Config.rapidResetCheckPeriod()))
                .build();

        // Create the web server and configure
        webServer = WebServerConfig.builder()
                .port(serverConfig.port())
                .addProtocol(http2Config)
                .addProtocol(pbjConfig)
                .addRouting(serviceBuilder.httpRoutingBuilder())
                .addRouting(serviceBuilder.grpcRoutingBuilder())
                .connectionConfig(ConnectionConfig.builder()
                        .sendBufferSize(serverConfig.socketSendBufferSizeBytes())
                        .receiveBufferSize(serverConfig.socketSendBufferSizeBytes())
                        .build())
                .build();

        // Init the app metrics
        historicalOldestBlockGauge =
                metrics.getOrCreate(new LongGauge.Config(METRICS_CATEGORY, "app_historical_oldest_block")
                        .withDescription("The oldest block the BN has access to"));
        historicalLatestBlockGauge =
                metrics.getOrCreate(new LongGauge.Config(METRICS_CATEGORY, "app_historical_newest_block")
                        .withDescription("The newest block the BN has"));
        appStateStatus = metrics.getOrCreate(new LongGauge.Config(METRICS_CATEGORY, "app_state_status")
                .withDescription("The current state of the BlockNode App"));
    }

    /**
     * Starts the block node server. This method initializes all the plugins, starts the web server,
     * and starts the metrics.
     */
    void start() {
        LOGGER.log(INFO, LIGHT_GREEN + "Starting BlockNode Server on port {0}", serverConfig.port());
        // Start the web server
        webServer.start();
        // Start metrics
        metricsProvider.start();
        // Start all the facilities & plugins
        LOGGER.log(INFO, "Starting plugins:");
        for (BlockNodePlugin plugin : loadedPlugins) {
            LOGGER.log(INFO, GREY + "    " + plugin.name());
            plugin.start();
        }
        // mark the server as started
        state.set(State.RUNNING);
        // log the server has started
        LOGGER.log(
                INFO,
                LIGHT_GREEN + "Started BlockNode Server : State = {0}, Historic blocks = {1}",
                state.get(),
                historicalBlockFacility
                        .availableBlocks()
                        .streamRanges()
                        .map(LongRange::toString)
                        .collect(Collectors.joining(", ")));
        // register the app metrics updater
        blockNodeContext.metrics().addUpdater(this::updateAppMetrics);
    }

    /**
     * Updates the gauge metrics with the latest block range values.
     * This method is called periodically to update the metrics
     * with the latest values.
     */
    private void updateAppMetrics() {
        long minBlock = historicalBlockFacility.availableBlocks().min();
        long maxBlock = historicalBlockFacility.availableBlocks().max();
        long currentState = state.get().ordinal();

        historicalOldestBlockGauge.set(minBlock);
        historicalLatestBlockGauge.set(maxBlock);
        appStateStatus.set(currentState);
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
        if (webServer != null) webServer.stop();
        // Stop all the facilities &  plugins
        for (BlockNodePlugin plugin : loadedPlugins) {
            LOGGER.log(INFO, "Stopping plugin: {0}", plugin.name());
            plugin.stop();
        }
        // Stop metrics
        if (metricsProvider != null) metricsProvider.stop();
        // finally exit
        LOGGER.log(INFO, "Bye bye");
        if (shouldExitJvmOnShutdown) System.exit(0);
    }

    /**
     * Main entrypoint for the block node server
     *
     * @param args Command line arguments. Not used at present.
     * @throws IOException if there is an error starting the server
     */
    public static void main(final String[] args) throws IOException {
        BlockNodeApp server = new BlockNodeApp(new ServiceLoaderFunction(), true);
        server.start();
    }
}
