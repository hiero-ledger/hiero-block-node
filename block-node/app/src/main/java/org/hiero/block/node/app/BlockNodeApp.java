// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

import static java.lang.System.Logger;
import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;
import static org.hiero.block.common.constants.StringsConstants.APPLICATION_PROPERTIES;
import static org.hiero.block.common.constants.StringsConstants.APPLICATION_TEST_PROPERTIES;
import static org.hiero.block.node.spi.BlockNodePlugin.METRICS_CATEGORY;

import com.hedera.hapi.block.stream.Block;
import com.hedera.pbj.grpc.helidon.config.PbjConfig;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.config.extensions.sources.ClasspathFileConfigSource;
import com.swirlds.config.extensions.sources.SystemPropertiesConfigSource;
import io.helidon.common.socket.SocketOptions;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.WebServerConfig;
import io.helidon.webserver.http2.Http2Config;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.LogManager;
import java.util.stream.Collectors;
import org.hiero.block.api.BlockNodeVersions;
import org.hiero.block.api.BlockNodeVersions.PluginVersion;
import org.hiero.block.api.TssData;
import org.hiero.block.node.app.config.AutomaticEnvironmentVariableConfigSource;
import org.hiero.block.node.app.config.ServerConfig;
import org.hiero.block.node.app.config.WebServerHttp2Config;
import org.hiero.block.node.app.config.node.NodeConfig;
import org.hiero.block.node.app.logging.CleanColorfulFormatter;
import org.hiero.block.node.app.logging.ConfigLogger;
import org.hiero.block.node.spi.ApplicationStateFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceLoaderFunction;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.health.HealthFacility;
import org.hiero.block.node.spi.historicalblocks.LongRange;
import org.hiero.block.node.spi.module.SemanticVersionUtility;
import org.hiero.block.node.spi.threading.ThreadPoolManager;
import org.hiero.metrics.ObservableGauge;
import org.hiero.metrics.core.MetricKey;
import org.hiero.metrics.core.MetricRegistry;

/** Main class for the block node server */
public class BlockNodeApp implements HealthFacility, ApplicationStateFacility {
    /** Constant mapped to PbjProtocolProvider.CONFIG_NAME in the PBJ Helidon Plugin */
    public static final String PBJ_PROTOCOL_PROVIDER_CONFIG_NAME = "pbj";
    /** Metric key for the oldest historical block available */
    public static final MetricKey<ObservableGauge> METRIC_APP_HISTORICAL_OLDEST_BLOCK =
            MetricKey.of("app_historical_oldest_block", ObservableGauge.class).addCategory(METRICS_CATEGORY);
    /** Metric key for the newest historical block available */
    public static final MetricKey<ObservableGauge> METRIC_APP_HISTORICAL_NEWEST_BLOCK =
            MetricKey.of("app_historical_newest_block", ObservableGauge.class).addCategory(METRICS_CATEGORY);
    /** Metric key for the current state status of the app */
    public static final MetricKey<ObservableGauge> METRIC_APP_STATE_STATUS =
            MetricKey.of("app_state_status", ObservableGauge.class).addCategory(METRICS_CATEGORY);
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
    BlockNodeContext blockNodeContext;
    /** list of all loaded plugins. Package so accessible for testing. */
    final List<BlockNodePlugin> loadedPlugins = new ArrayList<>();

    /** Create a ConcurrentLinkedQueue to hold TssData updates */
    private final ConcurrentLinkedQueue<TssData> tssDataUpdates = new ConcurrentLinkedQueue<>();

    /** The ScheduledExecutorService used by the ApplicationStateFacility to check for TssData updates */
    private ScheduledExecutorService applicationStateExecutor;

    /**
     * Constructor for the BlockNodeApp class. This constructor initializes the server configuration,
     * loads the plugins, and creates the web server.
     *
     * @param serviceLoader Optional function to load the service loader, if null then the default will be used
     * @param shouldExitJvmOnShutdown if true, the JVM will exit on shutdown, otherwise it will not
     * @throws IOException if there is an error starting the server
     */
    public BlockNodeApp(final ServiceLoaderFunction serviceLoader, final boolean shouldExitJvmOnShutdown)
            throws IOException {
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
                    LOGGER.log(INFO, "    " + module);
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
                .autoDiscoverExtensions()
                .withSource(new AutomaticEnvironmentVariableConfigSource(allConfigDataTypes, System::getenv))
                .withSource(SystemPropertiesConfigSource.getInstance())
                .withSources(new ClasspathFileConfigSource(Path.of(appProperties)))
                .withConfigDataTypes(allConfigDataTypes.toArray(new Class[0]));
        // Build the configuration
        final Configuration configuration = configurationBuilder.build();
        // Log the configuration
        ConfigLogger.log(configuration);
        // now that configuration is loaded we can get config for server
        serverConfig = configuration.getConfigData(ServerConfig.class);
        WebServerHttp2Config webServerHttp2Config = configuration.getConfigData(WebServerHttp2Config.class);
        // ==== METRICS ================================================================================================
        // discover all metrics providers via SPI
        MetricRegistry metricRegistry = MetricRegistry.builder()
                .discoverMetricProviders()
                .discoverMetricsExporter(configuration)
                .build();
        // ==== THREAD POOL MANAGER ====================================================================================
        final ThreadPoolManager threadPoolManager = new DefaultThreadPoolManager();
        // ==== CONTEXT ================================================================================================
        blockNodeContext = new BlockNodeContext(
                configuration,
                metricRegistry,
                this,
                blockMessagingService,
                historicalBlockFacility,
                this,
                serviceLoader,
                threadPoolManager,
                versionInfo(loadedPlugins),
                null);
        // ==== CREATE ROUTING BUILDERS ================================================================================
        // Create HTTP & GRPC routing builders
        final ServiceBuilderImpl serviceBuilder = new ServiceBuilderImpl();
        // ==== INITIALIZE PLUGINS =====================================================================================
        // Initialize all the facilities & plugins, adding routing for each plugin
        LOGGER.log(INFO, "Initializing plugins:");
        for (BlockNodePlugin plugin : loadedPlugins) {
            LOGGER.log(INFO, "    " + plugin.name());
            plugin.init(blockNodeContext, serviceBuilder);
        }
        // ==== LOAD & CONFIGURE WEB SERVER ============================================================================
        // Override the default message size in PBJ
        final PbjConfig pbjConfig = PbjConfig.builder()
                .name(PBJ_PROTOCOL_PROVIDER_CONFIG_NAME)
                .maxMessageSizeBytes(serverConfig.maxMessageSizeBytes())
                .build();

        // Http2 Config more info at
        // https://helidon.io/docs/v4/apidocs/io.helidon.webserver.http2/io/helidon/webserver/http2/Http2Config.html
        final Http2Config http2Config = Http2Config.builder()
                .flowControlTimeout(Duration.ofMillis(webServerHttp2Config.flowControlTimeout()))
                .initialWindowSize(webServerHttp2Config.initialWindowSize())
                .maxConcurrentStreams(webServerHttp2Config.maxConcurrentStreams())
                .maxEmptyFrames(webServerHttp2Config.maxEmptyFrames())
                .maxFrameSize(webServerHttp2Config.maxFrameSize())
                .maxHeaderListSize(webServerHttp2Config.maxHeaderListSize())
                .maxRapidResets(webServerHttp2Config.maxRapidResets())
                .rapidResetCheckPeriod(Duration.ofMillis(webServerHttp2Config.rapidResetCheckPeriod()))
                .build();

        // Create the web server and configure
        webServer = WebServerConfig.builder()
                .port(serverConfig.port())
                .addProtocol(http2Config)
                .addProtocol(pbjConfig)
                .addRouting(serviceBuilder.httpRoutingBuilder())
                .addRouting(serviceBuilder.grpcRoutingBuilder())
                .connectionOptions(SocketOptions.builder()
                        .socketSendBufferSize(serverConfig.socketSendBufferSizeBytes())
                        .socketReceiveBufferSize(serverConfig.socketReceiveBufferSizeBytes())
                        .tcpNoDelay(serverConfig.tcpNoDelay())
                        .build())
                .backlog(serverConfig.backlogSize())
                .writeQueueLength(serverConfig.writeQueueLength())
                .maxTcpConnections(serverConfig.maxTcpConnections())
                .idleConnectionPeriod(Duration.ofMinutes(serverConfig.idleConnectionPeriodMinutes()))
                .idleConnectionTimeout(Duration.ofMinutes(serverConfig.idleConnectionTimeoutMinutes()))
                .build();

        // Init the app metrics
        metricRegistry.register(ObservableGauge.builder(METRIC_APP_HISTORICAL_OLDEST_BLOCK)
                .setDescription("The oldest block the BN has access to")
                .observe(() -> historicalBlockFacility.availableBlocks().min()));
        metricRegistry.register(ObservableGauge.builder(METRIC_APP_HISTORICAL_NEWEST_BLOCK)
                .setDescription("The newest block the BN has")
                .observe(() -> historicalBlockFacility.availableBlocks().max()));
        metricRegistry.register(ObservableGauge.builder(METRIC_APP_STATE_STATUS)
                .setDescription("The current state of the BlockNode App")
                .observe(() -> state.get().ordinal()));
    }

    /**
     * Build the BlockNodeVersions for this BlockNodeServer
     */
    protected final BlockNodeVersions versionInfo(final List<BlockNodePlugin> plugins) {
        final List<PluginVersion> pluginVersions = new ArrayList<>();
        for (final BlockNodePlugin plugin : plugins) {
            pluginVersions.add(plugin.version());
        }

        return BlockNodeVersions.newBuilder()
                .installedPluginVersions(pluginVersions)
                .blockNodeVersion(SemanticVersionUtility.from(BlockNodeApp.class))
                .streamProtoVersion(SemanticVersionUtility.from(Block.class))
                .build();
    }

    /**
     * Starts the block node server. This method initializes all the plugins, starts the web server,
     * and starts the metrics.
     */
    public void start() {
        LOGGER.log(INFO, "Starting BlockNode Server on port {0,number,#}", serverConfig.port());
        // Start the web server
        webServer.start();
        // start the ApplicationStateFacility
        startApplicationStateFacility();
        // start the plugins
        startPlugins(loadedPlugins);
        // mark the server as started
        state.set(State.RUNNING);
        // log the server has started
        LOGGER.log(
                INFO,
                "Started BlockNode Server : State={0} HistoricBlockRange={1}",
                state.get(),
                historicalBlockFacility
                        .availableBlocks()
                        .streamRanges()
                        .map(LongRange::toString)
                        .collect(Collectors.joining(", ")));
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
        LOGGER.log(INFO, "Shutting down, reason={0} class={1}", reason, className);
        // stop the application state facility
        stopApplicationStateFacility();

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
        try {
            blockNodeContext.metricRegistry().close();
            LOGGER.log(DEBUG, "Metric registry successfully closed.");
        } catch (IOException e) {
            LOGGER.log(DEBUG, "Could not properly close metric registry.", e);
        }
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

    /**
     *  Start the loadedPlugins. Use a separate method to make starting plugins testable
     */
    protected void startPlugins(List<BlockNodePlugin> plugins) {
        // Start all the facilities & plugins asynchronously
        LOGGER.log(INFO, "Asynchronously Starting plugins:");
        // Asynchronously start the plugins
        plugins.parallelStream().forEach(plugin -> {
            LOGGER.log(INFO, "    " + plugin.name());
            plugin.start();
        });
    }

    /**
     * Allow plugins to update the TssData for this BlockNodeApp
     *
     * @param tssData - The TssData to be updated on the `BlockNodeContext`
     */
    @Override
    public void updateTssData(TssData tssData) {
        if (tssData != null) tssDataUpdates.add(tssData);
    }

    /**
     * UncaughtExceptionHandler for logging uncaught exceptions
     */
    static void uncaughtExceptionHandler(Thread thread, Throwable throwable) {
        LOGGER.log(WARNING, "Uncaught exception in ApplicationStateFacility thread: " + thread.getName(), throwable);
    }

    /**
     * Starts the ApplicationStateFacility. The thread will be used to check if there are any TssData updates to
     * process.
     */
    void startApplicationStateFacility() {
        LOGGER.log(INFO, "ApplicationStateFacility start called");

        // ==== LOAD APPLICATION STATE =================================================================================
        loadApplicationState(blockNodeContext.configuration());

        // Create thread executors via threadPoolManager.
        applicationStateExecutor = blockNodeContext
                .threadPoolManager()
                .createVirtualThreadScheduledExecutor(
                        1, "ApplicationStateScanner", BlockNodeApp::uncaughtExceptionHandler);

        NodeConfig nodeConfig = blockNodeContext.configuration().getConfigData(NodeConfig.class);

        // Schedule periodic gap detection task using autonomous executor
        applicationStateExecutor.scheduleAtFixedRate(
                this::checkForApplicationStateUpdates,
                nodeConfig.appStateUpdateInitialDelay(),
                nodeConfig.appStateUpdateScanInterval(),
                TimeUnit.MILLISECONDS);
    }

    private void checkForApplicationStateUpdates() {
        boolean updated = false;
        TssData tssData = tssDataUpdates.poll();
        while (tssData != null) {
            // Because we only update TssData for the most recent blockNumber,
            // |= will let us know if any TssData were updated.
            updated |= updateBlockNodeContext(tssData);
            tssData = tssDataUpdates.poll();
        }

        if (updated) {
            loadedPlugins.parallelStream().forEach(plugin -> plugin.onContextUpdate(blockNodeContext));
            LOGGER.log(INFO, "ApplicationStateFacility called plugin.onContextUpdate for all plugins");
            persistTssData(blockNodeContext.tssData());
            LOGGER.log(INFO, "ApplicationStateFacility persisted TssData");
        }
    }

    void stopApplicationStateFacility() {
        if (applicationStateExecutor != null) {
            applicationStateExecutor.shutdownNow();
            try {
                if (!applicationStateExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    final String executorTerminationMsg = "applicationStateExecutor did not terminate in time";
                    LOGGER.log(INFO, executorTerminationMsg);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Update the BlockNodeContext with the new TssData if the validFromBlock is greater than that of the context
     *
     * @param tssData The TssData to update
     * @return A boolean indicating if the BlockNodeContext was updated.
     */
    private boolean updateBlockNodeContext(TssData tssData) {
        boolean updated = false;
        if (blockNodeContext.tssData() == null
                || blockNodeContext.tssData().validFromBlock() < tssData.validFromBlock()) {
            blockNodeContext = new BlockNodeContext(
                    blockNodeContext.configuration(),
                    blockNodeContext.metricRegistry(),
                    blockNodeContext.serverHealth(),
                    blockNodeContext.blockMessaging(),
                    blockNodeContext.historicalBlockProvider(),
                    blockNodeContext.applicationStateFacility(),
                    blockNodeContext.serviceLoader(),
                    blockNodeContext.threadPoolManager(),
                    blockNodeContext.blockNodeVersions(),
                    tssData);
            LOGGER.log(INFO, "BlockNodeContext updated");
            updated = true;
        }
        return updated;
    }

    /**
     * Persist the TssData
     * Persists the TssData to the file path specified in the NodeConfig class.
     *
     * @param tssData The TssData to persist
     */
    private void persistTssData(TssData tssData) {
        final Path appStateDataFilePath =
                blockNodeContext.configuration().getConfigData(NodeConfig.class).appStateDataFilePath();
        try {
            Files.createDirectories(appStateDataFilePath.getParent());
            Bytes serialized = TssData.JSON.toBytes(tssData);
            Files.write(appStateDataFilePath, serialized.toByteArray());
            LOGGER.log(INFO, "Persisted Application State Data to file: {0}", appStateDataFilePath);
        } catch (IOException e) {
            LOGGER.log(
                    WARNING,
                    "Failed to persist Application State Data to %s: %s".formatted(appStateDataFilePath, e),
                    e);
        }
    }

    /**
     * Loads the ApplicationState
     *
     * Loads the ApplicationState from file path(s) specified in the NodeConfig class.
     *
     * This must be called after the blockNode context is created
     */
    private void loadApplicationState(Configuration configuration) {
        final Path tssDataJsonPath =
                configuration.getConfigData(NodeConfig.class).appStateDataFilePath();
        if (Files.exists(tssDataJsonPath)) {
            try {
                TssData tssData = TssData.JSON.parse(Bytes.wrap(Files.readAllBytes(tssDataJsonPath)));
                updateTssData(tssData);
                LOGGER.log(INFO, "Loaded Application State Data from file: {0}", tssDataJsonPath);
            } catch (ParseException | IOException e) {
                LOGGER.log(ERROR, "Failed to read Application State Data file: " + tssDataJsonPath, e);
            }
        }
    }
}
