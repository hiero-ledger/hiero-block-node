// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

import static java.lang.System.Logger;
import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.TRACE;
import static java.lang.System.Logger.Level.WARNING;
import static org.hiero.block.common.constants.StringsConstants.APPLICATION_PROPERTIES;
import static org.hiero.block.common.constants.StringsConstants.APPLICATION_TEST_PROPERTIES;
import static org.hiero.block.node.base.ParseHelper.standardParse;
import static org.hiero.block.node.spi.BlockNodePlugin.METRICS_CATEGORY;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.pbj.grpc.helidon.PbjRouting;
import com.hedera.pbj.grpc.helidon.config.PbjConfig;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.config.extensions.sources.ClasspathFileConfigSource;
import com.swirlds.config.extensions.sources.SystemPropertiesConfigSource;
import io.helidon.common.socket.SocketOptions;
import io.helidon.webserver.ListenerConfig;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.WebServerConfig;
import io.helidon.webserver.http.HttpRouting;
import io.helidon.webserver.http2.Http2Config;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.LogManager;
import java.util.stream.Collectors;
import org.hiero.block.api.BlockNodeVersions;
import org.hiero.block.api.BlockNodeVersions.PluginVersion;
import org.hiero.block.api.BlockRange;
import org.hiero.block.api.NetworkData;
import org.hiero.block.api.TssData;
import org.hiero.block.internal.BlockRangesState;
import org.hiero.block.node.app.config.AutomaticEnvironmentVariableConfigSource;
import org.hiero.block.node.app.config.ServerConfig;
import org.hiero.block.node.app.config.WebServerHttp2Config;
import org.hiero.block.node.app.config.node.NodeConfig;
import org.hiero.block.node.app.config.state.ApplicationStateConfig;
import org.hiero.block.node.app.logging.CleanColorfulFormatter;
import org.hiero.block.node.app.logging.ConfigLogger;
import org.hiero.block.node.base.ranges.ConcurrentLongRangeSet;
import org.hiero.block.node.spi.ApplicationStateFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodeContext.Builder;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceLoaderFunction;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.health.HealthFacility;
import org.hiero.block.node.spi.historicalblocks.BlockRangeSet;
import org.hiero.block.node.spi.historicalblocks.LongRange;
import org.hiero.block.node.spi.module.SemanticVersionUtility;
import org.hiero.block.node.spi.threading.ThreadPoolManager;
import org.hiero.metrics.ObservableGauge;
import org.hiero.metrics.core.MetricKey;
import org.hiero.metrics.core.MetricRegistry;

/// Main class for the block node server
public class BlockNodeApp implements HealthFacility, ApplicationStateFacility {
    /// Constant mapped to PbjProtocolProvider.CONFIG\_NAME in the PBJ Helidon Plugin
    public static final String PBJ_PROTOCOL_PROVIDER_CONFIG_NAME = "pbj";
    /// Metric key for the oldest historical block available
    public static final MetricKey<ObservableGauge> METRIC_APP_HISTORICAL_OLDEST_BLOCK =
            MetricKey.of("app_historical_oldest_block", ObservableGauge.class).addCategory(METRICS_CATEGORY);
    /// Metric key for the newest historical block available
    public static final MetricKey<ObservableGauge> METRIC_APP_HISTORICAL_NEWEST_BLOCK =
            MetricKey.of("app_historical_newest_block", ObservableGauge.class).addCategory(METRICS_CATEGORY);
    /// Metric key for the current state status of the app
    public static final MetricKey<ObservableGauge> METRIC_APP_STATE_STATUS =
            MetricKey.of("app_state_status", ObservableGauge.class).addCategory(METRICS_CATEGORY);
    /// Number of stored blocks between automatic persistence of the block range sets
    private static final long BLOCK_RANGE_PERSIST_INTERVAL = 1000;
    /// Max protobuf/JSON message size for application-state files loaded from disk (small).
    private static final int MAX_APP_STATE_MESSAGE_SIZE_BYTES = 1 * 1024 * 1024;
    /** The logger for this class. */
    private static final Logger LOGGER = System.getLogger(BlockNodeApp.class.getName());
    /// The state of the server.
    private final AtomicReference<State> state = new AtomicReference<>(State.STARTING);
    /// The single WebServer instance serving all ports via named sockets. Package-private for testing.
    final WebServer webServer;
    /// All configured ports: primary first, then extra. Package-private for testing.
    final Set<Integer> allPorts;
    /// The server configuration.
    private final ServerConfig serverConfig;
    /// The historical block node facility
    private final HistoricalBlockFacilityImpl historicalBlockFacility;
    /// Should the shutdown() method exit the JVM.
    private final boolean shouldExitJvmOnShutdown;

    /// The block node context. It is marked as volatile for thread safety.
    /// It is written by the scheduled scanner thread, read by plugin threads.
    /// Plugins should take care to make a copy of the BlockNodeContext before
    /// they use it so that they get a consistent BlockNodeContext
    volatile BlockNodeContext blockNodeContext;
    /// list of all loaded plugins. Package so accessible for testing.
    final List<BlockNodePlugin> loadedPlugins = new ArrayList<>();

    /// Create a ConcurrentLinkedQueue to hold TssData updates
    private final ConcurrentLinkedQueue<TssData> tssDataUpdates = new ConcurrentLinkedQueue<>();

    /// Pending address book loaded at startup; consumed by the first checkForApplicationStateUpdates run.
    private final AtomicReference<NodeAddressBook> pendingAddressBook = new AtomicReference<>();

    /// Blocks reported as stored by plugins that do not serve them for retrieval
    final ConcurrentLongRangeSet storedBlocks = new ConcurrentLongRangeSet();

    /// Known inbound publishers loaded from configuration on startup; exposed for /statusz/inbound.
    private final AtomicReference<NetworkData> knownPublishers = new AtomicReference<>(NetworkData.DEFAULT);

    /// Designated inbound partners loaded from configuration on startup; exposed for /statusz/inbound.
    private final AtomicReference<NetworkData> inboundPartners = new AtomicReference<>(NetworkData.DEFAULT);

    /// Designated outbound partners loaded from configuration on startup; exposed for /statusz/outbound.
    private final AtomicReference<NetworkData> outboundPartners = new AtomicReference<>(NetworkData.DEFAULT);

    /// Backfill source connections reported by the backfill plugin; exposed for both /statusz endpoints.
    private final AtomicReference<NetworkData> backfillSources = new AtomicReference<>(NetworkData.DEFAULT);

    /// Block count at the time of the last scheduled persist; only read/written by the scanner thread
    private long lastPersistedBlockCount = 0;

    /// The ScheduledExecutorService used by the ApplicationStateFacility to check for TssData updates
    private ScheduledExecutorService applicationStateExecutor;

    /// Constructor for the BlockNodeApp class.
    /// This constructor initializes the server configuration, loads the
    /// plugins, and creates the web server.
    ///
    /// @param serviceLoader Optional function to load the service loader, if
    ///     null then the default will be used
    /// @param shouldExitJvmOnShutdown if true, the JVM will exit on shutdown,
    ///     otherwise it will not
    /// @throws IOException if there is an error starting the server
    public BlockNodeApp(final ServiceLoaderFunction serviceLoader, final boolean shouldExitJvmOnShutdown)
            throws IOException {
        this.shouldExitJvmOnShutdown = shouldExitJvmOnShutdown;
        // ==== LOAD LOGGING CONFIG ====================================================================================
        final boolean externalLogging = System.getProperty("java.util.logging.config.file") != null;
        if (externalLogging) {
            LOGGER.log(DEBUG, "External logging configuration found");
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
            LOGGER.log(DEBUG, "Using default logging configuration");
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
                    LOGGER.log(INFO, "    {0}", module);
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
        allConfigDataTypes.add(ApplicationStateConfig.class);
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
                null,
                null,
                new ArrayList<>(),
                new ArrayList<>());
        // ==== CREATE ROUTING BUILDERS ================================================================================
        // Create HTTP & GRPC routing builders; null port in plugin registrations resolves to server.port
        final ServiceBuilderImpl serviceBuilder = new ServiceBuilderImpl(serverConfig.port());
        // ==== INITIALIZE PLUGINS =====================================================================================
        // Initialize all the facilities & plugins, adding routing for each plugin
        for (BlockNodePlugin plugin : loadedPlugins) {
            LOGGER.log(INFO, "    {0}", plugin.name());
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

        // Build socket options shared by both servers
        final SocketOptions socketOptions = SocketOptions.builder()
                .socketSendBufferSize(serverConfig.socketSendBufferSizeBytes())
                .socketReceiveBufferSize(serverConfig.socketReceiveBufferSizeBytes())
                .tcpNoDelay(serverConfig.tcpNoDelay())
                .build();

        // Collect all ports registered by plugins; build a single WebServer with named sockets for extra ports.
        allPorts = new LinkedHashSet<>();
        allPorts.add(serverConfig.port());
        allPorts.addAll(serviceBuilder.grpcRoutingBuilders().keySet());
        allPorts.addAll(serviceBuilder.httpRoutingBuilders().keySet());

        webServer = buildWebServer(
                allPorts,
                http2Config,
                pbjConfig,
                socketOptions,
                serverConfig,
                serviceBuilder.grpcRoutingBuilders(),
                serviceBuilder.httpRoutingBuilders());

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

    /// Build the BlockNodeVersions for this BlockNodeServer
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

    /// Builds a single [WebServer].
    /// The first port in the set becomes the default socket; remaining ports
    /// are registered as named sockets (`"port-<portNumber>"`) so that all
    /// listeners share the same server process. The set must be non-empty.
    ///
    /// @param ports all ports to listen on; first element is the default socket
    /// @param http2Config the HTTP/2 configuration applied to every socket
    /// @param pbjConfig the PBJ protocol configuration applied to every socket
    /// @param socketOptions the socket-level options applied to every socket
    /// @param cfg the server configuration (timeouts, backlog, etc.)
    /// @param grpcBuilders per-port PBJ gRPC routing builders
    /// @param httpBuilders per-port HTTP routing builders
    /// @return a fully configured but not yet started [WebServer]
    private static WebServer buildWebServer(
            Set<Integer> ports,
            Http2Config http2Config,
            PbjConfig pbjConfig,
            SocketOptions socketOptions,
            ServerConfig cfg,
            Map<Integer, PbjRouting.Builder> grpcBuilders,
            Map<Integer, HttpRouting.Builder> httpBuilders) {
        final Iterator<Integer> portIterator = ports.iterator();
        final int primaryPort = portIterator.next();
        final WebServerConfig.Builder wsBuilder = WebServerConfig.builder().port(primaryPort);
        configureSocket(wsBuilder, primaryPort, http2Config, pbjConfig, socketOptions, cfg, grpcBuilders, httpBuilders);
        while (portIterator.hasNext()) {
            final int port = portIterator.next();
            final ListenerConfig.Builder socketBuilder =
                    ListenerConfig.builder().port(port);
            configureSocket(
                    socketBuilder, port, http2Config, pbjConfig, socketOptions, cfg, grpcBuilders, httpBuilders);
            wsBuilder.putSocket("port-" + port, socketBuilder.build());
        }
        return wsBuilder.build();
    }

    private static void configureSocket(
            ListenerConfig.BuilderBase<?, ?> builder,
            int port,
            Http2Config http2Config,
            PbjConfig pbjConfig,
            SocketOptions socketOptions,
            ServerConfig cfg,
            Map<Integer, PbjRouting.Builder> grpcBuilders,
            Map<Integer, HttpRouting.Builder> httpBuilders) {
        builder.addProtocol(http2Config);
        builder.addProtocol(pbjConfig);
        builder.connectionOptions(socketOptions);
        builder.backlog(cfg.backlogSize());
        builder.writeQueueLength(cfg.writeQueueLength());
        builder.maxTcpConnections(cfg.maxTcpConnections());
        builder.idleConnectionPeriod(Duration.ofMinutes(cfg.idleConnectionPeriodMinutes()));
        builder.idleConnectionTimeout(Duration.ofMinutes(cfg.idleConnectionTimeoutMinutes()));
        final HttpRouting.Builder http = httpBuilders.get(port);
        if (http != null) builder.addRouting(http);
        final PbjRouting.Builder grpc = grpcBuilders.get(port);
        if (grpc != null) builder.addRouting(grpc);
    }

    /// Starts the block node server. This method initializes all the plugins, starts the web server,
    /// and starts the metrics.
    public void start() {
        webServer.start();
        LOGGER.log(
                INFO,
                "BlockNode Server listening on port(s): {0}",
                allPorts.stream().map(String::valueOf).collect(Collectors.joining(", ")));
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

    /// {@inheritDoc}
    @Override
    public State blockNodeState() {
        return state.get();
    }

    /// {@inheritDoc}
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
        webServer.stop();
        // Stop all the facilities &  plugins
        for (BlockNodePlugin plugin : loadedPlugins) {
            LOGGER.log(INFO, "    {0}", plugin.name());
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
        LOGGER.log(INFO, "System Exiting");
        if (shouldExitJvmOnShutdown) System.exit(0);
    }

    /// Main entrypoint for the block node server
    ///
    /// @param args Command line arguments. Not used at present.
    /// @throws IOException if there is an error starting the server
    public static void main(final String[] args) throws IOException {
        BlockNodeApp server = new BlockNodeApp(new ServiceLoaderFunction(), true);
        server.start();
    }

    /// Start the loadedPlugins. Use a separate method to make starting plugins testable
    protected void startPlugins(List<BlockNodePlugin> plugins) {
        // Start all the facilities & plugins asynchronously
        // Asynchronously start the plugins
        plugins.parallelStream().forEach(plugin -> {
            plugin.start();
        });
    }

    /// Allow plugins to update the TssData for this BlockNodeApp.
    /// Uses a concurrentList to capture all TssData updates between scans.
    /// The ApplicationStateFacility scans for updates on a separate
    /// thread and will process any TssData updates that are newer than
    /// the current TssData.
    ///
    /// @param tssData The TssData to be updated on the \`BlockNodeContext\`
    @Override
    public void updateTssData(TssData tssData) {
        if (tssData != null) tssDataUpdates.add(tssData);
    }

    @Override
    public void addStoredBlockRange(LongRange blockRange) {
        storedBlocks.add(blockRange);
    }

    @Override
    public NetworkData knownPublishers() {
        return knownPublishers.get();
    }

    @Override
    public NetworkData inboundPartners() {
        return inboundPartners.get();
    }

    @Override
    public NetworkData outboundPartners() {
        return outboundPartners.get();
    }

    @Override
    public NetworkData backfillSources() {
        return backfillSources.get();
    }

    @Override
    public void updateBackfillSources(NetworkData sources) {
        backfillSources.set(sources != null ? sources : NetworkData.DEFAULT);
    }

    /// Allow plugins to update the NodeAddressBook for this BlockNodeApp.
    /// The address book is staged in a last-write-wins reference; if
    /// `updateAddressBook` is called more than once before the next
    /// `checkForApplicationStateUpdates` scan tick, only the most
    /// recent book is used. Null, empty, or all-blank-key books will
    ///  `return false;`
    ///
    /// @param nodeAddressBook the NodeAddressBook to store in BlockNodeContext
    /// @return true if the address book is queued for update, false if it was not
    @Override
    public boolean updateAddressBook(NodeAddressBook nodeAddressBook) {
        if (nodeAddressBook == null || nodeAddressBook.equals(blockNodeContext.nodeAddressBook())) return false;
        try {
            validateAddressBook(nodeAddressBook, "runtime update");
        } catch (IllegalStateException e) {
            LOGGER.log(INFO, "Rejecting invalid address book update: {0}", e);
            return false;
        }
        pendingAddressBook.set(nodeAddressBook);
        return true;
    }

    /// UncaughtExceptionHandler for logging uncaught exceptions
    static void uncaughtExceptionHandler(Thread thread, Throwable throwable) {
        LOGGER.log(WARNING, "Uncaught exception in ApplicationStateFacility thread: " + thread.getName(), throwable);
    }

    /// Starts the ApplicationStateFacility.
    /// The thread will be used to check if there are any TssData updates to process.
    void startApplicationStateFacility() {
        // ==== LOAD APPLICATION STATE =================================================================================
        loadApplicationState(blockNodeContext.configuration());

        // Flush any state loaded from disk (TssData queue + pending address book) into blockNodeContext
        // synchronously now, so plugins see the correct context when startPlugins() is called next.
        checkForApplicationStateUpdates();

        // Create thread executors via threadPoolManager.
        applicationStateExecutor = blockNodeContext
                .threadPoolManager()
                .createVirtualThreadScheduledExecutor(
                        1, "ApplicationStateScanner", BlockNodeApp::uncaughtExceptionHandler);

        ApplicationStateConfig appStateConfig =
                blockNodeContext.configuration().getConfigData(ApplicationStateConfig.class);

        // Schedule periodic check for live updates from running plugins.
        applicationStateExecutor.scheduleAtFixedRate(
                this::checkForApplicationStateUpdates,
                appStateConfig.updateInitialDelay(),
                appStateConfig.updateScanInterval(),
                TimeUnit.MILLISECONDS);
    }

    private void checkForApplicationStateUpdates() {

        // get any TssData update
        TssData tssData = getPendingTssData();
        if (tssData != null) {
            persistTssData(tssData);
        }

        final NodeAddressBook addressBook = pendingAddressBook.getAndSet(null);
        if (addressBook != null) {
            persistNodeAddressBook(addressBook);
        }

        if (updateBlockNodeContext(tssData, addressBook, storedBlocks, historicalBlockFacility.availableBlocks())) {
            loadedPlugins.parallelStream().forEach(plugin -> plugin.onContextUpdate(blockNodeContext));
        }

        // Persist block ranges whenever the running total crosses a BLOCK_RANGE_PERSIST_INTERVAL boundary.
        final long current = storedBlocks.size();
        if (current / BLOCK_RANGE_PERSIST_INTERVAL > lastPersistedBlockCount / BLOCK_RANGE_PERSIST_INTERVAL) {
            persistBlockRanges();
            lastPersistedBlockCount = current;
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
        // check for any pending Tss or Rsa updates and persist them.
        checkForApplicationStateUpdates();
        // calling persistBlockRanges separately so that it persists wherever it is.
        persistBlockRanges();
    }

    /// Get the latest TssData to update.
    ///
    /// @return The TssData to update or null if no updates pending.
    private TssData getPendingTssData() {
        boolean updated = false;
        TssData currTssData = blockNodeContext.tssData();
        TssData tssData = tssDataUpdates.poll();
        while (tssData != null) {
            if (currTssData == null || tssData.validFromBlock() > currTssData.validFromBlock()) {
                updated = true;
                currTssData = tssData;
            }
            tssData = tssDataUpdates.poll();
        }
        return updated ? currTssData : null;
    }

    /// Update the BlockNodeContext if either the provided TssData or NodeAddressBook is valid.
    /// TssData is considered valid if it is non-null and its `validFromBlock` is greater than
    /// the current context value. NodeAddressBook is considered valid if it is non-null.
    ///
    /// @param tssData the TssData to consider; may be null
    /// @param addressBook the NodeAddressBook to consider; may be null
    /// @return `true` if the BlockNodeContext was updated
    private boolean updateBlockNodeContext(
            TssData tssData, NodeAddressBook addressBook, BlockRangeSet storedBlocks, BlockRangeSet availableBlocks) {
        BlockNodeContext context = blockNodeContext;

        List<BlockRange> storedBlockRange = toBlockRange(storedBlocks);
        List<BlockRange> availableBlockRange = toBlockRange(availableBlocks);

        if (tssData == null
                && addressBook == null
                && storedBlockRange.hashCode() == context.storedBlocks().hashCode()
                && availableBlockRange.hashCode() == context.availableBlocks().hashCode()
                && storedBlockRange.equals(context.storedBlocks())
                && availableBlockRange.equals(context.availableBlocks())) {
            return false;
        }

        BlockNodeContext.Builder builder = new Builder(context);
        if (tssData != null) {
            builder.tssData(tssData);
        }

        if (addressBook != null) {
            builder.nodeAddressBook(addressBook);
        }

        // The next two items must remain in order (available first, stored second).
        if (availableBlockRange.hashCode() != context.availableBlocks().hashCode()
                || !availableBlockRange.equals(context.availableBlocks())) {
            builder.availableBlocks(availableBlockRange);
            storedBlockRange = mergeRanges(storedBlocks, availableBlocks);
        }
        if (storedBlockRange.hashCode() != context.storedBlocks().hashCode()
                || !storedBlockRange.equals(context.storedBlocks())) {
            builder.storedBlocks(storedBlockRange);
        }

        LOGGER.log(TRACE, "BlockNodeContext updated");
        // update the BlockNodeContext
        blockNodeContext = builder.build();
        return true;
    }

    /// Merge two sets of block ranges into a single list of block ranges.
    /// This is a very expensive method, O(n<sup>2</sup>), so it should be used
    /// carefully. Perhaps a future update to BlockRangeSet will add a more
    /// efficient merge process.
    ///
    /// @param storedBlocks a set of stored blocks to merge into the result.
    /// @param availableBlocks a set of available blocks to merge into the result.
    private List<BlockRange> mergeRanges(final BlockRangeSet storedBlocks, final BlockRangeSet availableBlocks) {
        ConcurrentLongRangeSet combined = new ConcurrentLongRangeSet();
        combined.addAll(storedBlocks.streamRanges().toList());
        combined.addAll(availableBlocks.streamRanges().toList());
        return combined.streamRanges()
                .map(longRange -> new BlockRange(longRange.start(), longRange.end()))
                .toList();
    }

    /// Convert BlockRangeSet to BlockRange
    private List<BlockRange> toBlockRange(BlockRangeSet blockRangeSet) {
        return blockRangeSet
                .streamRanges()
                .map(longRange -> new BlockRange(longRange.start(), longRange.end()))
                .toList();
    }

    /// Persist the TssData
    /// Persists the TssData to the file path specified in the ApplicationStateConfig class.
    ///
    /// @param tssData The TssData to persist
    private void persistTssData(TssData tssData) {
        final Path appStateDataFilePath = blockNodeContext
                .configuration()
                .getConfigData(ApplicationStateConfig.class)
                .tssBootstrapFilePath();
        try {
            Bytes serialized = TssData.JSON.toBytes(tssData);
            Files.write(appStateDataFilePath, serialized.toByteArray());
        } catch (IOException e) {
            LOGGER.log(WARNING, "Failed to persist TssData to %s: %s".formatted(appStateDataFilePath, e), e);
        }
    }

    private void persistNodeAddressBook(NodeAddressBook nodeAddressBook) {
        final Path filePath = blockNodeContext
                .configuration()
                .getConfigData(ApplicationStateConfig.class)
                .rsaBootstrapFilePath();
        try {
            final Path tmp = filePath.resolveSibling(filePath.getFileName() + ".tmp");
            final Bytes encoded = NodeAddressBook.JSON.toBytes(nodeAddressBook);
            Files.write(tmp, encoded.toByteArray());
            try {
                Files.move(tmp, filePath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException e) {
                LOGGER.log(
                        DEBUG,
                        "Atomic move not supported on this filesystem for {0}; falling back to non-atomic replace",
                        filePath);
                Files.move(tmp, filePath, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            LOGGER.log(
                    INFO,
                    "Failed to persist RSA address book to {0}: {1} — will re-fetch on next startup",
                    filePath,
                    e);
        }
    }

    /// Persists both block range sets as JSON to the single file specified in the ApplicationStateConfig.
    private void persistBlockRanges() {
        final Path filePath = blockNodeContext
                .configuration()
                .getConfigData(ApplicationStateConfig.class)
                .blockRangesFilePath();
        try {
            final Path tmp = filePath.resolveSibling(filePath.getFileName() + ".tmp");
            final Bytes json = BlockRangesState.JSON.toBytes(toBlockRangesState());
            Files.write(tmp, json.toByteArray());
            Files.deleteIfExists(filePath);
            Files.createLink(filePath, tmp);
            Files.deleteIfExists(tmp);
        } catch (IOException e) {
            LOGGER.log(WARNING, "Failed to persist block ranges to %s".formatted(filePath), e);
        }
    }

    private BlockRangesState toBlockRangesState() {
        final List<BlockRange> stored = storedBlocks
                .streamRanges()
                .map(r -> new BlockRange(r.start(), r.end()))
                .toList();
        final List<BlockRange> available = historicalBlockFacility
                .availableBlocks()
                .streamRanges()
                .map(r -> new BlockRange(r.start(), r.end()))
                .toList();
        return new BlockRangesState(stored, available);
    }

    /// Loads all ApplicationState from file paths specified in the ApplicationStateConfig class.
    /// Must be called after the BlockNodeContext is created and all plugins have been init'd.
    ///
    /// @param configuration the current configuration
    private void loadApplicationState(final Configuration configuration) {
        final ApplicationStateConfig appStateConfig = configuration.getConfigData(ApplicationStateConfig.class);

        // Load TssData (JSON format) — queued for processing on the next scanner tick.
        final Path tssDataJsonPath = appStateConfig.tssBootstrapFilePath();
        if (Files.exists(tssDataJsonPath)) {
            try {
                TssData tssData = standardParse(TssData.JSON, Bytes.wrap(Files.readAllBytes(tssDataJsonPath)));
                updateTssData(tssData);
            } catch (ParseException | IOException e) {
                LOGGER.log(WARNING, "Failed to read TssData file: " + tssDataJsonPath, e);
            }
        } else {
            // make sure the directory is created for the writes
            Path parent = tssDataJsonPath.getParent();
            try {
                Files.createDirectories(parent);
            } catch (IOException e) {
                LOGGER.log(WARNING, "Failed to create TssData directory: " + parent, e);
            }
        }

        // Load RSA NodeAddressBook (JSON format) — cached for processing on the next scanner tick.
        final Path rsaFilePath = appStateConfig.rsaBootstrapFilePath();
        if (Files.exists(rsaFilePath)) {
            try {
                final byte[] raw = Files.readAllBytes(rsaFilePath);
                final NodeAddressBook book = standardParse(NodeAddressBook.JSON, Bytes.wrap(raw));
                validateAddressBook(book, rsaFilePath.toString());
                pendingAddressBook.set(book);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to read RSA bootstrap file: " + rsaFilePath, e);
            } catch (ParseException e) {
                final String message =
                        "Corrupt RSA bootstrap file at %s — delete and restart to re-fetch from Mirror Node"
                                .formatted(rsaFilePath);
                throw new IllegalStateException(message, e);
            }
        } else {
            // make sure the directory is created for the writes
            Path parent = rsaFilePath.getParent();
            try {
                Files.createDirectories(parent);
            } catch (IOException e) {
                LOGGER.log(ERROR, "Failed to create RSA bootstrap directory: " + parent, e);
            }
        }

        // Load block ranges (JSON format) — restored directly into the in-memory range sets.
        final Path blockRangesPath = appStateConfig.blockRangesFilePath();
        if (Files.exists(blockRangesPath)) {
            try {
                final BlockRangesState rangeSet =
                        standardParse(BlockRangesState.JSON, Bytes.wrap(Files.readAllBytes(blockRangesPath)));
                rangeSet.storedBlocks().forEach(r -> storedBlocks.add(new LongRange(r.rangeStart(), r.rangeEnd())));
                LOGGER.log(INFO, "Loaded block ranges from file: {0}", blockRangesPath);
            } catch (ParseException | IOException | IllegalArgumentException e) {
                LOGGER.log(WARNING, "Failed to read block ranges file: " + blockRangesPath, e);
            }
        } else {
            Path parent = blockRangesPath.getParent();
            try {
                Files.createDirectories(parent);
            } catch (IOException e) {
                LOGGER.log(WARNING, "Failed to create block ranges directory: " + parent, e);
            }
        }

        // Load the connection-information sets (JSON-serialized NetworkData) used by the /statusz endpoints.
        // These are read-only configuration; absent or unreadable files yield an empty set.
        knownPublishers.set(loadNetworkData(appStateConfig.knownPublishersFilePath()));
        inboundPartners.set(loadNetworkData(appStateConfig.inboundPartnersFilePath()));
        outboundPartners.set(loadNetworkData(appStateConfig.outboundPartnersFilePath()));
    }

    /// Loads a [NetworkData] document from the given JSON file.
    /// Missing files and parse failures are logged and yield
    /// [NetworkData#DEFAULT] (an empty set) rather than throwing, mirroring
    /// the lenient handling used for other optional application-state files.
    ///
    /// @param path the JSON file to read
    /// @return the parsed NetworkData, or [NetworkData#DEFAULT] if
    ///     absent or unreadable
    static NetworkData loadNetworkData(final Path path) {
        if (path == null || !Files.exists(path)) {
            LOGGER.log(DEBUG, "Network data file not present, using empty set: {0}", path);
            return NetworkData.DEFAULT;
        }
        try {
            final NetworkData data = standardParse(
                    NetworkData.JSON, Bytes.wrap(Files.readAllBytes(path)), MAX_APP_STATE_MESSAGE_SIZE_BYTES);
            return data;
        } catch (ParseException | IOException e) {
            LOGGER.log(INFO, "Failed to read network data file %s.".formatted(path), e);
            return NetworkData.DEFAULT;
        }
    }

    /// Validates that the NodeAddressBook has at least one entry with a non-blank RSA\_PubKey.
    ///
    /// @param book the address book to validate
    /// @param source human-readable source name for error messages
    /// @throws IllegalStateException if the book is empty or has no usable entries
    static void validateAddressBook(final NodeAddressBook book, final String source) {
        if (book.nodeAddress().isEmpty()) {
            throw new IllegalStateException(
                    "RSA address book from %s contains no entries — cannot verify WRB proofs".formatted(source));
        }
        final long declared = book.nodeAddress().stream()
                .filter(a -> !a.rsaPubKey().isBlank())
                .count();
        final String noValidKeyMessage =
                "RSA address book from %s has %s entries but none have a valid RSA_PubKey".formatted(source, declared);
        if (declared == 0) {
            throw new IllegalStateException(noValidKeyMessage);
        }
        long usable = 0;
        final HexFormat hex = HexFormat.of();
        // Obtain KeyFactory once — provider lookup is not cheap and RSA must always be available.
        final KeyFactory kf;
        try {
            kf = KeyFactory.getInstance("RSA");
        } catch (NoSuchAlgorithmException e) {
            // RSA must be available in every JVM — this is a JVM misconfiguration
            throw new IllegalStateException("RSA KeyFactory not available", e);
        }
        for (final NodeAddress addr : book.nodeAddress()) {
            if (addr.rsaPubKey().isBlank()) {
                continue;
            }
            try {
                final byte[] keyBytes = hex.parseHex(addr.rsaPubKey());
                kf.generatePublic(new X509EncodedKeySpec(keyBytes));
                usable++;
            } catch (InvalidKeySpecException | IllegalArgumentException e) {
                LOGGER.log(INFO, "Malformed RSA_PubKey for node {0} — skipped: {1}", addr.nodeId(), e);
            }
        }
        if (usable == 0) {
            throw new IllegalStateException(noValidKeyMessage);
        }
    }
}
