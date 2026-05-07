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
import com.hedera.hapi.node.base.NodeAddressBook;
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
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
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
import org.hiero.block.node.app.config.state.ApplicationStateConfig;
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
    /** The block node context. Volatile: written by the scheduled scanner thread, read by plugin threads. */
    volatile BlockNodeContext blockNodeContext;
    /** list of all loaded plugins. Package so accessible for testing. */
    final List<BlockNodePlugin> loadedPlugins = new ArrayList<>();

    /** Create a ConcurrentLinkedQueue to hold TssData updates */
    private final ConcurrentLinkedQueue<TssData> tssDataUpdates = new ConcurrentLinkedQueue<>();

    /** Pending address book loaded at startup; consumed by the first checkForApplicationStateUpdates run. */
    private final AtomicReference<NodeAddressBook> pendingAddressBook = new AtomicReference<>();

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
     * Allow plugins to update the NodeAddressBook for this BlockNodeApp. The address book is staged
     * in a last-write-wins reference; if {@code updateAddressBook} is called more than once before
     * the next {@code checkForApplicationStateUpdates} scan tick, only the most recent book is used.
     * Null, empty, or all-blank-key books are silently rejected.
     *
     * @param nodeAddressBook the NodeAddressBook to store in BlockNodeContext
     */
    @Override
    public void updateAddressBook(NodeAddressBook nodeAddressBook) {
        if (nodeAddressBook == null) return;
        try {
            validateAddressBook(nodeAddressBook, "runtime update");
        } catch (IllegalStateException e) {
            LOGGER.log(WARNING, "Rejecting invalid address book update: {0}", e.getMessage());
            return;
        }
        pendingAddressBook.set(nodeAddressBook);
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
        boolean tssUpdated = false;
        TssData tssData = tssDataUpdates.poll();
        while (tssData != null) {
            // Because we only update TssData for the most recent blockNumber,
            // |= will let us know if any TssData were updated.
            tssUpdated |= updateBlockNodeContext(tssData, null);
            tssData = tssDataUpdates.poll();
        }

        // Consume any address book cached at startup (one-shot).
        boolean addressBookUpdated = false;
        final NodeAddressBook addressBook = pendingAddressBook.getAndSet(null);
        if (addressBook != null) {
            addressBookUpdated = updateBlockNodeContext(null, addressBook);
        }

        if (tssUpdated || addressBookUpdated) {
            loadedPlugins.parallelStream().forEach(plugin -> plugin.onContextUpdate(blockNodeContext));
            LOGGER.log(INFO, "ApplicationStateFacility called plugin.onContextUpdate for all plugins");
            if (tssUpdated) {
                persistTssData(blockNodeContext.tssData());
                LOGGER.log(INFO, "ApplicationStateFacility persisted TssData");
            }
            if (addressBookUpdated) {
                persistNodeAddressBook(blockNodeContext.nodeAddressBook());
                LOGGER.log(INFO, "ApplicationStateFacility persisted NodeAddressBook");
            }
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
     * Update the BlockNodeContext if either the provided TssData or NodeAddressBook is valid.
     * TssData is considered valid if it is non-null and its {@code validFromBlock} is greater than
     * the current context value. NodeAddressBook is considered valid if it is non-null.
     *
     * @param tssData     the TssData to consider; may be null
     * @param addressBook the NodeAddressBook to consider; may be null
     * @return {@code true} if the BlockNodeContext was updated
     */
    private boolean updateBlockNodeContext(TssData tssData, NodeAddressBook addressBook) {
        TssData newTss = blockNodeContext.tssData();
        NodeAddressBook newBook = blockNodeContext.nodeAddressBook();
        boolean updated = false;

        if (tssData != null && (newTss == null || newTss.validFromBlock() < tssData.validFromBlock())) {
            newTss = tssData;
            updated = true;
        }
        if (addressBook != null) {
            newBook = addressBook;
            updated = true;
        }

        if (updated) {
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
                    newTss,
                    newBook);
            LOGGER.log(INFO, "BlockNodeContext updated");
        }
        return updated;
    }

    /**
     * Persist the TssData
     * Persists the TssData to the file path specified in the ApplicationStateConfig class.
     *
     * @param tssData The TssData to persist
     */
    private void persistTssData(TssData tssData) {
        final Path appStateDataFilePath = blockNodeContext
                .configuration()
                .getConfigData(ApplicationStateConfig.class)
                .dataFilePath();
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

    private void persistNodeAddressBook(NodeAddressBook nodeAddressBook) {
        final Path filePath = blockNodeContext
                .configuration()
                .getConfigData(ApplicationStateConfig.class)
                .rsaBootstrapFilePath();
        try {
            Files.createDirectories(filePath.getParent());
            final Path tmp = filePath.resolveSibling(filePath.getFileName() + ".tmp");
            final Bytes encoded = NodeAddressBook.JSON.toBytes(nodeAddressBook);
            Files.write(tmp, encoded.toByteArray());
            try {
                Files.move(tmp, filePath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException e) {
                LOGGER.log(
                        WARNING,
                        "Atomic move not supported on this filesystem for {0}; falling back to non-atomic replace",
                        filePath);
                Files.move(tmp, filePath, StandardCopyOption.REPLACE_EXISTING);
            }
            LOGGER.log(INFO, "Persisted RSA address book to file: {0}", filePath);
        } catch (IOException e) {
            LOGGER.log(
                    WARNING,
                    "Failed to persist RSA address book to {0}: {1} — will re-fetch on next startup",
                    filePath,
                    e.getMessage());
        }
    }

    /**
     * Loads all ApplicationState from file paths specified in the ApplicationStateConfig class.
     * Must be called after the BlockNodeContext is created and all plugins have been init'd.
     *
     * @param configuration the current configuration
     */
    private void loadApplicationState(final Configuration configuration) {
        final ApplicationStateConfig appStateConfig = configuration.getConfigData(ApplicationStateConfig.class);

        // Load TssData (JSON format) — queued for processing on the next scanner tick.
        final Path tssDataJsonPath = appStateConfig.dataFilePath();
        if (Files.exists(tssDataJsonPath)) {
            try {
                TssData tssData = TssData.JSON.parse(Bytes.wrap(Files.readAllBytes(tssDataJsonPath)));
                updateTssData(tssData);
                LOGGER.log(INFO, "Loaded Application State Data from file: {0}", tssDataJsonPath);
            } catch (ParseException | IOException e) {
                LOGGER.log(ERROR, "Failed to read Application State Data file: " + tssDataJsonPath, e);
            }
        }

        // Load RSA NodeAddressBook (JSON format) — cached for processing on the next scanner tick.
        final Path rsaFilePath = appStateConfig.rsaBootstrapFilePath();
        if (Files.exists(rsaFilePath)) {
            try {
                final byte[] raw = Files.readAllBytes(rsaFilePath);
                final NodeAddressBook book = NodeAddressBook.JSON.parse(Bytes.wrap(raw));
                validateAddressBook(book, rsaFilePath.toString());
                pendingAddressBook.set(book);
                LOGGER.log(
                        INFO,
                        "Loaded RSA address book from file: {0} ({1} entries)",
                        rsaFilePath,
                        book.nodeAddress().size());
            } catch (IOException e) {
                throw new IllegalStateException("Failed to read RSA bootstrap file: " + rsaFilePath, e);
            } catch (ParseException e) {
                throw new IllegalStateException(
                        "Corrupt RSA bootstrap file at " + rsaFilePath
                                + " — delete and restart to re-fetch from Mirror Node",
                        e);
            }
        }
    }

    /**
     * Validates that the NodeAddressBook has at least one entry with a non-blank RSA_PubKey.
     *
     * @param book the address book to validate
     * @param source human-readable source name for error messages
     * @throws IllegalStateException if the book is empty or has no usable entries
     */
    static void validateAddressBook(final NodeAddressBook book, final String source) {
        if (book.nodeAddress().isEmpty()) {
            throw new IllegalStateException(
                    "RSA address book from " + source + " contains no entries — cannot verify WRB proofs");
        }
        final long usable = book.nodeAddress().stream()
                .filter(a -> !a.rsaPubKey().isBlank())
                .count();
        if (usable == 0) {
            throw new IllegalStateException("RSA address book from " + source + " has "
                    + book.nodeAddress().size() + " entries but none have a non-blank RSA_PubKey");
        }
    }
}
