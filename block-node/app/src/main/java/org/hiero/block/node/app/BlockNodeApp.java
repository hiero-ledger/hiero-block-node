// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

import static java.lang.System.Logger;
import static java.lang.System.Logger.Level.DEBUG;
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
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.config.extensions.sources.ClasspathFileConfigSource;
import com.swirlds.config.extensions.sources.SystemPropertiesConfigSource;
import io.helidon.common.socket.SocketOptions;
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
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.LogManager;
import java.util.stream.Collectors;
import org.hiero.block.api.BlockNodeVersions;
import org.hiero.block.api.BlockNodeVersions.PluginVersion;
import org.hiero.block.api.BlockRange;
import org.hiero.block.api.NetworkConnection.ConnectionReference;
import org.hiero.block.api.NetworkData;
import org.hiero.block.api.RangedAddressBookHistory;
import org.hiero.block.api.RangedNodeAddressBook;
import org.hiero.block.api.TssData;
import org.hiero.block.internal.BlockRangesState;
import org.hiero.block.node.app.config.AutomaticEnvironmentVariableConfigSource;
import org.hiero.block.node.app.config.ServerConfig;
import org.hiero.block.node.app.config.WebServerHttp2Config;
import org.hiero.block.node.app.config.state.ApplicationStateConfig;
import org.hiero.block.node.app.logging.CleanColorfulFormatter;
import org.hiero.block.node.app.logging.ConfigLogger;
import org.hiero.block.node.base.ranges.ConcurrentLongRangeSet;
import org.hiero.block.node.spi.ApplicationStateFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodeContext.Builder;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
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
    /// The logger for this class.
    private static final Logger LOGGER = System.getLogger(BlockNodeApp.class.getName());
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
    /// A connection reference that means "any host/any port".
    private static final ConnectionReference WILDCARD_CONNECTION =
            ConnectionReference.newBuilder().address("*").port("*").build();
    /// The "scheme" part of a grpc URI _without_ TLS.
    private static final String GRPC_SCHEME = "grpc";
    /// The "scheme" part of a grpc URI _with_ TLS.
    private static final String GRPC_TLS_SCHEME = "grpcs";
    /// The state of the server.
    private final AtomicReference<State> state = new AtomicReference<>(State.STARTING);
    /// A ServiceBuilder that creates, starts, and stops webservers.
    /// One "general" server for most plugins, and optional "additional" servers
    /// for plugins that need specific configuration changes.
    final ServiceBuilder serviceBuilder;
    /// package-private value for test assertions.
    final Set<Integer> portsEnabled;
    /// The server configuration.
    private final ServerConfig serverConfig;
    /// The configuration for the application state facility
    private final ApplicationStateConfig appStateConfig;
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

    /// Pending address book history loaded at startup; consumed by the first checkForApplicationStateUpdates run.
    private final AtomicReference<RangedAddressBookHistory> pendingAddressBookHistory = new AtomicReference<>();

    /** Cached O(log n) index built from the current address book history; rebuilt whenever history changes. */
    private volatile NavigableMap<Long, RangedNodeAddressBook> addressBookIndex = new TreeMap<>();

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
        // Init BlockNode Configuration
        String appProperties = getClass().getClassLoader().getResource(APPLICATION_TEST_PROPERTIES) != null
                ? APPLICATION_TEST_PROPERTIES
                : APPLICATION_PROPERTIES;
        final ConfigurationBuilder configurationBuilder =
                ConfigurationBuilder.create().autoDiscoverExtensions();
        // AutomaticEnvironmentVariableConfigSource does its own ConfigurationExtension lookup to learn every
        // config data type; it must be constructed after autoDiscoverExtensions() above returns, not from within
        // another extension's callback, or the nested ServiceLoader lookup deadlocks against the one in progress.
        configurationBuilder
                .withSource(new AutomaticEnvironmentVariableConfigSource(serviceLoader, System::getenv))
                .withSource(SystemPropertiesConfigSource.getInstance())
                .withSources(new ClasspathFileConfigSource(Path.of(appProperties)));
        // Build the configuration
        final Configuration configuration = configurationBuilder.build();
        // Log the configuration
        ConfigLogger.log(configuration);
        // now that configuration is loaded we can get config for server
        serverConfig = configuration.getConfigData(ServerConfig.class);
        appStateConfig = configuration.getConfigData(ApplicationStateConfig.class);
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

        // Create HTTP & GRPC routing builders; null port in plugin registrations resolves to server.port
        serviceBuilder = new ServiceBuilderImpl(serverConfig, http2Config, socketOptions);
        // ==== INITIALIZE PLUGINS =====================================================================================
        // Initialize all the facilities & plugins, adding routing for each plugin
        for (BlockNodePlugin plugin : loadedPlugins) {
            LOGGER.log(INFO, "    {0}", plugin.name());
            plugin.init(blockNodeContext, serviceBuilder);
        }

        // ==== LOAD & CONFIGURE WEB SERVER ============================================================================
        portsEnabled = serviceBuilder.buildGeneralWebServer();
        LOGGER.log(INFO, "BlockNode Primary Server configured on port(s): {0}", portsEnabled);

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

    /// Starts the block node server. This method initializes all the plugins, starts the web server,
    /// and starts the metrics.
    public void start() {
        // start the ApplicationStateFacility
        startApplicationStateFacility();
        // start the plugins
        startPlugins(loadedPlugins);
        // mark the server as started
        state.set(State.RUNNING);
        serviceBuilder.startAll();
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
        serviceBuilder.stopAll();
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

    /// Stages the supplied {@link RangedAddressBookHistory} for the next
    /// {@code checkForApplicationStateUpdates} scan tick. Last-write-wins if called multiple times
    /// before the next tick.
    ///
    /// @param history the history to store; must not be {@code null}
    /// @return {@code true} if queued, {@code false} if equal to the currently stored value
    @Override
    public boolean updateAddressBookHistory(RangedAddressBookHistory history) {
        if (history == null || history.equals(blockNodeContext.rangedAddressBookHistory())) return false;
        pendingAddressBookHistory.set(history);
        return true;
    }

    /// Returns the {@link NodeAddressBook} whose block range covers {@code blockNum}, using the
    /// cached index built from the current {@link RangedAddressBookHistory}.
    ///
    /// @param blockNum the block number to look up
    /// @return the matching {@link NodeAddressBook}, or {@code null} if no era covers it
    @Override
    public NodeAddressBook getAddressBookForBlock(long blockNum) {
        return AddressBookHistoryLookup.findAddressBookForBlock(addressBookIndex, blockNum);
    }

    private static boolean hasValidKey(NodeAddressBook book) {
        if (book == null || book.nodeAddress().isEmpty()) return false;
        return book.nodeAddress().stream()
                .anyMatch(a -> a.rsaPubKey() != null && !a.rsaPubKey().isBlank());
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

        final RangedAddressBookHistory addressBookHistory = pendingAddressBookHistory.getAndSet(null);
        if (addressBookHistory != null) {
            persistNodeAddressBookHistory(addressBookHistory);
            addressBookIndex = AddressBookHistoryLookup.buildIndex(addressBookHistory);
        }

        if (updateBlockNodeContext(
                tssData, addressBookHistory, storedBlocks, historicalBlockFacility.availableBlocks())) {
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

    /// Update the BlockNodeContext if any of the provided state values differ from what is
    /// currently stored. TssData is considered changed if non-null and its {@code validFromBlock}
    /// is greater than the current value. NodeAddressBook and RangedAddressBookHistory are
    /// considered changed if non-null.
    ///
    /// @param tssData the TssData to consider; may be null
    /// @param addressBookHistory the RangedAddressBookHistory to consider; may be null
    /// @return {@code true} if the BlockNodeContext was updated
    private boolean updateBlockNodeContext(
            TssData tssData,
            RangedAddressBookHistory addressBookHistory,
            BlockRangeSet storedBlocks,
            BlockRangeSet availableBlocks) {
        BlockNodeContext context = blockNodeContext;

        // Discard addressBookHistory if it is not strictly newer than the one already in context.
        if (addressBookHistory != null && !isNewerHistory(addressBookHistory, context.rangedAddressBookHistory())) {
            addressBookHistory = null;
        }

        List<BlockRange> storedBlockRange = mergeRanges(storedBlocks, availableBlocks);
        List<BlockRange> availableBlockRange = toBlockRange(availableBlocks);

        if (tssData == null
                && addressBookHistory == null
                && storedBlockRange.hashCode() == context.storedBlocks().hashCode()
                && availableBlockRange.hashCode() == context.availableBlocks().hashCode()
                && storedBlockRange.equals(context.storedBlocks())
                && availableBlockRange.equals(context.availableBlocks())) {
            return false;
        }

        Builder builder = new Builder(context);
        if (tssData != null) {
            builder.tssData(tssData);
        }

        if (addressBookHistory != null) {
            builder.rangedAddressBookHistory(addressBookHistory);
        }

        if (availableBlockRange.hashCode() != context.availableBlocks().hashCode()
                || !availableBlockRange.equals(context.availableBlocks())) {
            builder.availableBlocks(availableBlockRange);
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

    /// Returns {@code true} when {@code incoming} represents a strictly newer address-book history
    /// than {@code current}, meaning it should replace the existing context value.
    ///
    /// "Newer" is defined as:
    /// <ul>
    ///   <li>The current history is null or empty (always accept the first history).</li>
    ///   <li>The incoming history's last era starts at a higher block number.</li>
    ///   <li>Equal last-era start blocks but more total eras (defensive; should not occur in
    ///       normal operation).</li>
    /// </ul>
    private static boolean isNewerHistory(RangedAddressBookHistory incoming, RangedAddressBookHistory current) {
        if (current == null || current.addressBooks().isEmpty()) return true;
        if (incoming.addressBooks().isEmpty()) return false;
        final long currentLast = current.addressBooks().getLast().startBlock();
        final long incomingLast = incoming.addressBooks().getLast().startBlock();
        return incomingLast > currentLast
                || (incomingLast == currentLast
                        && incoming.addressBooks().size()
                                > current.addressBooks().size());
    }

    /// Merge two sets of block ranges into a single list of block ranges.
    /// This is a very expensive method, O(n<sup>2</sup>), so it should be used
    /// carefully. Perhaps a future update to BlockRangeSet will add a more
    /// efficient merge process. Called unconditionally on every scan (not only when
    /// `availableBlocks` changes) — `storedBlocks` can change independently (e.g. a plugin calling
    /// `addStoredBlockRange`), and skipping the merge in that case previously let `storedBlocks()`
    /// regress to the unmerged, durable-only set once `availableBlocks` stabilized.
    ///
    /// @param storedBlocks a set of stored blocks to merge into the result.
    /// @param availableBlocks a set of available blocks to merge into the result.
    private List<BlockRange> mergeRanges(final BlockRangeSet storedBlocks, final BlockRangeSet availableBlocks) {
        ConcurrentLongRangeSet combined = new ConcurrentLongRangeSet();
        combined.addAll(storedBlocks);
        combined.addAll(availableBlocks);
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
        final Path appStateDataFilePath = appStateConfig.tssBootstrapFilePath();
        try {
            Bytes serialized = TssData.JSON.toBytes(tssData);
            Files.write(appStateDataFilePath, serialized.toByteArray());
        } catch (IOException e) {
            LOGGER.log(WARNING, "Failed to persist TssData to %s: %s".formatted(appStateDataFilePath, e), e);
        }
    }

    private void persistNodeAddressBookHistory(RangedAddressBookHistory history) {
        final Path filePath = appStateConfig.rsaBootstrapFilePath();
        try {
            final Path tmp = filePath.resolveSibling(filePath.getFileName() + ".tmp");
            final Bytes encoded = RangedAddressBookHistory.JSON.toBytes(history);
            Files.write(tmp, encoded.toByteArray());
            try {
                Files.move(tmp, filePath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException e) {
                LOGGER.log(
                        INFO,
                        "Atomic move not supported on this filesystem for {0}; falling back to non-atomic replace",
                        filePath);
                Files.move(tmp, filePath, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            LOGGER.log(WARNING, "Failed to persist RSA address book history to {0}: {1}", filePath, e.getMessage());
        }
    }

    /// Persists both block range sets as JSON to the single file specified in the ApplicationStateConfig.
    private void persistBlockRanges() {
        final Path filePath = appStateConfig.blockRangesFilePath();
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

        // Load RSA address book history (JSON format) — takes precedence over the single-book file.
        // If parsing fails check for single-book file format, wrap it into a single open-ended era
        // so the rest of the application sees a consistent RangedAddressBookHistory regardless of
        // which file is present (backward-compatibility bridge).
        final Path historyFilePath = appStateConfig.rsaBootstrapFilePath();
        if (Files.exists(historyFilePath)) {
            try {
                final RangedAddressBookHistory history =
                        standardParse(RangedAddressBookHistory.JSON, Bytes.wrap(Files.readAllBytes(historyFilePath)));
                if (history.addressBooks().isEmpty()) {
                    // Try the old bootstrap file format, standard parse swallows it and returns an empty history
                    final NodeAddressBook book =
                            standardParse(NodeAddressBook.JSON, Bytes.wrap(Files.readAllBytes(historyFilePath)));
                    validateAddressBook(book, historyFilePath.toString());
                    final RangedAddressBookHistory wrapped = RangedAddressBookHistory.newBuilder()
                            .addressBooks(List.of(RangedNodeAddressBook.newBuilder()
                                    .addressBook(book)
                                    .startBlock(0L)
                                    .endBlock(-1L)
                                    .build()))
                            .build();
                    pendingAddressBookHistory.set(wrapped);
                } else {
                    pendingAddressBookHistory.set(history);
                }
            } catch (IOException e) {
                throw new IllegalStateException("Failed to read RSA address book history file: " + historyFilePath, e);
            } catch (ParseException e) {
                final String message =
                        "Corrupt RSA bootstrap file at %s — delete and restart to re-fetch from Mirror Node"
                                .formatted(historyFilePath);
                throw new IllegalStateException(message, e);
            }
        } else {
            // History file absent — ensure parent directory exists for future writes.
            final Path parent = historyFilePath.getParent();
            try {
                Files.createDirectories(parent);
            } catch (IOException e) {
                LOGGER.log(WARNING, "Failed to create RSA address book history directory: " + parent, e);
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
