// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;
import static org.hiero.block.common.constants.StringsConstants.APPLICATION_PROPERTIES;
import static org.hiero.block.common.constants.StringsConstants.APPLICATION_TEST_PROPERTIES;
import static org.hiero.block.node.app.ApplicationStateUtility.filterToUniqueConnections;
import static org.hiero.block.node.app.ApplicationStateUtility.isNewerHistory;
import static org.hiero.block.node.app.ApplicationStateUtility.loadNetworkData;
import static org.hiero.block.node.app.ApplicationStateUtility.mergeRanges;
import static org.hiero.block.node.app.ApplicationStateUtility.publisherConnectionsFrom;
import static org.hiero.block.node.app.ApplicationStateUtility.toBlockRange;
import static org.hiero.block.node.app.ApplicationStateUtility.validateAddressBook;
import static org.hiero.block.node.base.ParseHelper.standardParse;
import static org.hiero.block.node.spi.BlockNodePlugin.METRICS_CATEGORY;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.config.extensions.sources.ClasspathFileConfigSource;
import com.swirlds.config.extensions.sources.SystemPropertiesConfigSource;
import io.helidon.common.socket.SocketOptions;
import io.helidon.webserver.http2.Http2Config;
import java.io.IOException;
import java.lang.System.Logger;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.LogManager;
import java.util.stream.Collectors;
import org.hiero.block.api.BlockNodeVersions;
import org.hiero.block.api.BlockNodeVersions.PluginVersion;
import org.hiero.block.api.BlockRange;
import org.hiero.block.api.NetworkConnection;
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
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.ServiceLoaderFunction;
import org.hiero.block.node.spi.blockmessaging.AddressBookHistoryNotification;
import org.hiero.block.node.spi.blockmessaging.AvailableBlocksNotification;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.StoredBlocksNotification;
import org.hiero.block.node.spi.blockmessaging.TssDataNotification;
import org.hiero.block.node.spi.health.HealthFacility;
import org.hiero.block.node.spi.historicalblocks.BlockRangeSet;
import org.hiero.block.node.spi.historicalblocks.LongRange;
import org.hiero.block.node.spi.module.SemanticVersionUtility;
import org.hiero.block.node.spi.threading.ThreadPoolManager;
import org.hiero.metrics.LongGauge;
import org.hiero.metrics.LongGauge.Measurement;
import org.hiero.metrics.ObservableGauge;
import org.hiero.metrics.core.Label;
import org.hiero.metrics.core.MetricKey;
import org.hiero.metrics.core.MetricRegistry;

// @todo(3321) This class uses runtime exceptions (such as
//     illegal state exception) as flow control in many methods. We need to
//     rework the logic flow to remove all of those cases and move exception
//     handling up to where it can be resolved rather than just logging
//     warnings and continuing as though nothing failed or using the exception
//     to choose logic branches or replace return values.

/// Main class for the block node server
public class BlockNodeApp implements HealthFacility, ApplicationStateFacility {
    /// An address book history paired with the lookup index built from it, so both can be
    /// installed with one compare-and-set.
    ///
    /// @param history the current history, null until the first one is accepted
    /// @param index the index built from that history
    record AddressBookState(RangedAddressBookHistory history, NavigableMap<Long, RangedNodeAddressBook> index) {
        /// The state before any history has been accepted.
        static final AddressBookState EMPTY =
                new AddressBookState(null, Collections.unmodifiableNavigableMap(new TreeMap<>()));
    }

    /// The logger for this class.  This must be static because there are tests that
    /// create anonymous subclasses of this class (a less than ideal pattern).
    private static final Logger LOGGER = System.getLogger(BlockNodeApp.class.getCanonicalName());
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
    /// Metric key for the current version of the app; this stores the actual data
    /// in labels, because there is no string metric option.
    public static final MetricKey<LongGauge> METRIC_APP_VERSION =
            MetricKey.of("app_current_version", LongGauge.class).addCategory(METRICS_CATEGORY);
    /// The version of the block node, a value read from the module descriptor.
    public static final SemanticVersion BLOCK_NODE_VERSION = SemanticVersionUtility.from(BlockNodeApp.class);
    /// The version of the Block Stream specification compiled into this block node.
    public static final SemanticVersion BLOCK_STREAM_VERSION = SemanticVersionUtility.from(Block.class);
    /// Number of stored blocks between automatic persistence of the block range sets
    private static final long BLOCK_RANGE_PERSIST_INTERVAL = 1000;
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
    /// A metric used to publish the block node version.
    private final LongGauge versionMetric;
    // The measurement for the version metric (this is what is set and queried).
    private Measurement versionMetricInstance;

    /// The block node context. It is marked as volatile for thread safety.
    /// It is written once in the constructor, read by plugin threads.
    volatile BlockNodeContext blockNodeContext;
    /// The current TSS data. Compare-and-set, not plainly assigned: verification threads and the
    /// TSS bootstrap scanner both update it, and check-then-assign lets older data win.
    final AtomicReference<TssData> currentTssData = new AtomicReference<>();
    /// The current RSA address-book history and its lookup index, installed together so a reader
    /// never sees an index built from a different history. Compare-and-set for the same reason as
    /// currentTssData: the RSA bootstrap plugin updates it from two independent executors.
    final AtomicReference<AddressBookState> addressBookState = new AtomicReference<>(AddressBookState.EMPTY);
    /// The latest merged stored-blocks list (stored ConcurrentLongRangeSet merged with available
    /// blocks). Updated by [#addStoredBlockRange] and [ApplicationStateFacility#updateAvailableBlocks].
    /// Provider threads update it concurrently, so it is compare-and-set in
    /// `refreshStoredBlocks` rather than plainly assigned.
    final AtomicReference<List<BlockRange>> currentStoredBlocks = new AtomicReference<>(List.of());
    /// The current available-blocks list (union across all providers). Updated by
    /// [ApplicationStateFacility#updateAvailableBlocks] and [#startApplicationStateFacility].
    /// Compare-and-set for the same reason as `currentStoredBlocks`.
    final AtomicReference<List<BlockRange>> currentAvailableBlocks = new AtomicReference<>(List.of());
    /// list of all loaded plugins. Package so accessible for testing.
    final List<BlockNodePlugin> loadedPlugins = new ArrayList<>();
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
    /// Block count at the time of the last scheduled persist; only read/written by the dispatcher thread
    private long lastPersistedBlockCount = 0;
    /// The ScheduledExecutorService used by the ApplicationStateFacility to run the periodic block-range
    /// persist check and the queued TSS data and address book syncs.
    /// Volatile because the update methods submit to it from arbitrary plugin threads.
    private volatile ScheduledExecutorService applicationStateExecutor;
    /// The next expected block for publishers, used by Server Status plugins
    /// and set by Publisher plugins
    private volatile long nextExpectedBlock = -1L;

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
                versionInfo(loadedPlugins));
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
        // This metric is not _really_ a metric, but a one-time tag.
        // We don't have string metrics, so we use a label.
        Label versionString = new Label("VersionString", SemanticVersionUtility.asString(BLOCK_NODE_VERSION));
        versionMetric = metricRegistry.register(LongGauge.builder(METRIC_APP_VERSION)
                .setDescription("The current version of the BlockNode App, set only on startup")
                .addStaticLabels(versionString));
        versionMetricInstance = versionMetric.getOrCreateNotLabeled();
    }

    /// Build the BlockNodeVersions for this BlockNodeServer
    protected final BlockNodeVersions versionInfo(final List<BlockNodePlugin> plugins) {
        final List<PluginVersion> pluginVersions = new ArrayList<>();
        for (final BlockNodePlugin plugin : plugins) {
            pluginVersions.add(plugin.version());
        }

        return BlockNodeVersions.newBuilder()
                .installedPluginVersions(pluginVersions)
                .blockNodeVersion(BLOCK_NODE_VERSION)
                .streamProtoVersion(BLOCK_STREAM_VERSION)
                .build();
    }

    /// Starts the block node server. This method initializes all the plugins, starts the web server,
    /// and starts the metrics.
    public void start() {
        // startApplicationStateFacility starts the messaging facility (loadedPlugins.get(0)) and
        // loads persisted state, dispatching initial notifications directly.
        startApplicationStateFacility();
        // Start the remaining plugins; the messaging facility (index 0) is already running.
        startPlugins(loadedPlugins.subList(1, loadedPlugins.size()));
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
        // Publish a metric with the full version in a label,
        // and the major version as the value.
        versionMetricInstance.set(BLOCK_NODE_VERSION.major());
    }

    /// {@inheritDoc}
    @Override
    public State blockNodeState() {
        return state.get();
    }

    /// {@inheritDoc}
    @Override
    public void shutdown(String className, String reason) {
        try {
            state.set(State.SHUTTING_DOWN);
            LOGGER.log(INFO, "Shutting down, reason={0} class={1}", reason, className);
            // wait for the shutdown delay
            LockSupport.parkNanos(serverConfig.shutdownDelayMillis() * 1_000_000L);
            serviceBuilder.stopAll();
            // Stop the application state facility only once the servers are closed. It stops the
            // messaging facility, and stopping that while gRPC is still accepting would drop inbound
            // blocks into a halted ring buffer and reject every notification send.
            stopApplicationStateFacility();
            // Stop remaining plugins; messaging facility (index 0) already stopped by stopApplicationStateFacility.
            for (BlockNodePlugin plugin : loadedPlugins.subList(1, loadedPlugins.size())) {
                LOGGER.log(INFO, "\t{0}", plugin.name());
                plugin.stop();
            }
            // Stop metrics
            blockNodeContext.metricRegistry().close();
            LOGGER.log(DEBUG, "Metric registry successfully closed.");
            // finally exit
            LOGGER.log(INFO, "System Exiting");
            if (shouldExitJvmOnShutdown) System.exit(0);
        } catch (IOException e) {
            LOGGER.log(INFO, "Could not properly shut down due to IO failure.", e);
            if (shouldExitJvmOnShutdown) System.exit(0);
        }
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

    // -------------------- Application State Facility -------------------- //

    /// {@inheritDoc}
    ///
    /// Installs the data immediately if it is newer than the currently stored value; persisting it
    /// and dispatching the notification happen on the ApplicationStateDispatcher thread.
    @Override
    public void updateTssData(TssData tssData) {
        while (true) {
            final TssData current = currentTssData.get();
            if (tssData == null || (current != null && tssData.validFromBlock() <= current.validFromBlock())) {
                break;
            }
            if (currentTssData.compareAndSet(current, tssData)) {
                runOnDispatcherThread(this::syncTssData);
                break;
            }
        }
    }

    /// Runs one of the sync methods on the ApplicationStateDispatcher thread, or inline before that
    /// thread exists (startup) and after it is gone (shutdown). The executor is read once because
    /// it is volatile and shutdown clears it. Shutdown can still stop the executor between that read
    /// and the hand-off, so a rejected hand-off also runs inline rather than throwing into the caller.
    ///
    /// @param sync the sync method to run
    private void runOnDispatcherThread(final Runnable sync) {
        final ScheduledExecutorService executor = applicationStateExecutor;
        if (executor == null) {
            sync.run();
        } else {
            try {
                executor.execute(sync);
            } catch (final RejectedExecutionException e) {
                sync.run();
            }
        }
    }

    /// Persists the current TSS data and dispatches it to the plugins. Runs on the
    /// ApplicationStateDispatcher thread, so writes to the bootstrap file are serialized and a
    /// notification can never carry an older datum than one already dispatched. Reads the
    /// current value rather than taking one, so a queued run always publishes the newest.
    private void syncTssData() {
        final TssData tssData = currentTssData.get();
        if (tssData != null) {
            persistTssData(tssData);
            blockNodeContext.blockMessaging().sendTssDataUpdate(new TssDataNotification(tssData));
        }
    }

    @Override
    public void addStoredBlockRange(LongRange blockRange) {
        storedBlocks.add(blockRange);
        refreshStoredBlocks(historicalBlockFacility.availableBlocks());
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
    public long nextExpectedBlock() {
        return nextExpectedBlock;
    }

    @Override
    public TssData tssData() {
        return currentTssData.get();
    }

    @Override
    public RangedAddressBookHistory rangedAddressBookHistory() {
        return addressBookState.get().history();
    }

    @Override
    public List<BlockRange> storedBlocks() {
        return currentStoredBlocks.get();
    }

    @Override
    public void updateBackfillSources(NetworkData sources) {
        backfillSources.set(sources != null ? sources : NetworkData.DEFAULT);
    }

    @Override
    public void updateExpectedBlock(final long updatedExpectedBlock) {
        nextExpectedBlock = updatedExpectedBlock;
    }

    /// {@inheritDoc}
    ///
    /// Installs the history and its index immediately if the supplied history is newer than the
    /// currently stored value; persisting it, deriving the known publishers and dispatching the
    /// notification happen on the ApplicationStateDispatcher thread.
    ///
    /// @param history the history to store; must not be {@code null}
    /// @return {@code true} if accepted, {@code false} if rejected
    @Override
    public boolean updateAddressBookHistory(RangedAddressBookHistory history) {
        boolean updated = false;
        while (true) {
            final AddressBookState current = addressBookState.get();
            if (history == null || history.equals(current.history()) || !isNewerHistory(history, current.history())) {
                break;
            }
            if (addressBookState.compareAndSet(
                    current, new AddressBookState(history, AddressBookHistoryLookup.buildIndex(history)))) {
                // Update knownPublishers immediately on the caller's thread so that publisher
                // authentication reflects the new address book before this method returns.
                // The dispatcher also calls updateKnownPublishersFromAddressBook in
                // syncAddressBookHistory, where it reads the current (possibly newer) value.
                updateKnownPublishersFromAddressBook(history);
                runOnDispatcherThread(this::syncAddressBookHistory);
                updated = true;
                break;
            }
        }
        return updated;
    }

    /// Persists the current address book history, derives the known publishers from it and
    /// dispatches it to the plugins. Runs on the ApplicationStateDispatcher thread, so writes to the
    /// bootstrap file are serialized and a notification can never carry an older history than one
    /// already dispatched. Reads the current value rather than taking one, so a queued run always
    /// publishes the newest.
    private void syncAddressBookHistory() {
        final RangedAddressBookHistory history = addressBookState.get().history();
        if (history != null) {
            persistNodeAddressBookHistory(history);
            updateKnownPublishersFromAddressBook(history);
            blockNodeContext.blockMessaging().sendAddressBookHistoryUpdate(new AddressBookHistoryNotification(history));
        }
    }

    @Override
    public void updateAvailableBlocks() {
        // Capture one snapshot so both Available and Stored notifications are always derived from
        // the same moment in time, preserving the invariant stored >= available.
        final BlockRangeSet snapshot = historicalBlockFacility.availableBlocks();
        refreshAvailableBlocks(snapshot);
        refreshStoredBlocks(snapshot);
    }

    private void refreshAvailableBlocks(BlockRangeSet availableBlocks) {
        while (true) {
            final List<BlockRange> current = currentAvailableBlocks.get();
            final List<BlockRange> candidate = toBlockRange(availableBlocks);
            if (candidate.equals(current)) {
                // Nothing changed, either because there was no update or because another thread
                // already installed an identical snapshot; there is nothing to notify.
                break;
            }
            if (currentAvailableBlocks.compareAndSet(current, candidate)) {
                runOnDispatcherThread(this::syncAvailableBlocks);
                break;
            }
        }
    }

    /// Dispatches the current available blocks to the plugins. Runs on the ApplicationStateDispatcher
    /// thread and reads the current value rather than taking one, so a notification can never
    /// carry an older snapshot than one already dispatched.
    private void syncAvailableBlocks() {
        blockNodeContext
                .blockMessaging()
                .sendAvailableBlocksUpdate(new AvailableBlocksNotification(currentAvailableBlocks.get()));
    }

    private void refreshStoredBlocks(BlockRangeSet availableBlocks) {
        while (true) {
            final List<BlockRange> current = currentStoredBlocks.get();
            final List<BlockRange> candidate = mergeRanges(storedBlocks, availableBlocks);
            if (candidate.equals(current)) {
                break;
            }
            if (currentStoredBlocks.compareAndSet(current, candidate)) {
                runOnDispatcherThread(this::syncStoredBlocks);
                break;
            }
        }
    }

    /// Dispatches the current stored blocks to the plugins. Runs on the ApplicationStateDispatcher
    /// thread and reads the current value rather than taking one, so a notification can never
    /// carry an older snapshot than one already dispatched.
    private void syncStoredBlocks() {
        blockNodeContext
                .blockMessaging()
                .sendStoredBlocksUpdate(new StoredBlocksNotification(currentStoredBlocks.get()));
    }

    /// Rebuilds the set of known publisher connections from the newest era in the supplied
    /// address-book history and merges it with the connections already tracked in
    /// [#knownPublishers].
    ///
    /// The "newest" era is chosen by streaming the [RangedNodeAddressBook] entries, sorting them
    /// with a [RangedAddressBookComparator], and taking the last (greatest) element. Its
    /// [NodeAddressBook] is transformed into publisher connections by
    /// [ApplicationStateUtility#publisherConnectionsFrom],
    /// which are then merged with the currently known publishers.
    ///
    /// @param history the address-book history to derive publisher connections from; may be null
    private void updateKnownPublishersFromAddressBook(final RangedAddressBookHistory history) {
        if (history == null
                || history.addressBooks() == null
                || history.addressBooks().isEmpty()) {
            return;
        }
        // Stream the eras, order them with the comparator, and keep the last (i.e. newest) one.
        // The last era's address book is non-null by the comparator's ordering contract.
        final NodeAddressBook latestBook = history.addressBooks().stream()
                .sorted(new RangedAddressBookComparator())
                .toList()
                .getLast()
                .addressBook();

        final List<NetworkConnection> connections = publisherConnectionsFrom(latestBook, appStateConfig);
        // Merge in the publishers already known, then publish the combined, de-duplicated set.
        connections.addAll(knownPublishers.get().activeEndpoints());
        final List<NetworkConnection> uniqueConnections = filterToUniqueConnections(connections);
        knownPublishers.set(new NetworkData(uniqueConnections));
    }

    /// Returns the {@link NodeAddressBook} whose block range covers {@code blockNum}, using the
    /// cached index built from the current {@link RangedAddressBookHistory}.
    ///
    /// @param blockNum the block number to look up
    /// @return the matching {@link NodeAddressBook}, or {@code null} if no era covers it
    @Override
    public NodeAddressBook getAddressBookForBlock(long blockNum) {
        return AddressBookHistoryLookup.findAddressBookForBlock(
                addressBookState.get().index(), blockNum);
    }

    private static boolean hasValidKey(NodeAddressBook book) {
        if (book == null || book.nodeAddress().isEmpty()) return false;
        return book.nodeAddress().stream()
                .anyMatch(a -> a.rsaPubKey() != null && !a.rsaPubKey().isBlank());
    }

    /// Starts the ApplicationStateFacility.
    ///
    /// Starts the messaging facility first so that the update methods called during
    /// {@link #loadApplicationState} can dispatch notifications immediately, then dispatches the
    /// available and stored blocks the providers loaded during `init()`. Finally creates the
    /// dispatcher thread and schedules the block-range persist-interval check on it.
    ///
    /// The dispatcher is created last on purpose: the load above is single threaded, so its updates
    /// persist and dispatch inline rather than being handed to a thread that does not exist yet.
    void startApplicationStateFacility() {
        // Start the messaging facility first so update methods can dispatch notifications immediately.
        loadedPlugins.getFirst().start();

        // Load persisted state; update methods (updateTssData, updateAddressBookHistory, etc.)
        // dispatch notifications directly as each datum is loaded.
        loadApplicationState(blockNodeContext.configuration());

        // Anything dispatched during plugin init() was published before the messaging facility attached
        // any handler, so it was lost. Clear the snapshots so the startup available and stored blocks
        // (including the stored ranges just loaded) are dispatched now, whatever init() already recorded.
        currentAvailableBlocks.set(List.of());
        currentStoredBlocks.set(List.of());
        updateAvailableBlocks();

        // Create the dispatcher thread and schedule the periodic block-range persist-interval check on it.
        applicationStateExecutor = blockNodeContext
                .threadPoolManager()
                .createVirtualThreadScheduledExecutor(
                        1, "ApplicationStateDispatcher", ApplicationStateUtility::uncaughtExceptionHandler);
        applicationStateExecutor.scheduleAtFixedRate(
                this::persistBlockRangesIfDue,
                appStateConfig.updateInitialDelay(),
                appStateConfig.updateScanInterval(),
                TimeUnit.MILLISECONDS);
    }

    /// Periodically persists block ranges when the running total crosses a boundary.
    private void persistBlockRangesIfDue() {
        final long current = storedBlocks.size();
        if (current / BLOCK_RANGE_PERSIST_INTERVAL > lastPersistedBlockCount / BLOCK_RANGE_PERSIST_INTERVAL) {
            persistBlockRanges();
            lastPersistedBlockCount = current;
        }
    }

    void stopApplicationStateFacility() {
        final ScheduledExecutorService executor = applicationStateExecutor;
        if (executor != null) {
            // Clear the field first so that an update arriving during shutdown syncs inline
            // instead of being handed to an executor that is going away.
            applicationStateExecutor = null;
            // shutdown() cancels the periodic persist check but still runs the syncs already queued
            // and lets a running write finish, so nothing needs to be redone here.
            executor.shutdown();
            try {
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    final String executorTerminationMsg = "applicationStateExecutor did not terminate in time";
                    LOGGER.log(INFO, executorTerminationMsg);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        // Persist all block ranges at shutdown regardless of threshold.
        persistBlockRanges();
        // Stop the messaging facility that was started in startApplicationStateFacility.
        loadedPlugins.getFirst().stop();
    }

    /// Persist the TssData
    /// Persists the TssData to the file path specified in the ApplicationStateConfig class.
    ///
    /// @param tssData The TssData to persist
    private void persistTssData(TssData tssData) {
        final Path appStateDataFilePath = appStateConfig.tssBootstrapFilePath();
        try {
            replaceFile(appStateDataFilePath, TssData.JSON.toBytes(tssData));
        } catch (IOException e) {
            LOGGER.log(WARNING, "Failed to persist TssData to %s: %s".formatted(appStateDataFilePath, e), e);
        }
    }

    private void persistNodeAddressBookHistory(RangedAddressBookHistory history) {
        final Path filePath = appStateConfig.rsaBootstrapFilePath();
        try {
            replaceFile(filePath, RangedAddressBookHistory.JSON.toBytes(history));
        } catch (IOException e) {
            LOGGER.log(WARNING, "Failed to persist RSA address book history to %s".formatted(filePath), e);
        }
    }

    /// Persists both block range sets as JSON to the single file specified in the ApplicationStateConfig.
    private void persistBlockRanges() {
        final Path filePath = appStateConfig.blockRangesFilePath();
        try {
            replaceFile(filePath, BlockRangesState.JSON.toBytes(toBlockRangesState()));
        } catch (IOException e) {
            LOGGER.log(WARNING, "Failed to persist block ranges to {0}: {1}", filePath, e.getMessage());
        }
    }

    /// Writes the bytes to a uniquely named sibling `.tmp` file, then moves it over the target. The
    /// target is never deleted first, so a crash part way through leaves the previous file intact
    /// rather than a missing or half written one. Each call gets its own tmp file, so two threads
    /// replacing the same target (possible during shutdown) cannot corrupt each other's write; the
    /// last move wins. Falls back to a non-atomic replace on filesystems that do not support atomic
    /// moves.
    ///
    /// @param target the file to replace
    /// @param bytes the new content
    /// @throws IOException if the write or the move fails
    private static void replaceFile(final Path target, final Bytes bytes) throws IOException {
        final Path tmp =
                Files.createTempFile(target.getParent(), target.getFileName().toString(), ".tmp");
        try {
            Files.write(tmp, bytes.toByteArray());
            try {
                Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException e) {
                LOGGER.log(DEBUG, "Atomic move not supported for {0}; falling back to non-atomic replace", target);
                Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            // No-op after a successful move; otherwise stops a failed write leaving a new orphan each time.
            Files.deleteIfExists(tmp);
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
    /// Note: This method currently uses _exceptions_ for flow control, with try/catch
    /// almost every sub block of code and calling methods that throw instead
    /// of returning errors. This needs to be fixed.
    ///
    /// @param configuration the current configuration
    private void loadApplicationState(final Configuration configuration) {
        // Load TssData (JSON format). The dispatcher executor does not exist yet, so it is persisted and
        // dispatched inline.
        final Path tssDataJsonPath = appStateConfig.tssBootstrapFilePath();
        if (Files.exists(tssDataJsonPath)) {
            try {
                TssData tssData = standardParse(TssData.JSON, Bytes.wrap(Files.readAllBytes(tssDataJsonPath)));
                updateTssData(tssData);
            } catch (ParseException | IOException e) {
                // @todo(3321) this is using an exception to decide to log, but
                //      doesn't resolve the problem, so the code still fails
                //      later in the same method. This catch should be method level,
                //      or should be in the singular caller.
                LOGGER.log(WARNING, "Failed to read TssData file: " + tssDataJsonPath, e);
            }
        } else {
            // make sure the directory is created for the writes
            Path parent = tssDataJsonPath.getParent();
            try {
                Files.createDirectories(parent);
            } catch (IOException e) {
                // @todo(3321) this is using an exception to decide to log, but
                //      doesn't resolve the problem, so the code still fails
                //      later in the same method. This catch should be method level,
                //      or should be in the singular caller.
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
                    if (validateAddressBook(book, historyFilePath.toString())) {
                        final RangedAddressBookHistory wrapped = RangedAddressBookHistory.newBuilder()
                                .addressBooks(List.of(RangedNodeAddressBook.newBuilder()
                                        .addressBook(book)
                                        .startBlock(0L)
                                        .endBlock(-1L)
                                        .build()))
                                .build();
                        updateAddressBookHistory(wrapped);
                    } else {
                        // @todo(3321) This is bad design. This entire method uses
                        //     exceptions as flow control, and we need to fix that.
                        throw new IllegalStateException("Address book is not valid");
                    }
                } else {
                    updateAddressBookHistory(history);
                }
            } catch (IOException e) {
                throw new IllegalStateException("Failed to read RSA address book history file: " + historyFilePath, e);
            } catch (ParseException e) {
                // @todo(3321) This is bad design. This entire method uses
                //     exceptions as flow control, and we need to fix that.
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
                // @todo(3321) this is using an exception to decide to log, but
                //      doesn't resolve the problem, so the code still fails
                //      later in the same method. This catch should be method level,
                //      or should be in the singular caller.
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
                // @todo(3321) this is using an exception to decide to log, but
                //      doesn't resolve the problem, so the code still fails
                //      later in the same method. This catch should be method level,
                //      or should be in the singular caller.
                LOGGER.log(WARNING, "Failed to read block ranges file: " + blockRangesPath, e);
            }
        } else {
            Path parent = blockRangesPath.getParent();
            try {
                Files.createDirectories(parent);
            } catch (IOException e) {
                // @todo(3321) this is using an exception to decide to log, but
                //      doesn't resolve the problem, so the code still fails
                //      later in the same method. This catch should be method level,
                //      or should be in the singular caller.
                LOGGER.log(WARNING, "Failed to create block ranges directory: " + parent, e);
            }
        }

        // Load the connection-information sets (JSON-serialized NetworkData) used by the /statusz endpoints.
        // These are read-only configuration; absent or unreadable files yield an empty set.
        knownPublishers.set(loadNetworkData(appStateConfig.knownPublishersFilePath()));
        inboundPartners.set(loadNetworkData(appStateConfig.inboundPartnersFilePath()));
        outboundPartners.set(loadNetworkData(appStateConfig.outboundPartnersFilePath()));
    }
}
