// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;
import static org.hiero.block.common.constants.StringsConstants.APPLICATION_PROPERTIES;
import static org.hiero.block.common.constants.StringsConstants.APPLICATION_TEST_PROPERTIES;
import static org.hiero.block.node.spi.BlockNodePlugin.METRICS_CATEGORY;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.node.base.SemanticVersion;
import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.config.extensions.sources.ClasspathFileConfigSource;
import com.swirlds.config.extensions.sources.SystemPropertiesConfigSource;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.helidon.common.socket.SocketOptions;
import io.helidon.webserver.http2.Http2Config;
import java.io.IOException;
import java.lang.System.Logger;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.LogManager;
import java.util.stream.Collectors;
import org.hiero.block.api.BlockNodeVersions;
import org.hiero.block.api.BlockNodeVersions.PluginVersion;
import org.hiero.block.node.app.config.AutomaticEnvironmentVariableConfigSource;
import org.hiero.block.node.app.config.ServerConfig;
import org.hiero.block.node.app.config.WebServerHttp2Config;
import org.hiero.block.node.app.logging.CleanColorfulFormatter;
import org.hiero.block.node.app.logging.ConfigLogger;
import org.hiero.block.node.spi.ApplicationStateFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.ServiceLoaderFunction;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.health.HealthFacility;
import org.hiero.block.node.spi.historicalblocks.BlockRanges;
import org.hiero.block.node.spi.module.SemanticVersionUtility;
import org.hiero.block.node.spi.threading.ThreadPoolManager;
import org.hiero.metrics.LongGauge;
import org.hiero.metrics.LongGauge.Measurement;
import org.hiero.metrics.ObservableGauge;
import org.hiero.metrics.core.Label;
import org.hiero.metrics.core.MetricKey;
import org.hiero.metrics.core.MetricRegistry;

/// Main class for the block node server
public class BlockNodeApp implements HealthFacility {
    /// The logger for this class.  This must be static because there are tests that
    /// create anonymous subclasses of this class (a less than ideal pattern).
    private static final Logger LOGGER = System.getLogger(BlockNodeApp.class.getCanonicalName());
    /// Constant mapped to PbjProtocolProvider.CONFIG\_NAME in the PBJ Helidon Plugin
    public static final String PBJ_PROTOCOL_PROVIDER_CONFIG_NAME = "pbj";
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
    /// The application state facility, loaded as a plugin
    private final ApplicationStateFacility applicationStateFacility;
    /// The block messaging facility, loaded as a plugin
    private final BlockMessagingFacility blockMessagingFacility;
    /// The facilities that must be loaded, initialized and started before every other plugin, in that order,
    /// and are stopped after every other plugin, in the reverse order. Each is required: loading fails if
    /// one is missing.
    private final List<BlockNodePlugin> startupFacilities;
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
    /// list of all loaded plugins. Package so accessible for testing.
    final List<BlockNodePlugin> loadedPlugins = new ArrayList<>();
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
        blockMessagingFacility = serviceLoader
                .loadServices(BlockMessagingFacility.class)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No BlockMessagingFacility provided"));
        // Load the ApplicationStateFacility plugin. It must be initialized right after the messaging facility
        // and before any block provider, because providers may report blocks from their own init().
        applicationStateFacility = serviceLoader
                .loadServices(ApplicationStateFacility.class)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No ApplicationStateFacility provided. The"
                        + " facility-app-state plugin is required; if plugins.names is set, add facility-app-state"
                        + " to it."));
        startupFacilities = List.of(blockMessagingFacility, applicationStateFacility);
        loadedPlugins.addAll(startupFacilities);
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
                blockMessagingFacility,
                historicalBlockFacility,
                applicationStateFacility,
                serviceLoader,
                threadPoolManager,
                versionInfo(loadedPlugins));
        // ==== CREATE ROUTING BUILDERS ================================================================================
        // Http2 Config more info at
        // https://helidon.io/docs/v4/apidocs/io.helidon.webserver.http2/io/helidon/webserver/http2/Http2Config.html
        final Http2Config http2Config = Http2Config.builder()
                .name(WebServerHttp2Config.HELIDON_PROTOCOL_CONFIG_NAME)
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
        final SocketOptions socketOptions = buildSocketOptions(serverConfig);
        SocketBufferCheck.warnIfCapped(new SocketBufferCheck.BufferSizes(
                serverConfig.socketReceiveBufferSizeBytes(), serverConfig.socketSendBufferSizeBytes()));

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

    /// Builds the socket options shared by the servers.
    ///
    /// A buffer size of `0` is left unset, so no `setsockopt` is issued and the kernel autotunes that buffer.
    ///
    /// @param serverConfig the server configuration
    /// @return the socket options
    static SocketOptions buildSocketOptions(@NonNull final ServerConfig serverConfig) {
        final SocketOptions.Builder builder = SocketOptions.builder().tcpNoDelay(serverConfig.tcpNoDelay());
        if (serverConfig.socketSendBufferSizeBytes() > 0) {
            builder.socketSendBufferSize(serverConfig.socketSendBufferSizeBytes());
        }
        if (serverConfig.socketReceiveBufferSizeBytes() > 0) {
            builder.socketReceiveBufferSize(serverConfig.socketReceiveBufferSizeBytes());
        }
        return builder.build();
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
        // Start the facilities first, in order: the messaging facility must be running so the application state
        // facility can dispatch notifications while it loads the persisted state in its own start().
        for (final BlockNodePlugin facility : startupFacilities) {
            facility.start();
        }
        // Start the remaining plugins; the facilities are already running.
        startPlugins(otherPlugins());
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
                        .map(BlockRanges::toString)
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
            // Stop the other plugins first: they use the facilities from their own stop(), e.g. to
            // unregister handlers or report final state. A plugin that fails to stop is logged and skipped,
            // so the others and the facilities below still stop and application state is still persisted.
            for (BlockNodePlugin plugin : otherPlugins()) {
                LOGGER.log(INFO, "\t{0}", plugin.name());
                stopPlugin(plugin);
            }
            // Then stop the facilities in the reverse of their start order. The application state facility
            // goes before messaging because it sends notifications while it flushes, and it persists the
            // state last so it includes anything reported by the plugins above. Each is stopped even if the
            // one before it failed.
            for (final BlockNodePlugin facility : startupFacilities.reversed()) {
                stopPlugin(facility);
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

    /// The loaded plugins other than the startup facilities, in load order.
    ///
    /// @return the plugins that are started after, and stopped before, the startup facilities
    private List<BlockNodePlugin> otherPlugins() {
        return loadedPlugins.stream()
                .filter(plugin -> startupFacilities.stream().noneMatch(facility -> facility == plugin))
                .toList();
    }

    /// Stops one plugin. A failure is logged and swallowed so that it cannot prevent the remaining plugins
    /// and facilities from stopping.
    ///
    /// @param plugin the plugin to stop
    private static void stopPlugin(final BlockNodePlugin plugin) {
        try {
            plugin.stop();
        } catch (final Throwable e) {
            LOGGER.log(WARNING, "Plugin " + plugin.name() + " failed to stop", e);
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
        // A plugin that fails to start is logged and skipped so every plugin gets a chance to start and
        // all failures are reported, rather than the first exception aborting the parallel stream.
        plugins.parallelStream().forEach(plugin -> {
            try {
                plugin.start();
            } catch (final Throwable e) {
                LOGGER.log(WARNING, "Plugin " + plugin.name() + " failed to start", e);
            }
        });
    }
}
