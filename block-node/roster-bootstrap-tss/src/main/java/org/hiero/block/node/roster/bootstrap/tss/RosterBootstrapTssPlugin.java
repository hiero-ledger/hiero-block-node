// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.roster.bootstrap.tss;

import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;

import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.block.api.TssData;
import org.hiero.block.node.spi.ApplicationStateFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.metrics.LongCounter;
import org.hiero.metrics.ObservableGauge;
import org.hiero.metrics.core.MetricKey;
import org.hiero.metrics.core.MetricRegistry;

/// A block node plugin that tries to get the latest TssData and makes it available to the `BlockNodeApp` and
/// to the `ServerStatusServicePlugin`
///
/// The TssData is retrieved from TssData sources in the following order:
///  - `RosterBootstrapTssConfig` TssData fields (ledgerId, wrapsVerificationKey, etc)
///  - (todo) Peer BlockNodes Queries other peer BlockNodes periodically for TssData
public class RosterBootstrapTssPlugin implements BlockNodePlugin {
    public static final MetricKey<ObservableGauge> METRIC_TSS_DATA_PEERS =
            MetricKey.of("tss_data_peers", ObservableGauge.class).addCategory(METRICS_CATEGORY);
    public static final MetricKey<LongCounter> METRIC_TSS_DATA_ERRORS =
            MetricKey.of("tss_data_errors", LongCounter.class).addCategory(METRICS_CATEGORY);
    public static final MetricKey<LongCounter> METRIC_TSS_DATA_REQUESTS =
            MetricKey.of("tss_data_requests", LongCounter.class).addCategory(METRICS_CATEGORY);

    /// The logger for this class.
    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    /// The block node context, for access to core facilities.
    private volatile BlockNodeContext blockNodeContext;
    /// The application state facility, for updating application state.
    private ApplicationStateFacility applicationStateFacility;

    private boolean hasBNSourcesPath = false;
    /// The ScheduledExecutorService used by the RosterBootstrapPlugin to query peer BNs for TssData
    private ScheduledExecutorService queryPeerExecutor;
    /// The config information for the RosterBootstrapTssConfig
    private RosterBootstrapTssConfig rosterBootstrapTssConfig;
    // Metrics holder containing all backfill metrics
    private MetricsHolder metricsHolder;
    // The class that fetches the TssData
    private TssDataFetcher tssDataFetcher;
    // State touched by multiple threads
    private final AtomicLong currentBlockNodePeers = new AtomicLong(0);

    /// {@inheritDoc}
    @NonNull
    @Override
    public List<Class<? extends Record>> configDataTypes() {
        return List.of(RosterBootstrapTssConfig.class);
    }

    /// {@inheritDoc}
    @Override
    public void init(BlockNodeContext context, ServiceBuilder serviceBuilder) {
        this.applicationStateFacility = Objects.requireNonNull(context.applicationStateFacility());
        rosterBootstrapTssConfig = context.configuration().getConfigData(RosterBootstrapTssConfig.class);
        this.blockNodeContext = context;

        initMetrics();

        // Validate block node sources configuration
        final String sourcesPath = rosterBootstrapTssConfig.blockNodeSourcesPath();
        if (sourcesPath == null || sourcesPath.isBlank()) {
            LOGGER.log(WARNING, "No block node sources path configured, TssBootstrapPlugin will not query any peers");
            return;
        }

        Path blockNodeSourcesPath = Path.of(rosterBootstrapTssConfig.blockNodeSourcesPath());
        if (!Files.isRegularFile(blockNodeSourcesPath)) {
            final String blockNodeSourcesPathNotFoundMsg =
                    "Block node sources path does not exist or is not a regular file: [{0}], TssBootstrapPlugin will not query any peers";
            LOGGER.log(WARNING, blockNodeSourcesPathNotFoundMsg, rosterBootstrapTssConfig.blockNodeSourcesPath());
            return;
        }

        try {
            BlockNodeSource blockNodeSources =
                    BlockNodeSource.JSON.parse(Bytes.wrap(Files.readAllBytes(blockNodeSourcesPath)));
            // Let the logs know what we loaded.
            for (BlockNodeSourceConfig node : blockNodeSources.nodes()) {
                LOGGER.log(INFO, "Loaded peer BN source node: {0}", node);
            }
            hasBNSourcesPath = true;
            tssDataFetcher = new TssDataFetcher(blockNodeSources, rosterBootstrapTssConfig, metricsHolder);
        } catch (ParseException | IOException e) {
            final String parseFailedMsg =
                    "Failed to parse block node sources from path: [%s], TssBootstrapPlugin will not query any peers: %s"
                            .formatted(rosterBootstrapTssConfig.blockNodeSourcesPath(), e.getMessage());
            LOGGER.log(WARNING, parseFailedMsg, e);
        }
    }

    /// {@inheritDoc}
    @Override
    public void stop() {
        if (queryPeerExecutor != null) queryPeerExecutor.shutdown();
    }

    /// UncaughtExceptionHandler for logging uncaught exceptions
    private void uncaughtExceptionHandler(Thread thread, Throwable throwable) {
        LOGGER.log(WARNING, "Uncaught exception in RosterBootstrapTssPlugin thread: " + thread.getName(), throwable);
    }

    /// {@inheritDoc}
    @Override
    public void start() {
        // Don't start the querying thread if there are no peers defined.
        if (!hasBNSourcesPath) {
            LOGGER.log(WARNING, "RosterBootstrapTssPlugin: No BN Peers defined");
            return;
        }
        // save the reference of this volatile object.
        BlockNodeContext context = blockNodeContext;
        LOGGER.log(INFO, "RosterBootstrapTssPlugin start called");

        // Create thread executors via threadPoolManager.
        queryPeerExecutor = context.threadPoolManager()
                .createVirtualThreadScheduledExecutor(1, "queryPeerScanner", this::uncaughtExceptionHandler);

        // Schedule periodic checking of bn peers for their TssData
        queryPeerExecutor.scheduleAtFixedRate(
                this::queryPeerTssData,
                rosterBootstrapTssConfig.queryPeerInitialDelay(),
                rosterBootstrapTssConfig.queryPeerInterval(),
                TimeUnit.MILLISECONDS);
    }

    /// {@inheritDoc}
    /// This method is called on a separate thread. Make sure this.context is marked as `volatile`
    @Override
    public void onContextUpdate(BlockNodeContext context) {
        // save the context update
        this.blockNodeContext = context;
    }

    /// queries peer BlockNodes for their TssData
    private void queryPeerTssData() {
        List<TssData> tssDataList = tssDataFetcher.getTssData();
        for (TssData tssData : tssDataList) {
            applicationStateFacility.updateTssData(tssData);
        }
    }

    /// Initializes the metrics for the backfill process.
    private void initMetrics() {
        metricsHolder = MetricsHolder.createMetrics(blockNodeContext.metricRegistry(), currentBlockNodePeers);
    }

    /// Holder for all backfill-related metrics.
    /// This record groups all metrics used by the backfill plugin and its components,
    /// allowing them to be passed as a single parameter.
    public record MetricsHolder(LongCounter.Measurement tssDataRequests, LongCounter.Measurement tssDataErrors) {

        /// Factory method to create a MetricsHolder with all metrics registered.
        ///
        /// @param metricRegistry the metrics registry instance to register metrics with
        /// @return a new MetricsHolder with all metrics created
        public static MetricsHolder createMetrics(
                @NonNull final MetricRegistry metricRegistry, @NonNull final AtomicLong currentBlockNodePeers) {
            metricRegistry.register(ObservableGauge.builder(METRIC_TSS_DATA_PEERS)
                    .setDescription("Current number of block node peers")
                    .observe(() -> Math.max(currentBlockNodePeers.get(), 0)));

            return new MetricsHolder(
                    metricRegistry
                            .register(LongCounter.builder(METRIC_TSS_DATA_REQUESTS)
                                    .setDescription("Number of TssData requests."))
                            .getOrCreateNotLabeled(),
                    metricRegistry
                            .register(LongCounter.builder(METRIC_TSS_DATA_ERRORS)
                                    .setDescription("Number of TssData request errors."))
                            .getOrCreateNotLabeled());
        }
    }
}
