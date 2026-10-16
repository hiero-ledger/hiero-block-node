// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.realtimeblocksubscribe;

import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.metrics.LongCounter;
import org.hiero.metrics.ObservableGauge;
import org.hiero.metrics.core.MetricKey;
import org.hiero.metrics.core.MetricRegistry;

/**
 * Real-Time Block Subscribe Plugin. Consumes a peer BN's {@code BlockStreamSubscribeService.subscribeBlockStream}
 * RPC as an open-ended live tail and publishes the received blocks onto the Unvalidated Blocks
 * ring buffer for the local BN to verify and persist. See the design at
 * {@code docs/design/streaming/subscribe-client-plugin.md}.
 *
 * <p>This class is the plugin scaffold introduced by issue #3675. The actual streaming loop
 * ({@code SubscribeSessionRunner}, {@code SubscribedBlockPublisher}) lands with issues
 * #3676 and #3677 once the Unvalidated Blocks ring buffer (issue #3614) is available. Until then,
 * {@link #start()} logs that the plugin is enabled and leaves the runner un-started so the
 * module can be loaded safely in test deployments.
 */
public class RealTimeBlockSubscribePlugin implements BlockNodePlugin {

    /** Logger for the plugin. */
    private static final System.Logger LOGGER = System.getLogger(RealTimeBlockSubscribePlugin.class.getName());

    // --- Metrics (per design, section: Metrics). Values are placeholders until the session
    // runner lands; the metric keys are registered now so dashboards can be wired against them
    // without waiting for the full implementation.

    /** {@code node_id} of the currently-active peer; {@code -1} when no peer is active. */
    public static final MetricKey<ObservableGauge> METRIC_REAL_TIME_BLOCK_SUBSCRIBE_ACTIVE_PEER = MetricKey.of(
                    "real_time_block_subscribe_active_peer", ObservableGauge.class)
            .addCategory(METRICS_CATEGORY);

    /** Blocks with a {@code BlockEnd} received from the peer. Throughput signal. */
    public static final MetricKey<LongCounter> METRIC_REAL_TIME_BLOCK_SUBSCRIBE_BLOCKS_RECEIVED = MetricKey.of(
                    "real_time_block_subscribe_blocks_received", LongCounter.class)
            .addCategory(METRICS_CATEGORY);

    /** All stream ends, labeled by cause ({@code clean}, {@code error}, {@code transport}, {@code stale}). */
    public static final MetricKey<LongCounter> METRIC_REAL_TIME_BLOCK_SUBSCRIBE_STREAM_TERMINATIONS = MetricKey.of(
                    "real_time_block_subscribe_stream_terminations", LongCounter.class)
            .addCategory(METRICS_CATEGORY);

    /** {@code now - lastBlockEndReceivedAt}. Primary operator health signal. */
    public static final MetricKey<ObservableGauge> METRIC_REAL_TIME_BLOCK_SUBSCRIBE_LAG_MS = MetricKey.of(
                    "real_time_block_subscribe_lag_ms", ObservableGauge.class)
            .addCategory(METRICS_CATEGORY);

    /** Highest block number for which a notification was emitted onto the Unvalidated Blocks ring. */
    public static final MetricKey<ObservableGauge> METRIC_REAL_TIME_BLOCK_SUBSCRIBE_LAST_BLOCK_NUMBER = MetricKey.of(
                    "real_time_block_subscribe_last_block_number", ObservableGauge.class)
            .addCategory(METRICS_CATEGORY);

    // --- Plugin state

    /** Config loaded in {@link #init}. */
    private RealTimeBlockSubscribeConfiguration configuration;

    /** True once {@link #init} has validated the peer-sources file. */
    private boolean peerSourcesReady;

    /** Backing field for {@link #METRIC_REAL_TIME_BLOCK_SUBSCRIBE_ACTIVE_PEER}; set by the session runner. */
    private final AtomicLong activePeerNodeId = new AtomicLong(-1);

    /** Backing field for {@link #METRIC_REAL_TIME_BLOCK_SUBSCRIBE_LAG_MS}; set by the session runner. */
    private final AtomicLong lastBlockEndReceivedAtEpochMillis = new AtomicLong(0);

    /** Backing field for {@link #METRIC_REAL_TIME_BLOCK_SUBSCRIBE_LAST_BLOCK_NUMBER}; set by the session runner. */
    private final AtomicLong lastBlockNumberEmitted = new AtomicLong(-1);

    /** Default no-arg constructor required by the plugin SPI. */
    public RealTimeBlockSubscribePlugin() {
        super();
    }

    /** {@inheritDoc} */
    @Override
    public void init(@NonNull final BlockNodeContext context, @NonNull final ServiceBuilder serviceBuilder) {
        configuration = context.configuration().getConfigData(RealTimeBlockSubscribeConfiguration.class);

        initMetrics(context.metricRegistry());

        final String sourcesPath = configuration.blockNodeSourcesPath();
        if (sourcesPath == null || sourcesPath.isBlank()) {
            LOGGER.log(INFO, "No block node sources path configured, subscribe client will not run");
            peerSourcesReady = false;
            return;
        }

        final Path path = Path.of(sourcesPath);
        if (!Files.isRegularFile(path)) {
            LOGGER.log(
                    WARNING,
                    "Block node sources path does not exist or is not a regular file: [{0}], "
                            + "subscribe client will not run",
                    sourcesPath);
            peerSourcesReady = false;
            return;
        }

        // Peer-sources parsing + full validation (zero-entry rejection, schema check) lands with
        // issue #3676 when the session runner wires in BlockNodeSource. This scaffold only checks
        // that the file is present and readable.
        peerSourcesReady = true;
        LOGGER.log(
                INFO,
                "Subscribe client peer-sources file present at [{0}], deliveryMode={1}; "
                        + "session runner will start when #3676 lands",
                sourcesPath,
                configuration.deliveryMode());
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        if (!peerSourcesReady) {
            LOGGER.log(INFO, "Subscribe client not started: peer-sources not ready (see init log)");
            return;
        }
        // Session runner start lands with issue #3676. Intentionally a no-op in the scaffold so
        // the plugin can be deployed and metrics scraped without side-effects on the live path.
        LOGGER.log(INFO, "Subscribe client scaffold started; streaming loop is a no-op until #3676 lands.");
    }

    /** {@inheritDoc} */
    @Override
    public void stop() {
        // Nothing to tear down in the scaffold; the session runner's stop logic lands with #3676.
    }

    /** Register all metrics on the plugin's registry. Called from {@link #init}. */
    private void initMetrics(@NonNull final MetricRegistry metrics) {
        metrics.register(LongCounter.builder(METRIC_REAL_TIME_BLOCK_SUBSCRIBE_BLOCKS_RECEIVED)
                .setDescription("Blocks with a BlockEnd received from the active peer."));
        metrics.register(LongCounter.builder(METRIC_REAL_TIME_BLOCK_SUBSCRIBE_STREAM_TERMINATIONS)
                .setDescription("Subscribe-stream terminations, labelled by cause (clean, error, transport, stale)."));
        metrics.register(ObservableGauge.builder(METRIC_REAL_TIME_BLOCK_SUBSCRIBE_ACTIVE_PEER)
                .setDescription("node_id of the currently-active peer; -1 when none.")
                .observe(activePeerNodeId::get));
        metrics.register(ObservableGauge.builder(METRIC_REAL_TIME_BLOCK_SUBSCRIBE_LAG_MS)
                .setDescription("Milliseconds since the last BlockEnd was received from the active peer.")
                .observe(this::lagMillis));
        metrics.register(ObservableGauge.builder(METRIC_REAL_TIME_BLOCK_SUBSCRIBE_LAST_BLOCK_NUMBER)
                .setDescription("Highest block number this plugin has emitted a notification for.")
                .observe(lastBlockNumberEmitted::get));
    }

    /** Returns {@code now - lastBlockEndReceivedAt}; {@code 0} when no block has been seen yet. */
    private long lagMillis() {
        final long last = lastBlockEndReceivedAtEpochMillis.get();
        return last == 0L ? 0L : Math.max(0L, System.currentTimeMillis() - last);
    }
}
