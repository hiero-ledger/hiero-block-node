// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.TRACE;
import static java.lang.System.Logger.Level.WARNING;

import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.metrics.api.Counter;
import com.swirlds.metrics.api.LongGauge;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.block.node.app.config.node.NodeConfig;
import org.hiero.block.node.backfill.client.BackfillSource;
import org.hiero.block.node.backfill.client.BackfillSourceConfig;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.NewestBlockKnownToNetworkNotification;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.spi.historicalblocks.LongRange;

/**
 * BackfillPlugin is a BlockNodePlugin that detects gaps in historical blocks and
 * live blocks and fetches missing blocks from configured block nodes using gRPC.
 * It runs periodically to ensure that all historical blocks are available for
 * historical blocks, and on-demand for live blocks.
 */
public class BackfillPlugin implements BlockNodePlugin, BlockNotificationHandler {

    /** The logger for this class. */
    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    private final GapDetector gapDetector = new GapDetector();

    // Plugin infrastructure
    private BlockNodeContext context;
    private BackfillConfiguration backfillConfiguration;
    private long earliestManagedBlock;
    private boolean hasBNSourcesPath = false;
    private BackfillSource blockNodeSources;
    private ScheduledExecutorService periodicExecutor;

    // Two independent schedulers with dedicated executors: historical never blocks live-tail
    private BackfillTaskScheduler historicalScheduler;
    private BackfillTaskScheduler liveTailScheduler;
    private ExecutorService historicalExecutor;
    private ExecutorService liveTailExecutor;

    // State touched by multiple threads
    private final AtomicLong pendingBackfillBlocks = new AtomicLong(0);
    private volatile long lastAcknowledgedBlockObserved = -1;
    // Deduplication: highest block scheduled for live-tail (prevents overlapping submissions)
    private final AtomicLong liveTailHighWaterMark = new AtomicLong(-1);

    // Metrics holder containing all backfill metrics
    private MetricsHolder metricsHolder;

    /**
     * {@inheritDoc}
     */
    @NonNull
    @Override
    public List<Class<? extends Record>> configDataTypes() {
        return List.of(BackfillConfiguration.class);
    }

    /**
     * Initializes the metrics for the backfill process.
     */
    private void initMetrics() {
        metricsHolder = MetricsHolder.createMetrics(context.metrics());
        context.metrics().addUpdater(this::updateMetrics);
    }

    private void updateMetrics() {
        long pending = Math.max(pendingBackfillBlocks.get(), 0);
        metricsHolder.backfillPendingBlocksGauge().set(pending);
        metricsHolder.backfillInFlightGauge().set(pending);

        final BackfillStatus status = pending > 0 ? BackfillStatus.RUNNING : BackfillStatus.IDLE;
        // rely on ordinal for metric value, as enum names are not supported in metrics
        metricsHolder.backfillStatus().set(status.ordinal());
    }

    // Backfill status enum for metrics, using ordinal values
    // do not change order or add values in the middle
    private enum BackfillStatus {
        IDLE, // 0
        RUNNING // 1
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(BlockNodeContext context, ServiceBuilder serviceBuilder) {
        this.context = context;
        backfillConfiguration = context.configuration().getConfigData(BackfillConfiguration.class);
        earliestManagedBlock =
                context.configuration().getConfigData(NodeConfig.class).earliestManagedBlock();

        // Initialize metrics
        initMetrics();

        // Validate block node sources configuration
        final String sourcesPath = backfillConfiguration.blockNodeSourcesPath();
        if (sourcesPath == null || sourcesPath.isBlank()) {
            LOGGER.log(TRACE, "No block node sources path configured, backfill will not run");
            return;
        }

        Path blockNodeSourcesPath = Path.of(backfillConfiguration.blockNodeSourcesPath());
        if (!Files.isRegularFile(blockNodeSourcesPath)) {
            LOGGER.log(
                    TRACE,
                    "Block node sources path file does not exist: [%s], backfill will not run"
                            .formatted(backfillConfiguration.blockNodeSourcesPath()));
            return;
        }

        try {
            blockNodeSources = BackfillSource.JSON.parse(Bytes.wrap(Files.readAllBytes(blockNodeSourcesPath)));
        } catch (ParseException | IOException e) {
            LOGGER.log(
                    TRACE,
                    "Failed to parse block node sources from path: [%s], backfill will not run: %s"
                            .formatted(backfillConfiguration.blockNodeSourcesPath(), e.getMessage()));
            return;
        }

        // ready for backfill.
        hasBNSourcesPath = true;

        // Register the service
        context.blockMessaging().registerBlockNotificationHandler(this, false, "BackfillPlugin");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start() {
        if (!hasBNSourcesPath) {
            return;
        }

        LOGGER.log(
                TRACE,
                "Scheduling backfill process to start in [%s] milliseconds"
                        .formatted(backfillConfiguration.initialDelay()));

        periodicExecutor = context.threadPoolManager()
                .createVirtualThreadScheduledExecutor(
                        1, // single scheduler thread for scans
                        "BackfillPluginRunner",
                        (t, e) -> LOGGER.log(INFO, "Uncaught exception in thread: " + t.getName(), e));
        periodicExecutor.scheduleAtFixedRate(
                this::detectGaps,
                backfillConfiguration.initialDelay(),
                backfillConfiguration.scanInterval(),
                TimeUnit.MILLISECONDS);

        // Create two independent schedulers with dedicated executors for full isolation
        historicalExecutor = context.threadPoolManager()
                .createVirtualThreadScheduledExecutor(
                        1,
                        "BackfillHistoricalExecutor",
                        (t, e) -> LOGGER.log(WARNING, "Uncaught exception in thread: " + t.getName(), e));
        liveTailExecutor = context.threadPoolManager()
                .createVirtualThreadScheduledExecutor(
                        1,
                        "BackfillLiveTailExecutor",
                        (t, e) -> LOGGER.log(WARNING, "Uncaught exception in thread: " + t.getName(), e));

        historicalScheduler =
                createScheduler(historicalExecutor, backfillConfiguration.historicalQueueCapacity(), "Historical");
        liveTailScheduler =
                createScheduler(liveTailExecutor, backfillConfiguration.liveTailQueueCapacity(), "LiveTail");

        LOGGER.log(
                TRACE,
                "Initialized dual schedulers: historical(cap=[%s]), liveTail(cap=[%s])"
                        .formatted(
                                backfillConfiguration.historicalQueueCapacity(),
                                backfillConfiguration.liveTailQueueCapacity()));
    }

    private BackfillTaskScheduler createScheduler(ExecutorService executor, int queueCapacity, String schedulerName) {
        try {

            BackfillFetcher fetcher = new BackfillFetcher(blockNodeSources, backfillConfiguration, metricsHolder);
            // Create dedicated persistence awaiter for backpressure
            BackfillPersistenceAwaiter persistenceAwaiter = new BackfillPersistenceAwaiter();
            context.blockMessaging()
                    .registerBlockNotificationHandler(
                            persistenceAwaiter, false, "BackfillPersistenceAwaiter-" + schedulerName);
            BackfillRunner runner = new BackfillRunner(
                    fetcher,
                    backfillConfiguration,
                    context.blockMessaging(),
                    LOGGER,
                    metricsHolder,
                    pendingBackfillBlocks,
                    persistenceAwaiter);
            return new BackfillTaskScheduler(
                    executor,
                    gap -> {
                        try {
                            runner.run(gap);
                        } catch (ParseException | InterruptedException e) {
                            Thread.currentThread().interrupt();
                            LOGGER.log(TRACE, "Error executing gap=[%s]".formatted(gap), e);
                        }
                    },
                    queueCapacity,
                    fetcher,
                    persistenceAwaiter);
        } catch (Exception e) {
            LOGGER.log(INFO, "Failed to create scheduler: [%s]".formatted(e.getMessage()));
            return null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop() {
        // 1. Stop periodic scanner
        shutdownExecutor(periodicExecutor, "periodicExecutor");

        // 2. Close schedulers (clears queues, awaiters, and releases blocked threads)
        if (historicalScheduler != null) {
            historicalScheduler.close();
        }
        if (liveTailScheduler != null) {
            liveTailScheduler.close();
        }

        // 3. Shutdown executors and wait for termination
        shutdownExecutor(historicalExecutor, "historicalExecutor");
        shutdownExecutor(liveTailExecutor, "liveTailExecutor");

        LOGGER.log(TRACE, "Stopped backfill plugin");
    }

    private void shutdownExecutor(ExecutorService executor, String name) {
        if (executor == null) {
            return;
        }
        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                LOGGER.log(WARNING, "Executor [%s] did not terminate in time".formatted(name));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void detectGaps() {
        LOGGER.log(TRACE, "Detecting gaps in blocks");

        List<LongRange> blockRanges = context.historicalBlockProvider()
                .availableBlocks()
                .streamRanges()
                .toList();

        long startBound = Math.max(0, backfillConfiguration.startBlock());
        long endCap = backfillConfiguration.endBlock() >= 0 ? backfillConfiguration.endBlock() : Long.MAX_VALUE;
        if (startBound > endCap) {
            LOGGER.log(TRACE, "Configured startBlock > endBlock; nothing to backfill");
            return;
        }

        long blockRangesLastValue =
                blockRanges.isEmpty() ? -1 : blockRanges.getLast().end();
        greedyBackfillRecentBlocks(lastAcknowledgedBlockObserved, blockRangesLastValue);
        lastAcknowledgedBlockObserved = blockRangesLastValue;

        long liveTailBoundary = Math.min(
                blockRanges.isEmpty()
                        ? earliestManagedBlock
                        : blockRanges.getLast().end(),
                endCap);
        List<GapDetector.Gap> typedGaps = gapDetector.findTypedGaps(blockRanges, startBound, liveTailBoundary, endCap);
        if (!typedGaps.isEmpty()) {
            metricsHolder.backfillGapsDetected().add(typedGaps.size());
        }
        for (GapDetector.Gap gap : typedGaps) {
            LOGGER.log(
                    TRACE,
                    "Detected gap type=[%s] from start=[%s] to end=[%s]"
                            .formatted(
                                    gap.type(), gap.range().start(), gap.range().end()));
            scheduleGap(gap);
        }

        if (typedGaps.isEmpty()) {
            LOGGER.log(TRACE, "No gaps detected in historical blocks");
        }
    }

    private void scheduleGap(GapDetector.Gap gap) {
        if (gap.range().size() <= 0) {
            return;
        }

        // Deduplicate live-tail gaps using high-water mark
        GapDetector.Gap effectiveGap = gap;
        if (gap.type() == GapDetector.Type.LIVE_TAIL) {
            long hwm = liveTailHighWaterMark.get();
            if (gap.range().end() <= hwm) {
                // Already scheduled this range
                LOGGER.log(TRACE, "Skipping duplicate live-tail gap [%s], hwm=[%s]".formatted(gap.range(), hwm));
                return;
            }
            if (gap.range().start() <= hwm) {
                // Partial overlap - adjust start
                long newStart = hwm + 1;
                effectiveGap =
                        new GapDetector.Gap(new LongRange(newStart, gap.range().end()), GapDetector.Type.LIVE_TAIL);
                LOGGER.log(
                        TRACE, "Adjusted live-tail gap from [%s] to [%s]".formatted(gap.range(), effectiveGap.range()));
            }
            // Update high-water mark
            liveTailHighWaterMark.updateAndGet(
                    current -> Math.max(current, gap.range().end()));
        }

        BackfillTaskScheduler scheduler =
                (effectiveGap.type() == GapDetector.Type.HISTORICAL) ? historicalScheduler : liveTailScheduler;
        if (scheduler != null) {
            scheduler.submit(effectiveGap);
        }
    }

    /**
     * Opportunistically fetch the newest available range from peers to stay close to head.
     */
    private void greedyBackfillRecentBlocks(long lastObserved, long lastLocal) {
        if (!backfillConfiguration.greedy()) {
            return;
        }
        if (lastObserved > 0 && lastLocal > lastObserved) {
            return;
        }
        if (liveTailScheduler == null) {
            return;
        }
        long baseline = Math.max(lastLocal, backfillConfiguration.startBlock() - 1);
        LongRange peerRange = liveTailScheduler.getFetcher().getNewAvailableRange(baseline);
        if (peerRange == null || peerRange.size() <= 0 || peerRange.start() < 0) {
            return;
        }
        long cappedEnd = backfillConfiguration.endBlock() >= 0
                ? Math.min(backfillConfiguration.endBlock(), peerRange.end())
                : peerRange.end();
        if (cappedEnd < peerRange.start()) {
            return;
        }
        scheduleGap(new GapDetector.Gap(new LongRange(peerRange.start(), cappedEnd), GapDetector.Type.LIVE_TAIL));
    }

    // Package-private for test visibility
    LongRange computeChunk(
            @NonNull NodeSelectionStrategy.NodeSelection selection,
            @NonNull Map<BackfillSourceConfig, List<LongRange>> availability,
            long gapEnd,
            long batchSize) {
        return BackfillRunner.computeChunk(selection, availability, gapEnd, batchSize);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handlePersisted(PersistedNotification notification) {
        if (notification.blockSource() == BlockSource.BACKFILL) {
            // Add more detailed logging for persistence notifications
            LOGGER.log(
                    TRACE,
                    "Received backfill persisted notification for block=[%s]".formatted(notification.blockNumber()));

            metricsHolder.backfillBlocksBackfilled().increment();
            pendingBackfillBlocks.updateAndGet(v -> Math.max(0, v - 1));
        } else {
            LOGGER.log(TRACE, "Received non-backfill persisted notification: [%s]".formatted(notification));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleVerification(VerificationNotification notification) {
        if (notification.source() == BlockSource.BACKFILL) {
            LOGGER.log(
                    TRACE, "Received verification notification for block [%s]".formatted(notification.blockNumber()));
            if (!notification.success()) {
                LOGGER.log(INFO, "Block verification failed, block=[%s]".formatted(notification.blockNumber()));
                metricsHolder.backfillFetchErrors().increment();
                pendingBackfillBlocks.updateAndGet(v -> Math.max(0, v - 1));
                // If a block verification fails, we will backfill it again later on the next gap detection run.
            }
        }
    }

    @Override
    public void handleNewestBlockKnownToNetwork(NewestBlockKnownToNetworkNotification notification) {
        if (!hasBNSourcesPath) {
            LOGGER.log(TRACE, "No block node sources path configured, skipping on-demand backfill");
            return;
        }

        long lastPersistedBlock =
                context.historicalBlockProvider().availableBlocks().max();
        long startBackfillFrom = Math.max(lastPersistedBlock + 1, backfillConfiguration.startBlock());
        long newestBlockKnown = notification.blockNumber();
        long cappedEnd = backfillConfiguration.endBlock() >= 0
                ? Math.min(backfillConfiguration.endBlock(), newestBlockKnown)
                : newestBlockKnown;
        if (cappedEnd < startBackfillFrom) {
            LOGGER.log(
                    TRACE,
                    "Newest block [%s] is before startBackfillFrom [%s], skipping on-demand backfill"
                            .formatted(cappedEnd, startBackfillFrom));
            return;
        }
        scheduleGap(new GapDetector.Gap(new LongRange(startBackfillFrom, cappedEnd), GapDetector.Type.LIVE_TAIL));
    }

    /**
     * Holder for all backfill-related metrics.
     * This record groups all metrics used by the backfill plugin and its components,
     * allowing them to be passed as a single parameter.
     */
    public record MetricsHolder(
            Counter backfillGapsDetected,
            Counter backfillFetchedBlocks,
            Counter backfillBlocksBackfilled,
            Counter backfillFetchErrors,
            Counter backfillRetries,
            LongGauge backfillStatus,
            LongGauge backfillPendingBlocksGauge,
            LongGauge backfillInFlightGauge) {

        /**
         * Factory method to create a MetricsHolder with all metrics registered.
         *
         * @param metrics the metrics instance to register metrics with
         * @return a new MetricsHolder with all metrics created
         */
        public static MetricsHolder createMetrics(@NonNull final com.swirlds.metrics.api.Metrics metrics) {
            return new MetricsHolder(
                    metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "backfill_gaps_detected")
                            .withDescription("Number of gaps detected during the backfill process.")),
                    metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "backfill_blocks_fetched")
                            .withDescription("Number of blocks fetched during the backfill process.")),
                    metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "backfill_blocks_backfilled")
                            .withDescription("Number of blocks backfilled during the backfill process.")),
                    metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "backfill_fetch_errors")
                            .withDescription("Number of errors encountered during the backfill process.")),
                    metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "backfill_retries")
                            .withDescription("Number of retries during the backfill process.")),
                    metrics.getOrCreate(
                            new LongGauge.Config(METRICS_CATEGORY, "backfill_status")
                                    .withDescription(
                                            "Current status of the backfill process (e.g., idle = 0, running = 1, error = 2).")),
                    metrics.getOrCreate(new LongGauge.Config(METRICS_CATEGORY, "backfill_pending_blocks")
                            .withDescription("Current amount of blocks pending to be backfilled.")),
                    metrics.getOrCreate(new LongGauge.Config(METRICS_CATEGORY, "backfill_inflight_blocks")
                            .withDescription("Current in-flight backfill blocks awaiting verification/persistence.")));
        }
    }
}
