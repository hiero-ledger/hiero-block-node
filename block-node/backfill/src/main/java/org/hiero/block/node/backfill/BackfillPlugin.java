// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.TRACE;

import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
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
import java.util.function.Consumer;
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
import org.hiero.metrics.LongCounter;
import org.hiero.metrics.ObservableGauge;
import org.hiero.metrics.core.MetricKey;
import org.hiero.metrics.core.MetricRegistry;

/**
 * BackfillPlugin is a BlockNodePlugin that detects gaps in historical blocks and
 * live blocks and fetches missing blocks from configured block nodes using gRPC.
 * It runs periodically to ensure that all historical blocks are available for
 * historical blocks, and on-demand for live blocks.
 */
public class BackfillPlugin implements BlockNodePlugin, BlockNotificationHandler {

    public static final MetricKey<ObservableGauge> METRIC_BACKFILL_STATUS =
            MetricKey.of("backfill_status", ObservableGauge.class).addCategory(METRICS_CATEGORY);
    public static final MetricKey<ObservableGauge> METRIC_BACKFILL_PENDING_BLOCKS =
            MetricKey.of("backfill_pending_blocks", ObservableGauge.class).addCategory(METRICS_CATEGORY);
    public static final MetricKey<ObservableGauge> METRIC_BACKFILL_INFLIGHT_BLOCKS =
            MetricKey.of("backfill_inflight_blocks", ObservableGauge.class).addCategory(METRICS_CATEGORY);
    public static final MetricKey<LongCounter> METRIC_BACKFILL_GAPS_DETECTED =
            MetricKey.of("backfill_gaps_detected", LongCounter.class).addCategory(METRICS_CATEGORY);
    public static final MetricKey<LongCounter> METRIC_BACKFILL_BLOCKS_FETCHED =
            MetricKey.of("backfill_blocks_fetched", LongCounter.class).addCategory(METRICS_CATEGORY);
    public static final MetricKey<LongCounter> METRIC_BACKFILL_BLOCKS_BACKFILLED =
            MetricKey.of("backfill_blocks_backfilled", LongCounter.class).addCategory(METRICS_CATEGORY);
    public static final MetricKey<LongCounter> METRIC_BACKFILL_FETCH_ERRORS =
            MetricKey.of("backfill_fetch_errors", LongCounter.class).addCategory(METRICS_CATEGORY);
    public static final MetricKey<LongCounter> METRIC_BACKFILL_RETRIES =
            MetricKey.of("backfill_retries", LongCounter.class).addCategory(METRICS_CATEGORY);

    /** The logger for this class. */
    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    private final GapDetector gapDetector = new GapDetector();

    // Plugin infrastructure
    private BlockNodeContext context;
    private BackfillConfiguration backfillConfiguration;
    private long earliestManagedBlock;
    private boolean hasBNSourcesPath = false;
    private BackfillSource blockNodeSources;
    private ScheduledExecutorService autonomousExecutor;

    // Two independent schedulers with dedicated executors: historical never blocks live-tail
    private BackfillTaskScheduler historicalScheduler;
    private BackfillTaskScheduler liveTailScheduler;
    private ScheduledExecutorService historicalExecutor;
    private ScheduledExecutorService liveTailExecutor;

    // Dedicated fetcher for the autonomous scan's peer availability queries.
    // Must be separate from scheduler fetchers because HTTP/2 connections (Helidon WebClient)
    // are not thread-safe and the autonomous scan runs concurrently with the scheduler drains.
    private BackfillFetcher autonomousFetcher;

    // State touched by multiple threads
    private final AtomicLong pendingBackfillBlocks = new AtomicLong(0);
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
        metricsHolder = MetricsHolder.createMetrics(context.metricRegistry(), pendingBackfillBlocks);
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
            final String blockNodeSourcesPathNotFoundMsg =
                    "Block node sources path does not exist or is not a regular file: [{0}], backfill will not run";
            LOGGER.log(TRACE, blockNodeSourcesPathNotFoundMsg, backfillConfiguration.blockNodeSourcesPath());
            return;
        }

        try {
            blockNodeSources = BackfillSource.JSON.parse(Bytes.wrap(Files.readAllBytes(blockNodeSourcesPath)));
        } catch (ParseException | IOException e) {
            final String parseFailedMsg =
                    "Failed to parse block node sources from path: [%s], backfill will not run: %s"
                            .formatted(backfillConfiguration.blockNodeSourcesPath(), e.getMessage());
            LOGGER.log(TRACE, parseFailedMsg, e);
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

        final String schedulingMsg = "Scheduling backfill process to start in [{0}] milliseconds";
        LOGGER.log(TRACE, schedulingMsg, backfillConfiguration.initialDelay());

        // Create platform thread executors via threadPoolManager.
        // Platform threads required because Helidon WebClient HTTP/2 has issues with virtual threads in containers.
        Thread.UncaughtExceptionHandler handler =
                (thread, e) -> LOGGER.log(INFO, "Uncaught exception in thread: " + thread.getName(), e);

        autonomousExecutor =
                context.threadPoolManager().createSingleThreadScheduledExecutor("BackfillPluginRunner", handler);

        // Schedule periodic gap detection task using autonomous executor
        autonomousExecutor.scheduleAtFixedRate(
                this::detectAndScheduleGaps,
                backfillConfiguration.initialDelay(),
                backfillConfiguration.scanInterval(),
                TimeUnit.MILLISECONDS);

        // Create dedicated platform thread executors for backfill tasks
        historicalExecutor =
                context.threadPoolManager().createSingleThreadScheduledExecutor("BackfillHistoricalExecutor", handler);
        liveTailExecutor =
                context.threadPoolManager().createSingleThreadScheduledExecutor("BackfillLiveTailExecutor", handler);

        // Create a dedicated fetcher for the autonomous scan's peer availability queries.
        // This avoids sharing HTTP/2 connections with the scheduler fetchers, which run
        // concurrently on different threads and cause HPACK dynamic table corruption.
        autonomousFetcher = new BackfillFetcher(blockNodeSources, backfillConfiguration, metricsHolder);

        historicalScheduler =
                createScheduler(historicalExecutor, backfillConfiguration.historicalQueueCapacity(), "Historical");
        liveTailScheduler =
                createScheduler(liveTailExecutor, backfillConfiguration.liveTailQueueCapacity(), "LiveTail");

        final String initializedSchedulersMsg =
                "Initialized dual schedulers: historical(cap=[{0}]), liveTail(cap=[{1}])";
        LOGGER.log(
                TRACE,
                initializedSchedulersMsg,
                backfillConfiguration.historicalQueueCapacity(),
                backfillConfiguration.liveTailQueueCapacity());
    }

    private BackfillTaskScheduler createScheduler(ExecutorService executor, int queueCapacity, String schedulerName) {
        try {

            BackfillFetcher fetcher = new BackfillFetcher(blockNodeSources, backfillConfiguration, metricsHolder);
            // Create dedicated persistence awaiter for system backpressure
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
            GapProcessor gapProcessor = new GapProcessor(runner, schedulerName);
            return new BackfillTaskScheduler(executor, gapProcessor, queueCapacity, fetcher, persistenceAwaiter);
        } catch (RuntimeException e) {
            final String createSchedulerFailedMsg = "Failed to create scheduler: [%s]".formatted(e.getMessage());
            LOGGER.log(INFO, createSchedulerFailedMsg, e);
            return null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop() {
        // 1. Stop periodic scanner
        shutdownExecutor(autonomousExecutor, "periodicExecutor");

        // 2. Close schedulers (clears queues, awaiters, and releases blocked threads)
        if (historicalScheduler != null) {
            try {
                historicalScheduler.close();
            } catch (RuntimeException e) {
                LOGGER.log(INFO, "Error closing historicalScheduler: " + e.getMessage(), e);
            }
        }
        if (liveTailScheduler != null) {
            try {
                liveTailScheduler.close();
            } catch (RuntimeException e) {
                LOGGER.log(INFO, "Error closing liveTailScheduler: " + e.getMessage(), e);
            }
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
                final String executorTerminationMsg = "Executor [{0}] did not terminate in time";
                LOGGER.log(INFO, executorTerminationMsg, name);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void detectAndScheduleGaps() {
        LOGGER.log(TRACE, "Detecting gaps in blocks");

        // 1. Get stored blocks
        List<LongRange> blockRanges = context.historicalBlockProvider()
                .availableBlocks()
                .streamRanges()
                .toList();

        // 2. Determine range to scan
        //    - Lower bound: configured startBlock
        //    - Upper bound: greedy → peer max, non-greedy → store max (capped by config)
        long startBound = Math.max(0, backfillConfiguration.startBlock());
        long endCap = determineEndCap(blockRanges);
        if (endCap < 0 || startBound > endCap) {
            final String nothingToBackfillMsg = "Nothing to backfill: startBound=[{0}] endCap=[{1}]";
            LOGGER.log(TRACE, nothingToBackfillMsg, startBound, endCap);
            return;
        }

        // 3. Find gaps and classify as HISTORICAL or LIVE_TAIL
        //    - Boundary: empty store → earliestManagedBlock-1, has blocks → last stored block
        //    - Blocks <= boundary → HISTORICAL, blocks > boundary → LIVE_TAIL
        long liveTailBoundary = blockRanges.isEmpty()
                ? earliestManagedBlock - 1
                : blockRanges.getLast().end();
        List<GapDetector.Gap> gaps = gapDetector.findTypedGaps(blockRanges, startBound, liveTailBoundary, endCap);

        // 4. Submit each gap to appropriate scheduler
        final String detectedGapMsg = "Detected gap type=[{0}] range=[{1}]";
        for (GapDetector.Gap gap : gaps) {
            LOGGER.log(TRACE, detectedGapMsg, gap.type(), gap.range());
            scheduleGap(gap);
            metricsHolder.backfillGapsDetected.increment();
        }
    }

    /**
     * Determine the upper limit for gap detection.
     * - Greedy mode: max latestAvailableBlock from peers
     * - Non-greedy: last stored block
     * Both are capped by configured endBlock if set.
     */
    private long determineEndCap(List<LongRange> blockRanges) {
        long configEnd = backfillConfiguration.endBlock();
        long storeMax = blockRanges.isEmpty() ? -1 : blockRanges.getLast().end();

        if (backfillConfiguration.greedy() && liveTailScheduler != null) {
            long peerMax = getPeerMaxAvailableBlock(storeMax);
            long upper = peerMax >= 0 ? peerMax : storeMax;
            return configEnd >= 0 ? Math.min(configEnd, upper) : upper;
        } else {
            return configEnd >= 0 ? Math.min(configEnd, storeMax) : storeMax;
        }
    }

    /**
     * Query peers for the maximum available block number.
     * Uses a dedicated fetcher to avoid sharing HTTP/2 connections with the scheduler fetchers.
     */
    private long getPeerMaxAvailableBlock(long baseline) {
        try {
            LongRange peerRange = autonomousFetcher.getNewAvailableRange(baseline);
            return peerRange != null && peerRange.size() > 0 ? peerRange.end() : -1;
        } catch (RuntimeException e) {
            final String peerAvailabilityFailedMsg = "Failed to get peer availability: %s".formatted(e.getMessage());
            LOGGER.log(TRACE, peerAvailabilityFailedMsg, e);
            return -1;
        }
    }

    private void scheduleGap(GapDetector.Gap gap) {
        if (gap.range().size() <= 0) {
            return;
        }

        // Skip historical gaps if scheduler is already processing (historical gaps don't change)
        // This is to avoid scheduling duplicate historical gaps on each scan while them are still in progress
        if (gap.type() == GapDetector.Type.HISTORICAL
                && historicalScheduler != null
                && historicalScheduler.isRunning()) {
            final String skippingHistoricalGapMsg = "Skipping historical gap [{0}], scheduler already running";
            LOGGER.log(TRACE, skippingHistoricalGapMsg, gap.range());
            return;
        }

        // Deduplicate live-tail gaps using high-water mark
        GapDetector.Gap effectiveGap = gap;
        if (gap.type() == GapDetector.Type.LIVE_TAIL) {
            long highWaterMark = liveTailHighWaterMark.get();
            if (gap.range().end() <= highWaterMark) {
                // Already scheduled this range
                final String skippingDuplicateLiveTailMsg =
                        "Skipping duplicate live-tail gap [{0}], highWaterMark=[{1}]";
                LOGGER.log(TRACE, skippingDuplicateLiveTailMsg, gap.range(), highWaterMark);
                return;
            }
            if (gap.range().start() <= highWaterMark) {
                // Partial overlap - adjust start
                long newStart = highWaterMark + 1;
                effectiveGap =
                        new GapDetector.Gap(new LongRange(newStart, gap.range().end()), GapDetector.Type.LIVE_TAIL);
                final String adjustedLiveTailGapMsg = "Adjusted live-tail gap from [{0}] to [{1}]";
                LOGGER.log(TRACE, adjustedLiveTailGapMsg, gap.range(), effectiveGap.range());
            }
            // Update high-water mark
            liveTailHighWaterMark.updateAndGet(
                    current -> Math.max(current, gap.range().end()));
            final String updatedHighWaterMarkMsg = "Updated liveTailHighWaterMark to [{0}]";
            LOGGER.log(TRACE, updatedHighWaterMarkMsg, gap.range().end());
        }

        // Submit the (possibly adjusted) gap to the appropriate scheduler
        final String submittingGapMsg = "Submitting gap type=[{0}] range=[{1}] to scheduler";
        LOGGER.log(TRACE, submittingGapMsg, effectiveGap.type(), effectiveGap.range());
        submitGap(effectiveGap);
    }

    /**
     * Submits a gap to the appropriate scheduler based on its type.
     *
     * @param gap the gap to submit
     */
    private void submitGap(GapDetector.Gap gap) {
        BackfillTaskScheduler scheduler =
                (gap.type() == GapDetector.Type.HISTORICAL) ? historicalScheduler : liveTailScheduler;
        scheduler.submit(gap);
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
            final String backfillPersistedMsg = "Received backfill persisted notification for block=[{0}]";
            LOGGER.log(TRACE, backfillPersistedMsg, notification.blockNumber());

            metricsHolder.backfillBlocksBackfilled().increment();
            pendingBackfillBlocks.updateAndGet(v -> Math.max(0, v - 1));
        } else {
            final String nonBackfillPersistedMsg = "Received non-backfill persisted notification: [{0}]";
            LOGGER.log(TRACE, nonBackfillPersistedMsg, notification);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleVerification(VerificationNotification notification) {
        if (notification.source() == BlockSource.BACKFILL) {
            final String verificationNotificationMsg = "Received verification notification for block [{0}]";
            LOGGER.log(TRACE, verificationNotificationMsg, notification.blockNumber());
            if (!notification.success()) {
                final String blockVerificationFailedMsg = "Block verification failed, block=[{0}]";
                LOGGER.log(INFO, blockVerificationFailedMsg, notification.blockNumber());
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
            final String skippingOnDemandBackfillMsg =
                    "Newest block [{0}] is before startBackfillFrom [{1}], skipping on-demand backfill";
            LOGGER.log(TRACE, skippingOnDemandBackfillMsg, cappedEnd, startBackfillFrom);
            return;
        }
        scheduleGap(new GapDetector.Gap(new LongRange(startBackfillFrom, cappedEnd), GapDetector.Type.LIVE_TAIL));
    }

    /**
     * Processes gaps by delegating to the BackfillRunner and handling high-water mark updates.
     */
    private class GapProcessor implements Consumer<GapDetector.Gap> {
        private final BackfillRunner runner;
        private final String schedulerName;

        GapProcessor(BackfillRunner runner, String schedulerName) {
            this.runner = runner;
            this.schedulerName = schedulerName;
        }

        @Override
        public void accept(GapDetector.Gap gap) {
            try {
                final String processingGapMsg = "Scheduler processing gap type=[{0}] range=[{1}] for [{2}]";
                LOGGER.log(TRACE, processingGapMsg, gap.type(), gap.range(), schedulerName);
                long lastSuccessfulBlock = runner.run(gap);
                // Reset highWaterMark if the gap didn't complete, allowing re-detection
                if (gap.type() == GapDetector.Type.LIVE_TAIL
                        && lastSuccessfulBlock < gap.range().end()) {
                    liveTailHighWaterMark.updateAndGet(current -> Math.min(current, lastSuccessfulBlock));
                    final String resetHighWaterMarkMsg =
                            "Reset liveTailHighWaterMark to [{0}] after incomplete gap [{1}]";
                    LOGGER.log(INFO, resetHighWaterMarkMsg, lastSuccessfulBlock, gap.range());
                }
            } catch (ParseException | InterruptedException e) {
                Thread.currentThread().interrupt();
                final String errorExecutingGapMsg = "Error executing gap=[%s]".formatted(gap);
                LOGGER.log(INFO, errorExecutingGapMsg, e);
            }
        }
    }

    /**
     * Holder for all backfill-related metrics.
     * This record groups all metrics used by the backfill plugin and its components,
     * allowing them to be passed as a single parameter.
     */
    public record MetricsHolder(
            LongCounter.Measurement backfillGapsDetected,
            LongCounter.Measurement backfillFetchedBlocks,
            LongCounter.Measurement backfillBlocksBackfilled,
            LongCounter.Measurement backfillFetchErrors,
            LongCounter.Measurement backfillRetries) {

        /**
         * Factory method to create a MetricsHolder with all metrics registered.
         *
         * @param metricRegistry the metrics registry instance to register metrics with
         * @return a new MetricsHolder with all metrics created
         */
        public static MetricsHolder createMetrics(
                @NonNull final MetricRegistry metricRegistry, @NonNull final AtomicLong pendingBackfillBlocks) {
            metricRegistry.register(ObservableGauge.builder(METRIC_BACKFILL_STATUS)
                    .setDescription("Current status of the backfill process (e.g., idle = 0, running = 1, error = 2).")
                    .observe(() -> determineStatus(pendingBackfillBlocks)));
            metricRegistry.register(ObservableGauge.builder(METRIC_BACKFILL_PENDING_BLOCKS)
                    .setDescription("Current amount of blocks pending to be backfilled.")
                    .observe(() -> Math.max(pendingBackfillBlocks.get(), 0)));
            metricRegistry.register(ObservableGauge.builder(METRIC_BACKFILL_INFLIGHT_BLOCKS)
                    .setDescription("Current in-flight backfill blocks awaiting verification/persistence.")
                    .observe(() -> Math.max(pendingBackfillBlocks.get(), 0)));

            return new MetricsHolder(
                    metricRegistry
                            .register(LongCounter.builder(METRIC_BACKFILL_GAPS_DETECTED)
                                    .setDescription("Number of gaps detected during the backfill process."))
                            .getOrCreateNotLabeled(),
                    metricRegistry
                            .register(LongCounter.builder(METRIC_BACKFILL_BLOCKS_FETCHED)
                                    .setDescription("Number of blocks fetched during the backfill process."))
                            .getOrCreateNotLabeled(),
                    metricRegistry
                            .register(LongCounter.builder(METRIC_BACKFILL_BLOCKS_BACKFILLED)
                                    .setDescription("Number of blocks backfilled during the backfill process."))
                            .getOrCreateNotLabeled(),
                    metricRegistry
                            .register(LongCounter.builder(METRIC_BACKFILL_FETCH_ERRORS)
                                    .setDescription("Number of errors encountered during the backfill process."))
                            .getOrCreateNotLabeled(),
                    metricRegistry
                            .register(LongCounter.builder(METRIC_BACKFILL_RETRIES)
                                    .setDescription("Number of retries during the backfill process."))
                            .getOrCreateNotLabeled());
        }

        private static int determineStatus(AtomicLong pendingBackfillBlocks) {
            long pending = Math.max(pendingBackfillBlocks.get(), 0);
            final BackfillStatus status = pending > 0 ? BackfillStatus.RUNNING : BackfillStatus.IDLE;
            return status.ordinal();
        }
    }
}
