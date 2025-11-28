// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.TRACE;

import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import com.swirlds.metrics.api.Counter;
import com.swirlds.metrics.api.LongGauge;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.BackfilledBlockNotification;
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

    // Plugin infrastructure
    private BlockNodeContext context;
    private BackfillConfiguration backfillConfiguration;
    private boolean hasBNSourcesPath = false;
    private ScheduledExecutorService scheduler;

    // Backfill state
    private List<LongRange> detectedGaps = new ArrayList<>();
    private BackfillGrpcClient backfillGrpcClientAutonomous;
    private BackfillGrpcClient backfillGrpcClientOnDemand;

    // State touched by multiple threads
    private final AtomicReference<CountDownLatch> autonomousLatch = new AtomicReference<>(new CountDownLatch(0));
    private final AtomicReference<CountDownLatch> onDemandLatch = new AtomicReference<>(new CountDownLatch(0));
    private volatile boolean autonomousError = false;
    private volatile boolean onDemandError = false;
    private final AtomicLong autonomousBackfillEndBlock = new AtomicLong(-1);
    private final AtomicLong onDemandBackfillStartBlock = new AtomicLong(-1);
    private final AtomicLong lastAcknowledgedBlockObserved = new AtomicLong(-1);

    // Metrics
    private Counter backfillGapsDetected;
    private Counter backfillFetchedBlocks;
    private Counter backfillBlocksBackfilled;
    private Counter backfillFetchErrors;
    private Counter backfillRetries;
    private LongGauge backfillStatus; // 0 = idle, 1 = running, 2 = autonomous-error, 3 = on-demand-error
    private LongGauge backfillPendingBlocksGauge;

    private AtomicReference<CountDownLatch> getLatch(BackfillType t) {
        if (t == BackfillType.AUTONOMOUS) {
            return autonomousLatch;
        } else if (t == BackfillType.ON_DEMAND) {
            return onDemandLatch;
        } else {
            // This should never happen (as the code is right now is impossible)
            // but we throw an exception to be safe in the future in case the enum is extended
            LOGGER.log(INFO, "Unknown backfill type={0}", t);
            throw new IllegalArgumentException("Unknown backfill type: " + t);
        }
    }

    /**
     * {@inheritDoc}
     */
    @NonNull
    @Override
    public List<Class<? extends Record>> configDataTypes() {
        return List.of(BackfillConfiguration.class);
    }

    /***
     * Initializes the metrics for the backfill process.
     */
    private void initMetrics() {
        final var metrics = context.metrics();
        backfillGapsDetected = metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "backfill_gaps_detected")
                .withDescription("Number of gaps detected during the backfill process."));
        backfillFetchedBlocks = metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "backfill_blocks_fetched")
                .withDescription("Number of blocks fetched during the backfill process."));
        backfillBlocksBackfilled =
                metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "backfill_blocks_backfilled")
                        .withDescription("Number of blocks backfilled during the backfill process."));
        backfillFetchErrors = metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "backfill_fetch_errors")
                .withDescription("Number of errors encountered during the backfill process."));
        backfillRetries = metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "backfill_retries")
                .withDescription("Number of retries during the backfill process."));
        backfillStatus = metrics.getOrCreate(new LongGauge.Config(METRICS_CATEGORY, "backfill_status")
                .withDescription("Current status of the backfill process (e.g., idle = 0, running = 1, error = 2)."));
        backfillPendingBlocksGauge =
                metrics.getOrCreate(new LongGauge.Config(METRICS_CATEGORY, "backfill_pending_blocks")
                        .withDescription("Current amount of blocks pending to be backfilled."));

        metrics.addUpdater(this::updateMetrics);
    }

    private void updateMetrics() {
        // calculate the gauge of pending blocks metrics
        long pendingBackfillBlocks =
                autonomousLatch.get().getCount() + onDemandLatch.get().getCount();
        backfillPendingBlocksGauge.set(pendingBackfillBlocks);

        final BackfillStatus status;
        if (pendingBackfillBlocks > 0) {
            status = BackfillStatus.RUNNING;
        } else if (pendingBackfillBlocks == 0 && !autonomousError && !onDemandError) {
            status = BackfillStatus.IDLE;
        } else if (autonomousError) {
            status = BackfillStatus.ERROR;
        } else if (onDemandError) { // onDemandError
            status = BackfillStatus.ON_DEMAND_ERROR;
        } else {
            status = BackfillStatus.UNKNOWN;
        }

        backfillStatus.set(status.ordinal());
    }

    private enum BackfillStatus {
        IDLE, // 0
        RUNNING, // 1
        ERROR, // 2
        ON_DEMAND_ERROR, // 3
        UNKNOWN // 4
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(BlockNodeContext context, ServiceBuilder serviceBuilder) {
        this.context = context;
        backfillConfiguration = context.configuration().getConfigData(BackfillConfiguration.class);
        autonomousLatch.set(new CountDownLatch(0));
        onDemandLatch.set(new CountDownLatch(0));

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
                    "Block node sources path file does not exist: {0}, backfill will not run",
                    backfillConfiguration.blockNodeSourcesPath());
            return;
        }

        // Initialize the gRPC client
        try {
            backfillGrpcClientAutonomous = new BackfillGrpcClient(
                    blockNodeSourcesPath,
                    backfillConfiguration.maxRetries(),
                    this.backfillRetries,
                    backfillConfiguration.initialRetryDelay(),
                    backfillConfiguration.grpcOverallTimeout(),
                    backfillConfiguration.enableTLS());

            backfillGrpcClientOnDemand = new BackfillGrpcClient(
                    blockNodeSourcesPath,
                    backfillConfiguration.maxRetries(),
                    this.backfillRetries,
                    backfillConfiguration.initialRetryDelay(),
                    backfillConfiguration.grpcOverallTimeout(),
                    backfillConfiguration.enableTLS());

            LOGGER.log(TRACE, "Initialized gRPC client with sources path: {0}", blockNodeSourcesPath);
        } catch (Exception e) {
            LOGGER.log(INFO, "Failed to initialize gRPC client: {0}", e);
            hasBNSourcesPath = false;
            return;
        }

        // set the flag indicating that we have a valid block node sources path
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
                "Scheduling backfill process to start in {0} milliseconds",
                backfillConfiguration.initialDelay());
        scheduler = Executors.newScheduledThreadPool(
                2,
                Thread.ofVirtual().factory()); // Two threads: one for autonomous backfill, one for on-demand backfill
        //        scheduler = context.threadPoolManager().createVirtualThreadScheduledExecutor(
        //            2,  // Two threads: one for autonomous backfill, one for on-demand backfill
        //            "BackfillPluginRunner",
        //            (t, e) -> LOGGER.log(ERROR, "Uncaught exception in thread: " + t.getName(), e));

        scheduler.scheduleAtFixedRate(
                this::detectGaps,
                backfillConfiguration.initialDelay(),
                backfillConfiguration.scanInterval(),
                TimeUnit.MILLISECONDS);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop() {
        if (scheduler != null) {
            scheduler.shutdownNow();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    LOGGER.log(TRACE, "Scheduler did not terminate in time");
                    // terminate forcefully
                    scheduler.shutdown();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            LOGGER.log(TRACE, "Stopped backfill process");
        }
    }

    private boolean isAutonomousBackfillRunning() {
        return autonomousBackfillEndBlock.get() != -1;
    }

    private boolean isOnDemandBackfillRunning() {
        return onDemandBackfillStartBlock.get() != -1;
    }

    private void greedyBackfillRecentBlocks(long lastAcknowledgedBlockObserved, long lastAcknowledgedBlock) {
        if (!backfillConfiguration.greedy()) {
            return;
        }

        // Skip if lack acknowledged block observed has increased
        if (lastAcknowledgedBlockObserved > 0 && lastAcknowledgedBlock > lastAcknowledgedBlockObserved) {
            LOGGER.log(
                    TRACE,
                    "Last acknowledged block observed has increased from {0} to {1}, skipping greedy backfill",
                    lastAcknowledgedBlockObserved,
                    lastAcknowledgedBlock);
            return;
        }

        try {
            LOGGER.log(TRACE, "Greedy backfilling recent blocks to stay close to network");

            // greedy backfill newer blocks available from peer BN sources
            detectedGaps = new ArrayList<>();
            LongRange detectedRecentGapRange = backfillGrpcClientAutonomous.getNewAvailableRange(lastAcknowledgedBlock);

            // to-do: consider on-demand backfill range and remove from detectedRecentGapRange if overlapping
            if (isOnDemandBackfillRunning()) {
                // if on-demand backfill is running, we need to adjust the detected recent gap range
            }

            if (detectedRecentGapRange.size() > 0 && detectedRecentGapRange.start() >= 0) {
                detectedGaps.add(detectedRecentGapRange);

                // backfill recent gaps first to prioritize staying close to the network
                LOGGER.log(
                        TRACE,
                        "Detected recent gaps, numGaps={0} totalMissingBlocks={1}",
                        detectedGaps.size(),
                        detectedRecentGapRange.size());

                autonomousBackfillEndBlock.set(detectedRecentGapRange.end());
                processDetectedGaps();
                autonomousBackfillEndBlock.set(-1);
            } else {
                LOGGER.log(TRACE, "No recent gaps detected from other block node sources");
            }

            autonomousError = false;
        } catch (Exception e) {
            LOGGER.log(TRACE, "Error during backfill autonomous process: {0}", e);
            autonomousError = true;
            autonomousBackfillEndBlock.set(-1);
        }
    }

    private void detectGaps() {
        // Skip if already running
        if (isAutonomousBackfillRunning()) {
            LOGGER.log(
                    TRACE,
                    "Autonomous backfill is already running up to {0}, skipping autonomous gap detection",
                    autonomousBackfillEndBlock.get());
            return;
        }

        // Skip if OnDemand backfill is running
        if (isOnDemandBackfillRunning() && !backfillConfiguration.greedy()) {
            LOGGER.log(
                    TRACE,
                    "On-Demand backfill is running starting from block {0}, skipping autonomous gap detection",
                    onDemandBackfillStartBlock.get());
            return;
        }

        try {
            LOGGER.log(TRACE, "Detecting gaps in blocks");

            // Calculate total missing blocks
            long pendingBlocks = 0;

            // Check for gaps between ranges
            List<LongRange> blockRanges = context.historicalBlockProvider()
                    .availableBlocks()
                    .streamRanges()
                    .toList();

            // greedy backfill newer blocks available from peer BN sources
            greedyBackfillRecentBlocks(
                    lastAcknowledgedBlockObserved.get(), blockRanges.getLast().end());
            lastAcknowledgedBlockObserved.set(
                    blockRanges.getLast().end()); // update the last observed acknowledged block

            // backfill missing historical blocks from peer BN sources
            detectedGaps = new ArrayList<>();
            long expectedFirstBlock = backfillConfiguration.startBlock();
            long previousRangeEnd = expectedFirstBlock - 1;
            for (LongRange range : blockRanges) {
                if (range.start() > previousRangeEnd + 1) {
                    LongRange gap = new LongRange(previousRangeEnd + 1, range.start() - 1);
                    detectedGaps.add(gap);
                    pendingBlocks += gap.size();
                    LOGGER.log(
                            TRACE,
                            "Detected gap in historical blocks from start={0,number,#} to end={1,number,#}",
                            gap.start(),
                            gap.end());
                }
                previousRangeEnd = range.end();
            }

            // increase only if detectedGaps is not empty
            if (!detectedGaps.isEmpty()) {
                backfillGapsDetected.add(detectedGaps.size());
                LOGGER.log(
                        TRACE,
                        "Detected historical gaps, numGaps={0} totalMissingBlocks={1}",
                        detectedGaps.size(),
                        pendingBlocks);
                processDetectedGaps();
            } else {
                LOGGER.log(TRACE, "No gaps detected in historical blocks");
            }

            autonomousError = false;
        } catch (Exception e) {
            LOGGER.log(TRACE, "Error during backfill autonomous process: {0}", e);
            autonomousError = true;
            autonomousBackfillEndBlock.set(-1);
        }
    }

    private void processDetectedGaps() throws ParseException, InterruptedException {
        // Process each gap
        for (LongRange gap : detectedGaps) {
            LOGGER.log(TRACE, "Fetching blocks from start={0} to end={1}", gap.start(), gap.end());
            backfillGap(gap, BackfillType.AUTONOMOUS);
        }
    }

    /**
     * Backfills a specific gap by fetching blocks from the gRPC client and
     * sends backfilled block notifications for each missing block.
     *
     * @param gap the range of blocks to backfill
     * @throws InterruptedException if the thread is interrupted while waiting
     * @throws ParseException if there is an error parsing the block header
     */
    private void backfillGap(LongRange gap, BackfillType backfillType) throws InterruptedException, ParseException {
        // Reset client status to retry previously unavailable nodes
        BackfillGrpcClient backfillGrpcClient;

        if (backfillType.equals(BackfillType.AUTONOMOUS)) {
            backfillGrpcClient = backfillGrpcClientAutonomous;
        } else {
            backfillGrpcClient = backfillGrpcClientOnDemand;
        }
        backfillGrpcClient.resetStatus();

        // Process gap in smaller chunks
        List<LongRange> chunks = chunkifyGap(gap);

        for (LongRange chunk : chunks) {
            List<BlockUnparsed> batchOfBlocks = backfillGrpcClient.fetchBlocks(chunk);
            // Set up latch for verification and persistence tracking
            // normally will be decremented only by persisted notifications
            // however if it fails verification, it will be decremented as well
            // to avoid deadlocks, since blocks that fail verification are not persisted
            getLatch(backfillType).set(new CountDownLatch(batchOfBlocks.size()));

            if (batchOfBlocks.isEmpty()) {
                LOGGER.log(DEBUG, "No blocks fetched for gap {0}, skipping", chunk);
                continue; // Skip empty batches
            }

            // Process each fetched block
            for (BlockUnparsed blockUnparsed : batchOfBlocks) {
                long blockNumber = extractBlockNumber(blockUnparsed);
                context.blockMessaging()
                        .sendBackfilledBlockNotification(new BackfilledBlockNotification(blockNumber, blockUnparsed));

                LOGGER.log(TRACE, "Backfilling block {0}", blockNumber);
                backfillFetchedBlocks.increment();
            }

            // Wait for verification and persistence to complete
            // Timeout is set using a configuration config as multiplier of the per-block processing timeout
            long timeout = (long) backfillConfiguration.perBlockProcessingTimeout() * batchOfBlocks.size();
            boolean backfillFinished = getLatch(backfillType).get().await(timeout, TimeUnit.MILLISECONDS);

            // Check if the backfill finished successfully
            if (backfillFinished) {
                // just log a victory message for each chunk
                LOGGER.log(TRACE, "Successfully backfilled gap {0}", chunk);
            } else {
                LOGGER.log(TRACE, "Backfill for gap {0} did not finish in time", chunk);
                backfillFetchErrors.increment();
                // If it didn't finish, we will retry it later but move on to next chunk
            }

            // Cooldown between batches
            Thread.sleep(backfillConfiguration.delayBetweenBatches());
        }

        LOGGER.log(
                TRACE,
                "Completed backfilling task (completion only) of type {0} gap from {1} to {2} ",
                backfillType,
                gap.start(),
                gap.end());
        if (backfillType.equals(BackfillType.ON_DEMAND)) {
            onDemandBackfillStartBlock.set(-1); // Reset on-demand start block after backfill
        }
    }

    /**
     * Extracts the block number from the BlockUnparsed object.
     *
     * @param blockUnparsed the BlockUnparsed object containing the block header
     * @return the block number
     * @throws ParseException if there is an error parsing the block header
     */
    private long extractBlockNumber(BlockUnparsed blockUnparsed) throws ParseException {
        return BlockHeader.PROTOBUF
                .parse(blockUnparsed.blockItems().getFirst().blockHeaderOrThrow())
                .number();
    }

    /**
     * Chunks a gap into smaller ranges based on the configured fetch batch size.
     * @param gap the LongRange representing the gap to be chunked
     * @return a list of LongRange chunks
     */
    private List<LongRange> chunkifyGap(LongRange gap) {
        long start = gap.start();
        long end = gap.end();
        long batchSize = backfillConfiguration.fetchBatchSize();
        List<LongRange> chunks = new ArrayList<>();

        while (start <= end) {
            long chunkEnd = Math.min(start + batchSize - 1, end);
            chunks.add(new LongRange(start, chunkEnd));
            start += batchSize;
        }
        return chunks;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handlePersisted(PersistedNotification notification) {
        if (notification.blockSource() == BlockSource.BACKFILL) {
            BackfillType backfillType = getBackfillTypeForBlock(notification.blockNumber());

            // Add more detailed logging for persistence notifications
            LOGGER.log(
                    TRACE,
                    "Received backfillType={0} persisted notification for block={1,number,#}",
                    backfillType,
                    notification.blockNumber());

            backfillBlocksBackfilled.increment();
            // decrement the latch for the backfill type
            getLatch(backfillType).get().countDown();
        } else {
            LOGGER.log(TRACE, "Received non-backfill persisted notification: {0}", notification);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleVerification(VerificationNotification notification) {
        if (notification.source() == BlockSource.BACKFILL) {
            LOGGER.log(TRACE, "Received verification notification for block {0,number,#}", notification.blockNumber());
            if (!notification.success()) {
                LOGGER.log(INFO, "Block verification failed, block={0,number,#}", notification.blockNumber());
                backfillFetchErrors.increment();
                // lastly, count down the latch to signal that this block has been processed
                BackfillType backfillType = getBackfillTypeForBlock(notification.blockNumber());
                getLatch(backfillType).get().countDown();
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

        // Skip if another On-Demand backfill is already running
        if (isOnDemandBackfillRunning()) {
            LOGGER.log(TRACE, "On-Demand backfill is already running, skipping new on-demand backfill");
            return;
        }

        // we should create  new Gap and a new task to backfill it
        long lastPersistedBlock =
                context.historicalBlockProvider().availableBlocks().max();
        long newestBlockKnown = notification.blockNumber();
        LongRange gap = new LongRange(lastPersistedBlock + 1, newestBlockKnown);
        LOGGER.log(
                TRACE,
                "Detected new block known to network: {0,number,#}, starting backfill task for gap: {1}",
                newestBlockKnown,
                gap);

        lastAcknowledgedBlockObserved.set(lastPersistedBlock); // update the last observed acknowledged block

        // if the gap is not empty, we can backfill it
        if (gap.size() > 0) {
            try {
                // Skip if greedy autonomous backfill is greedy is more aggressive and will like cover more blocks
                if (isAutonomousBackfillRunning() && backfillConfiguration.greedy()) {
                    LOGGER.log(TRACE, "Autonomous backfill is running, skipping on-demand gap detection");
                    return;
                }

                // Set the start block for on-demand backfill BEFORE scheduling the task
                onDemandBackfillStartBlock.set(gap.start());
                onDemandError = false;

                // use the scheduler to run the backfill in its own thread (only call backfillGap once)
                scheduler.execute(() -> {
                    try {
                        LOGGER.log(TRACE, "Starting on-demand backfill for gap: {0}", gap);
                        backfillGap(gap, BackfillType.ON_DEMAND);
                        lastAcknowledgedBlockObserved.set(context.historicalBlockProvider()
                                .availableBlocks()
                                .max()); // update the last observed acknowledged block
                    } catch (ParseException | InterruptedException | RuntimeException e) {
                        LOGGER.log(TRACE, "Error backfilling gap {0}: {1}", gap, e);
                        backfillFetchErrors.add(1);
                        onDemandError = true;
                        onDemandBackfillStartBlock.set(-1); // Reset on error to allow new backfills
                    }
                });

            } catch (RuntimeException e) {
                LOGGER.log(TRACE, "Error scheduling backfill for gap {0}: {1}", gap, e);
                backfillFetchErrors.add(1);
                onDemandError = true;
                onDemandBackfillStartBlock.set(-1); // Reset on error to allow new backfills
            }
        } else {
            LOGGER.log(TRACE, "No gap to backfill for newest block known: {0}", newestBlockKnown);
        }
    }

    // to-do: remove method, instead update notification with backfill types instead of just backfill
    private BackfillType getBackfillTypeForBlock(long blockNumber) {
        // Determine if the block is old (Autonomous) or new (On-Demand)
        long onDemandStartBlock = onDemandBackfillStartBlock.get();
        if (onDemandStartBlock != -1 && blockNumber >= onDemandStartBlock) {
            return BackfillType.ON_DEMAND;
        }
        return BackfillType.AUTONOMOUS;
    }

    private enum BackfillType {
        AUTONOMOUS,
        ON_DEMAND
    }
}
