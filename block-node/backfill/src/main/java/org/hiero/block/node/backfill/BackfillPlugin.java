// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.TRACE;
import static java.lang.System.Logger.Level.WARNING;
import static java.util.concurrent.locks.LockSupport.parkNanos;

import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import com.swirlds.metrics.api.Counter;
import com.swirlds.metrics.api.LongGauge;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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

    private static final String METRICS_CATEGORY = "backfill";

    /** The logger for this class. */
    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    // Plugin infrastructure
    private BlockNodeContext context;
    private BackfillConfiguration backfillConfiguration;
    private boolean hasBNSourcesPath = false;
    private ScheduledExecutorService scheduler;

    // Backfill state
    private List<LongRange> detectedGaps = new ArrayList<>();
    private BackfillGrpcClient backfillGrpcClient;
    private final Map<BackfillType, CountDownLatch> backfillLatches = new HashMap<>();
    private boolean autonomousError = false;
    private boolean onDemandError = false;
    private long onDemandBackfillStartBlock = -1;

    // Metrics
    private Counter backfillGapsDetected;
    private Counter backfillFetchedBlocks;
    private Counter backfillBlocksBackfilled;
    private Counter backfillFetchErrors;
    private Counter backfillRetries;
    private LongGauge backfillStatus; // 0 = idle, 1 = running, 2 = autonomous-error, 3 = on-demand-error
    private LongGauge backfillPendingBlocksGauge;

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
                backfillLatches.get(BackfillType.AUTONOMOUS).getCount()
                        + backfillLatches.get(BackfillType.ON_DEMAND).getCount();
        backfillPendingBlocksGauge.set(pendingBackfillBlocks);

        // Update the backfill status based on the current state
        if (pendingBackfillBlocks > 0) {
            backfillStatus.set(1); // 1 = running
        } else if (pendingBackfillBlocks == 0 && !autonomousError && !onDemandError) {
            backfillStatus.set(0); // 0 = idle
        } else if (autonomousError) {
            backfillStatus.set(2); // 2 = error
        } else if (onDemandError) {
            backfillStatus.set(3); // 3 = on-demand error
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(BlockNodeContext context, ServiceBuilder serviceBuilder) {
        this.context = context;
        backfillConfiguration = context.configuration().getConfigData(BackfillConfiguration.class);
        backfillLatches.put(BackfillType.AUTONOMOUS, new CountDownLatch(0));
        backfillLatches.put(BackfillType.ON_DEMAND, new CountDownLatch(0));

        // Initialize metrics
        initMetrics();

        // Validate block node sources configuration
        final String sourcesPath = backfillConfiguration.blockNodeSourcesPath();
        if (sourcesPath == null || sourcesPath.isBlank()) {
            LOGGER.log(INFO, "No block node sources path configured, backfill will not run");
            return;
        }

        Path blockNodeSourcesPath = Path.of(backfillConfiguration.blockNodeSourcesPath());
        if (!Files.isRegularFile(blockNodeSourcesPath)) {
            LOGGER.log(
                    INFO,
                    "Block node sources path file does not exist: {0}, backfill will not run",
                    backfillConfiguration.blockNodeSourcesPath());
            return;
        }

        // Initialize the gRPC client
        try {
            backfillGrpcClient = new BackfillGrpcClient(
                    blockNodeSourcesPath,
                    backfillConfiguration.maxRetries(),
                    this.backfillRetries,
                    backfillConfiguration.initialRetryDelayMs());
            LOGGER.log(INFO, "Initialized gRPC client with sources path: {0}", blockNodeSourcesPath);
        } catch (Exception e) {
            LOGGER.log(WARNING, "Failed to initialize gRPC client: {0}", e.getMessage());
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
                INFO,
                "Scheduling backfill process to start in {0} milliseconds",
                backfillConfiguration.initialDelayMs());
        scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(
                this::detectGaps,
                backfillConfiguration.initialDelayMs(),
                backfillConfiguration.scanIntervalMs(),
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
                    LOGGER.log(INFO, "Scheduler did not terminate in time");
                    // terminate forcefully
                    scheduler.shutdown();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            LOGGER.log(INFO, "Stopped backfill process");
        }
    }

    private boolean isAutonomousBackfillRunning() {
        long pendingAutonomousBlocks =
                backfillLatches.get(BackfillType.AUTONOMOUS).getCount();
        return pendingAutonomousBlocks > 0;
    }

    private boolean isOnDemandBackfillRunning() {
        return onDemandBackfillStartBlock != -1;
    }

    private void detectGaps() {
        // Skip if already running
        if (isAutonomousBackfillRunning()) {
            LOGGER.log(INFO, "Gap detection already in progress, skipping this execution");
            return;
        }

        // Skip if OnDemand backfill is running
        if (isOnDemandBackfillRunning()) {
            LOGGER.log(INFO, "On-Demand backfill is running, skipping autonomous gap detection");
            return;
        }

        try {
            LOGGER.log(INFO, "Detecting gaps in historical blocks");
            detectedGaps = new ArrayList<>();

            // Get the configured first block available
            long expectedFirstBlock = backfillConfiguration.startBlock();
            long previousRangeEnd = expectedFirstBlock - 1;

            // Check for gaps between ranges
            List<LongRange> blockRanges = context.historicalBlockProvider()
                    .availableBlocks()
                    .streamRanges()
                    .toList();

            // Calculate total missing blocks
            long pendingBlocks = 0;

            for (LongRange range : blockRanges) {
                if (range.start() > previousRangeEnd + 1) {
                    LongRange gap = new LongRange(previousRangeEnd + 1, range.start() - 1);
                    detectedGaps.add(gap);
                    pendingBlocks += gap.size();
                    LOGGER.log(INFO, "Detected gap in historical blocks from {0} to {1}", gap.start(), gap.end());
                }
                previousRangeEnd = range.end();
            }

            // increase only if detectedGaps is not empty
            if (!detectedGaps.isEmpty()) backfillGapsDetected.add(detectedGaps.size());

            LOGGER.log(INFO, "Detected {0} gaps with {1} total missing blocks", detectedGaps.size(), pendingBlocks);

            // Process detected gaps
            if (!detectedGaps.isEmpty()) {
                processDetectedGaps();
            } else {
                LOGGER.log(INFO, "No gaps detected in historical blocks");
            }
            autonomousError = false;
        } catch (Exception e) {
            LOGGER.log(INFO, "Error during backfill autonomous process: {0}", e.getMessage());
            autonomousError = true;
        }
    }

    private void processDetectedGaps() throws ParseException, InterruptedException {
        // Process each gap
        for (LongRange gap : detectedGaps) {
            LOGGER.log(INFO, "Fetching blocks from {0} to {1}", gap.start(), gap.end());
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
        backfillGrpcClient.resetStatus();

        // Process gap in smaller chunks
        List<LongRange> chunks = chunkifyGap(gap);

        for (LongRange chunk : chunks) {
            List<BlockUnparsed> batchOfBlocks = backfillGrpcClient.fetchBlocks(chunk);
            // Set up latch for verification and persistence tracking
            // normally will be decremented only by persisted notifications
            // however if it fails verification, it will be decremented as well
            // to avoid deadlocks, since blocks that fail verification are not persisted
            backfillLatches.put(backfillType, new CountDownLatch(batchOfBlocks.size()));

            // Process each fetched block
            for (BlockUnparsed blockUnparsed : batchOfBlocks) {
                long blockNumber = extractBlockNumber(blockUnparsed);
                context.blockMessaging()
                        .sendBackfilledBlockNotification(new BackfilledBlockNotification(blockNumber, blockUnparsed));

                // Give messaging system some time to process
                parkNanos(500_000); // 500 microseconds

                LOGGER.log(INFO, "Backfilling block {0}", blockNumber);
                backfillFetchedBlocks.increment();
            }

            // Wait for verification and persistence to complete
            // give about 1 second for each block to be processed (should be much faster)
            boolean backfillFinished = backfillLatches.get(backfillType).await(batchOfBlocks.size(), TimeUnit.SECONDS);

            // Check if the backfill finished successfully
            if (!backfillFinished) {
                LOGGER.log(INFO, "Backfill for gap {0} did not finish in time", chunk);
                backfillFetchErrors.increment();
                // If it didn't finish, we will retry it later but move on to next chunk
            } else {
                // just log a victory message for each chunk
                LOGGER.log(TRACE, "Successfully backfilled gap {0}", chunk);
            }

            // Cooldown between batches
            Thread.sleep(backfillConfiguration.delayBetweenBatchesMs());
        }

        LOGGER.log(INFO, "Completed backfilling of type {0} gap from {1} to {2}", backfillType, gap.start(), gap.end());
        if (backfillType.equals(BackfillType.ON_DEMAND)) {
            onDemandBackfillStartBlock = -1; // Reset on-demand start block after backfill
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
            BackfillType backfillType = getBackfillTypeForBlock(notification.endBlockNumber());

            // Add more detailed logging for persistence notifications
            LOGGER.log(
                    INFO,
                    "Received {0} persisted notification for block {1}",
                    backfillType,
                    notification.endBlockNumber());

            backfillBlocksBackfilled.increment();
            // decrement the latch for the backfill type
            backfillLatches.get(backfillType).countDown();
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
            LOGGER.log(TRACE, "Received verification notification for block {0}", notification.blockNumber());
            if (!notification.success()) {
                LOGGER.log(WARNING, "Block {0} verification failed", notification.blockNumber());
                backfillFetchErrors.increment();
                // lastly, count down the latch to signal that this block has been processed
                BackfillType backfillType = getBackfillTypeForBlock(notification.blockNumber());
                backfillLatches.get(backfillType).countDown();
                // If a block verification fails, we will backfill it again later on the next gap detection run.
            }
        }
    }

    @Override
    public void handleNewestBlockKnownToNetwork(NewestBlockKnownToNetworkNotification notification) {
        // we should create  new Gap and a new task to backfill it
        long lastPersistedBlock =
                context.historicalBlockProvider().availableBlocks().max();
        long newestBlockKnown = notification.blockNumber();
        LongRange gap = new LongRange(lastPersistedBlock + 1, newestBlockKnown);
        LOGGER.log(
                INFO,
                "Detected new block known to network: {0}, starting backfill task for gap: {1}",
                newestBlockKnown,
                gap);

        if (!hasBNSourcesPath) {
            LOGGER.log(INFO, "No block node sources path configured, skipping on-demand backfill");
            return;
        }

        // Skip if Autonomous backfill is running
        if (isAutonomousBackfillRunning()) {
            LOGGER.log(INFO, "Autonomous backfill is running, skipping on-demand backfill");
            return;
        }

        // Skip if another On-Demand backfill is already running
        if (isOnDemandBackfillRunning()) {
            LOGGER.log(INFO, "On-Demand backfill is already running, skipping new on-demand backfill");
            return;
        }

        // if the gap is not empty, we can backfill it
        if (gap.size() > 0) {
            try {
                // Set the start block for on-demand backfill BEFORE scheduling the task
                onDemandBackfillStartBlock = gap.start();

                // use the scheduler to run the backfill in its own thread (only call backfillGap once)
                scheduler.execute(() -> {
                    try {
                        LOGGER.log(INFO, "Starting on-demand backfill for gap: {0}", gap);
                        backfillGap(gap, BackfillType.ON_DEMAND);
                    } catch (Exception e) {
                        LOGGER.log(INFO, "Error backfilling gap {0}: {1}", gap, e.getMessage());
                        backfillFetchErrors.add(1);
                        onDemandError = true;
                        onDemandBackfillStartBlock = -1; // Reset on error to allow new backfills
                    }
                });

                onDemandError = false;
            } catch (Exception e) {
                LOGGER.log(INFO, "Error scheduling backfill for gap {0}: {1}", gap, e.getMessage());
                backfillFetchErrors.add(1);
                onDemandError = true;
                onDemandBackfillStartBlock = -1; // Reset on error to allow new backfills
            }
        } else {
            LOGGER.log(INFO, "No gap to backfill for newest block known: {0}", newestBlockKnown);
        }
    }

    private BackfillType getBackfillTypeForBlock(long blockNumber) {
        // Determine if the block is old (Autonomous) or new (On-Demand)
        if (onDemandBackfillStartBlock != -1 && blockNumber >= onDemandBackfillStartBlock) {
            return BackfillType.ON_DEMAND;
        }
        return BackfillType.AUTONOMOUS;
    }

    private enum BackfillType {
        AUTONOMOUS,
        ON_DEMAND
    }
}
