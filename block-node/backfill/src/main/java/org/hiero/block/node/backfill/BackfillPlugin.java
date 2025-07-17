// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import static java.lang.System.Logger.Level.INFO;

import com.hedera.hapi.block.stream.output.BlockHeader;
import com.swirlds.metrics.api.Counter;
import com.swirlds.metrics.api.LongGauge;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.BackfilledBlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;

public class BackfillPlugin implements BlockNodePlugin, BlockNotificationHandler {

    /** The logger for this class. */
    private final System.Logger LOGGER = System.getLogger(getClass().getName());
    /** The block node context, for access to core facilities. */
    private BlockNodeContext context;
    /** The configuration for verification */
    @SuppressWarnings("FieldCanBeLocal")
    private BackfillConfiguration backfillConfiguration;

    // Metrics
    /** Metric for Number of gaps detected during the backfill process.	. */
    private Counter backfillGapsDetected;
    /** Metric for Number of blocks fetched during the backfill process. */
    private Counter backfillFetchedBlocks;
    /** Metric for Number of blocks backfilled during the backfill process. */
    private Counter backfillBlocksBackfilled;
    /** Metric for Number of errors encountered during the backfill process. */
    private Counter backfillFetchErrors;
    /** Metric for Number of retries during the backfill process. */
    private Counter backfillRetries;
    /** Current status of the backfill process (e.g., idle = 0, running = 1, error = 2). */
    private LongGauge backfillStatus;
    /** Current amount of blocks pending to be backfilled. */
    private LongGauge backfillPendingBlocksGauge;
    /** boolean indicating if the block node sources path is set. */
    private boolean hasBNSourcesPath = false;
    /** list of detected gaps in the historical blocks. */
    private List<BlockGap> detectedGaps;
    /** gRPC client for fetching blocks from block node sources. */
    private BackfillGrpcClient backfillGrpcClient;
    /** Pending block counter for backfill queue. */
    private AtomicLong backfillPendingBlocks = new AtomicLong(0);
    /** Executor service for scheduling the gap detection task. */
    private ScheduledExecutorService scheduler;
    /** Flag to indicate if the gap detection task is currently running. */
    private volatile boolean isDetectingGaps = false;

    private CountDownLatch pendingVerificationAndPersistenceLatch;

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
     * @param context the block node context containing the metrics registry
     */
    private void initMetrics(BlockNodeContext context) {
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
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(BlockNodeContext context, ServiceBuilder serviceBuilder) {
        detectedGaps = new java.util.ArrayList<>();
        this.context = context;
        backfillConfiguration = context.configuration().getConfigData(BackfillConfiguration.class);
        // initialize metrics
        initMetrics(context);
        // Set initial status to idle
        backfillStatus.set(0); // 0 = idle
        // if BN sources path is not set, log and stop the plugin.
        if (backfillConfiguration.blockNodeSourcesPath().isEmpty()) {
            LOGGER.log(INFO, "BackfillPlugin: No block node sources path configured, backfill will not run");
            return;
        }
        // if path is set but file does not exist, log and stop the plugin.
        Path blockNodeSourcesPath = Path.of(backfillConfiguration.blockNodeSourcesPath());
        if (!Files.isRegularFile(blockNodeSourcesPath)) {
            LOGGER.log(
                    INFO,
                    "BackfillPlugin: Block node sources path does not exist: {0}, backfill will not run",
                    backfillConfiguration.blockNodeSourcesPath());
            return;
        }

        hasBNSourcesPath = true;
        // Initialize the gRPC client with the block node sources path
        try {
            backfillGrpcClient = new BackfillGrpcClient(
                    blockNodeSourcesPath, backfillConfiguration.maxRetries(), this.backfillRetries);
            LOGGER.log(INFO, "BackfillPlugin: Initialized gRPC client with sources path: {0}", blockNodeSourcesPath);
        } catch (Exception e) {
            LOGGER.log(INFO, "BackfillPlugin: Failed to initialize gRPC client: {0}", e.getMessage());
        }

        // register the service
        context.blockMessaging().registerBlockNotificationHandler(this, false, "BackfillPlugin");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start() {
        if (hasBNSourcesPath) {
            LOGGER.log(INFO, "BackfillPlugin: Starting backfill process");
            // Schedule the gap detection task to run every X minutes
            // giving 1-minute initial delay to allow the system to complete startup
            int interval = backfillConfiguration.scanIntervalMins() * 60;
            scheduler = Executors.newSingleThreadScheduledExecutor();
            scheduler.scheduleAtFixedRate(this::detectGaps, 10, interval, TimeUnit.SECONDS);
        }
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
                    LOGGER.log(INFO, "BackfillPlugin: Scheduler did not terminate in time.");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            LOGGER.log(INFO, "BackfillPlugin: Stopped backfill process");
        }
    }

    private void detectGaps() {
        // If the task is already running, skip this execution
        if (isDetectingGaps) {
            LOGGER.log(INFO, "BackfillPlugin: Gap detection already in progress, skipping this execution");
            return;
        }

        // Set the flag to indicate that the task is running
        isDetectingGaps = true;
        backfillStatus.set(1); // 1 = running

        try {
            LOGGER.log(INFO, "BackfillPlugin: Detecting gaps in historical blocks");
            detectedGaps = new java.util.ArrayList<>();

            // Get the configured first block available
            long expectedFirstBlock = backfillConfiguration.firstBlockAvailable();
            long previousRangeEnd = expectedFirstBlock - 1; // Initialize to one before the expected start

            // Get all available block ranges
            var blockRanges = context.historicalBlockProvider()
                    .availableBlocks()
                    .streamRanges()
                    .toList();

            // Check for gaps between ranges (including initial gap)
            for (var range : blockRanges) {
                // If there's a gap between the previous range end and current range start
                if (range.start() > previousRangeEnd + 1) {
                    BlockGap gap = new BlockGap(previousRangeEnd + 1, range.start() - 1);
                    detectedGaps.add(gap);
                    LOGGER.log(
                            INFO,
                            "BackfillPlugin: Detected a gap in the historical blocks from {0} to {1}",
                            gap.startBlockNumber(),
                            gap.endBlockNumber());
                }
                previousRangeEnd = range.end();
            }

            // if detectedGap is not empty, start the backfill process
            if (!detectedGaps.isEmpty()) {
                // update metrics
                backfillGapsDetected.add(detectedGaps.size());
                // Calculate the total number of pending blocks across all detected gaps
                long pendingBlocks = detectedGaps.stream()
                        .mapToLong(gap -> gap.endBlockNumber() - gap.startBlockNumber() + 1)
                        .sum();
                // Update the pending blocks counter and gauge
                backfillPendingBlocks.set(pendingBlocks);
                backfillPendingBlocksGauge.set(pendingBlocks);

                LOGGER.log(
                        INFO,
                        "BackfillPlugin: Detected {0} gaps with {1} total missing blocks",
                        detectedGaps.size(),
                        pendingBlocks);

                // Start the backfill process for each detected gap
                for (BlockGap gap : detectedGaps) {
                    LOGGER.log(
                            INFO,
                            "BackfillPlugin: Fetching blocks from {0} to {1}",
                            gap.startBlockNumber(),
                            gap.endBlockNumber());
                    try {
                        backfillGap(gap);
                    } catch (Exception e) {
                        LOGGER.log(INFO, "BackfillPlugin: Error fetching blocks for gap {0}: {1}", gap, e.getMessage());
                        backfillFetchErrors.add(1);
                    }
                }

            } else {
                LOGGER.log(INFO, "BackfillPlugin: No gaps detected in historical blocks");
            }
        } finally {
            // Reset the flag to indicate that the task is no longer running
            isDetectingGaps = false;
            backfillStatus.set(0); // 0 = idle
        }
    }

    private void backfillGap(BlockGap gap) {
        // for each gap task, reset the status of bn clients to unknown
        // this allows to retry fetching blocks from the nodes previously marked as unavailable
        backfillGrpcClient.resetStatus();
        // Split the gap into smaller chunks based on the batch size
        List<BlockGap> chunks = chunkifyGap(gap);
        // for each chunk, fetch the blocks and backfill them
        for (BlockGap chunk : chunks) {
            try {
                List<BlockUnparsed> batchOfBlocks = backfillGrpcClient.fetchBlocks(chunk);
                // create a latch to wait for the blocks to be persisted
                pendingVerificationAndPersistenceLatch = new CountDownLatch(batchOfBlocks.size());
                for (BlockUnparsed blockUnparsed : batchOfBlocks) {
                    // Process each block, e.g., backfill it into the historical block provider
                    long blockNumber = BlockHeader.PROTOBUF
                            .parse(blockUnparsed.blockItems().getFirst().blockHeaderOrThrow())
                            .number();
                    context.blockMessaging()
                            .sendBackfilledBlockNotification(
                                    new BackfilledBlockNotification(blockNumber, blockUnparsed));

                    LOGGER.log(INFO, "BackfillPlugin: Backfilling block {0} from chunk {1}", blockNumber, chunk);
                    // update metrics
                    backfillFetchedBlocks.increment();
                }
                // wait until the blocks are persisted before continuing to the next chunk
                pendingVerificationAndPersistenceLatch.await();
                // wait cooldown period in between chunks to avoid overwhelming the system
                Thread.sleep(backfillConfiguration.coolDownTimeBetweenBatchesMs());

            } catch (Exception e) {
                LOGGER.log(INFO, "BackfillPlugin: Error backfilling chunk {0}: {1}", chunk, e.getMessage());
                backfillFetchErrors.add(1);
            }
        }
    }

    private List<BlockGap> chunkifyGap(BlockGap gap) {
        // Split the gap into smaller chunks based on the batch size
        long start = gap.startBlockNumber();
        long end = gap.endBlockNumber();
        long batchSize = backfillConfiguration.fetchBatchSize();
        List<BlockGap> chunks = new java.util.ArrayList<>();

        while (start <= end) {
            long chunkEnd = Math.min(start + batchSize - 1, end);
            chunks.add(new BlockGap(start, chunkEnd));
            start += batchSize;
        }
        return chunks;
    }

    @Override
    public void handlePersisted(PersistedNotification notification) {
        if (notification.blockSource() == BlockSource.BACKFILL) {
            pendingVerificationAndPersistenceLatch.countDown();
            // update metrics
            final long pendingBlocks = backfillPendingBlocks.decrementAndGet();
            backfillPendingBlocksGauge.set(pendingBlocks);
            backfillBlocksBackfilled.increment();
        }
    }

    @Override
    public void handleVerification(VerificationNotification notification) {
        // This method is called when a block is verified
        // We can use it to update the metrics or perform any other actions needed
        if (!notification.success() && notification.source() == BlockSource.BACKFILL) {
            LOGGER.log(INFO, "BackfillPlugin: Block {0} verification failed", notification.blockNumber());
            // if verification failed it will retry the block later at the next backfill run
            // decrement the pending blocks counter
            pendingVerificationAndPersistenceLatch.countDown();
            // update metrics
            backfillFetchErrors.increment();
        }
    }
}
