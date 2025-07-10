// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;

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
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.BackfilledBlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
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
    private AtomicLong backfillPendingBlocks = new AtomicLong(0);
    private volatile boolean isDetectingGaps = false;
    private CountDownLatch pendingVerificationAndPersistenceLatch;

    // Metrics
    private Counter backfillGapsDetected;
    private Counter backfillFetchedBlocks;
    private Counter backfillBlocksBackfilled;
    private Counter backfillFetchErrors;
    private Counter backfillRetries;
    private LongGauge backfillStatus;
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
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(BlockNodeContext context, ServiceBuilder serviceBuilder) {
        this.context = context;
        backfillConfiguration = context.configuration().getConfigData(BackfillConfiguration.class);

        // Initialize metrics
        initMetrics();
        backfillStatus.set(0); // 0 = idle

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

    private void detectGaps() {
        // Skip if already running
        if (isDetectingGaps) {
            LOGGER.log(INFO, "Gap detection already in progress, skipping this execution");
            return;
        }

        // Set running state
        isDetectingGaps = true;
        backfillStatus.set(1); // 1 = running

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

            // Update metrics
            backfillPendingBlocks.set(pendingBlocks);
            backfillPendingBlocksGauge.set(pendingBlocks);
            backfillGapsDetected.add(detectedGaps.size());

            LOGGER.log(INFO, "Detected {0} gaps with {1} total missing blocks", detectedGaps.size(), pendingBlocks);

            // Process detected gaps
            if (!detectedGaps.isEmpty()) {
                processDetectedGaps();
            } else {
                LOGGER.log(INFO, "No gaps detected in historical blocks");
            }
            backfillStatus.set(0); // 0 = idle
        } catch (Exception e) {
            LOGGER.log(INFO, "Error during backfill autonomous process: {0}", e.getMessage());
            backfillStatus.set(2); // 2 = error
        } finally {
            isDetectingGaps = false;
        }
    }

    private void processDetectedGaps() {
        // Process each gap
        for (LongRange gap : detectedGaps) {
            LOGGER.log(INFO, "Fetching blocks from {0} to {1}", gap.start(), gap.end());
            try {
                backfillGap(gap);
            } catch (Exception e) {
                LOGGER.log(INFO, "Error fetching blocks for gap {0}: {1}", gap, e.getMessage());
                backfillFetchErrors.add(1);
            }
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
    private void backfillGap(LongRange gap) throws InterruptedException, ParseException {
        // Reset client status to retry previously unavailable nodes
        backfillGrpcClient.resetStatus();

        // Process gap in smaller chunks
        List<LongRange> chunks = chunkifyGap(gap);

        for (LongRange chunk : chunks) {
            List<BlockUnparsed> batchOfBlocks;

            try {
                batchOfBlocks = backfillGrpcClient.fetchBlocks(chunk);
            } catch (Exception e) {
                LOGGER.log(INFO, "Error fetching blocks for chunk {0}: {1}", chunk, e.getMessage());
                backfillFetchErrors.add(1);
                continue;
            }

            // Set up latch for verification and persistence tracking
            // normally will be decremented only by persisted notifications
            // however if it fails verification, it will be decremented as well
            // to avoid deadlocks, since blocks that fail verification are not persisted
            pendingVerificationAndPersistenceLatch = new CountDownLatch(batchOfBlocks.size());

            // Process each fetched block
            for (BlockUnparsed blockUnparsed : batchOfBlocks) {
                long blockNumber = extractBlockNumber(blockUnparsed);
                context.blockMessaging()
                        .sendBackfilledBlockNotification(new BackfilledBlockNotification(blockNumber, blockUnparsed));

                LOGGER.log(INFO, "Backfilling block {0}", blockNumber);
                backfillFetchedBlocks.increment();
            }

            // Wait for verification and persistence to complete
            pendingVerificationAndPersistenceLatch.await();

            // Cooldown between batches
            Thread.sleep(backfillConfiguration.delayBetweenBatchesMs());
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
            final long pendingBlocks = backfillPendingBlocks.decrementAndGet();
            backfillPendingBlocksGauge.set(pendingBlocks);
            backfillBlocksBackfilled.increment();
            // lastly, count down the latch to signal that this block has been processed
            pendingVerificationAndPersistenceLatch.countDown();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleVerification(VerificationNotification notification) {
        if (notification.source() == BlockSource.BACKFILL) {
            if (!notification.success()) {
                LOGGER.log(WARNING, "Block {0} verification failed", notification.blockNumber());
                backfillFetchErrors.increment();
                // lastly, count down the latch to signal that this block has been processed
                pendingVerificationAndPersistenceLatch.countDown();
            }
        }
    }
}
