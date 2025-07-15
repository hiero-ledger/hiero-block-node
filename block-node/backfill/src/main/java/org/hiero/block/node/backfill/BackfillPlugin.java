// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import static java.lang.System.Logger.Level.INFO;

import com.hedera.hapi.block.stream.Block;
import com.swirlds.metrics.api.Counter;
import com.swirlds.metrics.api.LongGauge;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;

public class BackfillPlugin implements BlockNodePlugin {

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
    private Counter backfillBlocksFetched;
    /** Metric for Number of blocks backfilled during the backfill process. */
    private Counter backfillBlocksBackfilled;
    /** Metric for Number of errors encountered during the backfill process. */
    private Counter backfillFetchErrors;
    /** Metric for Number of retries during the backfill process. */
    private Counter backfillRetries;
    /** Current status of the backfill process (e.g., idle = 0, running = 1, error = 2). */
    private LongGauge backfillStatus;
    /** Current amount of blocks pending to be backfilled. */
    private LongGauge backfillPendingBlocks;

    private boolean hasBNSourcesPath = false;

    private List<BlockGap> detectedGaps;

    private BackfillGrpcClient backfillGrpcClient;

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
        backfillBlocksFetched = metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "backfill_blocks_fetched")
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
        backfillPendingBlocks = metrics.getOrCreate(new LongGauge.Config(METRICS_CATEGORY, "backfill_pending_blocks")
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
            backfillGrpcClient = new BackfillGrpcClient(blockNodeSourcesPath, backfillConfiguration.maxRetries());
            LOGGER.log(INFO, "BackfillPlugin: Initialized gRPC client with sources path: {0}", blockNodeSourcesPath);
        } catch (Exception e) {
            LOGGER.log(INFO, "BackfillPlugin: Failed to initialize gRPC client: {0}", e.getMessage());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start() {
        if (hasBNSourcesPath) {
            LOGGER.log(INFO, "BackfillPlugin: Starting backfill process");
            detectGaps();
        }
    }

    private void detectGaps() {
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

        if (!detectedGaps.isEmpty()) {
            // Gaps detected, proceed to backfill
            backfillGapsDetected.add(detectedGaps.size());

            long pendingBlocks = detectedGaps.stream()
                    .mapToLong(gap -> gap.endBlockNumber() - gap.startBlockNumber() + 1)
                    .sum();
            backfillPendingBlocks.set(pendingBlocks);
            LOGGER.log(
                    INFO,
                    "BackfillPlugin: Detected {0} gaps with {1} total missing blocks",
                    detectedGaps.size(),
                    pendingBlocks);

            // Start the backfill process

            // fetch blocks in batches.
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
    }

    private void backfillGap(BlockGap gap) {
        // Split the gap into smaller chunks based on the batch size
        List<BlockGap> chunks = chunkifyGap(gap);
        // for each chunk, fetch the blocks and backfill them
        for (BlockGap chunk : chunks) {
            try {
                List<Block> batchOfBlocks = backfillGrpcClient.fetchBlocks(chunk);
                for (Block block : batchOfBlocks) {
                    // Process each block, e.g., backfill it into the historical block provider
                    LOGGER.log(
                            INFO,
                            "BackfillPlugin: Backfilling block {0} from chunk {1}",
                            block.items().getFirst().blockHeader().number(),
                            chunk);
                }

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
}
