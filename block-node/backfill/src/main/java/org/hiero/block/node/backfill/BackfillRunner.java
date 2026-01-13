// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.TRACE;

import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.backfill.client.BackfillSourceConfig;
import org.hiero.block.node.spi.blockmessaging.BackfilledBlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.historicalblocks.LongRange;

/**
 * Executes backfill tasks by planning availability, fetching blocks from peers,
 * and dispatching them to the messaging facility.
 */
final class BackfillRunner {
    private final BackfillFetcher fetcher;
    private final BackfillConfiguration config;
    private final BlockMessagingFacility messaging;
    private final System.Logger logger;

    @NonNull
    private final BackfillPlugin.MetricsHolder metricsHolder;

    private final AtomicLong pendingBackfillBlocks;
    private final BackfillPersistenceAwaiter persistenceAwaiter;

    /**
     * Creates a new runner with the specified dependencies.
     *
     * @param fetcher the fetcher for retrieving blocks from peer nodes
     * @param config the backfill configuration
     * @param messaging the messaging facility to dispatch blocks to
     * @param logger the logger for this runner
     * @param metricsHolder holder for backfill metrics
     * @param pendingBackfillBlocks counter for tracking in-flight blocks
     * @param persistenceAwaiter the awaiter for blocking until blocks are persisted
     */
    BackfillRunner(
            @NonNull BackfillFetcher fetcher,
            @NonNull BackfillConfiguration config,
            @NonNull BlockMessagingFacility messaging,
            @NonNull System.Logger logger,
            @NonNull BackfillPlugin.MetricsHolder metricsHolder,
            @NonNull AtomicLong pendingBackfillBlocks,
            @NonNull BackfillPersistenceAwaiter persistenceAwaiter) {
        this.fetcher = fetcher;
        this.config = config;
        this.messaging = messaging;
        this.logger = logger;
        this.metricsHolder = metricsHolder;
        this.pendingBackfillBlocks = pendingBackfillBlocks;
        this.persistenceAwaiter = persistenceAwaiter;
    }

    /**
     * Runs the backfill process for the specified gap.
     *
     * @param gap the gap to backfill
     * @return the last successfully backfilled block number, or {@code gap.range().start() - 1} if no blocks were backfilled
     * @throws ParseException if block parsing fails
     * @throws InterruptedException if the thread is interrupted
     */
    long run(@NonNull GapDetector.Gap gap) throws ParseException, InterruptedException {
        return backfillGap(gap.range(), gap.type());
    }

    /**
     * Backfills a gap by iteratively fetching chunks of blocks from available nodes.
     * <p>
     * The method follows this flow for each iteration:
     * <ol>
     *   <li>Fetch next chunk - selects a node, computes chunk range, fetches blocks</li>
     *   <li>Extract block numbers - parses headers once to get block numbers</li>
     *   <li>Send for persistence - dispatches blocks to messaging facility</li>
     *   <li>Await persistence - blocks until all blocks in chunk are persisted</li>
     *   <li>Delay between batches - configurable pause before next iteration</li>
     * </ol>
     *
     * @param gap the range of blocks to backfill
     * @param gapType the type of gap (HISTORICAL or LIVE_TAIL)
     * @return the last successfully backfilled block number, or {@code gap.start() - 1} if no blocks were backfilled
     */
    private long backfillGap(LongRange gap, GapDetector.Type gapType) throws InterruptedException, ParseException {
        final String startingBackfillGapMsg = "Starting backfillGap type=[{0}] range=[{1}]";
        logger.log(INFO, startingBackfillGapMsg, gapType, gap);
        Map<BackfillSourceConfig, List<LongRange>> availability = planAvailabilityForGap(fetcher, gap);
        long currentBlock = gap.start();
        long batchSize = config.fetchBatchSize();
        long lastSuccessfulBlock = gap.start() - 1;

        backfillLoop:
        while (currentBlock <= gap.end()) {
            ChunkFetchResult result = fetchNextChunk(currentBlock, gap.end(), batchSize, availability);
            availability = result.availability();

            switch (result.outcome()) {
                case EXHAUSTED -> {
                    final String backfillExhaustedMsg =
                            "Backfill exhausted for gap [{0}]->[{1}], last successful block=[{2}]";
                    logger.log(INFO, backfillExhaustedMsg, gap.start(), gap.end(), lastSuccessfulBlock);
                    break backfillLoop;
                }
                case RETRY -> {
                    final String retryingBackfillMsg = "Retrying backfill for gap [{0}]->[{1}] at block [{2}]";
                    logger.log(DEBUG, retryingBackfillMsg, gap.start(), gap.end(), currentBlock);
                    continue;
                }
                case SUCCESS -> {
                    List<Long> blockNumbers = extractBlockNumbers(result.blocks());
                    sendBlocksForPersistence(result.blocks(), blockNumbers, result.chunk());
                    awaitBlocksPersistence(blockNumbers, result.chunk());

                    lastSuccessfulBlock = result.chunk().end();
                    Thread.sleep(Math.max(0, config.delayBetweenBatches()));
                    currentBlock = result.chunk().end() + 1;
                }
                // Should never happen due to enum coverage
                default -> throw new IllegalStateException("Unexpected outcome: " + result.outcome());
            }
        }

        if (lastSuccessfulBlock >= gap.end()) {
            final String successfullyCompletedMsg = "Successfully completed backfilling gap [{0}]->[{1}]";
            logger.log(TRACE, successfullyCompletedMsg, gap.start(), gap.end());
        } else if (lastSuccessfulBlock >= gap.start()) {
            final String partiallyBackfilledMsg = "Partially backfilled gap [{0}]->[{1}], completed up to block [{2}]";
            logger.log(INFO, partiallyBackfilledMsg, gap.start(), gap.end(), lastSuccessfulBlock);
        } else {
            final String failedToBackfillMsg = "Failed to backfill any blocks for gap [{0}]->[{1}]";
            logger.log(INFO, failedToBackfillMsg, gap.start(), gap.end());
        }
        return lastSuccessfulBlock;
    }

    /**
     * Outcome of a chunk fetch attempt.
     */
    private enum FetchOutcome {
        /** Blocks fetched successfully, continue processing */
        SUCCESS,
        /** Node failed but others available, retry with updated availability */
        RETRY,
        /** No nodes available after replanning, exit loop */
        EXHAUSTED
    }

    /**
     * Result of attempting to fetch a chunk of blocks.
     *
     * @param availability updated availability map (may have nodes removed on failure)
     * @param chunk the range of blocks fetched, or null if fetch failed
     * @param blocks the fetched blocks, or empty list if fetch failed
     * @param outcome the outcome indicating how the main loop should proceed
     */
    private record ChunkFetchResult(
            Map<BackfillSourceConfig, List<LongRange>> availability,
            LongRange chunk,
            List<BlockUnparsed> blocks,
            FetchOutcome outcome) {}

    /**
     * Attempts to fetch the next chunk of blocks from an available node.
     * <p>
     * This method handles three failure scenarios:
     * <ul>
     *   <li>No node selected - replans availability and signals retry or break</li>
     *   <li>Chunk computation failed - removes node and signals retry or break</li>
     *   <li>Fetch returned empty - removes node, replans, and signals retry or break</li>
     * </ul>
     *
     * @param currentBlock the starting block number to fetch
     * @param gapEnd the end of the gap being backfilled
     * @param batchSize maximum number of blocks to fetch in one chunk
     * @param availability current map of nodes to their available block ranges
     * @return result containing fetched blocks and control flow signals
     */
    private ChunkFetchResult fetchNextChunk(
            long currentBlock, long gapEnd, long batchSize, Map<BackfillSourceConfig, List<LongRange>> availability) {

        // Step 1: Select a node that can serve the requested block range
        Optional<NodeSelectionStrategy.NodeSelection> selection =
                fetcher.selectNextChunk(currentBlock, gapEnd, availability);

        if (selection.isEmpty()) {
            final String noAvailableNodesMsg = "No available nodes found for block [{0}]";
            logger.log(TRACE, noAvailableNodesMsg, currentBlock);
            metricsHolder.backfillFetchErrors().increment();
            Map<BackfillSourceConfig, List<LongRange>> replanned = replanAvailability(currentBlock, gapEnd);
            FetchOutcome outcome = replanned.isEmpty() ? FetchOutcome.EXHAUSTED : FetchOutcome.RETRY;
            return new ChunkFetchResult(replanned, null, List.of(), outcome);
        }

        // Step 2: Compute the chunk range based on node's available ranges and batch size
        NodeSelectionStrategy.NodeSelection nodeChoice = selection.get();
        LongRange chunk = computeChunk(nodeChoice, availability, gapEnd, batchSize);

        if (chunk == null) {
            availability.remove(nodeChoice.nodeConfig());
            FetchOutcome outcome = availability.isEmpty() ? FetchOutcome.EXHAUSTED : FetchOutcome.RETRY;
            return new ChunkFetchResult(availability, null, List.of(), outcome);
        }

        // Step 3: Fetch the blocks from the selected node
        List<BlockUnparsed> blocks = fetcher.fetchBlocksFromNode(nodeChoice.nodeConfig(), chunk);

        if (blocks.isEmpty()) {
            availability.remove(nodeChoice.nodeConfig());
            final String noBlocksFetchedMsg = "No blocks fetched for gap [{0}], skipping";
            logger.log(DEBUG, noBlocksFetchedMsg, chunk);
            Map<BackfillSourceConfig, List<LongRange>> replanned = replanAvailability(currentBlock, gapEnd);
            FetchOutcome outcome = replanned.isEmpty() ? FetchOutcome.EXHAUSTED : FetchOutcome.RETRY;
            return new ChunkFetchResult(replanned, null, List.of(), outcome);
        }

        return new ChunkFetchResult(availability, chunk, blocks, FetchOutcome.SUCCESS);
    }

    /**
     * Extracts block numbers from a list of unparsed blocks.
     * <p>
     * Block numbers are extracted once and cached to avoid parsing the block header twice
     * (once for sending, once for awaiting persistence).
     *
     * @param blocks the list of unparsed blocks
     * @return list of block numbers in the same order as the input blocks
     */
    private List<Long> extractBlockNumbers(List<BlockUnparsed> blocks) throws ParseException {
        List<Long> blockNumbers = new ArrayList<>(blocks.size());
        for (BlockUnparsed blockUnparsed : blocks) {
            blockNumbers.add(extractBlockNumber(blockUnparsed));
        }
        return blockNumbers;
    }

    /**
     * Sends blocks to the messaging facility for persistence.
     * <p>
     * For each block: increments metrics, registers for persistence tracking,
     * sends notification, and increments pending counter.
     *
     * @param blocks the unparsed blocks to send
     * @param blockNumbers pre-extracted block numbers (same order as blocks)
     * @param chunk the range being processed (for logging)
     */
    private void sendBlocksForPersistence(List<BlockUnparsed> blocks, List<Long> blockNumbers, LongRange chunk) {
        for (int i = 0; i < blocks.size(); i++) {
            long blockNumber = blockNumbers.get(i);
            BlockUnparsed blockUnparsed = blocks.get(i);
            metricsHolder.backfillFetchedBlocks().increment();
            // always track persistence before sending backfill notification to avoid race conditions
            persistenceAwaiter.trackBlock(blockNumber);
            messaging.sendBackfilledBlockNotification(new BackfilledBlockNotification(blockNumber, blockUnparsed));
            final String backfillingBlockMsg = "Backfilling block [{0}]";
            logger.log(TRACE, backfillingBlockMsg, blockNumber);
            pendingBackfillBlocks.incrementAndGet();
        }
        final String finishedSendingChunkMsg = "Finished sending chunk [{0}], waiting for persistence";
        logger.log(TRACE, finishedSendingChunkMsg, chunk);
    }

    /**
     * Waits for all blocks in the chunk to be persisted before continuing.
     * <p>
     * This provides backpressure to prevent fetching faster than persistence can handle.
     * Blocks that timeout will be re-detected in a future gap scan.
     *
     * @param blockNumbers the block numbers to await
     * @param chunk the range being processed (for logging)
     */
    private void awaitBlocksPersistence(List<Long> blockNumbers, LongRange chunk) throws InterruptedException {
        for (long blockNumber : blockNumbers) {
            boolean persisted = persistenceAwaiter.awaitPersistence(blockNumber, config.perBlockProcessingTimeout());
            if (!persisted) {
                final String persistenceTimedOutMsg = "Block [{0}] persistence timed out, will be re-detected";
                logger.log(INFO, persistenceTimedOutMsg, blockNumber);
            }
        }
        final String allBlocksPersistedMsg = "All blocks in chunk [{0}] persisted";
        logger.log(TRACE, allBlocksPersistedMsg, chunk);
    }

    /**
     * Initializes availability by querying all configured nodes for their block ranges.
     */
    private Map<BackfillSourceConfig, List<LongRange>> planAvailabilityForGap(
            BackfillFetcher backfillFetcher, LongRange gap) {
        return backfillFetcher.getAvailabilityForRange(gap);
    }

    /**
     * Re-queries node availability for the remaining gap range.
     * Called when current availability becomes stale (e.g., after node failures).
     */
    private Map<BackfillSourceConfig, List<LongRange>> replanAvailability(long startBlock, long gapEnd) {
        LongRange remaining = new LongRange(startBlock, gapEnd);
        return planAvailabilityForGap(fetcher, remaining);
    }

    /**
     * Computes the chunk range to fetch from a selected node.
     * <p>
     * The chunk is bounded by:
     * <ul>
     *   <li>The node's available range that covers the start block</li>
     *   <li>The configured batch size</li>
     *   <li>The end of the gap being backfilled</li>
     * </ul>
     *
     * @param selection the selected node and starting block
     * @param availability map of nodes to their available ranges
     * @param gapEnd the end of the gap being backfilled
     * @param batchSize maximum blocks to include in the chunk
     * @return the chunk range, or null if no covering range found
     */
    static LongRange computeChunk(
            @NonNull NodeSelectionStrategy.NodeSelection selection,
            @NonNull Map<BackfillSourceConfig, List<LongRange>> availability,
            long gapEnd,
            long batchSize) {
        List<LongRange> ranges = availability.get(selection.nodeConfig());
        if (ranges == null) {
            return null;
        }

        long start = selection.startBlock();
        LongRange coveringRange = ranges.stream()
                .filter(range -> start >= range.start() && start <= range.end())
                .findFirst()
                .orElse(null);
        if (coveringRange == null) {
            return null;
        }

        long chunkEnd = Math.min(Math.min(start + batchSize - 1, coveringRange.end()), gapEnd);
        return new LongRange(start, chunkEnd);
    }

    /**
     * Extracts the block number from an unparsed block by parsing its header.
     */
    private long extractBlockNumber(BlockUnparsed blockUnparsed) throws ParseException {
        return BlockHeader.PROTOBUF
                .parse(blockUnparsed.blockItems().getFirst().blockHeaderOrThrow())
                .number();
    }
}
