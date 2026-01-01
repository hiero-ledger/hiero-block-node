// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.TRACE;
import static java.lang.System.Logger.Level.WARNING;

import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
    private final BackfillMetricsCallback metricsCallback;
    private final BackfillPersistenceAwaiter persistenceAwaiter;

    /**
     * Creates a new runner with the specified dependencies.
     *
     * @param fetcher the fetcher for retrieving blocks from peer nodes
     * @param config the backfill configuration
     * @param messaging the messaging facility to dispatch blocks to
     * @param logger the logger for this runner
     * @param metricsCallback callback for reporting metrics
     * @param persistenceAwaiter the awaiter for blocking until blocks are persisted
     */
    BackfillRunner(
            @NonNull BackfillFetcher fetcher,
            @NonNull BackfillConfiguration config,
            @NonNull BlockMessagingFacility messaging,
            @NonNull System.Logger logger,
            @NonNull BackfillMetricsCallback metricsCallback,
            @NonNull BackfillPersistenceAwaiter persistenceAwaiter) {
        this.fetcher = fetcher;
        this.config = config;
        this.messaging = messaging;
        this.logger = logger;
        this.metricsCallback = metricsCallback;
        this.persistenceAwaiter = persistenceAwaiter;
    }

    /**
     * Runs the backfill process for the specified gap.
     *
     * @param gap the gap to backfill
     * @throws ParseException if block parsing fails
     * @throws InterruptedException if the thread is interrupted
     */
    void run(@NonNull TypedGap gap) throws ParseException, InterruptedException {
        backfillGap(gap.range(), gap.type());
    }

    private void backfillGap(LongRange gap, GapType gapType) throws InterruptedException, ParseException {
        logger.log(TRACE, "Starting backfillGap type=[%s] range=[%s]".formatted(gapType, gap));
        Map<BackfillSourceConfig, List<LongRange>> availability = planAvailabilityForGap(fetcher, gap);
        long currentBlock = gap.start();
        long batchSize = config.fetchBatchSize();
        while (currentBlock <= gap.end()) {
            Optional<NodeSelectionStrategy.NodeSelection> selection =
                    fetcher.selectNextChunk(currentBlock, gap.end(), availability);
            if (selection.isEmpty()) {
                logger.log(TRACE, "No available nodes found for block [%s]".formatted(currentBlock));
                metricsCallback.onFetchError(new RuntimeException("No available nodes for block " + currentBlock));
                availability = replanAvailability(currentBlock, gap.end());
                if (availability.isEmpty()) {
                    break;
                }
                continue;
            }

            NodeSelectionStrategy.NodeSelection nodeChoice = selection.get();
            LongRange chunk = computeChunk(nodeChoice, availability, gap.end(), batchSize);
            if (chunk == null) {
                availability.remove(nodeChoice.nodeConfig());
                if (availability.isEmpty()) {
                    break;
                }
                continue;
            }
            List<BlockUnparsed> batchOfBlocks = fetcher.fetchBlocksFromNode(nodeChoice.nodeConfig(), chunk);

            if (batchOfBlocks.isEmpty()) {
                availability.remove(nodeChoice.nodeConfig());
                logger.log(DEBUG, "No blocks fetched for gap [%s], skipping".formatted(chunk));
                availability = replanAvailability(currentBlock, gap.end());
                if (availability.isEmpty()) {
                    break;
                }
                continue;
            }

            for (BlockUnparsed blockUnparsed : batchOfBlocks) {
                long blockNumber = extractBlockNumber(blockUnparsed);
                metricsCallback.onBlockFetched(blockNumber);
                persistenceAwaiter.trackBlock(blockNumber);
                messaging.sendBackfilledBlockNotification(new BackfilledBlockNotification(blockNumber, blockUnparsed));
                logger.log(TRACE, "Backfilling block [%s]".formatted(blockNumber));
                metricsCallback.onBlockDispatched(blockNumber);
            }
            logger.log(TRACE, "Finished sending chunk [%s], waiting for persistence".formatted(chunk));

            // Wait for all blocks in batch to be persisted before fetching more
            for (BlockUnparsed blockUnparsed : batchOfBlocks) {
                long blockNumber = extractBlockNumber(blockUnparsed);
                boolean persisted =
                        persistenceAwaiter.awaitPersistence(blockNumber, config.perBlockProcessingTimeout());
                if (!persisted) {
                    logger.log(WARNING, "Block [%s] persistence timed out, will be re-detected".formatted(blockNumber));
                }
            }
            logger.log(TRACE, "All blocks in chunk [%s] persisted".formatted(chunk));

            Thread.sleep(Math.max(0, config.delayBetweenBatches()));
            currentBlock = chunk.end() + 1;
        }

        logger.log(TRACE, "Completed backfilling task gap [%s]->[%s]".formatted(gap.start(), gap.end()));
    }

    private Map<BackfillSourceConfig, List<LongRange>> planAvailabilityForGap(
            BackfillFetcher backfillFetcher, LongRange gap) {
        backfillFetcher.resetStatus();
        return backfillFetcher.getAvailabilityForRange(gap);
    }

    private Map<BackfillSourceConfig, List<LongRange>> replanAvailability(long startBlock, long gapEnd) {
        LongRange remaining = new LongRange(startBlock, gapEnd);
        return planAvailabilityForGap(fetcher, remaining);
    }

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

    private long extractBlockNumber(BlockUnparsed blockUnparsed) throws ParseException {
        return BlockHeader.PROTOBUF
                .parse(blockUnparsed.blockItems().getFirst().blockHeaderOrThrow())
                .number();
    }
}
