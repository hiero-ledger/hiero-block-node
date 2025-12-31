// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.TRACE;

import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import com.swirlds.metrics.api.Counter;
import edu.umd.cs.findbugs.annotations.NonNull;
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
 * Executes backfill tasks: plans availability, fetches, and dispatches blocks.
 */
final class BackfillRunner {
    private final BackfillFetcher fetcher;
    private final BackfillConfiguration config;
    private final BlockMessagingFacility messaging;
    private final System.Logger logger;
    private final AtomicLong pendingBackfillBlocks;
    private final Counter fetchedBlocks;
    private final Counter fetchErrors;

    BackfillRunner(
            BackfillFetcher fetcher,
            BackfillConfiguration config,
            BlockMessagingFacility messaging,
            System.Logger logger,
            AtomicLong pendingBackfillBlocks,
            Counter fetchedBlocks,
            Counter fetchErrors) {
        this.fetcher = fetcher;
        this.config = config;
        this.messaging = messaging;
        this.logger = logger;
        this.pendingBackfillBlocks = pendingBackfillBlocks;
        this.fetchedBlocks = fetchedBlocks;
        this.fetchErrors = fetchErrors;
    }

    BackfillTaskScheduler.LongRangeResult run(BackfillTask task) throws ParseException, InterruptedException {
        return new BackfillTaskScheduler.LongRangeResult(
                backfillGap(task.gap().range(), task.gap().type()));
    }

    private LongRange backfillGap(LongRange gap, GapType gapType) throws InterruptedException, ParseException {
        logger.log(TRACE, "Starting backfillGap type={0} range={1}", gapType, gap);
        Map<BackfillSourceConfig, List<LongRange>> availability = planAvailabilityForGap(fetcher, gap);
        long currentBlock = gap.start();
        long batchSize = config.fetchBatchSize();
        while (currentBlock <= gap.end()) {
            Optional<BackfillFetcher.NodeSelection> selection =
                    fetcher.selectNextChunk(currentBlock, gap.end(), availability);
            if (selection.isEmpty()) {
                logger.log(TRACE, "No available nodes found for block {0}", currentBlock);
                fetchErrors.increment();
                break;
            }

            BackfillFetcher.NodeSelection nodeChoice = selection.get();
            LongRange chunk = computeChunk(nodeChoice, availability, gap.end(), batchSize);
            if (chunk == null) {
                availability.remove(nodeChoice.nodeConfig());
                continue;
            }
            List<BlockUnparsed> batchOfBlocks = fetcher.fetchBlocksFromNode(nodeChoice.nodeConfig(), chunk);

            if (batchOfBlocks.isEmpty()) {
                availability.remove(nodeChoice.nodeConfig());
                logger.log(DEBUG, "No blocks fetched for gap {0}, skipping", chunk);
                continue;
            }

            for (BlockUnparsed blockUnparsed : batchOfBlocks) {
                long blockNumber = extractBlockNumber(blockUnparsed);
                pendingBackfillBlocks.incrementAndGet();
                messaging.sendBackfilledBlockNotification(new BackfilledBlockNotification(blockNumber, blockUnparsed));
                logger.log(TRACE, "Backfilling block {0}", blockNumber);
                fetchedBlocks.increment();
            }
            logger.log(TRACE, "Finished sending chunk {0}", chunk);

            Thread.sleep(Math.max(0, config.delayBetweenBatches()));
            currentBlock = chunk.end() + 1;
        }

        logger.log(TRACE, "Completed backfilling task gap {0}->{1}", gap.start(), gap.end());
        return null;
    }

    private Map<BackfillSourceConfig, List<LongRange>> planAvailabilityForGap(
            BackfillFetcher backfillFetcher, LongRange gap) {
        backfillFetcher.resetStatus();
        return backfillFetcher.getAvailabilityForRange(gap);
    }

    static LongRange computeChunk(
            @NonNull BackfillFetcher.NodeSelection selection,
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
