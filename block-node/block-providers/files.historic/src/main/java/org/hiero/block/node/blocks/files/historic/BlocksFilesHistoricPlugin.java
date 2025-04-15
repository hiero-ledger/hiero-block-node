// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.historic;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.lang.System.Logger.Level;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.hiero.block.node.base.ranges.ConcurrentLongRangeSet;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.BlockProviderPlugin;
import org.hiero.block.node.spi.historicalblocks.BlockRangeSet;
import org.hiero.block.node.spi.historicalblocks.LongRange;

/**
 * This plugin provides a block provider that stores historical blocks in file. It is designed to store them in the
 * most compressed optimal way possible. It is designed to be used with the
 */
public class BlocksFilesHistoricPlugin implements BlockProviderPlugin, BlockNotificationHandler {
    /** The logger for this class. */
    private final System.Logger LOGGER = System.getLogger(getClass().getName());
    /** The executor service for moving blocks to zip files in a background thread. */
    private final ExecutorService zipMoveExecutorService = Executors.newSingleThreadExecutor();
    /** The block node context. */
    private BlockNodeContext context;
    /** The zip block archive. */
    private ZipBlockArchive zipBlockArchive;
    /** The number of blocks per zip file. */
    private int numberOfBlocksPerZipFile;
    /** The set of available blocks. */
    private final ConcurrentLongRangeSet availableBlocks = new ConcurrentLongRangeSet();
    /** List of all zip ranges that are in progress, so we do not start a duplicate job. */
    private final CopyOnWriteArrayList<LongRange> inProgressZipRanges = new CopyOnWriteArrayList<>();

    // ==== BlockProviderPlugin Methods ================================================================================

    /**
     * {@inheritDoc}
     */
    @NonNull
    @Override
    public List<Class<? extends Record>> configDataTypes() {
        return List.of(FilesHistoricConfig.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(BlockNodeContext context, ServiceBuilder serviceBuilder) {
        this.context = context;
        final FilesHistoricConfig config = context.configuration().getConfigData(FilesHistoricConfig.class);
        // create plugin data root directory if it does not exist
        try {
            Files.createDirectories(config.rootPath());
        } catch (IOException e) {
            LOGGER.log(Level.ERROR, "Could not create root directory", e);
            context.serverHealth().shutdown(name(), "Could not create root directory");
        }
        // register to listen to block notifications
        context.blockMessaging().registerBlockNotificationHandler(this, false, "Blocks Files Historic");
        numberOfBlocksPerZipFile = (int) Math.pow(10, config.powersOfTenPerZipFileContents());
        zipBlockArchive = new ZipBlockArchive(context, config);
        // get the first and last block numbers from the zipBlockArchive
        availableBlocks.add(zipBlockArchive.minStoredBlockNumber(), zipBlockArchive.maxStoredBlockNumber());
    }

    /**
     * On plugin start, check if there are any batches of blocks that need to be moved to zip files.
     */
    @Override
    public void start() {
        // determine if there are any batches of blocks that need to be moved to zip files
        // get the largest stored block number
        long largestStoredBlockNumber =
                context.historicalBlockProvider().availableBlocks().max();
        if (largestStoredBlockNumber > availableBlocks.max()) {
            // complete if there are any blocks that need to be moved to zip files
            long startBlockNumber = availableBlocks.max() + 1;
            while ((startBlockNumber + numberOfBlocksPerZipFile) < largestStoredBlockNumber) {
                // move the batch of blocks to a zip file
                startMovingBatchOfBlocksToZipFile(
                        new LongRange(startBlockNumber, startBlockNumber + numberOfBlocksPerZipFile));
                // move to next batch
                startBlockNumber += numberOfBlocksPerZipFile;
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int defaultPriority() {
        return 1_000;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BlockAccessor block(long blockNumber) {
        // check if the block number is in the range of blocks
        if (blockNumber < availableBlocks.min() || blockNumber > availableBlocks.max()) {
            return null;
        }
        return zipBlockArchive.blockAccessor(blockNumber);
    }

    /**
     * {@inheritDoc}
     */
    public BlockRangeSet availableBlocks() {
        return availableBlocks;
    }

    // ==== BlockNotificationHandler Methods ===========================================================================

    /**
     * {@inheritDoc}
     *
     * Called on message handling thread. Each time any block provider persists one or more new blocks. We will also
     * receive block persisted notifications from our self. So we only care about notifications from plugins with higher
     * priority. As we will move blocks from higher priority plugins to zip files, once enough blocks are available.
     */
    @Override
    public void handlePersisted(PersistedNotification notification) {
        if (notification.blockProviderPriority() > defaultPriority()) {
            // compute the min and max block in next batch to zip
            long minBlockNumber = availableBlocks().max() + 1;
            long maxBlockNumber = minBlockNumber + numberOfBlocksPerZipFile - 1;
            // check all those blocks are available
            while (context.historicalBlockProvider().availableBlocks().max() >= maxBlockNumber) {
                if (context.historicalBlockProvider().availableBlocks().contains(minBlockNumber, maxBlockNumber)) {
                    final LongRange batchRange = new LongRange(minBlockNumber, maxBlockNumber);
                    // move the batch of blocks to a zip file
                    startMovingBatchOfBlocksToZipFile(batchRange);
                }
                // try the next batch just in case there is more than one that became available
                minBlockNumber += numberOfBlocksPerZipFile;
                maxBlockNumber += numberOfBlocksPerZipFile;
            }
        }
    }

    // ==== Private Methods ============================================================================================

    /**
     * Start moving a batch of blocks to a zip file in background as long as batch is not already in progress or queued
     * to be started.
     *
     * @param batchRange The range of blocks to move to zip file.
     */
    private void startMovingBatchOfBlocksToZipFile(final LongRange batchRange) {
        // check if the batch of blocks is already in progress
        if (inProgressZipRanges.contains(batchRange)) {
            LOGGER.log(
                    System.Logger.Level.DEBUG,
                    "Batch of blocks[{%1} -> {%2}] is already in progress",
                    batchRange.start(),
                    batchRange.end());
            return;
        }
        // add the batch of blocks to the in progress ranges
        inProgressZipRanges.add(batchRange);
        // move the batch of blocks to a zip file
        zipMoveExecutorService.submit(() -> moveBatchOfBlocksToZipFile(batchRange));
    }

    /**
     * Move a batch of blocks to a zip file. This should be called on background thread through executor service.
     *
     * @param batchRange The range of blocks to move to zip file.
     */
    private void moveBatchOfBlocksToZipFile(final LongRange batchRange) {
        final long batchFirstBlockNumber = batchRange.start();
        final long batchLastBlockNumber = batchRange.end();
        // move the batch of blocks to a zip file
        try {
            LOGGER.log(
                    System.Logger.Level.DEBUG,
                    "Moving batch of blocks[%d -> %d] to zip file",
                    batchFirstBlockNumber,
                    batchLastBlockNumber);
            zipBlockArchive.writeNewZipFile(batchFirstBlockNumber);
            // update the first and last block numbers
            availableBlocks.add(batchFirstBlockNumber, batchLastBlockNumber);
            // log done
            LOGGER.log(
                    System.Logger.Level.INFO,
                    "Moved batch of blocks[%d -> %d] to zip file",
                    batchFirstBlockNumber,
                    batchLastBlockNumber);
            // now all the blocks are in the zip file and accessible, send notification
            context.blockMessaging()
                    .sendBlockPersisted(
                            new PersistedNotification(batchFirstBlockNumber, batchLastBlockNumber, defaultPriority()));
            // remove the batch of blocks from in progress ranges
            inProgressZipRanges.remove(batchRange);
        } catch (Exception e) {
            LOGGER.log(
                    System.Logger.Level.ERROR,
                    "Failed to move batch of blocks[" + batchFirstBlockNumber + " -> " + batchLastBlockNumber
                            + "] to zip file",
                    e);
        }
    }
}
