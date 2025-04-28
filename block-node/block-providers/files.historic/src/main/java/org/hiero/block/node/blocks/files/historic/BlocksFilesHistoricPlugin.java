// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.historic;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.lang.System.Logger.Level;
import java.nio.file.Files;
import java.util.List;
import java.util.Objects;
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
public final class BlocksFilesHistoricPlugin implements BlockProviderPlugin, BlockNotificationHandler {
    /** The logger for this class. */
    private final System.Logger LOGGER = System.getLogger(getClass().getName());
    /** The executor service for moving blocks to zip files in a background thread. */
    private final ExecutorService zipMoveExecutorService;
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
    /** Config used internally. Should come from the context, this is temporary until we have some needed logic. */
    private FilesHistoricConfig config;

    /** Constructor, used for normal plugin loading */
    public BlocksFilesHistoricPlugin() {
        this.zipMoveExecutorService = Executors.newSingleThreadExecutor();
    }

    /** Constructor, used only for testing temporarily while we wait for
     * extended functionality to support overriding config converters
     * PR #18617 in hiero-consensus-node.
     */
    BlocksFilesHistoricPlugin(final FilesHistoricConfig config, final ExecutorService zipMoveExecutorService) {
        this.config = Objects.requireNonNull(config);
        this.zipMoveExecutorService = Objects.requireNonNull(zipMoveExecutorService);
    }

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
    public void init(final BlockNodeContext context, final ServiceBuilder serviceBuilder) {
        this.context = Objects.requireNonNull(context);
        final FilesHistoricConfig localConfig =
                this.config == null ? context.configuration().getConfigData(FilesHistoricConfig.class) : this.config;
        // create plugin data root directory if it does not exist
        try {
            Files.createDirectories(localConfig.rootPath());
        } catch (IOException e) {
            LOGGER.log(Level.ERROR, "Could not create root directory", e);
            context.serverHealth().shutdown(name(), "Could not create root directory");
        }
        // register to listen to block notifications
        context.blockMessaging().registerBlockNotificationHandler(this, false, "Blocks Files Historic");
        numberOfBlocksPerZipFile = (int) Math.pow(10, localConfig.powersOfTenPerZipFileContents());
        zipBlockArchive = new ZipBlockArchive(context, localConfig);
        // get the first and last block numbers from the zipBlockArchive
        final long firstZippedBlock = zipBlockArchive.minStoredBlockNumber();
        final long latestZippedBlock = zipBlockArchive.maxStoredBlockNumber();
        if (latestZippedBlock > firstZippedBlock) {
            // we never expect to enter here, if we do, we have an issue that
            // needs to be investigated
            throw new IllegalStateException(
                    "Latest zipped block number [%d] cannot be greater than the first zipped block number [%d]"
                            .formatted(latestZippedBlock, firstZippedBlock));
        }
        if (firstZippedBlock >= 0) {
            // add the blocks to the available blocks only if the range is a valid one (positive)
            availableBlocks.add(firstZippedBlock, latestZippedBlock);
        }
    }

    /**
     * On plugin start, check if there are any batches of blocks that need to be moved to zip files.
     */
    @Override
    public void start() {
        attemptZipping();
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
            attemptZipping();
        } // todo this is not enough of an assertion that the blocks will be coming from the right place
        //     as notifications are async and things can happen, when we get the accessors later, we should
        //     be able to get accessors only from places that have higher priority than us. We should probably
        //     have that as a feature in the block accessor api. (meaning we should be able to query the
        //     historical block facility for blocks that are coming from higher priority plugins)
    }

    // ==== Private Methods ============================================================================================
    private void attemptZipping() {
        // compute the min and max block in next batch to zip
        long minBlockNumber = availableBlocks().max() + 1;
        long maxBlockNumber = minBlockNumber + numberOfBlocksPerZipFile - 1;
        // check all those blocks are available
        final BlockRangeSet historicalAvailable =
                context.historicalBlockProvider().availableBlocks();
        // while we can zip blocks, we must keep zipping
        while (historicalAvailable.max() >= maxBlockNumber) {
            if (historicalAvailable.contains(minBlockNumber, maxBlockNumber)) {
                final LongRange batchRange = new LongRange(minBlockNumber, maxBlockNumber);
                // move the batch of blocks to a zip file
                startMovingBatchOfBlocksToZipFile(batchRange);
            }
            // try the next batch just in case there is more than one that became available
            minBlockNumber += numberOfBlocksPerZipFile;
            maxBlockNumber += numberOfBlocksPerZipFile;
        }
    }

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
        } catch (final IOException e) {
            LOGGER.log(
                    System.Logger.Level.ERROR,
                    "Failed to move batch of blocks[" + batchFirstBlockNumber + " -> " + batchLastBlockNumber
                            + "] to zip file",
                    e);
            return;
        }
        // if we have reached here, then the batch of blocks has been zipped,
        // now we need to make some updates
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
    }
}
