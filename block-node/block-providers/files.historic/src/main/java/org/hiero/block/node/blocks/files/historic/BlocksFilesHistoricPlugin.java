// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.historic;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.BlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockNotification.Type;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.BlockProviderPlugin;

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
    /** The first block number stored by this plugin */
    private final AtomicLong firstBlockNumber = new AtomicLong(0);
    /** The last block number stored by this plugin */
    private final AtomicLong lastBlockNumber = new AtomicLong(0);

    // ==== BlockProviderPlugin Methods ================================================================================

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(BlockNodeContext context) {
        this.context = context;
        final FilesHistoricConfig config = context.configuration().getConfigData(FilesHistoricConfig.class);
        context.blockMessaging().registerBlockNotificationHandler(this, false, "Blocks Files Historic");
        numberOfBlocksPerZipFile = (int) Math.pow(10, config.digitsPerZipFileName());
        zipBlockArchive = new ZipBlockArchive(context, config);
        // get the first and last block numbers from the zipBlockArchive
        firstBlockNumber.set(zipBlockArchive.minStoredBlockNumber());
        lastBlockNumber.set(zipBlockArchive.maxStoredBlockNumber());
    }

    /**
     * On plugin start, check if there are any batches of blocks that need to be moved to zip files.
     */
    @Override
    public void start() {
        // determine if there are any batches of blocks that need to be moved to zip files
        // get the largest stored block number
        long largestStoredBlockNumber = context.historicalBlockProvider().latestBlockNumber();
        if (largestStoredBlockNumber > lastBlockNumber.get()) {
            // complete if there are any blocks that need to be moved to zip files
            long startBlockNumber = lastBlockNumber.get() + 1;
            while ((startBlockNumber + numberOfBlocksPerZipFile) < largestStoredBlockNumber) {
                // move the batch of blocks to a zip file
                final long firstBlock = startBlockNumber;
                zipMoveExecutorService.submit(() -> moveBatchOfBlocksToZipFile(firstBlock));
                // move to next batch
                startBlockNumber += numberOfBlocksPerZipFile;
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String name() {
        return "Files Historic";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int defaultPriority() {
        return 1000;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BlockAccessor block(long blockNumber) {
        // check if the block number is in the range of blocks
        if (blockNumber < firstBlockNumber.get() || blockNumber > lastBlockNumber.get()) {
            return null;
        }
        return zipBlockArchive.blockAccessor(blockNumber);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long latestBlockNumber() {
        return lastBlockNumber.get();
    }

    // ==== BlockNotificationHandler Methods ===========================================================================

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleBlockNotification(BlockNotification notification) {
        if (notification.type() == Type.BLOCK_PERSISTED) {
            // check if this crosses a block zip file boundary
            if (notification.blockNumber() > lastBlockNumber.get()
                    && notification.blockNumber() % numberOfBlocksPerZipFile == 0) {
                final long firstBlockNumber = notification.blockNumber() - numberOfBlocksPerZipFile;
                // move the batch of blocks to a zip file
                zipMoveExecutorService.submit(() -> moveBatchOfBlocksToZipFile(firstBlockNumber));
            }
        }
    }

    // ==== Private Methods ============================================================================================

    /**
     * Move a batch of blocks to a zip file. This should be called on background thread through executor service.
     *
     * @param batchFirstBlockNumber The first block number in the batch
     */
    private void moveBatchOfBlocksToZipFile(long batchFirstBlockNumber) {
        final long batchLastBlockNumber = (batchFirstBlockNumber + numberOfBlocksPerZipFile - 1);
        // move the batch of blocks to a zip file
        try {
            LOGGER.log(
                    System.Logger.Level.DEBUG,
                    "Moving batch of blocks[%d -> %d] to zip file",
                    batchFirstBlockNumber,
                    batchLastBlockNumber);
            final List<BlockAccessor> blockAccessors = zipBlockArchive.writeNewZipFile(batchFirstBlockNumber);
            // update the first and last block numbers
            firstBlockNumber.getAndUpdate(value ->
                    value == UNKNOWN_BLOCK_NUMBER ? numberOfBlocksPerZipFile : Math.min(value, batchFirstBlockNumber));
            lastBlockNumber.getAndUpdate(value -> Math.max(value, batchLastBlockNumber));
            // log done
            LOGGER.log(
                    System.Logger.Level.INFO,
                    "Moved batch of blocks[%d -> %d] to zip file",
                    batchFirstBlockNumber,
                    batchLastBlockNumber);
            // now all the blocks are in the zip file and accessible, delete the original blocks
            for (BlockAccessor blockAccessor : blockAccessors) {
                blockAccessor.delete();
            }
        } catch (Exception e) {
            LOGGER.log(
                    System.Logger.Level.ERROR,
                    "Failed to move batch of blocks[" + batchFirstBlockNumber + " -> " + batchLastBlockNumber
                            + "] to zip file",
                    e);
        }
    }
}
