// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.historic;

import com.swirlds.metrics.api.Counter;
import com.swirlds.metrics.api.LongGauge;
import com.swirlds.metrics.api.Metrics;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.lang.System.Logger.Level;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.block.node.base.ranges.ConcurrentLongRangeSet;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.BlockProviderPlugin;
import org.hiero.block.node.spi.historicalblocks.BlockRangeSet;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;
import org.hiero.block.node.spi.historicalblocks.LongRange;

/**
 * This plugin provides a block provider that stores historical blocks in file. It is designed to store them in the
 * most compressed optimal way possible. It is designed to be used with the
 */
public final class BlocksFilesHistoricPlugin implements BlockProviderPlugin, BlockNotificationHandler {
    /** The logger for this class. */
    private final System.Logger LOGGER = System.getLogger(getClass().getName());
    /** The executor service for moving blocks to zip files in a background thread. */
    private ExecutorService zipMoveExecutorService;
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
    /** Running total of bytes stored in the historic tier */
    private final AtomicLong totalBytesStored = new AtomicLong(0);

    // Metrics
    /** Counter for blocks written to the historic tier */
    private Counter blocksWrittenCounter;
    /** Counter for blocks read from the historic tier */
    private Counter blocksReadCounter;
    /** Gauge for the number of blocks stored in the historic tier */
    private LongGauge blocksStoredGauge;
    /** Gauge for the total bytes stored in the historic tier */
    private LongGauge bytesStoredGauge;

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
        final FilesHistoricConfig config = context.configuration().getConfigData(FilesHistoricConfig.class);

        // Initialize metrics
        initMetrics(context.metrics());

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
        // create the executor service for moving blocks to zip files
        zipMoveExecutorService = context.threadPoolManager().createSingleThreadExecutor("FilesHistoricZipMove");
        zipBlockArchive = new ZipBlockArchive(context, config);
        // get the first and last block numbers from the zipBlockArchive
        final long firstZippedBlock = zipBlockArchive.minStoredBlockNumber();
        final long latestZippedBlock = zipBlockArchive.maxStoredBlockNumber();
        // todo(1138) let's make sure that we have this case covered by an E2E
        //   test where we will assert the correct behavior of the plugin after
        //   a restart has happened. We will be able to correctly assert this
        //   logic as we will be seeing a failing CI otherwise.
        if (firstZippedBlock > latestZippedBlock) {
            // we never expect to enter here, if we do, we have an issue that
            // needs to be investigated
            // the first zipped block number must always be less than or equal
            // to the latest zipped block number
            throw new IllegalStateException(
                    "First zipped block number [%d] cannot be greater than the latest zipped block number [%d]"
                            .formatted(firstZippedBlock, latestZippedBlock));
        }
        if (firstZippedBlock >= 0) {
            // add the blocks to the available blocks only if the range is a valid one (positive)
            availableBlocks.add(firstZippedBlock, latestZippedBlock);

            // Initialize total bytes stored by querying the zip block archive
            totalBytesStored.set(zipBlockArchive.calculateTotalStoredBytes());
        }

        // Register gauge updater
        context.metrics().addUpdater(this::updateGauges);
    }

    /**
     * Initialize metrics for this plugin.
     */
    private void initMetrics(Metrics metrics) {
        blocksWrittenCounter = metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "files_historic_blocks_written")
                .withDescription("Blocks written to files.historic provider"));

        blocksReadCounter = metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "files_historic_blocks_read")
                .withDescription("Blocks read from files.historic provider"));

        blocksStoredGauge = metrics.getOrCreate(new LongGauge.Config(METRICS_CATEGORY, "files_historic_blocks_stored")
                .withDescription("Blocks stored in files.historic provider"));

        bytesStoredGauge =
                metrics.getOrCreate(new LongGauge.Config(METRICS_CATEGORY, "files_historic_total_bytes_stored")
                        .withDescription("Bytes stored in files.historic provider"));
    }

    /**
     * Update gauge metrics with current state.
     */
    private void updateGauges() {
        // Update blocks stored gauge with the count of available blocks
        blocksStoredGauge.set(availableBlocks.size());

        // Use the running total instead of calculating it each time
        bytesStoredGauge.set(totalBytesStored.get());
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
        // Increment the blocks read counter
        blocksReadCounter.increment();
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
        // all operations are queued up here until we release the caller thread
        // by the block messaging facility, so we can safely perform
        // calculations and decide if we will submit a task or not
        if (notification.blockProviderPriority() > defaultPriority()) {
            attemptZipping();
        }
        // @todo(1069) this is not enough of an assertion that the blocks will be coming from the right place
        //     as notifications are async and things can happen, when we get the accessors later, we should
        //     be able to get accessors only from places that have higher priority than us. We should probably
        //     have that as a feature in the block accessor api. (meaning we should be able to query the
        //     historical block facility for blocks that are coming from higher priority plugins)
    }

    // ==== Private Methods ============================================================================================
    private void attemptZipping() {
        // compute the min and max block in next batch to zip
        // since we ensure no gaps in the zip file are possible, and also we
        // have a power of 10 number of blocks per zip file, it is safe to
        // simply add +1 to the latest available block number and have that as
        // the start of the next batch of blocks to zip
        long minBlockNumber = availableBlocks.max() + 1;
        long maxBlockNumber = minBlockNumber + numberOfBlocksPerZipFile - 1;
        // make sure the historical block facility has a higher max than our
        // desired batch max
        final BlockRangeSet historicalAvailable =
                context.historicalBlockProvider().availableBlocks();
        // while we can zip blocks, we must keep zipping
        // we loop here because the historical block facility can have
        // multiple batches of blocks available for zipping potentially, so we
        // need to queue them all up
        while (historicalAvailable.max() >= maxBlockNumber) {
            // since we know that we have a power of 10 number of blocks per zip file,
            // we know our batch always starts with a number that is a multiple of
            // numberOfBlocksPerZipFile, so we can check if the start is valid
            // if the start is not valid, we will skip this batch and when we have
            // enough blocks available, for the next one, we will start that and
            // then the min will be updated, missed batches will be handled
            // in another fashion
            final boolean isValidStart = minBlockNumber % numberOfBlocksPerZipFile == 0;
            // we can do a quick pre-check to see if the blocks are available
            // this pre-check asserts the min and max are contained however,
            // not the whole range, this will be asserted when we gather the batch
            final boolean blocksAvailablePreCheck = historicalAvailable.contains(minBlockNumber, maxBlockNumber);
            if (isValidStart && blocksAvailablePreCheck) {
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
            // if the batch is in progress, we must not submit a task
            LOGGER.log(
                    System.Logger.Level.DEBUG,
                    "Batch of blocks[{%1} -> {%2}] is already in progress",
                    batchRange.start(),
                    batchRange.end());
        } else {
            // if the batch is not in progress, we must submit a task
            // add the batch of blocks to the in progress ranges
            inProgressZipRanges.add(batchRange);
            // move the batch of blocks to a zip file (submit a task)
            zipMoveExecutorService.submit(() -> moveBatchOfBlocksToZipFile(batchRange));
        }
    }

    /**
     * Move a batch of blocks to a zip file. This should be called on background thread through executor service.
     *
     * @param batchRange The range of blocks to move to zip file.
     */
    private void moveBatchOfBlocksToZipFile(final LongRange batchRange) {
        try {
            // first off, let's create our batch of blocks
            final long batchFirstBlockNumber = batchRange.start();
            final long batchLastBlockNumber = batchRange.end();
            final List<BlockAccessor> batch = new ArrayList<>(numberOfBlocksPerZipFile);
            final HistoricalBlockFacility historicalBlockFacility = context.historicalBlockProvider();
            // gather batch, if there are no gaps, then we can proceed with zipping
            for (long blockNumber = batchFirstBlockNumber; blockNumber <= batchLastBlockNumber; blockNumber++) {
                final BlockAccessor currentAccessor = historicalBlockFacility.block(blockNumber);
                if (currentAccessor == null) {
                    break;
                } else {
                    batch.add(currentAccessor);
                }
            }
            // if there are any gaps, then we cannot zip the batch
            if (batch.size() != numberOfBlocksPerZipFile) {
                // we have a gap in the batch, so we cannot zip it
                LOGGER.log(
                        System.Logger.Level.DEBUG,
                        "Batch of blocks[%d -> %d] has a gap, skipping zipping",
                        batchFirstBlockNumber,
                        batchLastBlockNumber);
            } else {
                // move the batch of blocks to a zip file
                try {
                    LOGGER.log(
                            System.Logger.Level.DEBUG,
                            "Moving batch of blocks[%d -> %d] to zip file",
                            batchFirstBlockNumber,
                            batchLastBlockNumber);
                    // Write the zip file and get result with file size
                    final long zipFileSize = zipBlockArchive.writeNewZipFile(batch);
                    // Metrics updates
                    // Update total bytes stored with the new zip file size
                    totalBytesStored.addAndGet(zipFileSize);
                    // Increment the blocks written counter
                    blocksWrittenCounter.add(numberOfBlocksPerZipFile);
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
                        Level.INFO,
                        "Moved batch of blocks[%d -> %d] to zip file",
                        batchFirstBlockNumber,
                        batchLastBlockNumber);
                // now all the blocks are in the zip file and accessible, send notification
                context.blockMessaging()
                        .sendBlockPersisted(new PersistedNotification(
                                batchFirstBlockNumber, batchLastBlockNumber, defaultPriority()));
            }
        } finally {
            // always make sure to remove the batch of blocks from in progress ranges
            inProgressZipRanges.remove(batchRange);
        }
    }
}
