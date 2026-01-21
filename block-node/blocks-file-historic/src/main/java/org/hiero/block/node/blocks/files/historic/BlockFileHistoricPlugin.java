// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.historic;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.TRACE;
import static java.lang.System.Logger.Level.WARNING;
import static java.nio.file.FileVisitResult.CONTINUE;
import static org.hiero.block.node.base.BlockFile.nestedDirectoriesAllBlockNumbers;
import static org.hiero.block.node.blocks.files.historic.BlockPath.computeBlockPath;

import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
import com.swirlds.metrics.api.Counter;
import com.swirlds.metrics.api.LongGauge;
import com.swirlds.metrics.api.Metrics;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.System.Logger.Level;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.base.BlockFile;
import org.hiero.block.node.base.ranges.ConcurrentLongRangeSet;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.BlockAccessorBatch;
import org.hiero.block.node.spi.historicalblocks.BlockProviderPlugin;
import org.hiero.block.node.spi.historicalblocks.BlockRangeSet;
import org.hiero.block.node.spi.historicalblocks.LongRange;

/**
 * This plugin provides a block provider that stores historical blocks in file. It is designed to store them in the
 * most compressed optimal way possible. It is designed to be used with the
 */
public final class BlockFileHistoricPlugin implements BlockProviderPlugin, BlockNotificationHandler {
    /** A message logged when gaps are encountered while archiving */
    private static final String GAP_FOUND_MESSAGE =
            "Staged block {0} was not found! Cannot proceed to upload archive block batch: {1} - {2}";
    /** The logger for this class. */
    private final System.Logger LOGGER = System.getLogger(getClass().getName());
    /** The executor service for moving blocks to zip files in a background thread. */
    private ExecutorService zipMoveExecutorService;
    /** The block node context. */
    private BlockNodeContext context;
    /** The zip block archive. */
    private ZipBlockArchive zipBlockArchive;
    /** The number of blocks per zip file. */
    private long numberOfBlocksPerZipFile;
    /** The set of available blocks. */
    private final ConcurrentLongRangeSet availableBlocks = new ConcurrentLongRangeSet();
    /** The set of available temporary blocks (not yet zipped). */
    private final ConcurrentLongRangeSet availableStagedBlocks = new ConcurrentLongRangeSet();
    /** List of all zip ranges that are in progress, so we do not start a duplicate job. */
    private final Deque<LongRange> inProgressZipRanges = new ConcurrentLinkedDeque<>();
    /** Running total of bytes stored in the historic tier */
    private final AtomicLong totalBytesStored = new AtomicLong(0);
    /** The config used for this plugin */
    private FilesHistoricConfig config;
    /** The Storage Retention Policy Threshold */
    private long blockRetentionThreshold;
    /** Path for staging verified blocks before they are zipped */
    private Path stagingPath;
    /** root path for temporary hard links to zip files */
    private Path linksRootPath;
    /** Path where we create zip files before moving them to the links root path */
    private Path zipWorkRootPath;
    // Metrics
    /** Counter for blocks written to the historic tier */
    private Counter blocksWrittenCounter;
    /** Counter for blocks read from the historic tier */
    private Counter blocksReadCounter;
    /** Gauge for the number of blocks stored in the historic tier */
    private LongGauge blocksStoredGauge;
    /** Gauge for the total bytes stored in the historic tier */
    private LongGauge bytesStoredGauge;
    /** Counter for failed zip deletions from the historic tier */
    private Counter zipsDeletedFailedCounter;

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
        try {
            this.context = Objects.requireNonNull(context);
            config = context.configuration().getConfigData(FilesHistoricConfig.class);
            blockRetentionThreshold = config.blockRetentionThreshold();
            // Initialize metrics
            initMetrics(context.metrics());
            // create plugin data root directory if it does not exist
            final Path dataRootPath = config.rootPath();
            linksRootPath = dataRootPath.resolve("links");
            stagingPath = dataRootPath.resolve("staging");
            zipWorkRootPath = dataRootPath.resolve("zipwork");
            Files.createDirectories(stagingPath);
            nestedDirectoriesAllBlockNumbers(stagingPath, config.compression()).forEach(blockNumber -> {
                availableStagedBlocks.add(blockNumber);
            });
            // attempt to clear any existing links root directory
            if (Files.isDirectory(linksRootPath, LinkOption.NOFOLLOW_LINKS)) {
                Files.walkFileTree(linksRootPath, new RecursiveFileDeleteVisitor());
            }
            if (Files.isDirectory(zipWorkRootPath, LinkOption.NOFOLLOW_LINKS)) {
                Files.walkFileTree(zipWorkRootPath, new RecursiveFileDeleteVisitor());
            }
            Files.createDirectories(dataRootPath);
            Files.createDirectories(linksRootPath);
            Files.createDirectories(zipWorkRootPath);
            // register to listen to block notifications
            context.blockMessaging().registerBlockNotificationHandler(this, false, "Blocks Files Historic");
            numberOfBlocksPerZipFile = intPowerOfTen(config.powersOfTenPerZipFileContents());
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
        } catch (IOException e) {
            LOGGER.log(ERROR, "Could not initialize historic plugin due to I/O exception", e);
            // ------------------------------------
            // DO NOT shutdown the server, handle this correctly instead.
            context.serverHealth().shutdown(name(), "Could not create root directory");
        }
    }

    /**
     * Simple power of ten function that avoids the inaccuracies possible
     * with floating point and also ensures the value must fit within
     * a long.
     *
     * @param powerToCreate the exponent to raise 10 to.  This must be
     *     between 1 and 18, inclusive.
     * @return 10 raised to (powerToCreate), or -1 if the result would not
     *     fit within a long primitive.
     */
    private long intPowerOfTen(final int powerToCreate) {
        if (powerToCreate > 18 || powerToCreate < 1) {
            return -1;
        } else {
            long currentTotal = 1;
            for (int i = 0; i < powerToCreate; i++) {
                currentTotal *= 10;
            }
            return currentTotal;
        }
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
        zipsDeletedFailedCounter =
                metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "files_historic_zips_deleted_failed")
                        .withDescription("Zips failed deletion from files.historic provider"));
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

    @Override
    public void handleVerification(VerificationNotification notification) {
        if (notification != null && notification.success()) {
            writeBlockToStagingPath(notification.block(), notification.blockNumber());
        }
        try {
            attemptZipping();
            cleanup();
        } catch (final RuntimeException e) {
            final String message = "Failed to handle persistence notification due to %s".formatted(e);
            LOGGER.log(WARNING, message, e);
        }
    }

    // ==== Private Methods ============================================================================================
    private void writeBlockToStagingPath(final BlockUnparsed block, final long blockNumber) {
        if (block == null || block.blockItems() == null || block.blockItems().isEmpty()) {
            return;
        }

        BlockItemUnparsed firstItem = block.blockItems().getFirst();
        if (!firstItem.hasBlockHeader()) {
            LOGGER.log(WARNING, "Block {0} has no block header, cannot write to staging path", blockNumber);
            return;
        }

        final BlockHeader header = getBlockHeader(block);
        final long headerNumber = header == null ? -1 : header.number();
        if (headerNumber != blockNumber) {
            LOGGER.log(
                    WARNING,
                    "Block number mismatch between notification {0} and block header {1}, not writing block",
                    blockNumber,
                    headerNumber);
            return;
        }

        final Path verifiedBlockPath = BlockFile.nestedDirectoriesBlockFilePath(
                stagingPath, blockNumber, config.compression(), config.maxFilesPerDir());
        createDirectoryOrFail(verifiedBlockPath);
        writeBlockOrFail(block, blockNumber, verifiedBlockPath);
    }

    private BlockHeader getBlockHeader(final BlockUnparsed block) {
        Bytes headerBytes = block.blockItems().getFirst().blockHeader();
        try {
            return BlockHeader.PROTOBUF.parse(headerBytes);
        } catch (final ParseException e) {
            LOGGER.log(INFO, "Failed to parse block header", e);
            return null;
        }
    }

    private void createDirectoryOrFail(final Path verifiedBlockPath) {
        try {
            // create parent directory if it does not exist
            Files.createDirectories(verifiedBlockPath.getParent());
        } catch (final IOException e) {
            final String message = "Failed to create directories for path %s due to %s"
                    .formatted(verifiedBlockPath.toAbsolutePath(), e);
            LOGGER.log(INFO, message, e);
            throw new UncheckedIOException(e.getMessage(), e);
        }
    }

    private void writeBlockOrFail(final BlockUnparsed block, final long blockNumber, final Path verifiedBlockPath) {
        try (final WritableStreamingData streamingData = new WritableStreamingData(new BufferedOutputStream(
                config.compression().wrapStream(Files.newOutputStream(verifiedBlockPath)), 16384))) {
            BlockUnparsed.PROTOBUF.write(block, streamingData);
            streamingData.flush();
            streamingData.close();
            LOGGER.log(TRACE, "Wrote verified block {0} to file {1}", blockNumber, verifiedBlockPath.toAbsolutePath());
            // update the oldest and newest verified block numbers
            availableStagedBlocks.add(blockNumber);
        } catch (final IOException e) {
            final String message = "Failed to write file for block %d due to %s".formatted(blockNumber, e);
            LOGGER.log(WARNING, message, e);
        }
    }

    private void attemptZipping() {
        // compute the min and max block in next batch to zip
        // since we ensure no gaps in the zip file are possible, and also we
        // have a power of 10 number of blocks per zip file, it is safe to
        // simply add +1 to the latest available block number and have that as
        // the start of the next batch of blocks to zip
        long minBlockNumber = availableBlocks.max() + 1;
        long maxBlockNumber = minBlockNumber + numberOfBlocksPerZipFile - 1;
        // while we can zip blocks, we must keep zipping
        // we loop here because the historical block facility can have
        // multiple batches of blocks available for zipping potentially, so we
        // need to queue them all up
        while (availableStagedBlocks.max() >= maxBlockNumber) {
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
            final boolean blocksAvailablePreCheck = availableStagedBlocks.contains(minBlockNumber, maxBlockNumber);
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

    private void cleanup() {
        // we only take action if the threshold is greater than 0L
        if (blockRetentionThreshold > 0L) {
            final long totalStored = availableBlocks.size();
            // calculate excess blocks to delete, the retention threshold
            // is the number of zips (archived batches) to retain
            long excess = totalStored - (blockRetentionThreshold * numberOfBlocksPerZipFile);
            // the numberOfBlocksPerZipFile should generally be immutable once set
            // for the first time when the block node was originally started
            // we can rely on the check below to ensure we are deleting the correct
            // number of blocks
            while (excess >= numberOfBlocksPerZipFile) {
                // if we have passed the above check, we can delete at least one zip file
                // we assume there are no gaps in the zips, the number of blocks per zip file
                // setting is not possible to change after starting the system for the first time,
                // also the number of blocks per zip file is a power of ten, so we can safely
                // say that whatever the range is, it will always start/end with a predictable number
                // e.g. 0-9, 10-19, 20-29 (batch 10s) or 10_000-19_999, 20_000-29_999 (batch 10_000s) etc.
                // depending on the setting
                final long minBlockNumberStored = availableBlocks.min();
                // no need to compute existing below, we need the path to the zip file, we do not need to
                // check if the minBlockNumberStored exists, moreover we do not need to know actual block compression
                // type.
                final Path zipToDelete =
                        BlockPath.computeBlockPath(config, minBlockNumberStored).zipFilePath();
                if (Files.exists(zipToDelete)) {
                    try {
                        // since we keep track of the whole zip file size, that is
                        // what we should decrement the total bytes stored by
                        final long zipFileSize = Files.size(zipToDelete);
                        Files.delete(zipToDelete);
                        totalBytesStored.addAndGet(-zipFileSize);
                        // we know that the minBlockNumberStored is for sure the beginning of the batch
                        // of blocks that we are deleting, also we know that the numberOfBlocksPerZipFile
                        // is immutable once set originally when we first started the block node,
                        // so we can safely calculate and remove the range of blocks from the available blocks
                        availableBlocks.remove(
                                minBlockNumberStored, minBlockNumberStored + numberOfBlocksPerZipFile - 1);
                    } catch (final IOException e) {
                        LOGGER.log(INFO, "Failed to delete zip file: %s".formatted(zipToDelete), e);
                        zipsDeletedFailedCounter.increment();
                    }
                }
                excess -= numberOfBlocksPerZipFile;
            }
            updateGauges();
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
            final String message = "Batch of blocks[{0} -> {1}] is already in progress";
            LOGGER.log(DEBUG, message, batchRange.start(), batchRange.end());
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
        // first off, let's create our batch of blocks
        final long batchFirstBlockNumber = batchRange.start();
        final long batchLastBlockNumber = batchRange.end();
        try (final BlockAccessorBatch blockAccessors = gatherAccessors(batchFirstBlockNumber, batchLastBlockNumber)) {
            if (blockAccessors.isEmpty()) {
                final String message = "Could not get staged files for blocks {0} to {1}.";
                LOGGER.log(INFO, message, batchFirstBlockNumber, batchLastBlockNumber);
            } else {
                // move the batch of blocks to a zip file
                final String startMessage = "Moving batch of blocks [{0} -> {1}] to zip file.";
                LOGGER.log(TRACE, startMessage, batchFirstBlockNumber, batchLastBlockNumber);

                // compute the exact path where we need to move the created zip file
                final BlockPath firstBlockPath = computeBlockPath(config, blockAccessors.getFirstBlockNumber());

                // Compute the file name of the work zip directory so that if zipping fails, we don't leave
                // traces in the actual data area
                final Path zipWorkPath =
                        zipWorkRootPath.resolve(firstBlockPath.zipFilePath().getFileName());

                // Write the zip file in the zip work area
                zipBlockArchive.createZip(blockAccessors, zipWorkPath);

                // if we have reached here, this means that the zip file was created
                // successfully in the work zip area
                final long zipFileSize = Files.size(zipWorkPath);

                // create staging area directories if they don't exist
                Files.createDirectories(firstBlockPath.dirPath());
                // move the file from the work zip area to the data area
                Files.move(zipWorkPath, firstBlockPath.zipFilePath());

                // Metrics updates
                // Update total bytes stored with the new zip file size
                totalBytesStored.addAndGet(zipFileSize);
                // Increment the blocks written counter
                blocksWrittenCounter.add(numberOfBlocksPerZipFile);
                // -----------------------------------------------
                // @todo Remove, make staging file accessor handle this, if reasonable
                for (long blockNumber = batchFirstBlockNumber; blockNumber <= batchLastBlockNumber; blockNumber++) {
                    Path path = BlockFile.nestedDirectoriesBlockFilePath(
                            stagingPath, blockNumber, config.compression(), config.maxFilesPerDir());
                    if (Files.exists(path)) {
                        Files.delete(path);
                        availableStagedBlocks.remove(blockNumber);
                    }
                }
                // -----------------------------------------------
                // if we have reached here, then the batch of blocks has been
                // zipped and the staging files removed.
                // Now we need to update the first and last block numbers
                availableBlocks.add(batchFirstBlockNumber, batchLastBlockNumber);
                final String successMessage = "Successfully moved batch of blocks[{0} -> {1}] to zip file.";
                LOGGER.log(Level.TRACE, successMessage, batchFirstBlockNumber, batchLastBlockNumber);
                // now all the blocks are in the zip file and accessible, send notification
                // @todo is this needed? Does anything actually care when a zip file is completed?
                context.blockMessaging()
                        .sendBlockPersisted(new PersistedNotification(
                                batchLastBlockNumber, true, defaultPriority(), BlockSource.HISTORY));
            }
        } catch (final IOException e) {
            final String failMessage = "Failed to move batch of blocks [%d -> %d] to zip file"
                    .formatted(batchFirstBlockNumber, batchLastBlockNumber);
            LOGGER.log(WARNING, failMessage, e);
            cleanupZipWorkFiles();
        } finally {
            // always make sure to remove the batch of blocks from in progress ranges
            inProgressZipRanges.remove(batchRange);
        }
    }

    /**
     * This method attempts to gather the block accessors for the given
     * range of block numbers. The range must be gathered in full, no gaps
     * are allowed to happen. If failure during gathering occurs or a gap
     * is detected, this method will close any open accessors and return
     * null. Otherwise, it will return a list of accessors for the requested
     * range.
     */
    private BlockAccessorBatch gatherAccessors(final long startBlockNumber, final long endBlockNumber) {
        final BlockAccessorBatch accessors = new BlockAccessorBatch();
        try {
            for (long i = startBlockNumber; i <= endBlockNumber; i++) {
                Path path = BlockFile.nestedDirectoriesBlockFilePath(
                        stagingPath, i, config.compression(), config.maxFilesPerDir());
                final BlockAccessor accessor = new BlockStagingFileAccessor(path, config.compression(), i);
                if (accessor != null) {
                    accessors.add(accessor);
                } else {
                    LOGGER.log(WARNING, GAP_FOUND_MESSAGE, i, startBlockNumber, endBlockNumber);
                    accessors.close();
                    break;
                }
            }
        } catch (final RuntimeException e) {
            final String message =
                    "Failed to gather block accessors for range: %d - %d".formatted(startBlockNumber, endBlockNumber);
            LOGGER.log(WARNING, message, e);
            accessors.close();
        }
        return accessors;
    }

    /**
     * This method deletes any remaining zip files in the work area.
     * We know that it doesn't contain any subdirectories, so Files.delete is safe to use.
     */
    private void cleanupZipWorkFiles() {
        try (var files = Files.list(zipWorkRootPath)) {
            files.forEach(file -> {
                try {
                    Files.delete(file);
                } catch (IOException e) {
                    final String msg = "Failed to delete work zip file: %s".formatted(file);
                    LOGGER.log(INFO, msg);
                }
            });
        } catch (IOException e) {
            final String msg = "Failed to list work zip files in %s".formatted(zipWorkRootPath.toString());
            LOGGER.log(INFO, msg, e);
        }
    }

    /**
     * A basic file visitor to recursively delete files and directories up to
     * the provided root.
     */
    private static class RecursiveFileDeleteVisitor implements FileVisitor<Path> {

        @Override
        public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) throws IOException {
            Objects.requireNonNull(dir);
            return CONTINUE;
        }

        @Override
        @NonNull
        public FileVisitResult visitFile(@NonNull final Path file, @NonNull final BasicFileAttributes attrs)
                throws IOException {
            Files.delete(Objects.requireNonNull(file));
            return CONTINUE;
        }

        @Override
        public FileVisitResult visitFileFailed(final Path file, final IOException exc) throws IOException {
            throw Objects.requireNonNull(exc);
        }

        @Override
        @NonNull
        public FileVisitResult postVisitDirectory(@NonNull final Path dir, @Nullable final IOException e)
                throws IOException {
            if (e == null) {
                Files.delete(Objects.requireNonNull(dir));
                return CONTINUE;
            } else {
                throw e;
            }
        }
    }
}
