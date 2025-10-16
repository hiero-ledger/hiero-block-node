// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.recent;

import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.TRACE;
import static java.lang.System.Logger.Level.WARNING;
import static org.hiero.block.node.base.BlockFile.nestedDirectoriesAllBlockNumbers;
import static org.hiero.block.node.spi.blockmessaging.BlockSource.UNKNOWN;

import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
import com.swirlds.metrics.api.Counter;
import com.swirlds.metrics.api.LongGauge;
import com.swirlds.metrics.api.Metrics;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.base.BlockFile;
import org.hiero.block.node.base.ranges.ConcurrentLongRangeSet;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.BlockProviderPlugin;
import org.hiero.block.node.spi.historicalblocks.BlockRangeSet;

/**
 * This plugin is responsible for providing the "Files Recent" block provider. This stores incoming blocks in files in
 * the local filesystem. It stores block items as soon as they are received into a temporary file until the ends of the
 * block. The temporary file is stored in unverified path. Once the block is verified, it is moved to the live path.
 * This plugin assumes that it stores blocks forever until asked to delete them.
 * <h2>Threading</h2>
 * There are three threading interactions for this class. Any shared state between the three interactions needs to be
 * considered multithreaded, so handled with thread safe data structures.
 * <ul>
 *     <li><b>BlockProviderPlugin methods</b> - The init() and start() methods are called at startup only and only ever
 *     by one thread at a time and before any listeners are called. The reading methods block() and latestBlockNumber()
 *     need to be handled in a thread safe way. As they can be called on any thread. So any state accessed needs to be
 *     final or thread safe data structures.</li>
 *     <li><b>BlockNotificationHandler methods</b> - These are always called on the same single dedicated thread for
 *     this handler.</li>
 *     <li><b>BlockItemHandler methods</b> - These are always called on the same single dedicated thread for this
 *     handler.It should do all work on that thread and block it till work is done. By doing that it will provide back
 *     pressure into the messaging system. This is important as it stops the messaging system running ahead of the
 *     plugin resulting in missing block item chucks. If this plugin can not keep up with the incoming block items rate,
 *     the messaging system will provide back pressure through the provider to the consensus nodes pushing block items
 *     to the block node.</li>
 * </ul>
 * <h2>Unverified Blocks</h2>
 * The storage of unverified blocks is done in a configured directory. That directory can be in temporary storage as it
 * is not required to be persistent. On start-up, the plugin will delete any files in the unverified directory. This is
 * done to clean up any files that are left over from a previous run. The unverified directory does not have any special
 * subdirectory structure and blocks are just stored as individual files directly in that directory. This is fine as
 * there should never be more than a few unverified blocks at a time. The unverified blocks are stored in a compressed
 * format so they are ready to just be moved to the live directory when they are verified. The compression type is
 * configured and can be changed at any time. The compression level is also configured and can be changed at any time.
 */
public final class BlockFileRecentPlugin implements BlockProviderPlugin, BlockNotificationHandler {
    /** The maximum limit of blocks to be deleted in a single retention run. */
    private static final int RETENTION_ROUND_LIMIT = 1_000;
    /** The logger for this class. */
    private final System.Logger LOGGER = System.getLogger(getClass().getName());
    /** The configuration for this plugin. */
    private FilesRecentConfig config;
    /** The block messaging facility. */
    private BlockMessagingFacility blockMessaging;
    /** The set of available blocks. */
    private final ConcurrentLongRangeSet availableBlocks = new ConcurrentLongRangeSet();
    /** Running total of bytes stored in the recent tier */
    private final AtomicLong totalBytesStored = new AtomicLong(0);
    /** The Storage Retention Policy Threshold */
    private long blockRetentionThreshold;
    // Metrics
    /** Counter for blocks written to the recent tier */
    private Counter blocksWrittenCounter;
    /** Counter for blocks read from the recent tier */
    private Counter blocksReadCounter;
    /** Counter for blocks deleted from the recent tier */
    private Counter blocksDeletedCounter;
    /** Counter for blocks deleted from the recent tier that failed */
    private Counter blocksDeletedFailedCounter;
    /** Gauge for the number of blocks stored in the recent tier */
    private LongGauge blocksStoredGauge;
    /** Gauge for the total bytes stored in the recent tier */
    private LongGauge bytesStoredGauge;
    /** Persistence Writing total time in nanos **/
    private Counter persistenceLatencyNs;

    /**
     * Default constructor for the plugin. This is used for normal service loading.
     */
    public BlockFileRecentPlugin() {}

    /**
     * Constructor for the plugin. This is used for testing.
     *
     * @param config the config to use
     */
    BlockFileRecentPlugin(FilesRecentConfig config) {
        this.config = config;
    }

    // ==== BlockProviderPlugin Methods ================================================================================

    /**
     * {@inheritDoc}
     */
    @NonNull
    @Override
    public List<Class<? extends Record>> configDataTypes() {
        return List.of(FilesRecentConfig.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(final BlockNodeContext context, final ServiceBuilder serviceBuilder) {
        // load config if not already set by test
        if (this.config == null) {
            this.config = context.configuration().getConfigData(FilesRecentConfig.class);
        }
        blockRetentionThreshold = config.blockRetentionThreshold();
        this.blockMessaging = context.blockMessaging();
        // Initialize metrics
        initMetrics(context.metrics());
        // create plugin data root directory if it does not exist
        try {
            Files.createDirectories(config.liveRootPath());
        } catch (final IOException e) {
            LOGGER.log(ERROR, "Could not create root directory", e);
            context.serverHealth().shutdown(name(), "Could not create root directory");
        }
        // we want to listen to block notifications and to know when blocks are verified
        context.blockMessaging().registerBlockNotificationHandler(this, false, "BlocksFilesRecent");
        // scan file system to find the oldest and newest blocks
        // TODO this can be way for efficient, very brute force at the moment
        nestedDirectoriesAllBlockNumbers(config.liveRootPath(), config.compression())
                .forEach(blockNumber -> {
                    availableBlocks.add(blockNumber);
                    // Initialize total bytes stored counter
                    try {
                        Path blockFilePath = BlockFile.nestedDirectoriesBlockFilePath(
                                config.liveRootPath(), blockNumber, config.compression(), config.maxFilesPerDir());
                        if (Files.exists(blockFilePath)) {
                            totalBytesStored.addAndGet(Files.size(blockFilePath));
                        }
                    } catch (IOException e) {
                        LOGGER.log(INFO, "Failed to get size of block file for block " + blockNumber, e);
                    }
                });

        // Register gauge updater
        context.metrics().addUpdater(this::updateGauges);
    }

    /**
     * Initialize metrics for this plugin. vb
     */
    private void initMetrics(final Metrics metrics) {
        blocksWrittenCounter = metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "files_recent_blocks_written")
                .withDescription("Blocks written to files.recent provider"));

        blocksReadCounter = metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "files_recent_blocks_read")
                .withDescription("Blocks read from files.recent provider"));

        blocksDeletedCounter = metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "files_recent_blocks_deleted")
                .withDescription("Blocks deleted from files.recent provider"));

        blocksDeletedFailedCounter =
                metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "files_recent_blocks_deleted_failed")
                        .withDescription("Blocks failed deletion from files.recent provider"));

        blocksStoredGauge = metrics.getOrCreate(new LongGauge.Config(METRICS_CATEGORY, "files_recent_blocks_stored")
                .withDescription("Blocks stored in files.recent provider"));

        bytesStoredGauge = metrics.getOrCreate(new LongGauge.Config(METRICS_CATEGORY, "files_recent_total_bytes_stored")
                .withDescription("Bytes stored in files.recent provider"));

        persistenceLatencyNs =
                metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "files_recent_persistence_time_latency_ns")
                        .withDescription("Total time spent persisting blocks in files.recent provider in nanoseconds"));
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
     * {@inheritDoc}
     */
    @Override
    public int defaultPriority() {
        return 2_000;
    }

    /**
     * {@inheritDoc}
     * <p>
     * We only provide read access to verified blocks.
     */
    @Override
    public BlockAccessor block(final long blockNumber) {
        if (availableBlocks.contains(blockNumber)) {
            // we should have this block stored so go file the file and return accessor to it
            final Path verifiedBlockPath = BlockFile.nestedDirectoriesBlockFilePath(
                    config.liveRootPath(), blockNumber, config.compression(), config.maxFilesPerDir());
            if (Files.exists(verifiedBlockPath)) {
                // we have the block so return it
                blocksReadCounter.increment();
                return new BlockFileBlockAccessor(verifiedBlockPath, config.compression(), blockNumber);
            } else {
                LOGGER.log(
                        WARNING,
                        "Failed to find verified block file: {0}",
                        verifiedBlockPath.toAbsolutePath().toString());
            }
        }
        return null;
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
     * <p>
     * This method is called when a block verification notification is received. It is called on the block notification
     * thread.
     */
    @Override
    public void handleVerification(VerificationNotification notification) {
        try {
            final long startTime = System.nanoTime();
            LOGGER.log(
                    TRACE,
                    "Persistence Handle verification started for block: %d".formatted(notification.blockNumber()));
            if (notification != null && notification.success()) {
                // write the block to the live path and send notification of block persisted
                writeBlockToLivePath(notification.block(), notification.blockNumber(), notification.source());
                // we do a round of retention only if the retention threshold is set to
                // a positive value, otherwise we do not run it
                if (blockRetentionThreshold > 0L) {
                    // after writing the block, we need to trigger the retention policy
                    // calculate excess
                    final long excess = availableBlocks.size() - blockRetentionThreshold;
                    final long firstBlockToDelete = availableBlocks.min();
                    // determine how many blocks to delete, up to the retention round limit
                    final long blocksToDelete = Math.min(excess, RETENTION_ROUND_LIMIT);
                    // delete the blocks from the lowest block number up to calculated max
                    // gaps will be retried on subsequent retention runs, which are very
                    // frequent
                    final long lastBlockToDelete = firstBlockToDelete + blocksToDelete;
                    for (long i = firstBlockToDelete; i < lastBlockToDelete; i++) {
                        delete(i);
                    }
                }
            }
            final long endTime = System.nanoTime() - startTime;
            persistenceLatencyNs.add(endTime);
            LOGGER.log(
                    TRACE,
                    "Persistence Handle verification finished for block: %d, and it took %d ns to complete"
                            .formatted(notification.blockNumber(), endTime));
        } catch (final RuntimeException e) {
            final String message = "Failed to handle verification notification due to %s".formatted(e);
            LOGGER.log(WARNING, message, e);
        }
    }

    // ==== Action Methods =============================================================================================

    /**
     * Directly write a block to verified storage. This is used when the block is already verified when we receive it.
     *
     * @param block the block to write
     * @param blockNumber the block number of the block to write
     */
    private void writeBlockToLivePath(final BlockUnparsed block, final long blockNumber, final BlockSource source) {
        final BlockSource effectiveSource = source == null ? UNKNOWN : source;
        if (block != null && block.blockItems() != null && !block.blockItems().isEmpty()) {
            if (block.blockItems().getFirst().hasBlockHeader()) {
                final BlockHeader header = getBlockHeader(block);
                final long headerNumber = header == null ? -1 : header.number();
                if (headerNumber != blockNumber) {
                    LOGGER.log(
                            WARNING,
                            "Block number mismatch between notification {0} and block header {1}, not writing block",
                            blockNumber,
                            headerNumber);
                    sendBlockNotification(blockNumber, false, effectiveSource);
                } else {
                    final Path verifiedBlockPath = BlockFile.nestedDirectoriesBlockFilePath(
                            config.liveRootPath(), blockNumber, config.compression(), config.maxFilesPerDir());
                    createDirectoryOrFail(verifiedBlockPath);
                    writeBlockOrFail(block, blockNumber, effectiveSource, verifiedBlockPath);
                }
            } else {
                LOGGER.log(INFO, "Block {0} has no block header, cannot write to live path", blockNumber);
                sendBlockNotification(blockNumber, false, effectiveSource);
            }
        } else {
            sendBlockNotification(blockNumber, false, effectiveSource);
        }
    }

    private void sendBlockNotification(final long number, final boolean succeeded, final BlockSource source) {
        blockMessaging.sendBlockPersisted(new PersistedNotification(number, succeeded, defaultPriority(), source));
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

    private void writeBlockOrFail(
            final BlockUnparsed block, final long blockNumber, final BlockSource source, final Path verifiedBlockPath) {
        try (final WritableStreamingData streamingData = new WritableStreamingData(new BufferedOutputStream(
                config.compression().wrapStream(Files.newOutputStream(verifiedBlockPath)), 16384))) {
            BlockUnparsed.PROTOBUF.write(block, streamingData);
            streamingData.flush();
            streamingData.close();
            // Add the size of the newly written file to our total bytes counter
            totalBytesStored.addAndGet(Files.size(verifiedBlockPath));
            LOGGER.log(TRACE, "Wrote verified block {0} to file {1}", blockNumber, verifiedBlockPath.toAbsolutePath());
            // update the oldest and newest verified block numbers
            availableBlocks.add(blockNumber);
            // Send block persisted notification
            final BlockSource effectiveSource = source == null ? UNKNOWN : source;
            sendBlockNotification(blockNumber, true, effectiveSource);
            // Increment blocks written counter
            blocksWrittenCounter.increment();
        } catch (final IOException e) {
            final String message = "Failed to write file for block %d due to %s".formatted(blockNumber, e);
            LOGGER.log(WARNING, message, e);
            sendBlockNotification(blockNumber, false, source);
        }
    }

    private void createDirectoryOrFail(final Path verifiedBlockPath) {
        try {
            // create parent directory if it does not exist
            Files.createDirectories(verifiedBlockPath.getParent());
        } catch (final IOException e) {
            final String message = "Failed to create directories for path %s due to %s"
                    .formatted(verifiedBlockPath.toAbsolutePath(), e);
            LOGGER.log(WARNING, message, e);
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Delete a block file from the live path. This is used when the block is no longer needed.
     */
    private void delete(final long blockNumber) {
        // compute file path for the block
        final Path blockFilePath = BlockFile.nestedDirectoriesBlockFilePath(
                config.liveRootPath(), blockNumber, config.compression(), config.maxFilesPerDir());
        if (Files.exists(blockFilePath)) {
            // log we are deleting the block file
            deleteBlockCleanly(blockNumber, blockFilePath);
            // clean up any empty parent directories up to the base directory
            Path parentDir = blockFilePath.getParent();
            while (parentDir != null && !parentDir.equals(config.liveRootPath())) {
                parentDir = removeFolderAndGetParent(parentDir);
            }
        }
    }

    private Path removeFolderAndGetParent(Path parentDir) {
        try (final Stream<Path> filesList = Files.list(parentDir)) {
            if (filesList.findAny().isEmpty()) {
                // we did not find any files in the directory, so delete it
                Files.deleteIfExists(parentDir);
                // move up to the parent directory
                return parentDir.getParent();
            }
        } catch (final IOException e) {
            final String messageFormat = "Failed to remove parent directory `%s`";
            LOGGER.log(INFO, messageFormat.formatted(parentDir), e);
        }
        return null;
    }

    private static final String DELETE_MESSAGE = "%s deleting block file %s";

    private void deleteBlockCleanly(final long blockNumber, final Path blockFilePath) {
        try {
            // Get file size before deleting to update total bytes stored
            final long fileSize = Files.size(blockFilePath);
            // delete the block file and update counters
            final boolean deleted = Files.deleteIfExists(blockFilePath);
            if (deleted) {
                LOGGER.log(TRACE, DELETE_MESSAGE.formatted("Success", blockFilePath));
            } else {
                LOGGER.log(INFO, DELETE_MESSAGE.formatted("File missing", blockFilePath));
            }
            availableBlocks.remove(blockNumber);
            blocksDeletedCounter.increment();
            totalBytesStored.addAndGet(-fileSize);
        } catch (final IOException e) {
            LOGGER.log(INFO, DELETE_MESSAGE.formatted("Failure", blockFilePath), e);
            blocksDeletedFailedCounter.increment();
        }
    }
}
