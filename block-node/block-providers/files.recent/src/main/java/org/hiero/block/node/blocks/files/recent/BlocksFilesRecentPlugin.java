// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.recent;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.WARNING;
import static org.hiero.block.node.base.BlockFile.nestedDirectoriesAllBlockNumbers;

import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
import com.swirlds.metrics.api.Counter;
import com.swirlds.metrics.api.LongGauge;
import com.swirlds.metrics.api.Metrics;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.System.Logger.Level;
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
public final class BlocksFilesRecentPlugin implements BlockProviderPlugin, BlockNotificationHandler {
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
    private long retentionPolicyThreshold;
    // Metrics
    /** Counter for blocks written to the recent tier */
    private Counter blocksWrittenCounter;
    /** Counter for blocks read from the recent tier */
    private Counter blocksReadCounter;
    /** Counter for blocks deleted from the recent tier */
    private Counter blocksDeletedCounter;
    /** Gauge for the number of blocks stored in the recent tier */
    private LongGauge blocksStoredGauge;
    /** Gauge for the total bytes stored in the recent tier */
    private LongGauge bytesStoredGauge;

    /**
     * Default constructor for the plugin. This is used for normal service loading.
     */
    public BlocksFilesRecentPlugin() {}

    /**
     * Constructor for the plugin. This is used for testing.
     *
     * @param config the config to use
     */
    BlocksFilesRecentPlugin(FilesRecentConfig config) {
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
        this.retentionPolicyThreshold = this.config.retentionPolicyThreshold();
        this.blockMessaging = context.blockMessaging();
        // Initialize metrics
        initMetrics(context.metrics());
        // create plugin data root directory if it does not exist
        try {
            Files.createDirectories(config.liveRootPath());
        } catch (final IOException e) {
            LOGGER.log(Level.ERROR, "Could not create root directory", e);
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
                        LOGGER.log(WARNING, "Failed to get size of block file for block " + blockNumber, e);
                    }
                });

        // Register gauge updater
        context.metrics().addUpdater(this::updateGauges);
    }

    /**
     * Initialize metrics for this plugin. vb
     */
    private void initMetrics(Metrics metrics) {
        blocksWrittenCounter = metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "files_recent_blocks_written")
                .withDescription("Blocks written to files.recent provider"));

        blocksReadCounter = metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "files_recent_blocks_read")
                .withDescription("Blocks read from files.recent provider"));

        blocksDeletedCounter = metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "files_recent_blocks_deleted")
                .withDescription("Blocks deleted from files.recent provider"));

        blocksStoredGauge = metrics.getOrCreate(new LongGauge.Config(METRICS_CATEGORY, "files_recent_blocks_stored")
                .withDescription("Blocks stored in files.recent provider"));

        bytesStoredGauge = metrics.getOrCreate(new LongGauge.Config(METRICS_CATEGORY, "files_recent_total_bytes_stored")
                .withDescription("Bytes stored in files.recent provider"));
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
                        Level.WARNING,
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
        // write the block to the live path and send notification of block persisted
        writeBlockToLivePath(notification.block(), notification.blockNumber());
    }

    /**
     * {@inheritDoc}
     * <p>
     * This method is called when a block persisted notification is received. It is called on the block notification
     * thread. We will get notifications from ourselves and other plugins. We are looking for notifications from other
     * plugins with lower priority that have stored blocks so we can delete ours as we do not need to store them anymore
     * if another plugin has them.
     *
     * @param notification the block persisted notification to handle
     */
    @Override
    public void handlePersisted(PersistedNotification notification) {
        if (notification.blockProviderPriority() < defaultPriority()) {
            final long totalStored = availableBlocks.size();
            long excess = totalStored - retentionPolicyThreshold;
            availableBlocks.stream().sorted().limit(excess).forEach(this::delete);
        }
        // @todo(1268) Not sure if we still would want to filter by notification provider priority.
        //    In theory the new retention logic no longer supports that. We no longer delete the
        //    range of the notification. More than that, if we have only this plugin that will fire
        //    such notifications, effectively we will never cleanup. I think that should also be
        //    changed. Another way to do this is to delete in the verification notification method.
        //    In theory from there we get the new data and the idea of the policy is to be bound to
        //    block count and delete only if new data arrives. Maybe we could drop the handle
        //    persisted here all together? But we have to be careful because a big delete might
        //    take a long while and we do not want to hang the notification thread too long cause
        //    persistence relies on it (verification notification method). Am I missing something?
        //    On another note, if we do delete here, we limit the possibility of the retention
        //    policy to be applied only if there is something that pushes the PersistedNotification.
        //    This is an assumption that we should probably not make, after all, the retention
        //    policy is applied when new data comes in. This is not the place for that.
    }

    // ==== Action Methods =============================================================================================

    /**
     * Directly write a block to verified storage. This is used when the block is already verified when we receive it.
     *
     * @param block       the block to write
     * @param blockNumber the block number of the block to write
     */
    private void writeBlockToLivePath(final BlockUnparsed block, final long blockNumber) {
        final Path verifiedBlockPath = BlockFile.nestedDirectoriesBlockFilePath(
                config.liveRootPath(), blockNumber, config.compression(), config.maxFilesPerDir());
        try {
            // create parent directory if it does not exist
            Files.createDirectories(verifiedBlockPath.getParent());
        } catch (final IOException e) {
            LOGGER.log(
                    System.Logger.Level.ERROR,
                    "Failed to create directories for path: {0} error: {1}",
                    verifiedBlockPath.toAbsolutePath().toString(),
                    e);
            throw new UncheckedIOException(e);
        }
        try (final WritableStreamingData streamingData = new WritableStreamingData(new BufferedOutputStream(
                config.compression().wrapStream(Files.newOutputStream(verifiedBlockPath)), 1024 * 1024))) {
            BlockUnparsed.PROTOBUF.write(block, streamingData);
            streamingData.flush();

            // Add the size of the newly written file to our total bytes counter
            totalBytesStored.addAndGet(Files.size(verifiedBlockPath));

            LOGGER.log(
                    Level.DEBUG,
                    "Wrote verified block: {0} to file: {1}",
                    blockNumber,
                    verifiedBlockPath.toAbsolutePath().toString());
            // update the oldest and newest verified block numbers
            availableBlocks.add(blockNumber);
            // Send block persisted notification
            blockMessaging.sendBlockPersisted(new PersistedNotification(blockNumber, blockNumber, defaultPriority()));
            // Increment blocks written counter
            blocksWrittenCounter.increment();
        } catch (final IOException e) {
            LOGGER.log(
                    System.Logger.Level.ERROR,
                    "Failed to create verified file for block: {0}, error: {1}",
                    blockNumber,
                    e);
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Delete a block file from the live path. This is used when the block is no longer needed, determined by the
     * retention policy threshold.
     */
    private void delete(long blockNumber) {
        // compute file path for the block
        final Path blockFilePath = BlockFile.nestedDirectoriesBlockFilePath(
                config.liveRootPath(), blockNumber, config.compression(), config.maxFilesPerDir());
        try {
            // log we are deleting the block file
            LOGGER.log(DEBUG, "Deleting block file: " + blockFilePath);

            // Get file size before deleting to update total bytes stored
            if (Files.exists(blockFilePath)) {
                long fileSize = Files.size(blockFilePath);
                // delete the block file and update counters
                if (Files.deleteIfExists(blockFilePath)) {
                    availableBlocks.remove(blockNumber);
                    blocksDeletedCounter.increment();
                    totalBytesStored.addAndGet(-fileSize);
                }
            }

            // clean up any empty parent directories up to the base directory
            Path parentDir = blockFilePath.getParent();
            while (parentDir != null && !parentDir.equals(config.liveRootPath())) {
                try (final Stream<Path> filesList = Files.list(parentDir)) {
                    if (filesList.findAny().isPresent()) {
                        break;
                    }
                } catch (final IOException e) {
                    LOGGER.log(WARNING, "Failed to list files in directory: " + parentDir, e);
                    break; // If we cannot list, we cannot assert an empty parent directory
                }
                // we did not find any files in the directory, so delete it
                Files.deleteIfExists(parentDir);
                // move up to the parent directory
                parentDir = parentDir.getParent();
            }
        } catch (final IOException e) {
            LOGGER.log(WARNING, "Failed to delete block file: " + blockFilePath, e);
            throw new UncheckedIOException(e);
            // @todo(1268) if we fail here, should we really throw or just log and continue?
            //   a retry will essentially happen on the next persisted notification.
            //   With the current implementation, this method remains problematic because
            //   other IO happens asynchronously in the background (new persists) which can lead
            //   to exceptions during the delete of the parent(s). Do we have a better approach to
            //   clean up directories?
        }
    }
}
