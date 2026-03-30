// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.archive;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.TRACE;
import static java.util.Objects.requireNonNull;

import com.hedera.bucky.S3ClientException;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.cloud.archive.BlockArchiveTask.ArchiveResult;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;

/// A block node plugin that stores verified blocks in cloud storage aggregated into large `tar` archives.
///
/// On each [org.hiero.block.node.spi.blockmessaging.VerificationNotification], a
/// `SingleBlockStoreTask` compresses the block with ZStandard and passes it to the active
/// `BlockArchiveTask`, which streams the data directly to the remote `tar` file in block-number
/// order. No local staging occurs. A
/// [org.hiero.block.node.spi.blockmessaging.PersistedNotification] is published per block
/// only after durable remote storage is confirmed.
///
/// When blocks arrive out of order or an operation fails, the plugin initiates recovery via an
/// `ArchiveRecoveryTask` that consolidates any temporary `tar` files and resumes normal archiving.
public class ArchiveCloudStoragePlugin implements BlockNodePlugin, BlockNotificationHandler {

    /// The logger for this class.
    private static final System.Logger LOGGER = System.getLogger(ArchiveCloudStoragePlugin.class.getName());

    private BlockNodeContext context;
    private ArchiveCloudStorageConfig config;
    private ExecutorService virtualThreadExecutor;

    // The active task that is uploading blocks to the cloud storage
    BlockArchiveTask liveBlockArchiveTask = null;
    // Keep track of blocks that were received out of range for the previous task. They will be replayed when the next
    // task starts
    TreeMap<Long, BlockUnparsed> blocksStash = new TreeMap<>();

    /// {@inheritDoc}
    @Override
    @NonNull
    public List<Class<? extends Record>> configDataTypes() {
        return List.of(ArchiveCloudStorageConfig.class);
    }

    /// {@inheritDoc}
    @Override
    public void init(BlockNodeContext context, ServiceBuilder serviceBuilder) {
        this.context = requireNonNull(context);
        this.config = context.configuration().getConfigData(ArchiveCloudStorageConfig.class);
        // register to listen to block notifications
        context.blockMessaging().registerBlockNotificationHandler(this, false, "Archive Cloud Storage");
    }

    /// {@inheritDoc}
    @Override
    public void start() {
        virtualThreadExecutor = context.threadPoolManager().getVirtualThreadExecutor();
        LOGGER.log(TRACE, "Archive cloud storage plugin started");
    }

    /// {@inheritDoc}
    @Override
    public void handleVerification(VerificationNotification notification) {
        if (liveBlockArchiveTask == null) {
            // If we don't have a live archive task, create and start one. This may happen in two cases: the plugin
            // has just started and has received its first block, or the previous archive task has completed and now it
            // is time for uploading the next group of blocks
            final long groupSize = Math.powExact(10, config.groupingLevel());
            final long startBlock = (notification.blockNumber() / groupSize) * groupSize;
            liveBlockArchiveTask = new LiveBlockArchiveTask(config, context.blockMessaging(), startBlock, groupSize);
            try {
                liveBlockArchiveTask.init();
            } catch (S3ClientException | IOException e) {
                LOGGER.log(DEBUG, "Failed to initialize live block archive task", e);
                // Try to initialize the task again with next block
                liveBlockArchiveTask = null;
                context.blockMessaging()
                        .sendBlockPersisted(new PersistedNotification(
                                notification.blockNumber(), false, 1_000, BlockSource.CLOUD_ARCHIVE));
                return;
            }
            virtualThreadExecutor.submit(liveBlockArchiveTask);

            tryReplayStash();
        }

        // Submit the block to the archive task. Should be very quick, as the task is running in a different thread
        // and actual processing happens in the background.
        // liveBlockArchiveTask may be null here if the stash replay above already completed the task.
        if (liveBlockArchiveTask == null) {
            blocksStash.put(notification.blockNumber(), notification.block());
            return;
        }
        final ArchiveResult result = liveBlockArchiveTask.submit(notification.block(), notification.blockNumber());

        if (result == ArchiveResult.FINISHED) {
            // We have submitted all blocks for the current range. Time to create a new task and dereference the current
            // one.
            // It should not be a candidate for garbage collection yet. It will be garbage collected once the task
            // completes and the executor service has released its handle.
            liveBlockArchiveTask = null;
            LOGGER.log(TRACE, "Live block archive task completed for range {}", notification.blockNumber());
        } else if (result == ArchiveResult.BLOCK_OUT_OF_RANGE) {
            // Keep the blocks that are not for the current range in a stash. "Replay" them when the next task starts.
            blocksStash.put(notification.blockNumber(), notification.block());
            LOGGER.log(TRACE, "Block number {} is out of range for current archive task", notification.blockNumber());
        }
    }

    /// Replay any stashed blocks that now fall within the new task's range.
    private void tryReplayStash() {
        // Use an explicit iterator so we can stop early if the task reaches FINISHED mid-replay
        // and avoid submitting further blocks to an exhausted task.
        final Iterator<Map.Entry<Long, BlockUnparsed>> it =
                blocksStash.entrySet().iterator();
        while (it.hasNext() && liveBlockArchiveTask != null) {
            final Map.Entry<Long, BlockUnparsed> entry = it.next();
            final ArchiveResult stashResult = liveBlockArchiveTask.submit(entry.getValue(), entry.getKey());
            if (stashResult != ArchiveResult.BLOCK_OUT_OF_RANGE) {
                it.remove();
                if (stashResult == ArchiveResult.FINISHED) {
                    liveBlockArchiveTask = null;
                    LOGGER.log(
                            TRACE, "Live block archive task completed during stash replay at block {}", entry.getKey());
                }
            }
        }
    }

    @Override
    public void stop() {
        // If there are any block uploads in progress, abort them
        if (liveBlockArchiveTask != null) {
            liveBlockArchiveTask.abort();
        }
        LOGGER.log(TRACE, "Archive cloud storage plugin stopped");
    }
}
