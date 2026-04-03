// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.archive;

import static java.lang.System.Logger.Level.TRACE;
import static java.lang.System.Logger.Level.WARNING;
import static java.util.Objects.requireNonNull;
import static org.hiero.block.node.cloud.archive.BlockUploadTask.*;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
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
    private boolean handlerRegistered = false;

    /// The [Future] for the currently active [BlockUploadTask], or `null` when no upload is
    /// in progress.  Checked on every [handleVerification] call via [Future#isDone()] to detect
    /// completion and surface any exception before starting the next task.  Canceled (with
    /// interruption) by [stop()] to abort a mid-batch upload.
    Future<UploadResult> currentUploadFuture = null;
    /// The [BlockingQueue] shared with the active [BlockUploadTask].  The plugin enqueues blocks
    /// in ascending block-number order via [drainPendingToQueue]; the task's virtual thread
    /// consumes them one by one via [BlockingQueue#take].  Cleared (not replaced) each time a
    /// new task starts, so the same instance is reused across upload groups.
    final BlockingQueue<BlockUnparsed> currentBlockQueue = new LinkedBlockingQueue<>();

    private long currentGroupStart = -1;
    private long currentGroupSize = -1;

    // Staging area for within-group blocks that arrived before their predecessor(s).
    // Blocks are moved to `currentBlockQueue` in ascending block-number order by drainPendingToQueue().
    final SortedMap<Long, BlockUnparsed> currentGroupPending = new ConcurrentSkipListMap<>();
    // The block number that drainPendingToQueue() will enqueue next.
    private long nextBlockToQueue = -1;

    // Keep track of blocks that were received out of range for the previous task. They will be replayed when the next
    // task starts
    final Map<Long, BlockUnparsed> blocksStash = new ConcurrentSkipListMap<>();

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
        final List<String> violations = validateConfig();
        if (!violations.isEmpty()) {
            LOGGER.log(
                    WARNING,
                    "Archive cloud storage plugin is not active because of empty values for the following configurations: {0}",
                    String.join(", ", violations));
        } else {
            // register to listen to block notifications if config is valid
            context.blockMessaging().registerBlockNotificationHandler(this, false, "Archive Cloud Storage");
            handlerRegistered = true;
        }
    }

    private List<String> validateConfig() {
        List<String> violations = new ArrayList<>();
        if (config.endpointUrl().isEmpty()) {
            violations.add("endpoint URL");
        }
        if (config.regionName().isEmpty()) {
            violations.add("region name");
        }
        if (config.accessKey().isEmpty()) {
            violations.add("access key");
        }
        if (config.secretKey().isEmpty()) {
            violations.add("secret key");
        }
        if (config.bucketName().isEmpty()) {
            violations.add("bucket name");
        }
        return violations;
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
        if (notification == null || !notification.success()) {
            if (notification == null) {
                LOGGER.log(TRACE, "Received null verification notification, ignoring");
            } else {
                LOGGER.log(TRACE, "Received failed verification notification, ignoring");
            }
            return;
        }

        final long blockNumber = notification.blockNumber();

        // If the active task has completed, surface any exception and clean up.
        // Returns true when the task was cancelled (i.e. stop() was called): no new task should be
        // started in that case since the plugin is shutting down.
        if (handleCompletedUpload()) {
            return;
        }

        // Start a new upload task when there is none active
        if (currentUploadFuture == null) {
            currentGroupSize = Math.powExact(10, config.groupingLevel());
            currentGroupStart = (blockNumber / currentGroupSize) * currentGroupSize;
            nextBlockToQueue = currentGroupStart;
            currentBlockQueue.clear();
            currentUploadFuture = virtualThreadExecutor.submit(new BlockUploadTask(
                    config, context.blockMessaging(), currentGroupStart, currentGroupSize, currentBlockQueue));
            tryReplayStash();
        }

        // Stage the block; if it belongs to the active task's range, attempt to drain consecutive
        // blocks into the queue. Out-of-range blocks go to the cross-group stash as before.
        if (blockNumber >= currentGroupStart && blockNumber < currentGroupStart + currentGroupSize) {
            currentGroupPending.put(blockNumber, notification.block());
            drainPendingToQueue();
        } else {
            blocksStash.put(blockNumber, notification.block());
            LOGGER.log(TRACE, "Block number {0} is out of range for current upload task, stashing", blockNumber);
        }
    }

    /// Checks whether the active upload task has finished and, if so, clears the associated state.
    ///
    /// @return `true` when the task was cancelled (i.e., [stop()] was called), signalling that
    ///         [handleVerification] should stop and not start a new upload task
    private boolean handleCompletedUpload() {
        if (currentUploadFuture == null || !currentUploadFuture.isDone()) {
            return false;
        }
        try {
            if (currentUploadFuture.isCancelled()) {
                LOGGER.log(TRACE, "Block upload task was cancelled");
                currentUploadFuture = null;
                currentGroupPending.clear();
                return true;
            }
            final UploadResult uploadResult = currentUploadFuture.get();
            if (uploadResult == UploadResult.FAILED) {
                LOGGER.log(WARNING, "Block upload task failed");
                // TODO(1166) Handle properly S3 upload failure
            }
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException _) {
            LOGGER.log(WARNING, "Block upload task failed");
            // TODO(1166) Handle properly S3 upload failure
        }
        currentUploadFuture = null;
        currentGroupPending.clear();
        return false;
    }

    /// Moves consecutive blocks from [currentGroupPending] into [currentBlockQueue], starting
    /// from [nextBlockToQueue].  Stops as soon as there is a gap (the next expected block has not
    /// arrived yet).
    private void drainPendingToQueue() {
        while (currentGroupPending.containsKey(nextBlockToQueue)) {
            currentBlockQueue.offer(currentGroupPending.remove(nextBlockToQueue));
            nextBlockToQueue++;
        }
    }

    /// Moves all stashed blocks that fall within the current task's range into [currentGroupPending],
    /// then calls [drainPendingToQueue] to enqueue them in order.
    private void tryReplayStash() {
        final Iterator<Map.Entry<Long, BlockUnparsed>> it =
                blocksStash.entrySet().iterator();
        while (it.hasNext()) {
            final Map.Entry<Long, BlockUnparsed> entry = it.next();
            if (entry.getKey() >= currentGroupStart && entry.getKey() < currentGroupStart + currentGroupSize) {
                currentGroupPending.put(entry.getKey(), entry.getValue());
                it.remove();
            }
        }
        drainPendingToQueue();
    }

    @Override
    public void stop() {
        // Cancelling with true interrupts the virtual thread, causing blockQueue.take() to throw
        // InterruptedException inside BlockUploadTask.call()
        if (currentUploadFuture != null) {
            currentUploadFuture.cancel(true);
        }
        currentGroupPending.clear();
        currentBlockQueue.clear();
        if (handlerRegistered) {
            context.blockMessaging().unregisterBlockNotificationHandler(this);
        }
        LOGGER.log(TRACE, "Archive cloud storage plugin stopped");
    }
}
