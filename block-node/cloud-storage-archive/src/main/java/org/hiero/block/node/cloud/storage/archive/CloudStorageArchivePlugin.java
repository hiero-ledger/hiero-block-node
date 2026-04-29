// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.archive;

import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.TRACE;
import static java.lang.System.Logger.Level.WARNING;
import static java.util.Objects.requireNonNull;
import static org.hiero.block.node.cloud.storage.archive.BlockUploadTask.UploadResult;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;

/// A block node plugin that stores verified blocks in cloud storage aggregated into `tar` archives.
///
/// On each [org.hiero.block.node.spi.blockmessaging.VerificationNotification], the block is
/// serialised into a tar entry and fed through a [BlockingQueue] to the active [BlockUploadTask],
/// which accumulates bytes in memory and flushes fixed-size parts to S3-compatible storage via a
/// multipart upload.  No local staging occurs.  A
/// [org.hiero.block.node.spi.blockmessaging.PersistedNotification] is published per block only
/// after durable remote storage is confirmed.
///
/// Blocks are grouped by a configurable power-of-ten range (see
/// [CloudStorageArchiveConfig#groupingLevel()]).  Within a group, out-of-order arrivals are held
/// in [currentGroupPending] and drained in block-number order once gaps are filled.  Blocks that
/// arrive before their group's task has started are held in [blocksStash] and replayed when the
/// relevant task is created.
public class CloudStorageArchivePlugin implements BlockNodePlugin, BlockNotificationHandler {

    /// The logger for this class.
    private static final System.Logger LOGGER = System.getLogger(CloudStorageArchivePlugin.class.getName());

    /// The block node context, set during [init].
    private BlockNodeContext context;
    /// The plugin configuration, set during [init].
    private CloudStorageArchiveConfig config;
    /// Executor used to run each [BlockUploadTask] on a virtual thread.
    private ExecutorService virtualThreadExecutor;
    /// Whether the plugin configuration is valid.  Set during [init]; gates handler registration,
    /// startup recovery, and handler unregistration in [stop].
    private boolean configValid = false;

    /// The [Future] for the currently active [BlockUploadTask], or `null` when no upload is
    /// in progress.  Checked on every [handleVerification] call via [Future#isDone()] to detect
    /// completion and surface any exception before starting the next task.  Canceled (with
    /// interruption) by [stop()] to abort a mid-batch upload.
    Future<UploadResult> currentUploadFuture = null;
    /// The [BlockingQueue] shared with the active [BlockUploadTask].  The plugin enqueues
    /// [BlockWithSource] pairs in ascending block-number order via [drainPendingToQueue]; the
    /// task's virtual thread consumes them one by one via [BlockingQueue#take].  Replaced each
    /// time a new task starts.
    BlockingQueue<BlockWithSource> currentBlockQueue = new LinkedBlockingQueue<>();

    /// The [Future] for the [StartupRecoveryTask] submitted in [start()], or `null` once the
    /// result has been consumed by [completeRecovery()].  While non-null, all incoming verified
    /// blocks are routed to [blocksStash] and processing of the active upload task is deferred.
    private Future<RecoveryResult> recoveryFuture = null;

    /// The first block number of the range owned by the active [BlockUploadTask], or `-1` before
    /// the first task is started.
    private long currentGroupStart = -1;
    /// The number of blocks in the range owned by the active [BlockUploadTask], or `-1` before
    /// the first task is started.  Always a power of ten (see [CloudStorageArchiveConfig#groupingLevel()]).
    private long currentGroupSize = -1;

    // Staging area for within-group blocks that arrived before their predecessor(s).
    // Blocks are moved to `currentBlockQueue` in ascending block-number order by drainPendingToQueue().
    SortedMap<Long, BlockWithSource> currentGroupPending = new ConcurrentSkipListMap<>();
    // The block number that drainPendingToQueue() will enqueue next.
    private long nextBlockToQueue = -1;

    // Keep track of blocks that were received out of range for the previous task. They will be replayed when the
    // next task starts.
    final Map<Long, BlockWithSource> blocksStash = new ConcurrentSkipListMap<>();

    /// {@inheritDoc}
    @Override
    @NonNull
    public List<Class<? extends Record>> configDataTypes() {
        return List.of(CloudStorageArchiveConfig.class);
    }

    /// {@inheritDoc}
    @Override
    public void init(BlockNodeContext context, ServiceBuilder serviceBuilder) {
        this.context = requireNonNull(context);
        this.config = context.configuration().getConfigData(CloudStorageArchiveConfig.class);
        final List<String> violations = config.validate();
        if (!violations.isEmpty()) {
            // Should be reported to a health facility once we have it
            LOGGER.log(
                    WARNING,
                    "Cloud storage archive plugin is not active because of empty values for the following configurations: {0}",
                    String.join(", ", violations));
        } else {
            // register to listen to block notifications if config is valid
            context.blockMessaging().registerBlockNotificationHandler(this, false, "Cloud Storage Archive");
            configValid = true;
        }
    }

    /// {@inheritDoc}
    @Override
    public void start() {
        virtualThreadExecutor = context.threadPoolManager().getVirtualThreadExecutor();
        // Skip recovery when config is invalid: the handler is not registered, so the result
        // would never be consumed and the task would fail immediately with a bad-config S3 error.
        if (configValid) {
            recoveryFuture = virtualThreadExecutor.submit(new StartupRecoveryTask(config));
        }
        LOGGER.log(TRACE, "Cloud storage archive plugin started");
    }

    /// {@inheritDoc}
    @Override
    public void handleVerification(VerificationNotification notification) {
        try {
            if (notification != null && notification.success() && notification.block() != null) {
                // If the task was cancelled (i.e. stop() was called), do not start a new task or process
                // the current block since the plugin is shutting down.
                if (!checkCompletedUpload()) {
                    // While recovery is running, stash every block.  currentGroupStart is still -1, so
                    // routeNotification sends it directly to blocksStash without any extra logic.
                    if (recoveryFuture != null && recoveryFuture.isDone()) {
                        completeRecovery();
                    }
                    final long blockNumber = notification.blockNumber();
                    // Start a new upload task when there is none active
                    if (currentUploadFuture == null && recoveryFuture == null) {
                        startNewUploadTask(blockNumber);
                    }
                    // Route the verified block; if it belongs to the active task's range, attempt to drain
                    // consecutive blocks into the queue. Out-of-range blocks go to the cross-group stash.
                    routeVerifiedBlock(blockNumber, notification);
                }
            } else {
                logInvalidOrFailedNotification(notification);
            }
        } catch (InterruptedException ignore) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            LOGGER.log(WARNING, "Could not complete uploading blocks to cloud archive storage", e);
            // TODO(1166) Handle properly block upload task failure
        }
    }

    /// Checks whether the active [BlockUploadTask] has finished and cleans up its state.
    /// Returns `true` if the task was cancelled, signalling the caller to skip further processing.
    private boolean checkCompletedUpload() throws ExecutionException, InterruptedException {
        boolean cancelled = false;
        if (currentUploadFuture != null && currentUploadFuture.isDone()) {
            if (currentUploadFuture.isCancelled()) {
                LOGGER.log(TRACE, "Block upload task was cancelled");
                cancelled = true;
            } else {
                final UploadResult uploadResult = currentUploadFuture.get();
                if (uploadResult == UploadResult.FAILED) {
                    LOGGER.log(WARNING, "Block upload task failed");
                    // TODO(1166) Handle properly block upload task failure
                }
                // Clear the future regardless of outcome so the next block starts a fresh task.
                currentUploadFuture = null;
                currentGroupPending = new ConcurrentSkipListMap<>();
            }
        }
        return cancelled;
    }

    /// Consumes the result of the [StartupRecoveryTask] and, when the prior S3 state is discovered,
    /// sets [currentGroupStart], creates a fresh [BlockingQueue], and submits a [BlockUploadTask]
    /// for the recovered group before replaying any blocks that arrived during recovery.
    ///
    /// When the result is a fresh start (`currentGroupStart == -1`), [currentGroupStart] remains
    /// `-1` and the next call to [handleVerification] will fall through to [startNewUploadTask]
    /// as normal.
    ///
    /// When the result carries an upload ID, [BlockUploadTask] is created with the result so
    /// that it resumes the existing upload and starts its block loop from
    /// [RecoveryResult#nextBlockNumber()] rather than the group start.
    private void completeRecovery() throws ExecutionException, InterruptedException {
        try {
            final RecoveryResult result = recoveryFuture.get();
            if (result.currentGroupStart() != -1) {
                currentGroupSize = Math.powExact(10, config.groupingLevel());
                currentGroupStart = result.currentGroupStart();
                nextBlockToQueue = result.uploadId() != null ? result.nextBlockNumber() : currentGroupStart;
                currentBlockQueue = new LinkedBlockingQueue<>();
                currentUploadFuture = virtualThreadExecutor.submit(new BlockUploadTask(
                        config,
                        context.blockMessaging(),
                        currentGroupStart,
                        currentGroupSize,
                        currentBlockQueue,
                        result.uploadId() != null ? result : null));
                tryReplayStash();
            }
            // No else: currentGroupStart == -1 means a fresh start with no prior S3 state.
            // currentGroupStart stays -1, so the calling handleVerification picks it up via startNewUploadTask.
        } catch (ExecutionException e) {
            LOGGER.log(TRACE, "Startup recovery task failed", e);
            throw e;
        } finally {
            recoveryFuture = null;
        }
    }

    /// Logs an INFO message explaining why the given [notification] was ignored.  Called only
    /// when the notification is `null`, carries a failed verification, or has a `null` block.
    private void logInvalidOrFailedNotification(VerificationNotification notification) {
        if (notification == null) {
            LOGGER.log(INFO, "Received null verification notification, ignoring");
        } else if (!notification.success()) {
            LOGGER.log(INFO, "Received failed verification notification, ignoring");
        } else {
            LOGGER.log(INFO, "Received verification notification with null block, ignoring");
        }
    }

    /// Initialises a new [BlockUploadTask] for the group that contains [blockNumber], submits it
    /// to [virtualThreadExecutor], and replays any previously stashed blocks that fall
    /// within the new group's range.
    private void startNewUploadTask(long blockNumber) {
        currentGroupSize = Math.powExact(10, config.groupingLevel());
        // Integer division truncates, so this rounds blockNumber down to the nearest group boundary.
        currentGroupStart = (blockNumber / currentGroupSize) * currentGroupSize;
        nextBlockToQueue = currentGroupStart;
        // Create a fresh queue rather than clearing the existing one to ensure complete separation
        // from the previous task, which still holds a reference to the old queue instance.
        currentBlockQueue = new LinkedBlockingQueue<>();
        currentUploadFuture = virtualThreadExecutor.submit(new BlockUploadTask(
                config, context.blockMessaging(), currentGroupStart, currentGroupSize, currentBlockQueue));
        tryReplayStash();
    }

    /// Routes [notification] for [blockNumber] to the appropriate destination.  If the block falls
    /// within the active task's range it is added to [currentGroupPending] and consecutive blocks
    /// are drained into [currentBlockQueue]; otherwise it is stashed in [blocksStash] for replay
    /// when the matching task is created.
    private void routeVerifiedBlock(long blockNumber, VerificationNotification notification) {
        final BlockWithSource blockWithSource = new BlockWithSource(notification.block(), notification.source());
        if (blockNumber >= currentGroupStart && blockNumber < currentGroupStart + currentGroupSize) {
            currentGroupPending.put(blockNumber, blockWithSource);
            drainPendingToQueue();
        } else {
            blocksStash.put(blockNumber, blockWithSource);
            LOGGER.log(TRACE, "Block number {0} is out of range for current upload task, stashing", blockNumber);
        }
    }

    /// Moves consecutive notifications from [currentGroupPending] into [currentBlockQueue], starting
    /// from [nextBlockToQueue].  Stops as soon as there is a gap (the next expected block has not
    /// arrived yet).
    private void drainPendingToQueue() {
        while (currentGroupPending.containsKey(nextBlockToQueue)) {
            final BlockWithSource nextBlock = currentGroupPending.remove(nextBlockToQueue);
            if (nextBlock != null) {
                currentBlockQueue.offer(nextBlock);
                nextBlockToQueue++;
            } else {
                LOGGER.log(
                        WARNING,
                        "Block number {0} is missing from queue with pending blocks, while it should have been there",
                        nextBlockToQueue);
            }
        }
    }

    /// Moves all stashed notifications that fall within the current task's range into [currentGroupPending],
    /// then calls [drainPendingToQueue] to enqueue them in order.
    private void tryReplayStash() {
        final List<Long> blockStashKeys = new ArrayList<>(blocksStash.keySet());
        final long currentGroupEnd = currentGroupStart + currentGroupSize;
        for (final long blockNumber : blockStashKeys) {
            if (blockNumber >= currentGroupStart && blockNumber < currentGroupEnd) {
                final BlockWithSource block = blocksStash.remove(blockNumber);
                currentGroupPending.put(blockNumber, block);
            }
        }
        drainPendingToQueue();
    }

    /// {@inheritDoc}
    @Override
    public void stop() {
        // Cancelling with true interrupts the virtual thread, causing blockQueue.take() to throw
        // InterruptedException inside BlockUploadTask.call()
        if (currentUploadFuture != null) {
            currentUploadFuture.cancel(true);
        }
        if (recoveryFuture != null) {
            recoveryFuture.cancel(true);
        }
        // TODO(1166) Should we get the future here or at next verified block reception? Or both
        currentGroupPending.clear();
        currentBlockQueue.clear();
        if (configValid) {
            context.blockMessaging().unregisterBlockNotificationHandler(this);
        }
        LOGGER.log(TRACE, "Cloud storage archive plugin stopped");
    }
}
