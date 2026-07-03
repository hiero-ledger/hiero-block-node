// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.archive;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.TRACE;
import static java.lang.System.Logger.Level.WARNING;
import static java.util.Objects.requireNonNull;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
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
import org.hiero.block.node.spi.historicalblocks.LongRange;
import org.hiero.metrics.LongCounter;
import org.hiero.metrics.core.MetricKey;
import org.hiero.metrics.core.MetricRegistry;

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
/// in [currentGroupPending] and drained in block-number order once gaps are filled.
///
/// Blocks that arrive out of sequence are buffered briefly in [gapBuffer] (up to
/// [CloudStorageArchiveConfig#gapBufferSize()] entries).  If the gap closes before the buffer
/// fills, buffered blocks drain into the regular pipeline.  If the buffer fills first, all
/// buffered blocks are flushed to [TempArchiveUploadTask].  Blocks for groups other than the
/// currently-active regular group are also uploaded as temporary S3 archives.  Once all blocks
/// for a temporary group arrive, a [ConsolidationTask] merges them into a single final `.tar`.
///
/// [StartupRecoveryTask] scans both regular `.tar` keys and the `tmp/`
/// directory, rebuilding the temporary-archive tracker from `.meta` companion files, and returns
/// the full [RecoveryResult] including any durable temporary archives.
public class CloudStorageArchivePlugin implements BlockNodePlugin, BlockNotificationHandler {

    /// The logger for this class.
    private static final System.Logger LOGGER = System.getLogger(CloudStorageArchivePlugin.class.getName());

    public static final MetricKey<LongCounter> METRIC_CLOUD_ARCHIVE_BLOCKS_WRITTEN = MetricKey.of(
                    "cloud_storage_archive_blocks_written", LongCounter.class)
            .addCategory(METRICS_CATEGORY);
    public static final MetricKey<LongCounter> METRIC_CLOUD_ARCHIVE_FAILED_TASKS = MetricKey.of(
                    "cloud_storage_archive_failed_tasks", LongCounter.class)
            .addCategory(METRICS_CATEGORY);
    public static final MetricKey<LongCounter> METRIC_CLOUD_ARCHIVE_SUCCESSFUL_TASKS = MetricKey.of(
                    "cloud_storage_archive_successful_tasks", LongCounter.class)
            .addCategory(METRICS_CATEGORY);
    public static final MetricKey<LongCounter> METRIC_CLOUD_ARCHIVE_STORED_BYTES = MetricKey.of(
                    "cloud_storage_archive_stored_bytes", LongCounter.class)
            .addCategory(METRICS_CATEGORY);
    public static final MetricKey<LongCounter> METRIC_CLOUD_ARCHIVE_DUPLICATE_BLOCKS_DISCARDED = MetricKey.of(
                    "cloud_storage_archive_duplicate_blocks_discarded", LongCounter.class)
            .addCategory(METRICS_CATEGORY);

    /// The block node context, set during [init].
    private BlockNodeContext context;
    /// The plugin configuration, set during [init].
    private CloudStorageArchiveConfig config;
    /// Executor used to run each upload task on a virtual thread.
    private ExecutorService virtualThreadExecutor;
    /// Whether the plugin configuration is valid.  Set during [init]; gates handler registration,
    /// startup recovery, and handler unregistration in [stop].
    private boolean configValid = false;
    /// Holder for all cloud archive metrics, initialized during [init].
    private MetricsHolder metricsHolder;

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
    /// result has been consumed by [completeRecoveryIfReady].  While non-null, all incoming
    /// verified blocks are routed to [blocksStash] and processing of the active upload task is
    /// deferred.
    private Future<RecoveryResult> recoveryFuture = null;

    /// The first block number of the range owned by the active [BlockUploadTask].
    /// Only meaningful when [currentUploadFuture] is non-null.
    long currentGroupStart = 0;
    /// Number of blocks per archive group; always a power of ten (see [CloudStorageArchiveConfig#groupingLevel()]).
    /// Set once in [start()] and constant thereafter.
    private long groupSize = 0;

    // Staging area for within-group blocks that arrived before their predecessor(s).
    // Blocks are moved to `currentBlockQueue` in ascending block-number order by drainPendingToQueue().
    ConcurrentSkipListMap<Long, BlockWithSource> currentGroupPending = new ConcurrentSkipListMap<>();
    // The block number that drainPendingToQueue() will enqueue next.
    private long nextBlockToQueue = -1;

    // Stash used only while the startup recovery task is running.  After recovery completes,
    // out-of-range blocks go directly to temporary S3 archives instead of being held in memory.
    final Map<Long, BlockWithSource> blocksStash = new ConcurrentSkipListMap<>();

    /// The block number of the last block handed off to any upload task (regular or temp).
    /// Initialized to -1 (nothing archived yet).  Set from [RecoveryResult] after startup recovery.
    /// Updated by [drainPendingToQueue] for regular-task handoffs and by [routeToTempArchive]
    /// for temp-task handoffs.
    long lastHandedOffBlock = -1;

    /// In-memory buffer for blocks that arrive when a gap is detected (incoming block number >
    /// lastHandedOffBlock + 1).  Accumulates up to [CloudStorageArchiveConfig#gapBufferSize()] blocks.
    /// If the expected block arrives before the buffer is exhausted, the buffer drains to the regular
    /// task.  If the buffer fills without the gap closing, all buffered blocks are flushed to a
    /// [TempArchiveUploadTask].
    final NavigableMap<Long, BlockWithSource> gapBuffer = new ConcurrentSkipListMap<>();

    /// Completed temporary archive entries keyed by [TempArchiveEntry#firstBlock()].
    /// Rebuilt from S3 meta files during startup recovery and updated as temp uploads complete.
    /// Accessed only from the notification handler thread.
    final NavigableMap<Long, TempArchiveEntry> tempArchiveTracker = new ConcurrentSkipListMap<>();

    /// Live queues for streaming temp archive segments, one per aligned group.
    /// A queue is created and a [TempArchiveUploadTask] is submitted as soon as the first block
    /// of a segment arrives.  The task starts immediately and uploads S3 parts as its internal
    /// buffer fills.  When the segment is closed (gap or last block of group), the
    /// `TempArchiveUploadTask.SEGMENT_END` sentinel is placed in the queue so the task
    /// can finalise the upload.  Keyed by aligned groupStart.
    /// Accessed only from the notification handler thread.
    final Map<Long, BlockingQueue<BlockWithSource>> tempGroupActiveQueues = new ConcurrentHashMap<>();

    /// Tracks the next expected block number per group for contiguous-run gap detection.
    /// Keyed by aligned groupStart.
    /// Accessed only from the notification handler thread.
    final Map<Long, Long> tempGroupNextExpected = new ConcurrentHashMap<>();

    /// The first block of the currently active (streaming) temp segment per group.
    /// Set by [startNewTempSegment]; cleared by [closeActiveTempSegment].
    /// Accessed only from the notification handler thread.
    final Map<Long, Long> tempGroupActiveSegmentFirstBlock = new ConcurrentHashMap<>();

    /// Last block number successfully queued to each in-flight temp segment, keyed by the
    /// segment's firstBlock.  Updated in [routeToTempArchive] each time a block is enqueued.
    /// Removed when the segment's future completes and moves to [tempArchiveTracker].
    /// Used by [isBlockCoveredByAnyTempSegment] to distinguish true duplicates from late-arriving
    /// blocks that fell into an abandoned gap between two temp segments.
    /// Accessed only from the notification handler thread.
    final NavigableMap<Long, Long> tempSegmentLastBlock = new ConcurrentSkipListMap<>();

    /// In-flight temp archive upload futures keyed by the firstBlock of the uploaded segment.
    /// Accessed only from the notification handler thread.
    final ConcurrentSkipListMap<Long, Future<TempArchiveEntry>> tempUploadFutures = new ConcurrentSkipListMap<>();

    /// Blocks that could not be routed to a temp archive because
    /// [CloudStorageArchiveConfig#maxConcurrentTempArchives()] was already reached.
    /// Drained in block-number order whenever a slot frees up.
    /// Accessed only from the notification handler thread.
    final ConcurrentSkipListMap<Long, BlockWithSource> tempOverflowStash = new ConcurrentSkipListMap<>();

    /// Groups whose temp archives fully cover [groupStart, groupStart+groupSize), queued for
    /// consolidation.  Keyed by groupStart.
    /// Accessed only from the notification handler thread.
    final ConcurrentSkipListMap<Long, List<TempArchiveEntry>> pendingConsolidations = new ConcurrentSkipListMap<>();

    /// In-flight consolidation task futures keyed by groupStart.
    /// Accessed only from the notification handler thread.
    final ConcurrentHashMap<Long, Future<UploadResult>> consolidationFutures = new ConcurrentHashMap<>();

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
        metricsHolder = MetricsHolder.createMetrics(context.metricRegistry());
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
        groupSize = Math.powExact(10, config.groupingLevel());
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
                    checkAndDrainTempUploadResults();
                    drainOverflowStash();
                    checkAndDrainConsolidations();
                    // While recovery is running, stash every block.  routeVerifiedBlock() sends
                    // it directly to blocksStash without any extra logic.
                    completeRecoveryIfReady();
                    final long blockNumber = notification.blockNumber();
                    // Route the verified block into the regular or temp pipeline, or stash it
                    // while recovery is in progress.
                    routeVerifiedBlock(blockNumber, notification);
                }
            } else {
                logInvalidOrFailedNotification(notification);
            }
        } catch (IllegalStateException e) {
            LOGGER.log(WARNING, "Could not complete uploading blocks to cloud archive storage", e);
            metricsHolder.failedTasks().increment();
            triggerMidRunRecovery();
            // routeVerifiedBlock() was never reached (the exception bypassed it), so stash the
            // triggering block manually — it is safe to use blocksStash directly because
            // triggerMidRunRecovery() already cleared currentUploadFuture.
            if (notification != null && notification.block() != null) {
                blocksStash.put(
                        notification.blockNumber(), new BlockWithSource(notification.block(), notification.source()));
            }
        }
    }

    /// Checks whether the active [BlockUploadTask] has finished and cleans up its state.
    /// Returns `true` if the task was cancelled, signalling the caller to skip further processing.
    private boolean checkCompletedUpload() {
        boolean cancelled = false;
        if (currentUploadFuture != null && currentUploadFuture.isDone()) {
            if (currentUploadFuture.isCancelled()) {
                LOGGER.log(TRACE, "Block upload task was cancelled");
                cancelled = true;
            } else {
                final UploadResult uploadResult = currentUploadFuture.resultNow();
                if (uploadResult == UploadResult.FAILED) {
                    LOGGER.log(WARNING, "Block upload task failed");
                    metricsHolder.failedTasks().increment();
                    // False PersistedNotifications were already sent by the task for the affected blocks.
                    // Trigger S3 recovery so the next task resumes from the last confirmed part boundary.
                    triggerMidRunRecovery();
                } else {
                    metricsHolder.successfulTasks().increment();
                    currentUploadFuture = null;
                    currentGroupPending = new ConcurrentSkipListMap<>();
                    LOGGER.log(TRACE, "Upload task completed successfully");
                }
            }
        }
        return cancelled;
    }

    /// Drains completed [TempArchiveUploadTask] futures and updates the tracker.
    ///
    /// [TempArchiveUploadTask] already calls `ApplicationStateFacility.addStoredBlockRange()` per
    /// part, so no additional range registration is needed here.
    private void checkAndDrainTempUploadResults() {
        final Iterator<Map.Entry<Long, Future<TempArchiveEntry>>> it =
                tempUploadFutures.entrySet().iterator();
        while (it.hasNext()) {
            final Map.Entry<Long, Future<TempArchiveEntry>> entry = it.next();
            if (!entry.getValue().isDone()) {
                continue;
            }
            if (entry.getValue().isCancelled()) {
                it.remove();
                continue;
            }
            it.remove();
            try {
                final TempArchiveEntry result = entry.getValue().resultNow();
                tempArchiveTracker.put(result.firstBlock(), result);
                tempSegmentLastBlock.remove(result.firstBlock());
                metricsHolder.successfulTasks().increment();
                final long groupStart = (result.firstBlock() / groupSize) * groupSize;
                checkGroupCoverage(groupStart);
                LOGGER.log(TRACE, "Temp archive completed: blocks [{0}, {1}]", result.firstBlock(), result.lastBlock());
            } catch (IllegalStateException e) {
                LOGGER.log(
                        WARNING,
                        "Temp archive upload failed for firstBlock {0}; triggering mid-run recovery",
                        entry.getKey(),
                        e);
                metricsHolder.failedTasks().increment();
                triggerMidRunRecovery();
            }
        }
    }

    /// Submits pending [ConsolidationTask]s and drains completed ones.
    private void checkAndDrainConsolidations() {
        // Process completed consolidations.
        final Iterator<Map.Entry<Long, Future<UploadResult>>> it =
                consolidationFutures.entrySet().iterator();
        while (it.hasNext()) {
            final Map.Entry<Long, Future<UploadResult>> entry = it.next();
            if (!entry.getValue().isDone()) {
                continue;
            }
            final long groupStart = entry.getKey();
            if (entry.getValue().isCancelled()) {
                it.remove();
                continue;
            }
            it.remove();
            try {
                entry.getValue().resultNow();
                tempArchiveTracker.subMap(groupStart, groupStart + groupSize).clear();
                tempGroupNextExpected.remove(groupStart);
                metricsHolder.successfulTasks().increment();
                LOGGER.log(TRACE, "Consolidation completed for group {0}", groupStart);
            } catch (IllegalStateException e) {
                LOGGER.log(WARNING, "Consolidation task threw exception for group {0}", groupStart, e);
                metricsHolder.failedTasks().increment();
                checkGroupCoverage(groupStart);
            }
        }

        // Submit pending consolidations.
        final Iterator<Map.Entry<Long, List<TempArchiveEntry>>> pendingIt =
                pendingConsolidations.entrySet().iterator();
        while (pendingIt.hasNext()) {
            final Map.Entry<Long, List<TempArchiveEntry>> entry = pendingIt.next();
            final long groupStart = entry.getKey();
            if (!consolidationFutures.containsKey(groupStart)) {
                final Future<UploadResult> future =
                        virtualThreadExecutor.submit(newConsolidationTask(entry.getValue(), groupStart, groupSize));
                consolidationFutures.put(groupStart, future);
                pendingIt.remove();
                LOGGER.log(
                        TRACE, "Submitted consolidation task for group [{0}, {1})", groupStart, groupStart + groupSize);
            }
        }
    }

    /// Re-routes blocks from [tempOverflowStash] in block-number order while upload slots are free.
    /// Called after [checkAndDrainTempUploadResults] so that any just-freed slots are visible.
    /// Skipped during recovery to avoid racing with the recovery result.
    ///
    /// A block is always eligible to drain if its group already has an active streaming queue
    /// (adding to an existing segment costs no new slot).  A block whose group has no active queue
    /// is eligible only when a free slot is available.  The loop stops at the first ineligible entry
    /// so that the stash stays in ascending-key order for the next drain cycle.
    private void drainOverflowStash() {
        if (recoveryFuture == null) {
            while (!tempOverflowStash.isEmpty()) {
                final Map.Entry<Long, BlockWithSource> first = tempOverflowStash.firstEntry();
                final long blockNum = first.getKey();
                final long groupStart = (blockNum / groupSize) * groupSize;
                if (!tempGroupActiveQueues.containsKey(groupStart)
                        && tempUploadFutures.size() >= config.maxConcurrentTempArchives()) {
                    break;
                }
                tempOverflowStash.pollFirstEntry();
                routeToTempArchive(blockNum, first.getValue());
            }
        }
    }

    /// Checks whether the temp archives in [tempArchiveTracker] together cover the entire aligned
    /// group `[groupStart, groupStart + groupSize)`.  If so, queues the group for consolidation.
    private void checkGroupCoverage(long groupStart) {
        final long groupEnd = groupStart + groupSize - 1;

        final List<TempArchiveEntry> entries =
                tempArchiveTracker.subMap(groupStart, true, groupEnd, true).values().stream()
                        .toList();

        // Walk entries in block-number order, advancing the coverage cursor.
        // A gap (entry starts above the cursor) means the group is not yet fully covered.
        long covered = groupStart;
        for (final TempArchiveEntry e : entries) {
            if (e.firstBlock() > covered) {
                LOGGER.log(
                        TRACE,
                        "Group [{0}, {1}) not yet fully covered by temp archives; gap starts at block {2}",
                        groupStart,
                        groupStart + groupSize,
                        covered);
                break;
            }
            covered = Math.max(covered, e.lastBlock() + 1);
        }

        // If covered has advanced past the last block, all blocks in the group are accounted for.
        if (covered > groupEnd
                && !pendingConsolidations.containsKey(groupStart)
                && !consolidationFutures.containsKey(groupStart)) {
            pendingConsolidations.put(groupStart, entries);
            LOGGER.log(
                    TRACE,
                    "Group [{0}, {1}) is fully covered by temp archives; queued for consolidation",
                    groupStart,
                    groupStart + groupSize);
        }
    }

    /// Consumes the result of the [StartupRecoveryTask] and, when the prior S3 state is
    /// discovered, sets [currentGroupStart], creates a fresh [BlockingQueue], and submits a
    /// [BlockUploadTask] for the recovered group before replaying any blocks that arrived during
    /// recovery.  Also rebuilds [tempArchiveTracker] from any durable temporary archives found.
    ///
    /// When the result is a fresh start, no upload task is created and the next call to
    /// [handleVerification] will fall through to [tryStartNewUploadTask] as normal.
    private void completeRecoveryIfReady() {
        if (recoveryFuture != null && recoveryFuture.isDone()) {
            try {
                final RecoveryResult result = recoveryFuture.resultNow();

                // Rebuild the temporary-archive tracker from startup recovery.  These archives
                // survived a restart so their block ranges must be re-registered with the state
                // facility (the upload task's per-run registration did not survive the restart).
                if (result.tempArchives() != null) {
                    for (final TempArchiveEntry entry : result.tempArchives()) {
                        tempArchiveTracker.put(entry.firstBlock(), entry);
                        context.applicationStateFacility()
                                .addStoredBlockRange(new LongRange(entry.firstBlock(), entry.lastBlock()));
                    }
                    LOGGER.log(
                            TRACE,
                            "Rebuilt {0} temp archive entries from startup recovery",
                            result.tempArchives().size());
                    // Check whether any recovered group is already fully covered.
                    tempArchiveTracker.values().stream()
                            .mapToLong(e -> (e.firstBlock() / groupSize) * groupSize)
                            .distinct()
                            .forEach(this::checkGroupCoverage);
                }

                lastHandedOffBlock = result.lastHandedOffBlock();

                if (result.currentGroupStart() != -1) {
                    currentGroupStart = result.currentGroupStart();
                    nextBlockToQueue = result.uploadId() != null ? result.nextBlockNumber() : currentGroupStart;
                    if (nextBlockToQueue > 0) {
                        context.applicationStateFacility().addStoredBlockRange(new LongRange(0, nextBlockToQueue - 1));
                    }
                    currentBlockQueue = new LinkedBlockingQueue<>();
                    currentUploadFuture = virtualThreadExecutor.submit(new BlockUploadTask(
                            config,
                            context.blockMessaging(),
                            currentGroupStart,
                            groupSize,
                            currentBlockQueue,
                            result.uploadId() != null ? result : null,
                            metricsHolder,
                            context.applicationStateFacility()));
                }
                tryReplayStash();
            } finally {
                recoveryFuture = null;
            }
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

    /// Starts a new regular [BlockUploadTask] for the group containing [blockNumber], unless the
    /// group already has pre-existing temporary archive data (which means [ConsolidationTask]
    /// handles it exclusively).
    private void tryStartNewUploadTask(long blockNumber) {
        final long targetGroupStart = (blockNumber / groupSize) * groupSize;
        if (hasAnyTempDataForGroup(targetGroupStart)) {
            LOGGER.log(TRACE, "Group {0} has temp archive data; skipping regular upload", targetGroupStart);
        } else {
            startNewUploadTask(blockNumber);
        }
    }

    /// Returns `true` when the group starting at [groupStart] has any in-flight or completed
    /// temporary archive data (active streaming segments, in-flight futures, or tracker entries).
    private boolean hasAnyTempDataForGroup(long groupStart) {
        final long groupEnd = groupStart + groupSize;
        return tempGroupActiveQueues.containsKey(groupStart)
                || !tempUploadFutures.subMap(groupStart, groupEnd).isEmpty()
                || !tempArchiveTracker.subMap(groupStart, groupEnd).isEmpty();
    }

    /// Initialises a new [BlockUploadTask] for the group that contains [blockNumber], submits it
    /// to [virtualThreadExecutor], and replays any previously stashed blocks that fall within the
    /// new group's range.
    private void startNewUploadTask(long blockNumber) {
        // Integer division truncates, so this rounds blockNumber down to the nearest group boundary.
        currentGroupStart = (blockNumber / groupSize) * groupSize;
        nextBlockToQueue = currentGroupStart;
        // Create a fresh queue rather than clearing the existing one to ensure complete separation
        // from the previous task, which still holds a reference to the old queue instance.
        currentBlockQueue = new LinkedBlockingQueue<>();
        LOGGER.log(
                TRACE, "Starting upload task for group [{0}, {1})", currentGroupStart, currentGroupStart + groupSize);
        currentUploadFuture =
                virtualThreadExecutor.submit(newUploadTask(currentGroupStart, groupSize, currentBlockQueue));
        tryReplayStash();
    }

    /// Creates the [Callable] for a new [BlockUploadTask].  Extracted so that tests can override
    /// this method to return a task with controlled behavior without touching production S3
    /// infrastructure.
    Callable<UploadResult> newUploadTask(long firstBlock, long groupSize, BlockingQueue<BlockWithSource> queue) {
        return new BlockUploadTask(
                config,
                context.blockMessaging(),
                firstBlock,
                groupSize,
                queue,
                metricsHolder,
                context.applicationStateFacility());
    }

    /// Creates the [Callable] for a new [ConsolidationTask].  Extracted for test overriding.
    Callable<UploadResult> newConsolidationTask(List<TempArchiveEntry> entries, long groupStart, long groupSize) {
        return new ConsolidationTask(config, entries, groupStart, groupSize);
    }

    /// Creates the [Callable] for a new [TempArchiveUploadTask].  Extracted for test overriding.
    Callable<TempArchiveEntry> newTempArchiveUploadTask(
            String s3Key, long firstBlock, BlockingQueue<BlockWithSource> queue) {
        return new TempArchiveUploadTask(
                config,
                context.blockMessaging(),
                context.applicationStateFacility(),
                metricsHolder,
                s3Key,
                firstBlock,
                queue);
    }

    /// Starts a new upload task if none is active, then routes [blockNumber] to the regular pending
    /// queue (if the block falls within the current group) or to a temp archive otherwise.
    private void routeToCloudArchive(long blockNumber, BlockWithSource blockWithSource) {
        if (currentUploadFuture == null) {
            tryStartNewUploadTask(blockNumber);
        }
        if (currentUploadFuture != null
                && blockNumber >= currentGroupStart
                && blockNumber < currentGroupStart + groupSize) {
            currentGroupPending.put(blockNumber, blockWithSource);
            drainPendingToQueue();
        } else {
            routeToTempArchive(blockNumber, blockWithSource);
        }
    }

    /// Routes [blockNumber] to the appropriate destination.
    ///
    /// During recovery ([recoveryFuture] != null), all blocks are stashed for later replay.
    /// After recovery, contiguous blocks go to the regular pipeline via [currentGroupPending].
    /// A gap triggers [gapBuffer] accumulation; if the gap closes before the buffer fills,
    /// buffered blocks drain into the regular pipeline.  If the buffer fills first, all buffered
    /// blocks are flushed to a [TempArchiveUploadTask].  Retrograde or duplicate blocks are
    /// silently discarded.
    ///
    /// On a fresh start, a first arrival exactly on a group boundary is always safe to start the
    /// regular pipeline on directly -- no earlier block could ever belong to that group.  Any
    /// non-aligned first arrival is ambiguous -- out-of-order genesis vs. a live deployment whose
    /// earlier blocks are gone -- so it's treated as a gap like any other, letting the existing
    /// buffer/temp-archive logic resolve it instead of guessing from the block number alone.
    private void routeVerifiedBlock(long blockNumber, VerificationNotification notification) {
        final BlockWithSource blockWithSource = new BlockWithSource(notification.block(), notification.source());
        if (recoveryFuture != null) {
            // Stash until recovery completes so we don't race with the recovery result.
            blocksStash.put(blockNumber, blockWithSource);
            LOGGER.log(TRACE, "Block number {0} stashed during recovery", blockNumber);
            return;
        }

        // Retrograde relative to lastHandedOffBlock: discard unless the regular task still needs it.
        // Handling this first prevents retrograde blocks from entering gapBuffer, which would corrupt
        // the gap-detection logic and reduce effective buffer capacity.
        if (blockNumber <= lastHandedOffBlock) {
            if (currentUploadFuture != null
                    && blockNumber >= nextBlockToQueue
                    && blockNumber < currentGroupStart + groupSize) {
                // Temp-archive routing advanced lastHandedOffBlock, but the regular task still needs this block.
                LOGGER.log(TRACE, "Block {0} reclaimed for in-flight regular task", blockNumber);
                currentGroupPending.put(blockNumber, blockWithSource);
                drainPendingToQueue();
            } else if (currentUploadFuture == null
                    && blockNumber >= nextBlockToQueue
                    && !hasAnyTempDataForGroup((blockNumber / groupSize) * groupSize)) {
                // The regular task completed but temp-archive routing advanced lastHandedOffBlock,
                // making this block appear retrograde. Start a new regular task for its group.
                LOGGER.log(TRACE, "Block {0} starting new regular task", blockNumber);
                routeToCloudArchive(blockNumber, blockWithSource);
            } else if (blockNumber < nextBlockToQueue && blockNumber >= currentGroupStart) {
                // Already consumed by the current task's own group; a block below currentGroupStart
                // belongs to an earlier group the regular pipeline never handled, so it falls through.
                metricsHolder.duplicateBlocksDiscarded().increment();
                LOGGER.log(DEBUG, "Block {0} is before resume point ({1}); discarding", blockNumber, nextBlockToQueue);
            } else {
                // Neither the regular task nor the direct-to-regular path applies.
                // Route to temp; routeToTempArchive will detect whether this is a true duplicate
                // (already in some segment) or a late-arriving block from an abandoned gap,
                // and start a new segment for it in the latter case.
                routeToTempArchive(blockNumber, blockWithSource);
            }
        } else {
            // blockNumber > lastHandedOffBlock from here.
            final long expected = lastHandedOffBlock + 1;
            // A fresh start is unambiguous -- safe to begin the regular pipeline immediately --
            // only when it's an aligned group boundary: no earlier block could ever belong to
            // that group, regardless of whether this is a genesis chain or a live deployment
            // resuming mid-history.  Any non-aligned first arrival can't be told apart from a
            // live deployment whose earlier blocks are gone, so it falls through to gap
            // detection below, bounded by gapBufferSize like any other group.
            final boolean freshStartSafeForRegular = lastHandedOffBlock == -1 && blockNumber % groupSize == 0;

            if (blockNumber == expected || freshStartSafeForRegular) {
                if (!gapBuffer.isEmpty()) {
                    // The expected block has arrived, closing the gap.
                    gapBuffer.put(blockNumber, blockWithSource);
                    drainGapBufferToRegular();
                } else {
                    // Normal contiguous arrival: feed the regular pipeline.
                    routeToCloudArchive(blockNumber, blockWithSource);
                }
            } else {
                // blockNumber > expected: gap detected or continuing.
                final long groupStart = (blockNumber / groupSize) * groupSize;
                if (gapBuffer.isEmpty()
                        && (tempGroupActiveQueues.containsKey(groupStart)
                                || tempGroupNextExpected.containsKey(groupStart)
                                || groupStart > currentGroupStart + groupSize)) {
                    // On first gap detection, skip the buffer when the group already has temp data
                    // or is more than 1 group ahead -- buffering cannot help close the gap in those cases.
                    LOGGER.log(
                            TRACE,
                            "Gap detected at block {0} (expected {1}); routing directly to temp archive",
                            blockNumber,
                            expected);
                    routeToTempArchive(blockNumber, blockWithSource);
                } else {
                    if (gapBuffer.isEmpty()) {
                        LOGGER.log(TRACE, "Gap detected at block {0} (expected {1}); buffering", blockNumber, expected);
                    }
                    gapBuffer.put(blockNumber, blockWithSource);
                    if (gapBuffer.size() >= config.gapBufferSize()) {
                        flushGapBufferToTemp();
                    }
                }
            }
        }
    }

    /// Drains the contiguous prefix of [gapBuffer] into the regular pipeline now that the expected
    /// block has arrived (filling the gap).  Any remaining entries in [gapBuffer] after the drain
    /// represent a new gap and stay buffered.
    private void drainGapBufferToRegular() {
        LOGGER.log(TRACE, "Gap closed; draining {0} buffered blocks to regular pipeline", gapBuffer.size());
        while (!gapBuffer.isEmpty() && gapBuffer.firstKey() == lastHandedOffBlock + 1) {
            final Map.Entry<Long, BlockWithSource> entry = gapBuffer.pollFirstEntry();
            routeToCloudArchive(entry.getKey(), entry.getValue());
        }
    }

    /// Flushes all blocks currently in [gapBuffer].  Called when the buffer reaches
    /// [CloudStorageArchiveConfig#gapBufferSize()] without the gap closing.
    ///
    /// Blocks within the active regular task's remaining range
    /// ([nextBlockToQueue, currentGroupStart+groupSize)) are salvaged to [currentGroupPending]
    /// so the task is not left waiting for blocks that went to temp.  All other blocks go to
    /// a [TempArchiveUploadTask].
    private void flushGapBufferToTemp() {
        LOGGER.log(TRACE, "Gap buffer capacity reached ({0} blocks); flushing to temp archives", gapBuffer.size());
        boolean pendingChanged = false;
        for (final Map.Entry<Long, BlockWithSource> entry : gapBuffer.entrySet()) {
            final long blockNumber = entry.getKey();
            if (currentUploadFuture != null
                    && blockNumber >= nextBlockToQueue
                    && blockNumber < currentGroupStart + groupSize) {
                currentGroupPending.put(blockNumber, entry.getValue());
                pendingChanged = true;
            } else {
                routeToTempArchive(blockNumber, entry.getValue());
            }
        }
        gapBuffer.clear();
        if (pendingChanged) {
            drainPendingToQueue();
        }
    }

    /// Routes [blockNumber] to a streaming temporary S3 archive segment.
    ///
    /// Each aligned group maintains at most one active [TempArchiveUploadTask] whose queue receives
    /// blocks in arrival order.  A forward gap (blockNumber > nextExpected) closes the current
    /// segment and starts a new one at [blockNumber].  A backward arrival (blockNumber < nextExpected)
    /// is either a true duplicate (already covered by some in-flight or completed segment -- discarded)
    /// or a late-arriving block from an abandoned gap between two segments (the active segment is
    /// closed and a new one is started at [blockNumber]).  The final block of the group also closes
    /// the segment.
    private void routeToTempArchive(long blockNumber, BlockWithSource block) {
        final long groupStart = (blockNumber / groupSize) * groupSize;

        final long nextExpected = tempGroupNextExpected.getOrDefault(groupStart, blockNumber);
        if (isBlockCoveredByAnyTempSegment(blockNumber, groupStart)) {
            // Already uploaded to some in-flight or completed segment -- true duplicate; discard.
            metricsHolder.duplicateBlocksDiscarded().increment();
            LOGGER.log(DEBUG, "Discarding duplicate block {0} for temp group {1}", blockNumber, groupStart);
        } else {
            if (blockNumber < nextExpected) {
                // Block is in an abandoned gap between two temp segments -- close the active segment
                // (if any) so the new segment can start at blockNumber below.
                LOGGER.log(
                        TRACE,
                        "Block {0} in abandoned gap for temp group {1}; closing active segment to start new",
                        blockNumber,
                        groupStart);
                closeActiveTempSegment(groupStart);
            } else if (blockNumber > nextExpected) {
                // Forward gap detected: close the current segment and let the task complete with what it has.
                closeActiveTempSegment(groupStart);
                checkGroupCoverage(groupStart);
            }

            if (!tempGroupActiveQueues.containsKey(groupStart)
                    && tempUploadFutures.size() >= config.maxConcurrentTempArchives()) {
                tempOverflowStash.put(blockNumber, block);
                LOGGER.log(
                        DEBUG,
                        "Concurrent temp archive limit ({0}) reached; block {1} queued in overflow stash",
                        config.maxConcurrentTempArchives(),
                        blockNumber);
            } else {
                if (!tempGroupActiveQueues.containsKey(groupStart)) {
                    startNewTempSegment(groupStart, blockNumber);
                }
                tempGroupActiveQueues.get(groupStart).offer(block);
                lastHandedOffBlock = Math.max(lastHandedOffBlock, blockNumber);
                tempGroupNextExpected.put(groupStart, blockNumber + 1);
                final long segmentFirstBlock = tempGroupActiveSegmentFirstBlock.getOrDefault(groupStart, blockNumber);
                tempSegmentLastBlock.put(segmentFirstBlock, blockNumber);
                if (blockNumber == groupStart + groupSize - 1) {
                    closeActiveTempSegment(groupStart);
                    tempGroupNextExpected.remove(groupStart);
                    checkGroupCoverage(groupStart);
                } else if (isBlockCoveredByAnyTempSegment(blockNumber + 1, groupStart)) {
                    // The next expected block is already in another segment (e.g. a late-arriving
                    // block filled the gap up to an existing segment boundary).  Close this segment
                    // now so the upload task can finalise without waiting for a block that will
                    // never arrive in this segment's queue.
                    LOGGER.log(
                            TRACE,
                            "Gap bridged at {0} for temp group {1}; closing segment",
                            blockNumber + 1,
                            groupStart);
                    closeActiveTempSegment(groupStart);
                    tempGroupNextExpected.remove(groupStart);
                    checkGroupCoverage(groupStart);
                }
            }
        }
    }

    /// Returns true when [blockNumber] is already covered by any in-flight or completed temp
    /// archive segment for [groupStart].  Used to distinguish true duplicates (already uploaded)
    /// from late-arriving blocks that fell into an abandoned gap between two segments.
    private boolean isBlockCoveredByAnyTempSegment(long blockNumber, long groupStart) {
        final long groupEnd = groupStart + groupSize;
        boolean covered = false;
        for (long firstBlock : tempUploadFutures.subMap(groupStart, groupEnd).keySet()) {
            final long lastBlock = tempSegmentLastBlock.getOrDefault(firstBlock, -1L);
            if (blockNumber >= firstBlock && blockNumber <= lastBlock) {
                covered = true;
                break;
            }
        }
        if (!covered) {
            for (TempArchiveEntry entry :
                    tempArchiveTracker.subMap(groupStart, groupEnd).values()) {
                if (blockNumber >= entry.firstBlock() && blockNumber <= entry.lastBlock()) {
                    covered = true;
                    break;
                }
            }
        }
        return covered;
    }

    /// Starts a new streaming [TempArchiveUploadTask] for a segment beginning at [firstBlock]
    /// within the group aligned to [groupStart].
    private void startNewTempSegment(long groupStart, long firstBlock) {
        final String s3Key = TempArchiveKey.formatTar(firstBlock, config.objectKeyPrefix());
        final BlockingQueue<BlockWithSource> queue = new LinkedBlockingQueue<>();
        tempGroupActiveQueues.put(groupStart, queue);
        tempGroupActiveSegmentFirstBlock.put(groupStart, firstBlock);
        final Future<TempArchiveEntry> future =
                virtualThreadExecutor.submit(newTempArchiveUploadTask(s3Key, firstBlock, queue));
        tempUploadFutures.put(firstBlock, future);
        LOGGER.log(TRACE, "Started streaming temp archive for group {0}, firstBlock={1}", groupStart, firstBlock);
    }

    /// Closes the active temp archive segment for [groupStart] by placing [TempArchiveUploadTask#SEGMENT_END]
    /// in its queue, removing the queue from [tempGroupActiveQueues].
    private void closeActiveTempSegment(long groupStart) {
        final BlockingQueue<BlockWithSource> queue = tempGroupActiveQueues.remove(groupStart);
        tempGroupActiveSegmentFirstBlock.remove(groupStart);
        if (queue != null) {
            queue.offer(TempArchiveUploadTask.SEGMENT_END);
            LOGGER.log(TRACE, "Closed active temp segment for group {0}", groupStart);
        }
    }

    private void drainPendingToQueue() {
        while (currentGroupPending.containsKey(nextBlockToQueue)) {
            final BlockWithSource nextBlock = requireNonNull(currentGroupPending.remove(nextBlockToQueue));
            currentBlockQueue.offer(nextBlock);
            lastHandedOffBlock = Math.max(lastHandedOffBlock, nextBlockToQueue);
            nextBlockToQueue++;
        }
    }

    /// Replays blocks stashed during startup recovery.
    ///
    /// Blocks within the recovered regular group go to [currentGroupPending]; all others
    /// (pre-EMB or future-group) are routed to temporary S3 archives via [routeToTempArchive].
    private void tryReplayStash() {
        if (!blocksStash.isEmpty()) {
            final List<Long> keys = new ArrayList<>(blocksStash.keySet());
            final long groupEnd = currentUploadFuture != null ? currentGroupStart + groupSize : Long.MIN_VALUE;
            int replayed = 0;
            int tempRouted = 0;
            for (final long blockNumber : keys) {
                final BlockWithSource block = blocksStash.remove(blockNumber);
                if (block == null) {
                    continue;
                }
                if (currentUploadFuture != null && blockNumber >= currentGroupStart && blockNumber < groupEnd) {
                    currentGroupPending.put(blockNumber, block);
                    replayed++;
                } else {
                    routeToTempArchive(blockNumber, block);
                    tempRouted++;
                }
            }
            if (replayed > 0) {
                drainPendingToQueue();
            }
            LOGGER.log(
                    TRACE, "Replayed {0} stash blocks to regular pending, {1} to temp archives", replayed, tempRouted);
        }
    }

    /// Resets routing state and schedules an S3 recovery scan after an upload-task failure.
    ///
    /// Blocks that had not yet been drained to the task queue are preserved in [blocksStash] so
    /// [tryReplayStash] can feed them to the resumed task once [completeRecoveryIfReady] completes.
    /// Gap-buffered blocks are also moved to [blocksStash] so they are not stranded above the
    /// recovered [lastHandedOffBlock] and interfere with gap detection after recovery.
    private void triggerMidRunRecovery() {
        if (recoveryFuture == null) {
            final int movedCount = currentGroupPending.size() + gapBuffer.size();
            blocksStash.putAll(currentGroupPending);
            currentGroupPending = new ConcurrentSkipListMap<>();
            blocksStash.putAll(gapBuffer);
            gapBuffer.clear();
            if (currentUploadFuture != null) {
                currentUploadFuture.cancel(true);
            }
            currentUploadFuture = null;
            recoveryFuture = virtualThreadExecutor.submit(new StartupRecoveryTask(config));
            LOGGER.log(TRACE, "Mid-run recovery triggered; {0} pending blocks moved to stash", movedCount);
        }
    }

    /// {@inheritDoc}
    @Override
    public void stop() {
        if (configValid) {
            context.blockMessaging().unregisterBlockNotificationHandler(this);
        }
        // Cancelling with true interrupts the virtual thread, causing blockQueue.take() to throw
        // InterruptedException inside BlockUploadTask.call()
        if (currentUploadFuture != null) {
            currentUploadFuture.cancel(true);
        }
        if (recoveryFuture != null) {
            recoveryFuture.cancel(true);
        }
        tempUploadFutures.values().forEach(f -> f.cancel(true));
        consolidationFutures.values().forEach(f -> f.cancel(true));
        currentGroupPending.clear();
        currentBlockQueue.clear();
        gapBuffer.clear();
        tempArchiveTracker.clear();
        tempGroupActiveQueues.clear();
        tempGroupNextExpected.clear();
        tempGroupActiveSegmentFirstBlock.clear();
        tempSegmentLastBlock.clear();
        tempUploadFutures.clear();
        tempOverflowStash.clear();
        pendingConsolidations.clear();
        consolidationFutures.clear();
        LOGGER.log(TRACE, "Cloud storage archive plugin stopped");
    }

    boolean isRecoveryComplete() {
        return recoveryFuture != null && recoveryFuture.isDone();
    }

    long recoveredNextBlockNumber() throws InterruptedException, ExecutionException {
        return recoveryFuture.get().nextBlockNumber();
    }

    /// Holder for all cloud storage archive metrics.
    public record MetricsHolder(
            LongCounter.Measurement blocksWritten,
            LongCounter.Measurement failedTasks,
            LongCounter.Measurement successfulTasks,
            LongCounter.Measurement storedBytes,
            LongCounter.Measurement duplicateBlocksDiscarded) {

        /// Factory method to create and register all cloud archive metrics.
        public static MetricsHolder createMetrics(@NonNull final MetricRegistry metricRegistry) {
            return new MetricsHolder(
                    metricRegistry
                            .register(LongCounter.builder(METRIC_CLOUD_ARCHIVE_BLOCKS_WRITTEN)
                                    .setDescription("Number of blocks written to S3 cloud archive storage."))
                            .getOrCreateNotLabeled(),
                    metricRegistry
                            .register(LongCounter.builder(METRIC_CLOUD_ARCHIVE_FAILED_TASKS)
                                    .setDescription("Total number of failed cloud archive upload tasks."))
                            .getOrCreateNotLabeled(),
                    metricRegistry
                            .register(LongCounter.builder(METRIC_CLOUD_ARCHIVE_SUCCESSFUL_TASKS)
                                    .setDescription("Total number of successful cloud archive upload tasks."))
                            .getOrCreateNotLabeled(),
                    metricRegistry
                            .register(LongCounter.builder(METRIC_CLOUD_ARCHIVE_STORED_BYTES)
                                    .setDescription("Total number of bytes stored in S3 cloud archive storage."))
                            .getOrCreateNotLabeled(),
                    metricRegistry
                            .register(LongCounter.builder(METRIC_CLOUD_ARCHIVE_DUPLICATE_BLOCKS_DISCARDED)
                                    .setDescription(
                                            "Total number of duplicate blocks discarded by cloud archive routing."))
                            .getOrCreateNotLabeled());
        }
    }
}
