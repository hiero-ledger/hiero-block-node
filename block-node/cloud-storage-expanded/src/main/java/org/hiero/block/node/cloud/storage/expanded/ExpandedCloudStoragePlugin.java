// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.expanded;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.TRACE;
import static java.lang.System.Logger.Level.WARNING;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.hiero.block.node.cloud.storage.expanded.RetryBuffer.BufferedEntry;
import org.hiero.block.node.cloud.storage.expanded.RetryBuffer.RetryOutcome;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.spi.threading.ThreadPoolManager;
import org.hiero.metrics.LongCounter;
import org.hiero.metrics.LongGauge;
import org.hiero.metrics.core.MetricKey;
import org.hiero.metrics.core.MetricRegistry;

/// A block node plugin that uploads each individually-verified block as a compressed
/// `.blk.zstd` object directly to any S3-compatible object store (AWS S3, GCS via
/// S3-interop, etc.).
///
/// This plugin uploads one block per object. This makes individual blocks immediately
/// queryable and suits consumers that want block-level granularity in the cloud with
/// minimal latency.
///
/// ## Trigger: {@link VerificationNotification}
/// The plugin reacts to `handleVerification()` rather than `handlePersisted()`.
/// This allows cloud upload and local file storage (`blocks-file-recent`) to run in
/// parallel — each registered handler gets its own virtual thread. Block bytes are taken
/// directly from `notification.block()`, eliminating any dependency on the local
/// historical block provider.
///
/// ## Async upload via CompletionService
/// Each verified block is submitted as a {@link SingleBlockStoreTask} to a
/// {@link CompletionService} backed by a dedicated virtual-thread executor. The plugin
/// drains completed tasks immediately before each new notification, buffering results in
/// a {@link ConcurrentSkipListMap} keyed by block number so that
/// {@link PersistedNotification}s are always published in ascending block-number order.
///
/// ## Object key format
/// ```
/// {objectKeyPrefix}/AAAA/BBBB/CCCC/DDDD/EEE.blk.zstd
/// ```
/// The 19-digit zero-padded block number is split into 4-digit folder groups (4/4/4/4/3)
/// for lexicographic ordering and S3 prefix partitioning. Example:
/// ```
/// Block          1 → blocks/0000/0000/0000/0000/001.blk.zstd
/// Block  108273182 → blocks/0000/0000/0010/8273/182.blk.zstd
/// ```
///
/// ## S3 client implementation
/// Uploads are performed via {@link BuckyS3UploadClient}, a package-private concrete
/// class that wraps `com.hedera.bucky.S3Client` directly. Unit tests inject a
/// custom {@link S3UploadClient} implementation via the package-private constructor.
public class ExpandedCloudStoragePlugin implements BlockNodePlugin, BlockNotificationHandler {

    // ---- Metric keys --------------------------------------------------------

    /// Total number of blocks successfully uploaded to S3-compatible storage.
    public static final MetricKey<LongCounter> METRIC_EXPANDED_CLOUD_STORAGE_TOTAL_UPLOADS =
            MetricKey.of("cloud_expanded_total_uploads", LongCounter.class).addCategory(METRICS_CATEGORY);
    /// Total number of block uploads that failed (S3 error, I/O error, or compression error).
    public static final MetricKey<LongCounter> METRIC_EXPANDED_CLOUD_STORAGE_TOTAL_UPLOAD_FAILURES = MetricKey.of(
                    "cloud_expanded_total_upload_failures", LongCounter.class)
            .addCategory(METRICS_CATEGORY);
    /// Total compressed bytes successfully transferred to S3-compatible storage.
    public static final MetricKey<LongCounter> METRIC_EXPANDED_CLOUD_STORAGE_TOTAL_UPLOADED_BYTES =
            MetricKey.of("cloud_expanded_total_upload_bytes", LongCounter.class).addCategory(METRICS_CATEGORY);
    /// Total wall-clock time spent in S3 upload calls, in nanoseconds (includes all attempts, success and failure).
    public static final MetricKey<LongCounter> METRIC_EXPANDED_CLOUD_STORAGE_UPLOAD_LATENCY_NS =
            MetricKey.of("cloud_expanded_upload_latency_ns", LongCounter.class).addCategory(METRICS_CATEGORY);
    /// Current number of blocks buffered in memory and awaiting a background retry upload.
    public static final MetricKey<LongGauge> METRIC_EXPANDED_CLOUD_STORAGE_PENDING_RETRY_BLOCKS =
            MetricKey.of("cloud_expanded_pending_retry_blocks", LongGauge.class).addCategory(METRICS_CATEGORY);
    /// Total number of blocks recovered by a later background retry after an initial upload failure.
    public static final MetricKey<LongCounter> METRIC_EXPANDED_CLOUD_STORAGE_RETRY_SUCCESS_TOTAL = MetricKey.of(
                    "cloud_expanded_retry_success_total", LongCounter.class)
            .addCategory(METRICS_CATEGORY);
    /// Total number of blocks dropped after exhausting all background retry attempts, evicted from
    /// the retry buffer to make room for a newer failure, or still buffered when the plugin shuts
    /// down — the latter two are not necessarily a sign of S3 failures.
    public static final MetricKey<LongCounter> METRIC_EXPANDED_CLOUD_STORAGE_RETRY_EXHAUSTED_TOTAL = MetricKey.of(
                    "cloud_expanded_retry_exhausted_total", LongCounter.class)
            .addCategory(METRICS_CATEGORY);

    private static final System.Logger LOGGER = System.getLogger(ExpandedCloudStoragePlugin.class.getName());

    /// Plugin configuration, set during {@link #init}.
    private ExpandedCloudStorageConfig config;

    /// Messaging facility used to publish {@link PersistedNotification} results.
    private BlockMessagingFacility blockMessaging;

    /// The active S3 upload client. `null` before {@link #start} and after {@link #stop}.
    /// May be pre-set by the package-private test constructor.
    private S3UploadClient s3Client;

    /// `CompletionService` for async block upload tasks.
    private CompletionService<SingleBlockStoreTask.UploadResult> completionService;

    /// Dedicated virtual-thread executor created in {@link #start} and shut down in {@link #stop}.
    /// Using a dedicated executor (rather than the shared platform executor) allows {@link #stop}
    /// to call {@link ExecutorService#shutdown()} + {@link ExecutorService#awaitTermination} without
    /// affecting other plugins.
    private ExecutorService virtualThreadExecutor;

    /// Staging map for upload results awaiting publication.
    ///
    /// Completed {@link SingleBlockStoreTask.UploadResult}s are placed here keyed by block number.
    /// {@link #drainCompletedTasks()} then polls entries in ascending block-number order before
    /// publishing {@link PersistedNotification}s, ensuring monotonically increasing block-number
    /// delivery to downstream consumers.
    ///
    /// Note: strict sequential ordering (holding back block N+1 until block N completes) is not
    /// enforced here; only the order of already-completed results is sorted. Full gap-aware
    /// sequential delivery is a planned follow-up.
    private final ConcurrentSkipListMap<Long, SingleBlockStoreTask.UploadResult> pendingPublish =
            new ConcurrentSkipListMap<>();

    /// Metrics instance, saved in {@link #init} for use in {@link #start}.
    private MetricRegistry metricRegistry;

    /// Counters for upload events; non-null after {@link #start} succeeds.
    private MetricsHolder metricsHolder;

    /// Source of thread pools, cached in {@link #init} for use in {@link #start} (which takes no
    /// context parameter).
    private ThreadPoolManager threadPoolManager;

    /// In-memory buffer for blocks whose upload failed, awaiting background retry.
    /// Constructed in {@link #init} from config.
    private RetryBuffer retryBuffer;

    /// Scheduled tick that scans {@link #retryBuffer} for blocks due for another retry attempt.
    /// `null` when {@link ExpandedCloudStorageConfig#retryEnabled()} is `false`.
    private ScheduledExecutorService retryScheduler;

    /// `CompletionService` for async retry-upload tasks, separate from {@link #completionService}
    /// since retries don't participate in {@link #pendingPublish} ordering.
    private CompletionService<SingleBlockStoreTask.UploadResult> retryCompletionService;

    /// Block number of each outstanding retry {@link Future}. Only {@link #retryStagedBlocks}
    /// (single-threaded {@link #retryScheduler}) adds entries, so a value-check before submitting
    /// a duplicate is race-free despite not being atomic with the following `put()`.
    private final Map<Future<SingleBlockStoreTask.UploadResult>, Long> retryFutureBlockNumbers =
            new ConcurrentHashMap<>();

    // ---- Constructors -------------------------------------------------------

    /// No-arg constructor used by the Java {@link java.util.ServiceLoader}.
    public ExpandedCloudStoragePlugin() {}

    /// Package-private constructor for unit tests. Injects a pre-built
    /// {@link S3UploadClient} so tests do not need a real S3 endpoint.
    ///
    /// @param s3Client the upload client to use instead of creating one from config
    ExpandedCloudStoragePlugin(@NonNull final S3UploadClient s3Client) {
        this.s3Client = s3Client;
    }

    // ---- BlockNodePlugin ----------------------------------------------------

    /// {@inheritDoc}
    @Override
    public void init(@NonNull final BlockNodeContext context, @NonNull final ServiceBuilder serviceBuilder) {
        this.config = context.configuration().getConfigData(ExpandedCloudStorageConfig.class);
        this.blockMessaging = context.blockMessaging();
        if (config.retryEnabled()) {
            this.threadPoolManager = context.threadPoolManager();
            this.retryBuffer = new RetryBuffer(config);
        }
        metricRegistry = context.metricRegistry();
        // @todo(#XXXX) replace these warnings with proper plugin health reporting once
        //   the block node supports plugin-level healthy/unhealthy status indication.
        if (config.bucketName() == null || config.bucketName().isBlank()) {
            LOGGER.log(
                    WARNING,
                    "cloud.storage.expanded.bucketName is blank; S3 uploads will be skipped until configured.");
        }
        if (config.endpointUrl() == null || config.endpointUrl().isBlank()) {
            LOGGER.log(
                    WARNING,
                    "cloud.storage.expanded.endpointUrl is blank; S3 uploads will be skipped until configured.");
        }
        if (config.regionName() == null || config.regionName().isBlank()) {
            LOGGER.log(
                    WARNING,
                    "cloud.storage.expanded.regionName is blank; S3 uploads will be skipped until configured.");
        }
        blockMessaging.registerBlockNotificationHandler(this, false, name());
    }

    /// {@inheritDoc}
    @Override
    public void start() {
        if (s3Client == null) {
            try {
                s3Client = new BuckyS3UploadClient(config);
                LOGGER.log(INFO, "S3 client initialized successfully");
            } catch (final UploadException e) {
                final String msg = "Failed to initialize S3 client; uploads will be skipped";
                LOGGER.log(WARNING, msg, e);
            }
        }
        virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();
        completionService = new ExecutorCompletionService<>(virtualThreadExecutor);
        metricsHolder = Objects.requireNonNull(MetricsHolder.createMetrics(metricRegistry));
        if (config.retryEnabled()) {
            retryCompletionService = new ExecutorCompletionService<>(virtualThreadExecutor);
            final Thread.UncaughtExceptionHandler handler =
                    (thread, e) -> LOGGER.log(WARNING, "Uncaught exception in thread: " + thread.getName(), e);
            retryScheduler = threadPoolManager.createSingleThreadScheduledExecutor("CloudExpandedRetry", handler);
            retryScheduler.scheduleAtFixedRate(
                    this::retryStagedBlocks,
                    config.retryIntervalSeconds(),
                    config.retryIntervalSeconds(),
                    TimeUnit.SECONDS);
        }
    }

    /// {@inheritDoc}
    ///
    /// Unregisters from block notifications first to stop new uploads from being submitted,
    /// then shuts down the dedicated executor and waits up to `uploadTimeoutSeconds` for
    /// in-flight tasks to complete before closing the S3 client.
    @Override
    public void stop() {
        // Unregister first so no new upload tasks are submitted during drain.
        blockMessaging.unregisterBlockNotificationHandler(this);
        if (retryScheduler != null) {
            retryScheduler.shutdownNow();
            retryScheduler = null;
        }
        // Closed before awaiting termination below: otherwise a task still running past the
        // await timeout could stage() a new entry after flushPendingRetriesAsFailures() has
        // already drained the buffer, orphaning it forever.
        if (retryBuffer != null) {
            retryBuffer.close();
        }
        if (virtualThreadExecutor != null) {
            // Stop accepting new tasks (none expected since we just unregistered), then wait
            // for all running uploads to finish. The executor tracks running tasks authoritatively,
            // removing the need for manual in-flight counting.
            virtualThreadExecutor.shutdown();
            try {
                final boolean terminated =
                        virtualThreadExecutor.awaitTermination(config.uploadTimeoutSeconds(), TimeUnit.SECONDS);
                if (!terminated) {
                    LOGGER.log(
                            WARNING,
                            "Some upload tasks did not complete within the {0}s timeout.",
                            config.uploadTimeoutSeconds());
                }
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            // Final non-blocking sweep to publish results from tasks that completed during await.
            drainCompletedTasks();
            virtualThreadExecutor = null;
        }
        flushPendingRetriesAsFailures();
        if (s3Client != null) {
            try {
                s3Client.close();
            } catch (final Exception e) {
                LOGGER.log(WARNING, "Encountered error closing s3Client.", e);
            }
            s3Client = null;
        }
    }

    /// Reports every block still in {@link #retryBuffer} as a terminal failure. Since nothing
    /// persists across a restart, a block left buffered here would otherwise never receive any
    /// `PersistedNotification` at all once {@link #stop()} discards the buffer.
    private void flushPendingRetriesAsFailures() {
        if (retryBuffer == null) {
            return;
        }
        for (final BufferedEntry entry : retryBuffer.drainAll()) {
            sendPersistedNotification(entry.blockNumber(), false, entry.blockSource());
            metricsHolder.retryExhaustedTotal().increment();
            metricsHolder.uploadFailuresTotal().increment();
            LOGGER.log(WARNING, "Block {0}: retry buffer dropped at shutdown; reporting failure.", entry.blockNumber());
        }
        updatePendingRetryGauge();
    }

    // ---- BlockNotificationHandler -------------------------------------------

    /// {@inheritDoc}
    ///
    /// Drains any completed upload tasks (publishing {@link PersistedNotification} for
    /// each), then submits this block as a new {@link SingleBlockStoreTask} to the
    /// {@link CompletionService}.
    @Override
    public void handleVerification(@NonNull final VerificationNotification notification) {
        if (s3Client == null) {
            LOGGER.log(
                    TRACE, "Skipping upload for block {0}: S3 client is not configured.", notification.blockNumber());
        } else if (!notification.success()) {
            LOGGER.log(
                    TRACE, "Skipping upload for block {0}: verification did not succeed.", notification.blockNumber());
        } else if (notification.blockNumber() < 0) {
            LOGGER.log(INFO, "Skipping upload: invalid block number {0}.", notification.blockNumber());
        } else if (notification.block() == null) {
            LOGGER.log(INFO, "Skipping upload for block {0}: block payload is null.", notification.blockNumber());
        } else {
            // Drain results from previously submitted tasks before queuing new work.
            drainCompletedTasks();

            final String objectKey = buildBlockObjectKey(notification.blockNumber());
            completionService.submit(new SingleBlockStoreTask(
                    notification.blockNumber(),
                    notification.block(),
                    s3Client,
                    objectKey,
                    config.storageClass().name(),
                    notification.source(),
                    retryBuffer));
        }
    }

    // ---- Private helpers ----------------------------------------------------

    /// Polls the {@link CompletionService} for all currently-completed upload tasks,
    /// stages their results in the {@link #pendingPublish} map by block number, then
    /// publishes all staged results in ascending block-number order.
    ///
    /// This is a non-blocking drain — it only collects tasks that have already finished.
    /// Results are published in block-number order regardless of completion order, so
    /// downstream consumers receive monotonically increasing block-number notifications.
    ///
    /// Package-private visibility allows test helpers in this package to drive the drain
    /// loop without holding production threads.
    void drainCompletedTasks() {
        // Collect all currently-finished futures into the sorted staging map.
        Future<SingleBlockStoreTask.UploadResult> completed;
        while ((completed = completionService.poll()) != null) {
            processCompletedFuture(completed);
        }
        // Publish staged results in ascending block-number order.
        Map.Entry<Long, SingleBlockStoreTask.UploadResult> entry;
        while ((entry = pendingPublish.pollFirstEntry()) != null) {
            publishResult(entry.getValue());
        }
        drainCompletedRetries();
    }

    /// Extracts the {@link SingleBlockStoreTask.UploadResult} from a completed future and
    /// stages it in {@link #pendingPublish} for ordered publication.
    ///
    /// Cancelled tasks are logged at TRACE and skipped — cancellation is expected during
    /// normal shutdown. {@link ExecutionException} wraps an unexpected unchecked failure
    /// inside the task; the failure counter is incremented and the cause is logged.
    private void processCompletedFuture(final Future<SingleBlockStoreTask.UploadResult> completed) {
        if (!completed.isCancelled()) {
            try {
                final SingleBlockStoreTask.UploadResult result = completed.get();
                pendingPublish.put(result.blockNumber(), result);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.log(WARNING, "Interrupted while collecting upload result.", e);
            } catch (final ExecutionException e) {
                // SingleBlockStoreTask.call() catches all known exceptions, including any
                // RuntimeException, internally and returns an UploadResult. An ExecutionException
                // here means something more severe (e.g. an Error) escaped the task — count it as
                // a failure and log the root cause.
                metricsHolder.uploadFailuresTotal().increment();
                LOGGER.log(INFO, "Unexpected exception in upload task", e.getCause());
            }
        } else {
            LOGGER.log(TRACE, "Upload task was cancelled during shutdown.");
        }
    }

    /// Publishes a {@link PersistedNotification} for the given upload result and updates metrics.
    ///
    /// A failed upload whose bytes were staged for background retry does **not** publish a
    /// notification yet — `succeeded=false` is deferred until retries are exhausted (see
    /// {@link #processRetryResult}), so a transient S3 hiccup does not tear down live publisher
    /// connections or trigger a peer re-fetch while a local retry is still in progress.
    ///
    /// If staging this block evicted an older one to make room (buffer was at
    /// `retryMaxPendingBlocks`), that evicted block is reported as a terminal failure here too —
    /// it will never be retried.
    private void publishResult(final SingleBlockStoreTask.UploadResult result) {
        if (result.succeeded()) {
            // Clears any stale buffered entry from an earlier failed attempt for this block.
            if (retryBuffer != null) {
                retryBuffer.unstage(result.blockNumber());
            }
            sendPersistedNotification(result.blockNumber(), true, result.blockSource());
            metricsHolder.uploadsTotal().increment();
            metricsHolder.uploadBytesTotal().increment(result.bytesUploaded());
        } else if (result.stagedForRetry()) {
            LOGGER.log(
                    INFO,
                    "Block {0}: upload failed ({1}); buffered for background retry.",
                    result.blockNumber(),
                    result.status());
        } else {
            sendPersistedNotification(result.blockNumber(), false, result.blockSource());
            metricsHolder.uploadFailuresTotal().increment();
            LOGGER.log(
                    INFO,
                    "Block {0}: upload failed ({1}); PersistedNotification sent with succeeded=false.",
                    result.blockNumber(),
                    result.status());
        }
        if (result.evictedEntry() != null) {
            reportEvictedBlock(result.evictedEntry());
        }
        updatePendingRetryGauge();
        metricsHolder.uploadLatencyNs().increment(result.uploadDurationNs());
    }

    /// Reports a terminal `succeeded=false` for a block evicted from {@link #retryBuffer} to make
    /// room for a newer failure once `retryMaxPendingBlocks` was reached — it will never be retried.
    private void reportEvictedBlock(final BufferedEntry evicted) {
        sendPersistedNotification(evicted.blockNumber(), false, evicted.blockSource());
        metricsHolder.retryExhaustedTotal().increment();
        metricsHolder.uploadFailuresTotal().increment();
        LOGGER.log(
                INFO,
                "Block {0}: evicted from the retry buffer to make room for a newer failure; reporting failure.",
                evicted.blockNumber());
    }

    /// Sends a {@link PersistedNotification}, swallowing a {@link RejectedExecutionException} raised
    /// when the messaging facility has already stopped.
    ///
    /// The messaging facility's own {@code stop()} runs before this plugin's during
    /// {@code BlockNodeApp} shutdown (loadedPlugins order), so an upload that finishes during this
    /// plugin's final drain (see {@link #stop()}) can find messaging already stopped. Nothing
    /// downstream can consume the notification at that point, so log and continue rather than
    /// letting this escape {@link #stop()} and abort the app's shutdown sequence for every plugin
    /// still left to stop.
    private void sendPersistedNotification(final long blockNumber, final boolean succeeded, final BlockSource source) {
        try {
            blockMessaging.sendBlockPersisted(new PersistedNotification(blockNumber, succeeded, 0, source));
        } catch (final RejectedExecutionException e) {
            LOGGER.log(
                    INFO,
                    "Block {0}: could not deliver PersistedNotification; messaging facility already stopped.",
                    blockNumber);
        }
    }

    /// Scheduled tick (see {@link #start}) that first drains any retry attempts completed since
    /// the last tick, then scans {@link #retryBuffer} for blocks whose backoff has elapsed and
    /// submits a {@link RetryUploadTask} for each to {@link #retryCompletionService}.
    ///
    /// Retries run independently of {@link #completionService}/{@link #pendingPublish} — that
    /// machinery exists to keep the *live* block stream monotonically increasing; retries are
    /// out-of-band corrections for already-verified blocks and don't need it.
    ///
    /// Package-private visibility allows tests to drive a retry tick without waiting on the
    /// real scheduler interval.
    void retryStagedBlocks() {
        if (s3Client == null || retryCompletionService == null) {
            return;
        }
        drainCompletedRetries();
        for (final BufferedEntry entry : retryBuffer.dueForRetry(System.currentTimeMillis())) {
            if (!retryFutureBlockNumbers.containsValue(entry.blockNumber())) {
                final Future<SingleBlockStoreTask.UploadResult> future =
                        retryCompletionService.submit(new RetryUploadTask(
                                entry.blockNumber(),
                                entry.compressedBytes(),
                                s3Client,
                                entry.objectKey(),
                                entry.storageClass(),
                                entry.blockSource()));
                retryFutureBlockNumbers.put(future, entry.blockNumber());
            }
        }
    }

    /// Polls {@link #retryCompletionService} for retry attempts that have finished and applies
    /// each via {@link #processRetryResult}. Non-blocking — only collects tasks already done.
    private void drainCompletedRetries() {
        if (retryCompletionService == null) {
            return;
        }
        Future<SingleBlockStoreTask.UploadResult> completed;
        while ((completed = retryCompletionService.poll()) != null) {
            processCompletedRetryFuture(completed);
        }
    }

    /// Extracts the {@link SingleBlockStoreTask.UploadResult} from a completed retry future and
    /// hands it to {@link #processRetryResult}, clearing {@link #retryFutureBlockNumbers} for that
    /// block regardless of outcome so a later tick can retry it again if needed.
    ///
    /// Cancelled tasks are logged at TRACE and skipped — cancellation is not expected in practice
    /// since {@link #stop} uses {@code shutdown()} rather than {@code shutdownNow()}.
    /// {@link ExecutionException} wraps an unexpected unchecked failure inside the task, mirroring
    /// {@link #processCompletedFuture}.
    private void processCompletedRetryFuture(final Future<SingleBlockStoreTask.UploadResult> completed) {
        final Long blockNumber = retryFutureBlockNumbers.remove(completed);
        if (completed.isCancelled()) {
            LOGGER.log(TRACE, "Retry upload task was cancelled during shutdown.");
            return;
        }
        try {
            processRetryResult(completed.get());
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (final ExecutionException e) {
            metricsHolder.uploadFailuresTotal().increment();
            LOGGER.log(INFO, "Unexpected exception in retry upload task", e.getCause());
            // Still counts as an attempt
            if (blockNumber != null) {
                final RetryBuffer.FailureResult failure = retryBuffer.recordFailure(blockNumber);
                if (failure.outcome() == RetryOutcome.EXHAUSTED) {
                    sendPersistedNotification(blockNumber, false, failure.blockSource());
                    metricsHolder.retryExhaustedTotal().increment();
                    updatePendingRetryGauge();
                }
            }
        }
    }

    /// Applies the outcome of one background retry attempt: on success, clears the buffer and
    /// publishes the deferred `succeeded=true` notification; on failure, records another attempt
    /// and — only once retries are exhausted — publishes the deferred `succeeded=false`
    /// notification.
    private void processRetryResult(final SingleBlockStoreTask.UploadResult result) {
        final long blockNumber = result.blockNumber();
        if (result.succeeded()) {
            if (retryBuffer.unstage(blockNumber)) {
                sendPersistedNotification(blockNumber, true, result.blockSource());
                metricsHolder.retrySuccessTotal().increment();
                metricsHolder.uploadsTotal().increment();
                metricsHolder.uploadBytesTotal().increment(result.bytesUploaded());
                LOGGER.log(INFO, "Block {0}: recovered via background retry.", blockNumber);
            } else {
                // Already resolved elsewhere (e.g. flushed as a failure at shutdown) — a success
                // notification here would contradict the one already sent.
                LOGGER.log(
                        TRACE,
                        "Block {0}: retry succeeded but the block was already resolved; no notification sent.",
                        blockNumber);
            }
        } else {
            final RetryOutcome outcome = retryBuffer.recordFailure(blockNumber).outcome();
            if (outcome == RetryOutcome.EXHAUSTED) {
                sendPersistedNotification(blockNumber, false, result.blockSource());
                metricsHolder.retryExhaustedTotal().increment();
                metricsHolder.uploadFailuresTotal().increment();
                LOGGER.log(INFO, "Block {0}: exhausted background retries; reporting persistent failure.", blockNumber);
            } else if (outcome == RetryOutcome.NOT_STAGED) {
                // Already resolved elsewhere; a notification here would contradict the one already sent.
                LOGGER.log(
                        TRACE,
                        "Block {0}: retry attempt failed but the block was already resolved; no notification sent.",
                        blockNumber);
            } else {
                LOGGER.log(
                        DEBUG,
                        "Block {0}: retry attempt failed again ({1}); will retry later.",
                        blockNumber,
                        result.status());
            }
        }
        updatePendingRetryGauge();
    }

    /// Refreshes the {@code cloud_expanded_pending_retry_blocks} gauge from
    /// {@link RetryBuffer#pendingCount}.
    private void updatePendingRetryGauge() {
        if (retryBuffer != null) {
            metricsHolder.pendingRetryBlocks().set(retryBuffer.pendingCount());
        }
    }

    /// Builds the S3 object key for the given block number using the 4-digit folder
    /// hierarchy.
    ///
    /// Format: `{prefix}/AAAA/BBBB/CCCC/DDDD/EEE.blk.zstd`
    ///
    /// The 19-digit zero-padded block number is split as 4/4/4/4/3 digits:
    ///
    /// - Block 1 → `blocks/0000/0000/0000/0000/001.blk.zstd`
    /// - Block 108273182 → `blocks/0000/0000/0010/8273/182.blk.zstd`
    ///
    /// @param blockNumber the block number
    /// @return the S3 object key
    String buildBlockObjectKey(final long blockNumber) {
        final long seg1 = blockNumber / 1_000_000_000_000_000L;
        final long seg2 = blockNumber / 100_000_000_000L % 10_000L;
        final long seg3 = blockNumber / 10_000_000L % 10_000L;
        final long seg4 = blockNumber / 1_000L % 10_000L;
        final long seg5 = blockNumber % 1_000L;
        final String folderPath = String.format("%04d/%04d/%04d/%04d/%03d", seg1, seg2, seg3, seg4, seg5);
        final String prefix = config.objectKeyPrefix();
        return (prefix == null || prefix.isEmpty())
                ? folderPath + ".blk.zstd"
                : prefix + "/" + folderPath + ".blk.zstd";
    }

    // ---- Metrics ------------------------------------------------------------

    /// Holds all counters reported by this plugin.
    ///
    /// @param uploadsTotal        number of blocks successfully uploaded to S3 (first attempt or retry)
    /// @param uploadFailuresTotal number of blocks that ended in a terminal failure (compression error
    ///                            or retries exhausted)
    /// @param uploadBytesTotal    total compressed bytes successfully uploaded to S3
    /// @param uploadLatencyNs     total upload time in nanoseconds
    /// @param pendingRetryBlocks  current number of blocks buffered in memory awaiting a background retry
    /// @param retrySuccessTotal   number of blocks recovered by a later background retry
    /// @param retryExhaustedTotal number of blocks dropped after exhausting retries, or at shutdown
    public record MetricsHolder(
            LongCounter.Measurement uploadsTotal,
            LongCounter.Measurement uploadFailuresTotal,
            LongCounter.Measurement uploadBytesTotal,
            LongCounter.Measurement uploadLatencyNs,
            LongGauge.Measurement pendingRetryBlocks,
            LongCounter.Measurement retrySuccessTotal,
            LongCounter.Measurement retryExhaustedTotal) {

        /// Registers all counters with the given {@link MetricRegistry} instance.
        ///
        /// @param metricRegistry the metrics registry
        /// @return a new `MetricsHolder` with all counters registered
        public static MetricsHolder createMetrics(@NonNull final MetricRegistry metricRegistry) {
            return new MetricsHolder(
                    metricRegistry
                            .register(LongCounter.builder(METRIC_EXPANDED_CLOUD_STORAGE_TOTAL_UPLOADS)
                                    .setDescription("Number of blocks successfully uploaded to S3-compatible storage"))
                            .getOrCreateNotLabeled(),
                    metricRegistry
                            .register(
                                    LongCounter.builder(METRIC_EXPANDED_CLOUD_STORAGE_TOTAL_UPLOAD_FAILURES)
                                            .setDescription(
                                                    "Number of block uploads that ended in terminal failure (compression error or retries exhausted)"))
                            .getOrCreateNotLabeled(),
                    metricRegistry
                            .register(LongCounter.builder(METRIC_EXPANDED_CLOUD_STORAGE_TOTAL_UPLOADED_BYTES)
                                    .setDescription(
                                            "Total compressed bytes successfully uploaded to S3-compatible storage"))
                            .getOrCreateNotLabeled(),
                    metricRegistry
                            .register(LongCounter.builder(METRIC_EXPANDED_CLOUD_STORAGE_UPLOAD_LATENCY_NS)
                                    .setDescription(
                                            "Total time spent uploading blocks in cloud_expanded in nanoseconds"))
                            .getOrCreateNotLabeled(),
                    metricRegistry
                            .register(
                                    LongGauge.builder(METRIC_EXPANDED_CLOUD_STORAGE_PENDING_RETRY_BLOCKS)
                                            .setDescription(
                                                    "Number of blocks buffered in memory and awaiting a background retry upload"))
                            .getOrCreateNotLabeled(),
                    metricRegistry
                            .register(LongCounter.builder(METRIC_EXPANDED_CLOUD_STORAGE_RETRY_SUCCESS_TOTAL)
                                    .setDescription("Number of blocks recovered by a later background retry"))
                            .getOrCreateNotLabeled(),
                    metricRegistry
                            .register(
                                    LongCounter.builder(METRIC_EXPANDED_CLOUD_STORAGE_RETRY_EXHAUSTED_TOTAL)
                                            .setDescription(
                                                    "Number of blocks dropped after exhausting retries, or still buffered at shutdown"))
                            .getOrCreateNotLabeled());
        }
    }
}
