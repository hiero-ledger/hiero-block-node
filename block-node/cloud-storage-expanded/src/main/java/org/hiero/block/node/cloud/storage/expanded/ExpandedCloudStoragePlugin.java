// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.expanded;

import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.TRACE;
import static java.lang.System.Logger.Level.WARNING;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.metrics.LongCounter;
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
    @NonNull
    @Override
    public List<Class<? extends Record>> configDataTypes() {
        return List.of(ExpandedCloudStorageConfig.class);
    }

    /// {@inheritDoc}
    @Override
    public void init(@NonNull final BlockNodeContext context, @NonNull final ServiceBuilder serviceBuilder) {
        this.config = context.configuration().getConfigData(ExpandedCloudStorageConfig.class);
        this.blockMessaging = context.blockMessaging();
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
        if (s3Client != null) {
            try {
                s3Client.close();
            } catch (final Exception e) {
                LOGGER.log(WARNING, "Encountered error closing s3Client.", e);
            }
            s3Client = null;
        }
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
                    notification.source()));
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
                // SingleBlockStoreTask.call() catches all known exceptions internally and returns
                // an UploadResult. An ExecutionException here means an unexpected RuntimeException
                // escaped the task — count it as a failure and log the root cause.
                metricsHolder.uploadFailuresTotal().increment();
                LOGGER.log(WARNING, "Unexpected exception in upload task", e.getCause());
            }
        } else {
            LOGGER.log(TRACE, "Upload task was cancelled during shutdown.");
        }
    }

    /// Publishes a {@link PersistedNotification} for the given upload result and updates metrics.
    private void publishResult(final SingleBlockStoreTask.UploadResult result) {
        blockMessaging.sendBlockPersisted(
                new PersistedNotification(result.blockNumber(), result.succeeded(), 0, result.blockSource()));
        if (!result.succeeded()) {
            metricsHolder.uploadFailuresTotal().increment();
            LOGGER.log(
                    INFO,
                    "Block {0}: upload failed ({1}); PersistedNotification sent with succeeded=false.",
                    result.blockNumber(),
                    result.status());
        } else {
            metricsHolder.uploadsTotal().increment();
            metricsHolder.uploadBytesTotal().increment(result.bytesUploaded());
        }
        metricsHolder.uploadLatencyNs().increment(result.uploadDurationNs());
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
    /// @param uploadsTotal        number of blocks successfully uploaded to S3
    /// @param uploadFailuresTotal number of blocks that failed to upload (S3 error, timeout, etc.)
    /// @param uploadBytesTotal    total compressed bytes successfully uploaded to S3
    /// @param uploadLatencyNs     total upload time in nanoseconds
    public record MetricsHolder(
            LongCounter.Measurement uploadsTotal,
            LongCounter.Measurement uploadFailuresTotal,
            LongCounter.Measurement uploadBytesTotal,
            LongCounter.Measurement uploadLatencyNs) {

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
                                                    "Number of block uploads that failed (S3 error, timeout, compression error)"))
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
                            .getOrCreateNotLabeled());
        }
    }
}
