// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.expanded;

import static java.lang.System.Logger.Level.TRACE;
import static java.lang.System.Logger.Level.WARNING;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
/// {@link CompletionService} backed by a virtual-thread executor. The plugin drains
/// completed tasks immediately before each new notification, publishing a
/// {@link PersistedNotification} for every completed upload (success or failure). This
/// ensures callers waiting on `PersistedNotification` are notified even when uploads
/// overlap block boundaries.
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
/// custom {@link S3UploadClient} subclass via the package-private constructor.
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

    /// The active S3 upload client. `null` when the plugin is misconfigured or not yet
    /// started. May be pre-set by the package-private test constructor.
    private S3UploadClient s3Client;

    /// `CompletionService` for async block upload tasks.
    private CompletionService<SingleBlockStoreTask.UploadResult> completionService;

    /// Virtual-thread executor sourced from the thread-pool manager.
    private ExecutorService virtualThreadExecutor;

    /// Count of tasks submitted but not yet drained; used to bound the drain in
    /// {@link #stop()}.
    private final AtomicInteger inFlightCount = new AtomicInteger(0);

    /// Metrics instance, saved in {@link #init} for use in {@link #start}.
    private MetricRegistry metricRegistry;

    /// Counters for upload events; non-null only when the plugin is active.
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
        blockMessaging.registerBlockNotificationHandler(this, false, name());
        virtualThreadExecutor = context.threadPoolManager().getVirtualThreadExecutor();
    }

    /// {@inheritDoc}
    @Override
    public void start() {
        try {
            if (s3Client == null) {
                s3Client = new BuckyS3UploadClient(config);
            }
            completionService = new ExecutorCompletionService<>(virtualThreadExecutor);
            metricsHolder = Objects.requireNonNull(MetricsHolder.createMetrics(metricRegistry));
        } catch (final UploadException e) {
            LOGGER.log(WARNING, "Failed to create S3 client; plugin will be inactive.", e);
        }
    }

    /// {@inheritDoc}
    @Override
    public void stop() {
        if (completionService != null) {
            // Drain in-flight uploads before closing the S3 client. Each task has its own
            // uploadTimeoutSeconds deadline, so waiting that long guarantees all in-flight
            // tasks have either completed or timed out.
            drainInFlightTasks();

            // Final non-blocking sweep for any tasks that just landed.
            drainCompletedTasks();
        }
        if (s3Client != null) {
            try {
                s3Client.close();
            } catch (Exception e) {
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
        if (s3Client == null || completionService == null) {
            LOGGER.log(TRACE, "Skipping upload: null s3Client or completionService.");
        } else if (!notification.success()) {
            LOGGER.log(
                    TRACE, "Skipping upload for block {0}: verification did not succeed.", notification.blockNumber());
        } else if (notification.blockNumber() < 0) {
            LOGGER.log(TRACE, "Skipping upload: invalid block number {0}.", notification.blockNumber());
        } else if (notification.block() == null) {
            LOGGER.log(WARNING, "Skipping upload for block {0}: block payload is null.", notification.blockNumber());
        } else {
            // Drain results from previously submitted tasks before queuing new work.
            drainCompletedTasks();

            final String objectKey = buildBlockObjectKey(notification.blockNumber());
            inFlightCount.incrementAndGet();
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

    /// Polls the {@link CompletionService} for all currently-completed upload tasks and
    /// publishes a {@link PersistedNotification} for each result.
    ///
    /// This is a non-blocking drain — it only collects tasks that have already finished.
    /// Package-private visibility allows test helpers in this package to drive the drain
    /// loop without holding production threads or needing a pending-task counter.
    void drainCompletedTasks() {
        if (completionService == null) {
            return;
        }
        Future<SingleBlockStoreTask.UploadResult> completed;
        while ((completed = completionService.poll()) != null) {
            publishResult(completed);
        }
    }

    /// Drain in-flight uploads before closing the S3 client.
    ///
    /// Each task has its own `uploadTimeoutSeconds` deadline, so waiting that long
    /// guarantees all in-flight tasks have either completed or timed out.
    private void drainInFlightTasks() {
        try {
            final long deadline = System.currentTimeMillis() + (long) config.uploadTimeoutSeconds() * 1_000L;
            while (inFlightCount.get() > 0 && System.currentTimeMillis() < deadline) {
                final long remainingMs = deadline - System.currentTimeMillis();
                final Future<SingleBlockStoreTask.UploadResult> completed =
                        completionService.poll(Math.min(remainingMs, 200L), TimeUnit.MILLISECONDS);
                if (completed != null) {
                    publishResult(completed);
                }
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /// Decrements the in-flight counter and publishes a {@link PersistedNotification}
    /// for the given completed upload future. Handles {@link InterruptedException} and
    /// {@link ExecutionException} without propagating.
    private void publishResult(final Future<SingleBlockStoreTask.UploadResult> completed) {
        inFlightCount.decrementAndGet();
        try {
            // check for canceled result
            if (completed.isCancelled()) {
                LOGGER.log(WARNING, "Upload was cancelled");
                return;
            }

            final SingleBlockStoreTask.UploadResult result = completed.get();
            blockMessaging.sendBlockPersisted(
                    new PersistedNotification(result.blockNumber(), result.succeeded(), 0, result.blockSource()));
            if (!result.succeeded()) {
                metricsHolder.uploadFailuresTotal().increment();
                LOGGER.log(
                        WARNING,
                        "Block {0}: upload failed ({1}); PersistedNotification sent with succeeded=false.",
                        result.blockNumber(),
                        result.status());
            } else {
                metricsHolder.uploadsTotal().increment();
                metricsHolder.uploadBytesTotal().increment(result.bytesUploaded());
                LOGGER.log(TRACE, "Block {0}: upload succeeded.", result.blockNumber());
            }

            metricsHolder.uploadLatencyNs().increment(result.uploadDurationNs());
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.log(WARNING, "Interrupted while draining upload results.", e);
        } catch (final ExecutionException e) {
            LOGGER.log(WARNING, "Unexpected exception in upload task: ", e);
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
