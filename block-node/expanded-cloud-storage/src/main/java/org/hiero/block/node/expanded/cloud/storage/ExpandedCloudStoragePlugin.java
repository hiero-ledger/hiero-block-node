// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.expanded.cloud.storage;

import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.TRACE;
import static java.lang.System.Logger.Level.WARNING;
import static org.hiero.block.node.spi.BlockNodePlugin.METRICS_CATEGORY;

import com.swirlds.metrics.api.Counter;
import com.swirlds.metrics.api.Metrics;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.hiero.block.common.utils.StringUtilities;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;

/**
 * A block node plugin that uploads each individually-verified block as a compressed
 * {@code .blk.zstd} object directly to any S3-compatible object store (AWS S3, GCS via
 * S3-interop, etc.).
 *
 * <p>This plugin uploads one block per object. This makes individual blocks immediately queryable
 *  and suits consumers that want block-level granularity in the cloud with minimal latency.
 *
 * <h2>Trigger: {@link VerificationNotification}</h2>
 * The plugin reacts to {@code handleVerification()} rather than {@code handlePersisted()}.
 * This allows cloud upload and local file storage ({@code blocks-file-recent}) to run in
 * parallel — each registered handler gets its own virtual thread. Block bytes are taken
 * directly from {@code notification.block()}, eliminating any dependency on the local
 * historical block provider.
 *
 * <h2>Async upload via CompletionService</h2>
 * Each verified block is submitted as a {@link SingleBlockStoreTask} to a
 * {@link CompletionService} backed by a virtual-thread executor. The plugin drains completed
 * tasks immediately before each new notification, publishing a {@link PersistedNotification}
 * for every completed upload (success or failure). This ensures callers waiting on
 * {@code PersistedNotification} are notified even when uploads overlap block boundaries.
 *
 * <h2>Object key format</h2>
 * <pre>{objectKeyPrefix}/AAAA/BBBB/CCCC/DDDD/EEE.blk.zstd</pre>
 * The 19-digit zero-padded block number is split into 4-digit folder groups (4/4/4/4/3)
 * for lexicographic ordering and S3 prefix partitioning. Example:
 * <pre>
 * Block          1 → blocks/0000/0000/0000/0000/001.blk.zstd
 * Block  108273182 → blocks/0000/0000/0010/8273/182.blk.zstd
 * </pre>
 *
 * <h2>Enable / disable</h2>
 * The plugin is disabled when {@code expanded.cloud.storage.endpointUrl} is blank (the
 * default). Set it to a non-empty URL to activate uploads.
 *
 * <h2>S3 client implementation</h2>
 * Uploads are performed via {@link S3UploadClient}, a package-private abstract class whose
 * production instance wraps {@code com.hedera.bucky.S3Client} directly.
 */
public class ExpandedCloudStoragePlugin implements BlockNodePlugin, BlockNotificationHandler {

    private static final System.Logger LOGGER = System.getLogger(ExpandedCloudStoragePlugin.class.getName());

    /** Plugin configuration, set during {@link #init}. */
    private ExpandedCloudStorageConfig config;

    /** Messaging facility used to publish {@link PersistedNotification} results. */
    private BlockMessagingFacility blockMessaging;

    /**
     * The active S3 upload client. {@code null} when the plugin is disabled.
     * May be pre-set by the package-private test constructor.
     */
    private S3UploadClient s3Client;

    /** CompletionService for async block upload tasks. */
    private CompletionService<SingleBlockStoreTask.UploadResult> completionService;

    /** Virtual-thread executor sourced from the thread-pool manager. Non-null only when enabled. */
    private ExecutorService virtualThreadExecutor;

    /** Count of tasks submitted but not yet drained; used to bound the drain in {@link #stop()}. */
    private final AtomicInteger inFlightCount = new AtomicInteger();

    /** Metrics instance, saved in {@link #init} for use in {@link #start}. */
    private Metrics metrics;

    /** Counters for upload events; non-null only when the plugin is enabled. */
    private MetricsHolder metricsHolder;

    // ---- Constructors -------------------------------------------------------

    /** No-arg constructor used by the Java {@link java.util.ServiceLoader}. */
    public ExpandedCloudStoragePlugin() {}

    /**
     * Package-private constructor for unit tests. Injects a pre-built {@link S3UploadClient}
     * so tests do not need a real S3 endpoint.
     *
     * @param s3Client the upload client to use instead of creating one from config
     */
    ExpandedCloudStoragePlugin(@NonNull final S3UploadClient s3Client) {
        this.s3Client = s3Client;
    }

    // ---- BlockNodePlugin ----------------------------------------------------

    /** {@inheritDoc} */
    @NonNull
    @Override
    public List<Class<? extends Record>> configDataTypes() {
        return List.of(ExpandedCloudStorageConfig.class);
    }

    /** {@inheritDoc} */
    @Override
    public void init(@NonNull final BlockNodeContext context, @NonNull final ServiceBuilder serviceBuilder) {
        this.config = context.configuration().getConfigData(ExpandedCloudStorageConfig.class);
        this.blockMessaging = context.blockMessaging();

        if (StringUtilities.isBlank(config.endpointUrl())) {
            LOGGER.log(INFO, "Expanded Cloud Storage plugin is disabled. No endpoint URL configured.");
            return;
        }

        metrics = context.metrics();
        blockMessaging.registerBlockNotificationHandler(this, false, name());
        virtualThreadExecutor = context.threadPoolManager().getVirtualThreadExecutor();
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        if (virtualThreadExecutor == null) {
            return; // disabled — endpointUrl was blank in init()
        }
        if (s3Client == null) {
            try {
                s3Client = S3UploadClient.forConfig(config);
            } catch (final com.hedera.bucky.S3ClientException e) {
                LOGGER.log(WARNING, "Failed to create S3 client; plugin will be disabled.", e);
                return;
            }
        }
        completionService = new ExecutorCompletionService<>(virtualThreadExecutor);
        metricsHolder = MetricsHolder.createMetrics(metrics);
    }

    /** {@inheritDoc} */
    @Override
    public void stop() {
        if (completionService != null) {
            // Drain in-flight uploads before closing the S3 client. Each task has its own
            // uploadTimeoutSeconds deadline, so waiting that long guarantees all in-flight
            // tasks have either completed or timed out.
            final long deadline = System.currentTimeMillis() + (long) config.uploadTimeoutSeconds() * 1_000L;
            while (inFlightCount.get() > 0 && System.currentTimeMillis() < deadline) {
                final long remainingMs = deadline - System.currentTimeMillis();
                if (remainingMs <= 0) break;
                try {
                    final Future<SingleBlockStoreTask.UploadResult> completed =
                            completionService.poll(Math.min(remainingMs, 200L), TimeUnit.MILLISECONDS);
                    if (completed != null) {
                        publishResult(completed);
                    }
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            // Final non-blocking sweep for any tasks that just landed.
            drainCompletedTasks();
        }
        if (s3Client != null) {
            s3Client.close();
            s3Client = null;
        }
    }

    // ---- BlockNotificationHandler -------------------------------------------

    /**
     * {@inheritDoc}
     *
     * <p>Drains any completed upload tasks (publishing {@link PersistedNotification} for each),
     * then submits this block as a new {@link SingleBlockStoreTask} to the
     * {@link CompletionService}.
     */
    @Override
    public void handleVerification(@NonNull final VerificationNotification notification) {
        if (s3Client == null || completionService == null) {
            return;
        }

        // Drain results from previously submitted tasks before queuing new work.
        drainCompletedTasks();

        if (!notification.success()) {
            LOGGER.log(
                    TRACE, "Skipping upload for block {0}: verification did not succeed.", notification.blockNumber());
            return;
        }
        final long blockNumber = notification.blockNumber();
        if (blockNumber < 0) {
            LOGGER.log(TRACE, "Skipping upload: invalid block number {0}.", blockNumber);
            return;
        }
        final BlockUnparsed block = notification.block();
        if (block == null) {
            LOGGER.log(WARNING, "Skipping upload for block {0}: block payload is null.", blockNumber);
            return;
        }

        final String objectKey = buildObjectKey(blockNumber);
        inFlightCount.incrementAndGet();
        completionService.submit(new SingleBlockStoreTask(
                blockNumber,
                block,
                s3Client,
                objectKey,
                config.storageClass(),
                config.uploadTimeoutSeconds(),
                notification.source()));
    }

    // ---- Private helpers ----------------------------------------------------

    /**
     * Polls the {@link CompletionService} for all currently-completed upload tasks and
     * publishes a {@link PersistedNotification} for each result.
     *
     * <p>This is a non-blocking drain — it only collects tasks that have already finished.
     * Package-private visibility allows test helpers in this package to drive the drain loop
     * without holding production threads or needing a pending-task counter.
     */
    void drainCompletedTasks() {
        if (completionService == null) {
            return;
        }
        Future<SingleBlockStoreTask.UploadResult> completed;
        while ((completed = completionService.poll()) != null) {
            publishResult(completed);
        }
    }

    /**
     * Decrements the in-flight counter and publishes a {@link PersistedNotification} for the
     * given completed upload future. Handles {@link InterruptedException} and
     * {@link ExecutionException} without propagating.
     */
    private void publishResult(final Future<SingleBlockStoreTask.UploadResult> completed) {
        inFlightCount.decrementAndGet();
        try {
            final SingleBlockStoreTask.UploadResult result = completed.get();
            blockMessaging.sendBlockPersisted(
                    new PersistedNotification(result.blockNumber(), result.succeeded(), 0, result.blockSource()));
            if (!result.succeeded()) {
                if (metricsHolder != null) metricsHolder.uploadFailuresTotal().increment();
                LOGGER.log(
                        WARNING,
                        "Block {0}: upload failed; PersistedNotification sent with succeeded=false.",
                        result.blockNumber());
            } else {
                if (metricsHolder != null) {
                    metricsHolder.uploadsTotal().increment();
                    metricsHolder.uploadBytesTotal().add(result.bytesUploaded());
                }
                LOGGER.log(TRACE, "Block {0}: upload succeeded.", result.blockNumber());
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.log(WARNING, "Interrupted while draining upload results.", e);
        } catch (final ExecutionException e) {
            LOGGER.log(
                    WARNING,
                    "Unexpected exception in upload task: {0}",
                    e.getCause().getMessage());
        }
    }

    /**
     * Builds the S3 object key for the given block number using the 4‑digit folder hierarchy.
     *
     * <p>Format: {@code {prefix}/AAAA/BBBB/CCCC/DDDD/EEE.blk.zstd}
     * <p>The 19-digit zero-padded block number is split as 4/4/4/4/3 characters:
     * <ul>
     *   <li>Block 1 → {@code blocks/0000/0000/0000/0000/001.blk.zstd}</li>
     *   <li>Block 108273182 → {@code blocks/0000/0000/0010/8273/182.blk.zstd}</li>
     * </ul>
     *
     * @param blockNumber the block number
     * @return the S3 object key
     */
    String buildObjectKey(final long blockNumber) {
        final char[] buf = new char[19];
        long n = blockNumber;
        for (int i = 18; i >= 0; i--) {
            buf[i] = (char) ('0' + (n % 10));
            n /= 10;
        }
        final String folderPath = new String(buf, 0, 4)
                + "/" + new String(buf, 4, 4)
                + "/" + new String(buf, 8, 4)
                + "/" + new String(buf, 12, 4)
                + "/" + new String(buf, 16, 3);
        final String prefix = config.objectKeyPrefix();
        return (prefix == null || prefix.isEmpty())
                ? folderPath + ".blk.zstd"
                : prefix + "/" + folderPath + ".blk.zstd";
    }

    // ---- Metrics ------------------------------------------------------------

    /**
     * Holds all counters reported by this plugin.
     *
     * @param uploadsTotal        number of blocks successfully uploaded to S3
     * @param uploadFailuresTotal number of blocks that failed to upload (S3 error, timeout, etc.)
     * @param uploadBytesTotal    total compressed bytes successfully uploaded to S3
     */
    public record MetricsHolder(Counter uploadsTotal, Counter uploadFailuresTotal, Counter uploadBytesTotal) {

        /**
         * Registers all counters with the given {@link Metrics} instance.
         *
         * @param metrics the metrics registry
         * @return a new {@code MetricsHolder} with all counters registered
         */
        public static MetricsHolder createMetrics(@NonNull final Metrics metrics) {
            return new MetricsHolder(
                    metrics.getOrCreate(new Counter.Config(METRICS_CATEGORY, "expanded_cloud_storage_uploads_total")
                            .withDescription("Number of blocks successfully uploaded to S3-compatible storage")),
                    metrics.getOrCreate(
                            new Counter.Config(METRICS_CATEGORY, "expanded_cloud_storage_upload_failures_total")
                                    .withDescription(
                                            "Number of block uploads that failed (S3 error, timeout, compression error)")),
                    metrics.getOrCreate(new Counter.Config(
                                    METRICS_CATEGORY, "expanded_cloud_storage_upload_bytes_total")
                            .withDescription("Total compressed bytes successfully uploaded to S3-compatible storage")));
        }
    }
}
