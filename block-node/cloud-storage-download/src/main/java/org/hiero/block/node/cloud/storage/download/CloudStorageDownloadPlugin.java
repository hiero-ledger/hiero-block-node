// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.download;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.WARNING;

import com.hedera.bucky.RetryPolicy;
import com.hedera.bucky.S3Client;
import com.hedera.bucky.S3ClientInitializationException;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.StoredBlocksNotification;
import org.hiero.block.node.spi.blockmessaging.TssDataNotification;
import org.hiero.metrics.LongCounter;
import org.hiero.metrics.core.MetricKey;
import org.hiero.metrics.core.MetricRegistry;

/// A block node plugin that recovers missing blocks from the bucket the consensus nodes
/// upload blocks to, when this block node falls behind.
public class CloudStorageDownloadPlugin implements BlockNodePlugin {

    private static final System.Logger LOGGER = System.getLogger(CloudStorageDownloadPlugin.class.getName());

    /// How long stop waits for each thread pool to end before it closes the bucket client.
    private static final long STOP_WAIT_SECONDS = 5L;

    private static final MetricKey<LongCounter> METRIC_BLOCKS_PULLED =
            MetricKey.of("cloud_download_blocks_pulled", LongCounter.class).addCategory(METRICS_CATEGORY);

    private static final MetricKey<LongCounter> METRIC_BLOCKS_ALREADY_STORED = MetricKey.of(
                    "cloud_download_blocks_already_stored", LongCounter.class)
            .addCategory(METRICS_CATEGORY);

    /// Plugin configuration, set during {@link #init}.
    private CloudStorageDownloadConfig config;

    /// Whether the plugin is enabled and its configuration is valid.
    private boolean active;

    /// The bucket client. `null` before {@link #start}, unless set by the test constructor.
    private S3Client s3Client;

    /// Block node context, set during {@link #init}.
    private BlockNodeContext context;

    /// Probes the bucket and pulls; `null` until the plugin has started active.
    private BucketPoller poller;

    /// Runs the poll task; only this thread probes and pulls.
    private ScheduledExecutorService scheduler;

    /// Waits for the outcome of each pulled block; `null` until the plugin has started active.
    private PersistenceAwaiter awaiter;

    /// Runs the downloads of one window; `null` until the plugin has started active.
    private ExecutorService downloader;

    /// The plugin's counters; `null` until the plugin has started active.
    private MetricsHolder metricsHolder;

    /// No-arg constructor used by the Java {@link java.util.ServiceLoader}.
    public CloudStorageDownloadPlugin() {}

    /// Package-private constructor for unit tests, injecting the bucket client.
    ///
    /// @param s3Client the client to use instead of creating one from the configuration
    CloudStorageDownloadPlugin(@NonNull final S3Client s3Client) {
        this.s3Client = s3Client;
    }

    /// {@inheritDoc}
    @Override
    public void init(@NonNull final BlockNodeContext context, @NonNull final ServiceBuilder serviceBuilder) {
        this.context = context;
        config = context.configuration().getConfigData(CloudStorageDownloadConfig.class);
        if (config.enabled()) {
            boolean valid = isSet(config.endpointUrl(), "endpointUrl");
            valid = isSet(config.regionName(), "regionName") && valid;
            valid = isSet(config.bucketName(), "bucketName") && valid;
            valid = isSet(config.accessKey(), "accessKey") && valid;
            valid = isSet(config.secretKey(), "secretKey") && valid;
            valid = isPositive(config.aheadThresholdBlocks(), "aheadThresholdBlocks") && valid;
            valid = isPositive(config.downloadWindow(), "downloadWindow") && valid;
            active = valid;
        }
    }

    /// {@inheritDoc}
    @Override
    public void start() {
        if (active && s3Client == null) {
            try {
                // without a request timeout a request that connects and then stalls never returns
                final RetryPolicy retryPolicy = new RetryPolicy(
                        RetryPolicy.DEFAULT.maxAttempts(),
                        RetryPolicy.DEFAULT.baseDelayMs(),
                        RetryPolicy.DEFAULT.maxDelayMs(),
                        RetryPolicy.DEFAULT.totalTimeoutMs(),
                        config.requestTimeoutMs());
                s3Client = new S3Client(
                        config.regionName(),
                        config.endpointUrl(),
                        config.bucketName(),
                        config.accessKey(),
                        config.secretKey(),
                        retryPolicy);
            } catch (final S3ClientInitializationException e) {
                LOGGER.log(WARNING, "Failed to create the S3 client; cloud storage download is disabled", e);
                active = false;
            }
        }
        if (active) {
            awaiter = new PersistenceAwaiter();
            context.blockMessaging().registerBlockNotificationHandler(awaiter, false, name());
            downloader = context.threadPoolManager().getVirtualThreadExecutor("CloudStorageDownloadFetch");
            metricsHolder = MetricsHolder.createMetrics(context.metricRegistry());
            poller = new BucketPoller(config, s3Client, context.blockMessaging(), awaiter, downloader, metricsHolder);
            // register before reading the current state, so no update falls between the two
            context.blockMessaging().registerApplicationStateNotificationHandler(poller, false, name());
            poller.handleStoredBlocksUpdate(new StoredBlocksNotification(
                    context.applicationStateFacility().storedBlocks()));
            poller.handleTssDataUpdate(
                    new TssDataNotification(context.applicationStateFacility().tssData()));
            scheduler = context.threadPoolManager()
                    .createSingleThreadScheduledExecutor("CloudStorageDownload", new LoggingExceptionHandler());
            scheduler.scheduleWithFixedDelay(
                    poller, config.pollIntervalMs(), config.pollIntervalMs(), TimeUnit.MILLISECONDS);
        }
    }

    /// {@inheritDoc}
    @Override
    public void stop() {
        if (active) {
            context.blockMessaging().unregisterApplicationStateNotificationHandler(poller);
            context.blockMessaging().unregisterBlockNotificationHandler(awaiter);
            scheduler.shutdownNow();
            awaiter.releaseAll();
            downloader.shutdownNow();
            // let interrupted bucket calls return before the client closes under them
            try {
                if (!scheduler.awaitTermination(STOP_WAIT_SECONDS, TimeUnit.SECONDS)
                        || !downloader.awaitTermination(STOP_WAIT_SECONDS, TimeUnit.SECONDS)) {
                    LOGGER.log(DEBUG, "Cloud storage download threads still running after {0} s", STOP_WAIT_SECONDS);
                }
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            s3Client.close();
        }
    }

    /// Whether the plugin is enabled and its configuration is valid.
    ///
    /// @return `true` when the plugin is active
    boolean isActive() {
        return active;
    }

    /// The poller.
    ///
    /// @return the poller, `null` until the plugin has started active
    BucketPoller poller() {
        return poller;
    }

    /// The plugin's counters.
    ///
    /// @return the counters, `null` until the plugin has started active
    MetricsHolder metricsHolder() {
        return metricsHolder;
    }

    /// Checks that a required setting is not blank, logging a WARNING naming the property
    /// (never its value) when it is.
    private static boolean isSet(final String value, final String property) {
        final boolean set = value != null && !value.isBlank();
        if (!set) {
            LOGGER.log(
                    WARNING,
                    "cloud.storage.download.{0} is blank; cloud storage download is disabled until configured.",
                    property);
        }
        return set;
    }

    /// Checks that a setting is positive, logging a WARNING naming the property when it is not.
    private static boolean isPositive(final long value, final String property) {
        final boolean positive = value > 0;
        if (!positive) {
            LOGGER.log(
                    WARNING,
                    "cloud.storage.download.{0} must be positive; cloud storage download is disabled until configured.",
                    property);
        }
        return positive;
    }

    // TODO(#4080) add the failure counters
    // TODO(#4097) add the peer check skip counter
    /// Holds the counters reported by this plugin.
    ///
    /// @param blocksPulled blocks persisted from the bucket
    /// @param blocksAlreadyStored blocks in a pull range skipped because another source stored them first
    record MetricsHolder(LongCounter.Measurement blocksPulled, LongCounter.Measurement blocksAlreadyStored) {

        /// Registers the counters with the given registry.
        ///
        /// @param metricRegistry the metrics registry
        /// @return a new `MetricsHolder` with the counters registered
        static MetricsHolder createMetrics(@NonNull final MetricRegistry metricRegistry) {
            return new MetricsHolder(
                    metricRegistry
                            .register(LongCounter.builder(METRIC_BLOCKS_PULLED)
                                    .setDescription("Blocks persisted from the bucket"))
                            .getOrCreateNotLabeled(),
                    metricRegistry
                            .register(LongCounter.builder(METRIC_BLOCKS_ALREADY_STORED)
                                    .setDescription(
                                            "Blocks in a pull range skipped because another source stored them first"))
                            .getOrCreateNotLabeled());
        }
    }

    /// Logs an exception that escapes the poll thread.
    private static final class LoggingExceptionHandler implements Thread.UncaughtExceptionHandler {
        @Override
        public void uncaughtException(final Thread thread, final Throwable e) {
            LOGGER.log(WARNING, "Uncaught exception in thread " + thread.getName(), e);
        }
    }
}
