// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.download;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.WARNING;

import com.hedera.bucky.RetryPolicy;
import com.hedera.bucky.S3Client;
import com.hedera.bucky.S3ClientInitializationException;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.List;
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

    /// Blocks of a pull range, by the outcome in the `result` label.
    private static final MetricKey<LongCounter> METRIC_BLOCKS =
            MetricKey.of("cloud_download_blocks", LongCounter.class).addCategory(METRICS_CATEGORY);

    /// Label naming the outcome of a block.
    private static final String LABEL_RESULT = "result";

    /// Outcome of a block persisted from its bucket copy.
    private static final String RESULT_PERSISTED = "persisted";

    /// Outcome of a block another source stored first.
    private static final String RESULT_ALREADY_STORED = "already_stored";

    /// Prefix of every setting of this plugin, for error messages.
    private static final String PROPERTY_PREFIX = "cloud.storage.download.";

    /// Plugin configuration, set during {@link #init}.
    private CloudStorageDownloadConfig config;

    /// The bucket client. Created in {@link #init}, unless set by the test constructor.
    private S3Client s3Client;

    /// Block node context, set during {@link #init}.
    private BlockNodeContext context;

    /// Probes the bucket and pulls; `null` until the plugin has started.
    private BucketPoller poller;

    /// Runs the poll task; only this thread probes and pulls. `null` until the plugin has started.
    private ScheduledExecutorService scheduler;

    /// Waits for the outcome of each pulled block; `null` until the plugin has started.
    private PersistenceAwaiter awaiter;

    /// Runs the downloads of one window; `null` until the plugin has started.
    private ExecutorService downloader;

    /// The plugin's counters; `null` until the plugin has started.
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
    ///
    /// Validates the configuration and creates the bucket client. An invalid configuration
    /// throws, so a node that loads the plugin without configuring it does not start. The
    /// bucket itself is not contacted: whether it is reachable shows on the first probe.
    ///
    /// @throws IllegalStateException when a required setting is blank or not positive, or the
    ///     bucket client cannot be created from the settings
    @Override
    public void init(@NonNull final BlockNodeContext context, @NonNull final ServiceBuilder serviceBuilder) {
        this.context = context;
        config = context.configuration().getConfigData(CloudStorageDownloadConfig.class);
        final List<String> invalid = new ArrayList<>();
        requireSet(config.endpointUrl(), "endpointUrl", invalid);
        requireSet(config.regionName(), "regionName", invalid);
        requireSet(config.bucketName(), "bucketName", invalid);
        requireSet(config.accessKey(), "accessKey", invalid);
        requireSet(config.secretKey(), "secretKey", invalid);
        requirePositive(config.aheadThresholdBlocks(), "aheadThresholdBlocks", invalid);
        requirePositive(config.downloadWindow(), "downloadWindow", invalid);
        if (!invalid.isEmpty()) {
            throw new IllegalStateException(
                    "Cloud storage download settings blank or not positive: " + String.join(", ", invalid));
        }
        if (s3Client == null) {
            s3Client = createClient(config);
        }
    }

    /// {@inheritDoc}
    @Override
    public void start() {
        awaiter = new PersistenceAwaiter();
        context.blockMessaging().registerBlockNotificationHandler(awaiter, false, name());
        downloader = context.threadPoolManager().getVirtualThreadExecutor("CloudStorageDownloadFetch");
        metricsHolder = MetricsHolder.createMetrics(context.metricRegistry());
        poller = new BucketPoller(config, s3Client, context.blockMessaging(), awaiter, downloader, metricsHolder);
        // register before reading the current state, so no update falls between the two
        context.blockMessaging().registerApplicationStateNotificationHandler(poller, false, name());
        poller.handleStoredBlocksUpdate(
                new StoredBlocksNotification(context.applicationStateFacility().storedBlocks()));
        poller.handleTssDataUpdate(
                new TssDataNotification(context.applicationStateFacility().tssData()));
        scheduler = context.threadPoolManager()
                .createSingleThreadScheduledExecutor("CloudStorageDownload", new LoggingExceptionHandler());
        scheduler.scheduleWithFixedDelay(
                poller, config.pollIntervalMs(), config.pollIntervalMs(), TimeUnit.MILLISECONDS);
    }

    /// {@inheritDoc}
    @Override
    public void stop() {
        // nothing to stop when start did not get as far as scheduling the poll
        if (scheduler != null) {
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

    /// The poller.
    ///
    /// @return the poller, `null` until the plugin has started
    BucketPoller poller() {
        return poller;
    }

    /// The plugin's counters.
    ///
    /// @return the counters, `null` until the plugin has started
    MetricsHolder metricsHolder() {
        return metricsHolder;
    }

    /// Adds the full name of a required setting to the invalid ones when it is blank. Only the
    /// name is recorded, never the value.
    private static void requireSet(final String value, final String property, final List<String> invalid) {
        if (value == null || value.isBlank()) {
            invalid.add(PROPERTY_PREFIX + property);
        }
    }

    /// Adds the full name of a setting to the invalid ones when it is not positive.
    private static void requirePositive(final long value, final String property, final List<String> invalid) {
        if (value <= 0) {
            invalid.add(PROPERTY_PREFIX + property);
        }
    }

    /// Creates the bucket client from the validated settings. The client makes no request
    /// until it is used.
    ///
    /// @throws IllegalStateException when the client cannot be created from the settings
    private static S3Client createClient(final CloudStorageDownloadConfig config) {
        try {
            // without a request timeout a request that connects and then stalls never returns
            final RetryPolicy retryPolicy = new RetryPolicy(
                    RetryPolicy.DEFAULT.maxAttempts(),
                    RetryPolicy.DEFAULT.baseDelayMs(),
                    RetryPolicy.DEFAULT.maxDelayMs(),
                    RetryPolicy.DEFAULT.totalTimeoutMs(),
                    config.requestTimeoutMs());
            return new S3Client(
                    config.regionName(),
                    config.endpointUrl(),
                    config.bucketName(),
                    config.accessKey(),
                    config.secretKey(),
                    retryPolicy);
        } catch (final S3ClientInitializationException e) {
            throw new IllegalStateException("Cannot create the cloud storage download S3 client", e);
        }
    }

    // TODO(#4080) add the not_present, download_failed, verification_failed and persist_timeout
    //  results, the copy counters and the latency metrics
    /// Holds the measurements reported by this plugin.
    ///
    /// @param blocksPulled `cloud_download_blocks{result="persisted"}`: blocks persisted from
    ///     their bucket copy
    /// @param blocksAlreadyStored `cloud_download_blocks{result="already_stored"}`: blocks of a
    ///     pull range another source stored first
    record MetricsHolder(LongCounter.Measurement blocksPulled, LongCounter.Measurement blocksAlreadyStored) {

        /// Registers the counters with the given registry.
        ///
        /// @param metricRegistry the metrics registry
        /// @return a new `MetricsHolder` with the counters registered
        static MetricsHolder createMetrics(@NonNull final MetricRegistry metricRegistry) {
            final LongCounter blocks = metricRegistry.register(LongCounter.builder(METRIC_BLOCKS)
                    .setDescription("Blocks of a pull range by result (persisted|already_stored)")
                    .addDynamicLabelNames(LABEL_RESULT));
            return new MetricsHolder(
                    blocks.getOrCreateLabeled(LABEL_RESULT, RESULT_PERSISTED),
                    blocks.getOrCreateLabeled(LABEL_RESULT, RESULT_ALREADY_STORED));
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
