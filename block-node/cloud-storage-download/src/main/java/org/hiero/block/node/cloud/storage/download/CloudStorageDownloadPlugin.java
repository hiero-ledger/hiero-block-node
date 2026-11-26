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
import org.hiero.block.node.spi.blockmessaging.ApplicationStateNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.StoredBlocksNotification;
import org.hiero.block.node.spi.blockmessaging.TssDataNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.metrics.LongCounter;
import org.hiero.metrics.core.MetricKey;
import org.hiero.metrics.core.MetricRegistry;

/// A block node plugin that recovers missing blocks from the bucket the consensus nodes
/// upload blocks to, when this block node falls behind. The plugin is the only registered
/// handler and forwards the notifications to the poller and the awaiter.
public class CloudStorageDownloadPlugin
        implements BlockNodePlugin, BlockNotificationHandler, ApplicationStateNotificationHandler {

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

    /// Whether a required bucket setting is blank or the bucket client cannot be created, which
    /// leaves the plugin idle; set during {@link #init}.
    private boolean idle;

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
    /// Validates the configuration and creates the bucket client. A blank bucket setting, or a
    /// bucket client that cannot be created, is logged at WARNING and leaves the plugin idle, so
    /// a node that loads every plugin starts without download settings. Numeric settings are
    /// checked by the configuration itself before this runs. The bucket itself is not contacted:
    /// whether it is reachable shows on the first probe.
    @Override
    public void init(@NonNull final BlockNodeContext context, @NonNull final ServiceBuilder serviceBuilder) {
        this.context = context;
        config = context.configuration().getConfigData(CloudStorageDownloadConfig.class);
        final List<String> blank = new ArrayList<>();
        addIfBlank(config.endpointUrl(), "endpointUrl", blank);
        addIfBlank(config.regionName(), "regionName", blank);
        addIfBlank(config.bucketName(), "bucketName", blank);
        addIfBlank(config.accessKey(), "accessKey", blank);
        addIfBlank(config.secretKey(), "secretKey", blank);
        if (blank.isEmpty()) {
            if (s3Client == null) {
                s3Client = createClient(config);
                idle = s3Client == null;
            }
        } else {
            idle = true;
            // only the name is logged, never the value
            for (final String property : blank) {
                LOGGER.log(WARNING, property + " is blank; cloud storage download stays idle");
            }
        }
    }

    /// {@inheritDoc}
    ///
    /// Does nothing when {@link #init} left the plugin idle.
    @Override
    public void start() {
        if (!idle) {
            awaiter = new PersistenceAwaiter();
            downloader = context.threadPoolManager().getVirtualThreadExecutor("CloudStorageDownloadFetch");
            metricsHolder = MetricsHolder.createMetrics(context.metricRegistry());
            poller = new BucketPoller(config, s3Client, context.blockMessaging(), awaiter, downloader, metricsHolder);
            // register once the awaiter and poller exist, since the handlers forward to them, and
            // before reading the current state, so no update falls between the two
            context.blockMessaging().registerBlockNotificationHandler(this, false, name());
            context.blockMessaging().registerApplicationStateNotificationHandler(this, false, name());
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
        // nothing to stop when start did not get as far as scheduling the poll
        if (scheduler != null) {
            context.blockMessaging().unregisterApplicationStateNotificationHandler(this);
            context.blockMessaging().unregisterBlockNotificationHandler(this);
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

    /// {@inheritDoc}
    ///
    /// Forwards to the awaiter.
    @Override
    public void handlePersisted(@NonNull final PersistedNotification notification) {
        awaiter.handlePersisted(notification);
    }

    /// {@inheritDoc}
    ///
    /// Forwards to the awaiter.
    @Override
    public void handleVerification(@NonNull final VerificationNotification notification) {
        awaiter.handleVerification(notification);
    }

    /// {@inheritDoc}
    ///
    /// Forwards to the poller.
    @Override
    public void handleStoredBlocksUpdate(@NonNull final StoredBlocksNotification notification) {
        poller.handleStoredBlocksUpdate(notification);
    }

    /// {@inheritDoc}
    ///
    /// Forwards to the poller.
    @Override
    public void handleTssDataUpdate(@NonNull final TssDataNotification notification) {
        poller.handleTssDataUpdate(notification);
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

    /// Adds the full name of a required setting to the blank ones when it is blank. Only the
    /// name is recorded, never the value.
    private static void addIfBlank(final String value, final String property, final List<String> blank) {
        if (value == null || value.isBlank()) {
            blank.add(PROPERTY_PREFIX + property);
        }
    }

    /// Creates the bucket client from the validated settings. The client makes no request
    /// until it is used.
    ///
    /// @return the client, or `null` when it cannot be created from the settings, which is
    ///     logged at WARNING
    private static S3Client createClient(final CloudStorageDownloadConfig config) {
        S3Client client = null;
        try {
            // without a request timeout a request that connects and then stalls never returns
            final RetryPolicy retryPolicy = new RetryPolicy(
                    RetryPolicy.DEFAULT.maxAttempts(),
                    RetryPolicy.DEFAULT.baseDelayMs(),
                    RetryPolicy.DEFAULT.maxDelayMs(),
                    RetryPolicy.DEFAULT.totalTimeoutMs(),
                    config.requestTimeoutMs());
            client = new S3Client(
                    config.regionName(),
                    config.endpointUrl(),
                    config.bucketName(),
                    config.accessKey(),
                    config.secretKey(),
                    retryPolicy);
        } catch (final S3ClientInitializationException e) {
            LOGGER.log(
                    WARNING,
                    "Cannot create the cloud storage download S3 client; cloud storage download stays idle",
                    e);
        }
        return client;
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
