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
import org.hiero.block.node.spi.blockmessaging.AddressBookHistoryNotification;
import org.hiero.block.node.spi.blockmessaging.ApplicationStateNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.StoredBlocksNotification;
import org.hiero.block.node.spi.blockmessaging.TssDataNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.metrics.LongCounter;
import org.hiero.metrics.LongGauge;
import org.hiero.metrics.core.MetricKey;
import org.hiero.metrics.core.MetricRegistry;

/// A block node plugin that recovers missing blocks from the bucket the consensus nodes
/// upload blocks to, when this block node falls behind. The plugin handles the block
/// notifications and forwards them to the awaiter; a separate handler forwards the application
/// state notifications to the poller.
public class CloudStorageDownloadPlugin implements BlockNodePlugin, BlockNotificationHandler {

    private static final System.Logger LOGGER = System.getLogger(CloudStorageDownloadPlugin.class.getName());

    /// How long stop waits for each thread pool to end before it closes the bucket client.
    private static final long STOP_WAIT_SECONDS = 5L;

    /// The download deadline, in request timeouts.
    private static final long DOWNLOAD_DEADLINE_REQUEST_TIMEOUTS = 3L;

    /// Blocks of a pull range, by the outcome in the `result` label.
    private static final MetricKey<LongCounter> METRIC_BLOCKS =
            MetricKey.of("cloud_download_blocks", LongCounter.class).addCategory(METRICS_CATEGORY);

    /// Probes and downloads that failed with a storage fault.
    private static final MetricKey<LongCounter> METRIC_REQUEST_FAILURES =
            MetricKey.of("cloud_download_request_failures", LongCounter.class).addCategory(METRICS_CATEGORY);

    /// Label naming the outcome of a block.
    private static final String LABEL_RESULT = "result";

    /// Outcome of a block persisted from its bucket copy.
    private static final String RESULT_PERSISTED = "persisted";

    /// Outcome of a block another source stored first.
    private static final String RESULT_ALREADY_STORED = "already_stored";

    /// Outcome of a block no node folder had the object of.
    private static final String RESULT_NOT_PRESENT = "not_present";

    /// Outcome of a block present in the bucket with no usable copy.
    private static final String RESULT_DOWNLOAD_FAILED = "download_failed";

    /// Outcome of a block whose copies failed verification and that has no other copy.
    private static final String RESULT_VERIFICATION_FAILED = "verification_failed";

    /// Outcome of a block not persisted in time.
    private static final String RESULT_PERSIST_TIMEOUT = "persist_timeout";

    /// Downloaded copies that were unusable or failed verification.
    private static final MetricKey<LongCounter> METRIC_COPIES_REJECTED =
            MetricKey.of("cloud_download_copies_rejected", LongCounter.class).addCategory(METRICS_CATEGORY);

    /// Block copies read in full, usable or not.
    private static final MetricKey<LongCounter> METRIC_COPIES_DOWNLOADED =
            MetricKey.of("cloud_download_copies_downloaded", LongCounter.class).addCategory(METRICS_CATEGORY);

    /// Total time spent reading the downloaded copies, in nanoseconds.
    private static final MetricKey<LongCounter> METRIC_COPY_LATENCY_NS =
            MetricKey.of("cloud_download_copy_latency_ns", LongCounter.class).addCategory(METRICS_CATEGORY);

    /// Time from the consensus timestamp of the last probe block persisted during a pull, by the
    /// pull or by another source, to its persistence.
    private static final MetricKey<LongGauge> METRIC_PROBE_BLOCK_LATENCY_MS = MetricKey.of(
                    "cloud_download_probe_block_latency_ms", LongGauge.class)
            .addCategory(METRICS_CATEGORY);

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

    /// Receives the application state notifications. A separate object, because a handler's
    /// `isGating()` decides every registration it is used for: if this plugin implemented
    /// [ApplicationStateNotificationHandler], whose default is non-gating, that default would
    /// also decide its block notification registration.
    private final ApplicationStateNotificationHandler applicationStateHandler = new DownloadAsfHandler(this);

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
            // the request timeout covers only the response headers, so the body read needs a bound
            // of its own
            final long downloadDeadlineMs = DOWNLOAD_DEADLINE_REQUEST_TIMEOUTS * config.requestTimeoutMs();
            poller = new BucketPoller(
                    config, s3Client, context.blockMessaging(), awaiter, downloader, metricsHolder, downloadDeadlineMs);
            // register once the awaiter and poller exist, since the handlers forward to them, and
            // before reading the current state, so no update falls between the two
            context.blockMessaging().registerBlockNotificationHandler(this, false, name());
            context.blockMessaging()
                    .registerApplicationStateNotificationHandler(applicationStateHandler, false, name());
            poller.handleStoredBlocksUpdate(new StoredBlocksNotification(
                    context.applicationStateFacility().storedBlocks()));
            poller.handleAddressBookHistoryUpdate(new AddressBookHistoryNotification(
                    context.applicationStateFacility().rangedAddressBookHistory()));
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
            context.blockMessaging().unregisterApplicationStateNotificationHandler(applicationStateHandler);
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

    /// Holds the measurements reported by this plugin.
    ///
    /// @param blocksPulled `cloud_download_blocks{result="persisted"}`: blocks persisted from
    ///     their bucket copy
    /// @param blocksAlreadyStored `cloud_download_blocks{result="already_stored"}`: blocks of a
    ///     pull range another source stored first
    /// @param blocksNotPresent `cloud_download_blocks{result="not_present"}`: blocks no node
    ///     folder had the object of
    /// @param blocksDownloadFailed `cloud_download_blocks{result="download_failed"}`: blocks
    ///     present in the bucket with no usable copy in any node folder
    /// @param blocksVerificationFailed `cloud_download_blocks{result="verification_failed"}`:
    ///     blocks whose copies failed verification with no other copy to try
    /// @param blocksPersistTimeout `cloud_download_blocks{result="persist_timeout"}`: blocks not
    ///     persisted within the wait timeout
    /// @param copiesRejected `cloud_download_copies_rejected`: downloaded copies that could not
    ///     be decompressed or parsed, were not the block asked for, or failed verification
    /// @param requestFailures `cloud_download_request_failures`: probes and downloads that failed
    ///     with a storage fault, an error response or a download that did not finish in time
    /// @param copiesDownloaded `cloud_download_copies_downloaded`: block copies read in full,
    ///     usable or not
    /// @param copyLatencyNs `cloud_download_copy_latency_ns`: total time spent reading those
    ///     copies, in nanoseconds
    /// @param probeBlockLatencyMs `cloud_download_probe_block_latency_ms`: for the last probe
    ///     block a pull submitted and saw persisted, the time from its consensus timestamp to its
    ///     persistence, in milliseconds
    record MetricsHolder(
            LongCounter.Measurement blocksPulled,
            LongCounter.Measurement blocksAlreadyStored,
            LongCounter.Measurement blocksNotPresent,
            LongCounter.Measurement blocksDownloadFailed,
            LongCounter.Measurement blocksVerificationFailed,
            LongCounter.Measurement blocksPersistTimeout,
            LongCounter.Measurement copiesRejected,
            LongCounter.Measurement requestFailures,
            LongCounter.Measurement copiesDownloaded,
            LongCounter.Measurement copyLatencyNs,
            LongGauge.Measurement probeBlockLatencyMs) {

        /// Registers the counters with the given registry.
        ///
        /// @param metricRegistry the metrics registry
        /// @return a new `MetricsHolder` with the counters registered
        static MetricsHolder createMetrics(@NonNull final MetricRegistry metricRegistry) {
            final LongCounter blocks = metricRegistry.register(LongCounter.builder(METRIC_BLOCKS)
                    .setDescription("Blocks of a pull range by result (persisted|already_stored|not_present"
                            + "|download_failed|verification_failed|persist_timeout)")
                    .addDynamicLabelNames(LABEL_RESULT));
            return new MetricsHolder(
                    blocks.getOrCreateLabeled(LABEL_RESULT, RESULT_PERSISTED),
                    blocks.getOrCreateLabeled(LABEL_RESULT, RESULT_ALREADY_STORED),
                    blocks.getOrCreateLabeled(LABEL_RESULT, RESULT_NOT_PRESENT),
                    blocks.getOrCreateLabeled(LABEL_RESULT, RESULT_DOWNLOAD_FAILED),
                    blocks.getOrCreateLabeled(LABEL_RESULT, RESULT_VERIFICATION_FAILED),
                    blocks.getOrCreateLabeled(LABEL_RESULT, RESULT_PERSIST_TIMEOUT),
                    metricRegistry
                            .register(LongCounter.builder(METRIC_COPIES_REJECTED)
                                    .setDescription(
                                            "Downloaded block copies that were unusable or failed verification"))
                            .getOrCreateNotLabeled(),
                    metricRegistry
                            .register(LongCounter.builder(METRIC_REQUEST_FAILURES)
                                    .setDescription("Bucket probes and downloads that failed with a storage fault"))
                            .getOrCreateNotLabeled(),
                    metricRegistry
                            .register(LongCounter.builder(METRIC_COPIES_DOWNLOADED)
                                    .setDescription("Block copies read in full from the bucket, usable or not"))
                            .getOrCreateNotLabeled(),
                    metricRegistry
                            .register(LongCounter.builder(METRIC_COPY_LATENCY_NS)
                                    .setDescription("Total time spent reading block copies from the bucket, in ns"))
                            .getOrCreateNotLabeled(),
                    metricRegistry
                            .register(LongGauge.builder(METRIC_PROBE_BLOCK_LATENCY_MS)
                                    .setDescription("Time from the consensus timestamp of the last probe block"
                                            + " persisted during a pull to its persistence, in ms"))
                            .getOrCreateNotLabeled());
        }
    }

    /// Forwards the application state notifications to the poller.
    private static final class DownloadAsfHandler implements ApplicationStateNotificationHandler {
        /// The plugin whose poller receives the notifications.
        private final CloudStorageDownloadPlugin owner;

        /// Creates a handler for the given plugin.
        ///
        /// @param owner the plugin whose poller receives the notifications
        private DownloadAsfHandler(final CloudStorageDownloadPlugin owner) {
            this.owner = owner;
        }

        /// {@inheritDoc}
        @Override
        public void handleStoredBlocksUpdate(@NonNull final StoredBlocksNotification notification) {
            owner.poller.handleStoredBlocksUpdate(notification);
        }

        /// {@inheritDoc}
        @Override
        public void handleTssDataUpdate(@NonNull final TssDataNotification notification) {
            owner.poller.handleTssDataUpdate(notification);
        }

        /// {@inheritDoc}
        @Override
        public void handleAddressBookHistoryUpdate(@NonNull final AddressBookHistoryNotification notification) {
            owner.poller.handleAddressBookHistoryUpdate(notification);
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
