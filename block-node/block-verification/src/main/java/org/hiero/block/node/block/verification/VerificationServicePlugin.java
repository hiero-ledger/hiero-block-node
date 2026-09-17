// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification;

import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.block.api.BlockRange;
import org.hiero.block.node.block.verification.BackfilledBlockNotificationAdapter.Accepted;
import org.hiero.block.node.block.verification.BackfilledBlockNotificationAdapter.Adapted;
import org.hiero.block.node.block.verification.BackfilledBlockNotificationAdapter.Ignored;
import org.hiero.block.node.block.verification.BackfilledBlockNotificationAdapter.Rejected;
import org.hiero.block.node.block.verification.metrics.MetricsHolder;
import org.hiero.block.node.block.verification.session.BlockSessionHandler;
import org.hiero.block.node.block.verification.session.SessionFailureType;
import org.hiero.block.node.block.verification.session.eviction.GapAwareEvictionPolicy;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.BackfilledBlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockItemHandler;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification.FailureInfo;

/// Verification Service Plugin.
///
/// This plugin handles the verification of blocks received by any source.
/// The plugin is a notification handler and listens for
/// [BackfilledBlockNotification], this is one of the places data is received
/// from. It is also a live items handler and listens for [BlockItems], another
/// place data is received from. The plugin also listens for application state
/// updates.
///
/// This plugin is effectively the implementation of the verification component
/// design as specified in the design documentation.
public final class VerificationServicePlugin implements BlockNodePlugin, BlockItemHandler, BlockNotificationHandler {
    /// Logger for the plugin.
    private static final System.Logger LOGGER = System.getLogger(VerificationServicePlugin.class.getName());
    /// The last successfully verified block.
    private final AtomicLong lastVerifiedBlock;
    /// The set of recently verified blocks.
    private final ConcurrentLinkedDeque<Long> recentlyVerifiedBlocks;
    /// The block node context, for access to core facilities.
    private BlockNodeContext context;
    /// The metrics holder for all plugin metrics.
    @SuppressWarnings("FieldCanBeLocal")
    private MetricsHolder metricsHolder;
    /// The configuration for verification.
    @SuppressWarnings("FieldCanBeLocal")
    private VerificationConfig verificationConfig;
    /// The verification data provider.
    private VerificationDataProvider verificationDataProvider;
    /// The verification sessions handler.
    private BlockSessionHandler sessionHandler;
    /// The executor used for sessions.
    private ExecutorService executor;
    /// Dumps failing block bytes to disk for diagnostics.
    private BadBlockDumper badBlockDumper;

    /// Constructor.
    public VerificationServicePlugin() {
        this.lastVerifiedBlock = new AtomicLong(-1);
        this.recentlyVerifiedBlocks = new ConcurrentLinkedDeque<>();
    }

    /// {@inheritDoc}
    /// ---
    /// Initialize the plugin.
    /// Get config, initialize the executor, metrics and session handler.
    @Override
    public void init(final BlockNodeContext context, final ServiceBuilder serviceBuilder) {
        this.context = context;
        this.verificationConfig = context.configuration().getConfigData(VerificationConfig.class);
        this.executor = context.threadPoolManager()
                .getVirtualThreadExecutor(
                        "VerificationSession", VerificationServicePlugin::getUncaughtExceptionHandler);
        this.metricsHolder = MetricsHolder.create(context.metricRegistry());
        this.verificationDataProvider = new VerificationDataProvider(context);
        this.badBlockDumper = new BadBlockDumper(verificationConfig, resolveHostname());
        this.sessionHandler = new BlockSessionHandler(
                context,
                metricsHolder,
                verificationConfig,
                verificationDataProvider,
                lastVerifiedBlock,
                recentlyVerifiedBlocks,
                new ConcurrentSkipListMap<>(),
                executor,
                badBlockDumper,
                new GapAwareEvictionPolicy());
    }

    /// Uncaught exception handler method handle for verification pool.
    private static void getUncaughtExceptionHandler(final Thread thread, final Throwable throwable) {
        LOGGER.log(WARNING, "Uncaught exception in verification executor", throwable);
    }

    /// {@inheritDoc}
    /// ---
    /// Start the plugin.
    /// Register the plugin in messaging. Determine a starting point for the
    /// last verified block, that is the same as the latest persisted block.
    @Override
    public void start() {
        this.context.blockMessaging().registerBlockNotificationHandler(this, true, name());
        this.context.blockMessaging().registerBlockItemHandler(this, true, name());
        badBlockDumper.start(context.threadPoolManager());
    }

    /// {@inheritDoc}
    @Override
    public String name() {
        return VerificationServicePlugin.class.getSimpleName();
    }

    /// {@inheritDoc}
    /// ---
    /// Stop the plugin.
    /// Unregister the plugin from messaging and shutdown sessions.
    @Override
    public void stop() {
        // unregister from listening to incoming block items
        context.blockMessaging().unregisterBlockItemHandler(this);
        context.blockMessaging().unregisterBlockNotificationHandler(this);
        // immediately shutdown the executor
        executor.shutdownNow();
        badBlockDumper.stop();
    }

    /// {@inheritDoc}
    /// ---
    /// Receive application state updates.
    /// _NOTE_: we are expected to receive an update right after [#init(BlockNodeContext, ServiceBuilder)]
    /// and just before [#start()]. If any initial data is available, we will see it before starting.
    /// This is also an important assumption for setting the last verified block initially.
    @Override
    public void onContextUpdate(final BlockNodeContext updatedContext) {
        try {
            if (updatedContext != null) {
                verificationDataProvider.safeUpdateTssData(updatedContext.tssData(), false);
                updateLastVerifiedBlock(updatedContext.storedBlocks());
            }
        } catch (final RuntimeException e) {
            LOGGER.log(INFO, "onContextUpdate failed", e);
        }
    }

    /// Update the last verified block.
    /// If the latest update from application state has a high watermark for stored blocks
    /// higher than what we last verified, we want to roll forward.
    /// We expect updates here to happen very infrequently.
    private void updateLastVerifiedBlock(final List<BlockRange> storedBlocks) {
        if (storedBlocks != null && !storedBlocks.isEmpty()) {
            final BlockRange lastRange = storedBlocks.getLast();
            final long highestStoredBlock = lastRange.rangeEnd();
            long localLastVerified = lastVerifiedBlock.get();
            boolean updateHappened = false;
            while (highestStoredBlock > localLastVerified) {
                updateHappened = lastVerifiedBlock.compareAndSet(localLastVerified, highestStoredBlock);
                localLastVerified = lastVerifiedBlock.get();
            }
            if (updateHappened) {
                final String message = "onContextUpdate received, updated last verified block to {0}";
                LOGGER.log(INFO, message, lastVerifiedBlock.get());
            }
        }
    }

    // ==== BlockItemHandler Methods ===================================================================================

    /// {@inheritDoc}
    /// ---
    /// This is where items that are on the live items ring buffer received
    /// from. These items are coming from publisher.
    /// A block could be received in multiple batches of [BlockItems].
    /// Publisher must guarantee that once a block starts forwarding, detectable
    /// by [BlockItems#isStartOfNewBlock()], items, received afterward will be
    /// in order as received from the publisher. Once the end of the block
    /// currently being forwarded is received, detectable by
    /// [BlockItems#isEndOfBlock()], we should expect the next block to start.
    /// It is possible, however, that a block will never complete. So we can,
    /// and must, expect that we can receive the start of a new block before
    /// the end of the previous block. In those cases the active session of the
    /// previous block must be canceled.
    /// Sessions started here are high priority sessions.
    ///
    /// @param blockItems the immutable list of block items to handle
    @Override
    public void handleBlockItemsReceived(final BlockItems blockItems) {
        final BlockSource source = BlockSource.PUBLISHER;
        try {
            if (blockItems != null) {
                if (BlockStartValidator.isValidStart(blockItems)) {
                    sessionHandler.processLiveItems(blockItems);
                } else {
                    safeSendNotification(blockItems.blockNumber(), source, SessionFailureType.MISSING_MANDATORY_ITEM);
                }
            } else {
                LOGGER.log(INFO, "Received null block items on live items ring buffer");
            }
        } catch (final RuntimeException e) {
            LOGGER.log(INFO, "Failed to handle live block items in verification ", e);
            final long blockNumber = blockItems != null ? blockItems.blockNumber() : -1L;
            safeSendNotification(blockNumber, source, SessionFailureType.UNKNOWN_ERROR);
        }
    }

    /// {@inheritDoc}
    /// ---
    /// This is where we receive blocks from backfill.
    /// We will always receive a complete block, one per notification. The
    /// [BackfilledBlockNotificationAdapter] turns the notification into a
    /// single batch of [BlockItems] that both starts and ends the block, which
    /// is then propagated to the session handler as a low priority session.
    /// Everything specific to this delivery path lives in the adapter, which
    /// is temporary until whole blocks are delivered on their own ring buffer.
    ///
    /// @param notification the [BackfilledBlockNotification] received as an
    ///     event.
    @Override
    public void handleBackfilled(final BackfilledBlockNotification notification) {
        final BlockSource source = BlockSource.BACKFILL;
        try {
            final Adapted adapted = BackfilledBlockNotificationAdapter.adapt(notification);
            switch (adapted) {
                case Accepted accepted -> sessionHandler.processWholeBlock(accepted.blockItems(), source);
                case Rejected rejected -> safeSendNotification(rejected.blockNumber(), source, rejected.failure());
                case Ignored ignored -> LOGGER.log(INFO, "Received invalid backfill notification: {0}", notification);
            }
        } catch (final RuntimeException e) {
            LOGGER.log(INFO, "Failed to handle backfill notification in verification ", e);
            final long blockNumber = notification != null ? notification.blockNumber() : -1L;
            safeSendNotification(blockNumber, source, SessionFailureType.UNKNOWN_ERROR);
        }
    }

    /// {@inheritDoc}
    /// ---
    /// We want to handle persisted notification so that we can update our
    /// recently verified blocks. If a block, that was recently verified has
    /// failed to persist, we have to expect its reception again. We want, in
    /// those cases, to remove it from our set of recently verified blocks,
    /// because we want subsequent possible failures of verification to not
    /// be informational.
    /// Note that even if a subsequent failure happens before this update and
    /// an informational failure is propagated, this is still not disruptive
    /// because a failed persistence notification will inevitably follow
    /// immediately after.
    ///
    /// @param notification a [PersistedNotification] received as an event.
    @Override
    public void handlePersisted(final PersistedNotification notification) {
        try {
            if (notification != null && !notification.succeeded()) {
                recentlyVerifiedBlocks.remove(notification.blockNumber());
            }
        } catch (final RuntimeException e) {
            LOGGER.log(INFO, "Failed to handle persisted notification in verification ", e);
        }
    }

    /// Send a failure notification to messaging. Any failure to send is logged
    /// and swallowed; this method never throws.
    ///
    /// @param blockNumber the number of the block the failure is for
    /// @param blockSource the source of the block
    /// @param sessionFailureType the failure type to report
    private void safeSendNotification(
            final long blockNumber, final BlockSource blockSource, final SessionFailureType sessionFailureType) {
        try {
            final VerificationNotification notification = new VerificationNotification(
                    false,
                    FailureInfo.standard(sessionFailureType.asFailureType()),
                    blockNumber,
                    null,
                    null,
                    blockSource);
            context.blockMessaging().sendBlockVerification(notification);
        } catch (final RuntimeException e) {
            final String message = "Failed to send verification notification for block %d with source %s"
                    .formatted(blockNumber, blockSource);
            LOGGER.log(WARNING, message, e);
        }
    }

    /// Resolve the local hostname, used as the Block Node identity in bad block dumps.
    ///
    /// @return the local hostname, or `"unknown"` when it cannot be resolved
    private String resolveHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (final UnknownHostException e) {
            return "unknown";
        }
    }
}
