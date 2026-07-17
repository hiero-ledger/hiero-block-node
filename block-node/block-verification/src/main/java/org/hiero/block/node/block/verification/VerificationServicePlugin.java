// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;

import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.block.api.BlockRange;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.node.base.ParseHelper;
import org.hiero.block.node.block.verification.metrics.MetricsHolder;
import org.hiero.block.node.block.verification.session.BlockSessionHandler;
import org.hiero.block.node.block.verification.session.SessionFailureType;
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
    /// Exposes [VerificationConfig] as a configuration data type.
    @NonNull
    @Override
    public List<Class<? extends Record>> configDataTypes() {
        return List.of(VerificationConfig.class);
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
                badBlockDumper);
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
        if (updatedContext != null) {
            verificationDataProvider.safeUpdateTssData(updatedContext.tssData(), false);
            safeUpdateLastVerified(updatedContext.storedBlocks());
        }
    }

    /// Safe update the last verified block.
    /// If the latest update from application state has a high watermark for stored blocks
    /// higher than what we last verified, we want to roll forward.
    private void safeUpdateLastVerified(final List<BlockRange> storedBlocks) {
        if (storedBlocks != null && !storedBlocks.isEmpty()) {
            final BlockRange lastRange = storedBlocks.getLast();
            final long highestStoredBlock = lastRange.rangeEnd();
            long localLastVerified = lastVerifiedBlock.get();
            while (highestStoredBlock > localLastVerified) {
                lastVerifiedBlock.compareAndSet(localLastVerified, highestStoredBlock);
                localLastVerified = lastVerifiedBlock.get();
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
    ///
    /// @param blockItems the immutable list of block items to handle
    @Override
    public void handleBlockItemsReceived(final BlockItems blockItems) {
        final BlockSource source = BlockSource.PUBLISHER;
        try {
            if (blockItems != null) {
                if (validateStartOfBlock(blockItems)) {
                    sessionHandler.processBlockItems(blockItems, source);
                } else {
                    safeSendNotification(
                            blockItems.blockNumber(), source, SessionFailureType.MISSING_VERIFICATION_DATA);
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
    /// We will always receive a complete block, one per notification.
    /// We can safely wrap the block as [BlockItems] which is both the start
    /// and the end of the block. We then propagate the block to the session
    /// handler.
    ///
    /// @param notification the [BackfilledBlockNotification] received as an
    ///     event.
    @Override
    public void handleBackfilled(final BackfilledBlockNotification notification) {
        final BlockSource source = BlockSource.BACKFILL;
        try {
            if (notification != null
                    && notification.blockNumber() >= 0L
                    && notification.block() != null
                    && notification.block().blockItems() != null
                    && !notification.block().blockItems().isEmpty()) {
                final BlockItems blockItems =
                        new BlockItems(notification.block().blockItems(), notification.blockNumber(), true, true);
                if (validateStartOfBlock(blockItems)) {
                    sessionHandler.processBlockItems(blockItems, source);
                } else {
                    safeSendNotification(
                            blockItems.blockNumber(), source, SessionFailureType.MISSING_VERIFICATION_DATA);
                }
            } else {
                LOGGER.log(INFO, "Received invalid backfill notification: {0}", notification);
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
        if (notification != null && !notification.succeeded()) {
            recentlyVerifiedBlocks.remove(notification.blockNumber());
        }
    }

    /// Send a notification to messaging.
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

    private boolean validateStartOfBlock(final BlockItems blockItems) {
        try {
            final boolean result;
            if (blockItems.isStartOfNewBlock()) {
                final BlockItemUnparsed first = blockItems.blockItems().getFirst();
                if (first != null && first.hasBlockHeader()) {
                    final Bytes bytes = first.blockHeaderOrThrow();
                    final BlockHeader header = ParseHelper.standardParse(BlockHeader.PROTOBUF, bytes);
                    result = header.number() == blockItems.blockNumber();
                } else {
                    result = false;
                }
            } else {
                result = true;
            }
            return result;
        } catch (final ParseException e) {
            final String message = "Failed to parse block header";
            LOGGER.log(DEBUG, message, e);
            return false;
        }
    }

    private String resolveHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (final UnknownHostException e) {
            return "unknown";
        }
    }
}
