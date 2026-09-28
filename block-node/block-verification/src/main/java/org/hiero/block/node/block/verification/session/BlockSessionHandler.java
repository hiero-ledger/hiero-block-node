// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.block.node.block.verification.BadBlockDumper;
import org.hiero.block.node.block.verification.VerificationConfig;
import org.hiero.block.node.block.verification.VerificationDataProvider;
import org.hiero.block.node.block.verification.metrics.MetricsHolder;
import org.hiero.block.node.block.verification.metrics.SessionHandlerMetrics;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;

/// Handler for [BlockVerificationSession]s.
/// This handler is responsible for creating and starting sessions, routing
/// block items to them and cancelling live sessions that are superseded.
/// The handler receives data through two entry points, one per delivery path:
/// live items from the publisher stream ([#processLiveItems(BlockItems)]) start
/// [SessionPriority#HIGH] sessions, whole blocks delivered at once
/// ([#processWholeBlock(BlockItems, BlockSource)]) start [SessionPriority#LOW]
/// sessions. Each entry point is invoked serially by its own delivery thread;
/// the two threads only meet inside the [ActiveSessionsBuffer], which bounds
/// the number of running sessions and evicts when it is over its limit.
public final class BlockSessionHandler {
    /// The block node context, for access to core facilities.
    private final BlockNodeContext context;
    /// The holder for all verification metrics, passed to created sessions.
    private final MetricsHolder metricsHolder;
    /// Metrics recorded by this handler.
    private final SessionHandlerMetrics sessionHandlerMetrics;
    /// The configuration for verification.
    private final VerificationConfig verificationConfig;
    /// The last successfully verified block, shared with the sessions.
    private final AtomicLong lastVerifiedBlock;
    /// The set of recently verified blocks, shared with the sessions.
    private final ConcurrentLinkedDeque<Long> recentlyVerifiedBlocks;
    /// The source of unique ids for new sessions.
    private final AtomicLong nextUniqueSessionIdentifier;
    /// The executor used to run sessions.
    private final ExecutorService executor;
    /// Provider of the verification data, passed to created sessions.
    private final VerificationDataProvider verificationDataProvider;
    /// Dumps failing block bytes to disk for diagnostics, passed to created sessions.
    private final BadBlockDumper badBlockDumper;
    /// The bounded buffer every started session is activated in.
    private final ActiveSessionsBuffer buffer;
    /// The session currently receiving live items from the publisher, if any.
    /// Accessed only by the live delivery thread, which invokes
    /// [#processLiveItems(BlockItems)] serially, so no synchronization is needed.
    private BlockVerificationSession activePublisherSession;

    /// Constructor.
    ///
    /// @param context the block node context, must not be null
    /// @param metricsHolder the holder for all verification metrics, must not be null
    /// @param verificationConfig the configuration for verification, must not be null
    /// @param verificationDataProvider provider of the verification data, must not be null
    /// @param lastVerifiedBlock the last successfully verified block, must not be null
    /// @param recentlyVerifiedBlocks the set of recently verified blocks, must not be null
    /// @param executor the executor used to run sessions, must not be null
    /// @param badBlockDumper the bad block dumper for diagnostics, must not be null
    /// @param buffer the buffer to activate started sessions in, must not be null
    public BlockSessionHandler(
            final BlockNodeContext context,
            final MetricsHolder metricsHolder,
            final VerificationConfig verificationConfig,
            final VerificationDataProvider verificationDataProvider,
            final AtomicLong lastVerifiedBlock,
            final ConcurrentLinkedDeque<Long> recentlyVerifiedBlocks,
            final ExecutorService executor,
            final BadBlockDumper badBlockDumper,
            final ActiveSessionsBuffer buffer) {
        this.context = Objects.requireNonNull(context);
        this.metricsHolder = Objects.requireNonNull(metricsHolder);
        this.sessionHandlerMetrics = metricsHolder.sessionHandlerMetrics();
        this.verificationDataProvider = Objects.requireNonNull(verificationDataProvider);
        this.verificationConfig = Objects.requireNonNull(verificationConfig);
        this.lastVerifiedBlock = Objects.requireNonNull(lastVerifiedBlock);
        this.recentlyVerifiedBlocks = Objects.requireNonNull(recentlyVerifiedBlocks);
        this.executor = Objects.requireNonNull(executor);
        this.badBlockDumper = Objects.requireNonNull(badBlockDumper);
        this.buffer = Objects.requireNonNull(buffer);
        this.nextUniqueSessionIdentifier = new AtomicLong(0);
    }

    /// Process live [BlockItems] received from the publisher stream.
    /// Items supplied here must be validated beforehand.
    /// Publisher supplied items are received in series. The publisher guarantees
    /// that when a block starts, the items received afterward are in order. It
    /// cannot, however, guarantee that a block will finish: if a new block starts
    /// before the current one ended, the current session is cancelled as the
    /// stream has clearly moved on. Sessions started here are [SessionPriority#HIGH].
    ///
    /// @param blockItems the block items to process, must be validated beforehand, must not be null
    public void processLiveItems(final BlockItems blockItems) {
        Objects.requireNonNull(blockItems);
        if (blockItems.isStartOfNewBlock()) {
            startLiveBlock(blockItems);
        } else {
            continueLiveBlock(blockItems);
        }
        if (blockItems.isEndOfBlock()) {
            // the block is complete, no further items are expected for it
            activePublisherSession = null;
        }
    }

    /// Process a complete block delivered at once, as a single batch of
    /// [BlockItems] that both starts and ends the block.
    /// Items supplied here must be validated beforehand.
    /// Sessions started here are [SessionPriority#LOW].
    ///
    /// @param blockItems the complete block to process, must be validated beforehand and
    ///     must both start and end the block, must not be null
    /// @param blockSource the source the block was received from, must not be null
    /// @throws IllegalArgumentException if the batch does not both start and end the block
    public void processWholeBlock(final BlockItems blockItems, final BlockSource blockSource) {
        Objects.requireNonNull(blockItems);
        Objects.requireNonNull(blockSource);
        if (blockItems.isStartOfNewBlock() && blockItems.isEndOfBlock()) {
            final BlockVerificationSession session = startNewSession(blockItems, blockSource, SessionPriority.LOW);
            deliver(session, blockItems);
            buffer.activate(session);
        } else {
            throw new IllegalArgumentException(
                    "A whole block must be supplied as a single batch that both starts and ends the block");
        }
    }

    /// Start a new live block: supersede the session of a block that never
    /// ended, start the new session, deliver the first batch and activate.
    /// The first batch is delivered before activation so that no failure in
    /// the activation can leave a session without its items.
    ///
    /// @param blockItems the batch starting the block
    private void startLiveBlock(final BlockItems blockItems) {
        final BlockVerificationSession previous = activePublisherSession;
        if (previous != null && !previous.isFinished()) {
            // the previous block never ended, the publisher has moved on; the cancelled
            // session handles its result and leaves the buffer before the new one is activated
            previous.cancel();
        }
        final BlockVerificationSession session =
                startNewSession(blockItems, BlockSource.PUBLISHER, SessionPriority.HIGH);
        activePublisherSession = session;
        deliver(session, blockItems);
        buffer.activate(session);
    }

    /// Continue the live block currently being received, if any. Items that
    /// arrive while no block is being received are disregarded, and so are
    /// items for a session that has already produced its result.
    ///
    /// @param blockItems the batch continuing the block
    private void continueLiveBlock(final BlockItems blockItems) {
        final BlockVerificationSession session = activePublisherSession;
        if (session != null) {
            if (session.isFinished()) {
                // the session already produced its result, nothing is offered to it anymore
                activePublisherSession = null;
            } else {
                deliver(session, blockItems);
            }
        }
    }

    /// Deliver a batch to a session, marking the end of the block first when
    /// the batch ends it, so the session never observes the ending batch while
    /// still considered incomplete.
    ///
    /// @param session the session to deliver to
    /// @param blockItems the batch to deliver
    private static void deliver(final BlockVerificationSession session, final BlockItems blockItems) {
        if (blockItems.isEndOfBlock()) {
            session.markEndOfBlockReceived();
        }
        session.getBlockItemsDeque().offer(blockItems);
    }

    /// Start a new session and increment the blocks received metric.
    ///
    /// @param blockItems the first block items of the block to verify
    /// @param blockSource the source of the block
    /// @param priority the priority of the session
    /// @return the started session
    private BlockVerificationSession startNewSession(
            final BlockItems blockItems, final BlockSource blockSource, final SessionPriority priority) {
        final BlockVerificationSession session = createSession(blockItems, blockSource, priority);
        session.start();
        sessionHandlerMetrics.verificationBlocksReceived().increment();
        return session;
    }

    /// Create a new [CompletableVerificationSession] that removes itself from
    /// the buffer once its result has been handled.
    ///
    /// @param blockItems the first block items of the block to verify
    /// @param blockSource the source of the block
    /// @param priority the priority of the session
    /// @return a new, not yet started session
    private CompletableVerificationSession createSession(
            final BlockItems blockItems, final BlockSource blockSource, final SessionPriority priority) {
        return new CompletableVerificationSession(
                nextUniqueSessionIdentifier.getAndIncrement(),
                blockItems.blockNumber(),
                metricsHolder,
                blockSource,
                priority,
                verificationDataProvider,
                lastVerifiedBlock,
                recentlyVerifiedBlocks,
                executor,
                context,
                verificationConfig,
                buffer::remove,
                badBlockDumper);
    }
}
