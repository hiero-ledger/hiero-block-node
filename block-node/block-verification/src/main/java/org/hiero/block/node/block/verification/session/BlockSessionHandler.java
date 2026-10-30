// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import static java.lang.System.Logger.Level.INFO;

import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.block.node.block.verification.BadBlockDumper;
import org.hiero.block.node.block.verification.VerificationConfig;
import org.hiero.block.node.block.verification.VerificationDataProvider;
import org.hiero.block.node.block.verification.VerificationOutbox;
import org.hiero.block.node.block.verification.metrics.MetricsHolder;
import org.hiero.block.node.block.verification.metrics.SessionHandlerMetrics;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;

/// Handler for [BlockVerificationSession]s.
/// This handler manages the sessions and their lifecycle: it starts a session for
/// every block that starts and admits it, supplies every session with the block items
/// of its block, and cancels the publisher session of a block that never finished.
/// The [ActiveSessionsBuffer] holds the sessions for it: every admission clears the
/// sessions that are complete, and applies the eviction policy when the buffer is
/// still full.
public final class BlockSessionHandler {
    /// Logger for the handler.
    private static final System.Logger LOGGER = System.getLogger(BlockSessionHandler.class.getName());
    /// The holder for all verification metrics, passed to created sessions.
    private final MetricsHolder metricsHolder;
    /// Metrics recorded by this handler.
    private final SessionHandlerMetrics sessionHandlerMetrics;
    /// The configuration for verification, passed to created sessions.
    private final VerificationConfig verificationConfig;
    /// Provider of the verification data, passed to created sessions.
    private final VerificationDataProvider verificationDataProvider;
    /// The last successfully verified block, shared with the sessions.
    private final AtomicLong lastVerifiedBlock;
    /// The set of recently verified blocks, shared with the sessions.
    private final ConcurrentLinkedDeque<Long> recentlyVerifiedBlocks;
    /// The executor used to run sessions.
    private final ExecutorService executor;
    /// Dumps failing block bytes to disk for diagnostics, passed to created sessions.
    private final BadBlockDumper badBlockDumper;
    /// The buffer every created session is admitted to.
    private final ActiveSessionsBuffer activeSessions;
    /// The outbox created sessions send their notifications through.
    private final VerificationOutbox outbox;
    /// The source of unique ids for new sessions.
    private final AtomicLong nextUniqueSessionIdentifier;

    /// Constructor.
    ///
    /// @param metricsHolder the holder for all verification metrics, must not be null
    /// @param verificationConfig the configuration for verification, must not be null
    /// @param verificationDataProvider provider of the verification data, must not be null
    /// @param lastVerifiedBlock the last successfully verified block, must not be null
    /// @param recentlyVerifiedBlocks the set of recently verified blocks, must not be null
    /// @param executor the executor used to run sessions, must not be null
    /// @param badBlockDumper the bad block dumper for diagnostics, must not be null
    /// @param activeSessions the buffer to admit created sessions to, must not be null
    /// @param outbox the outbox created sessions send their notifications through, must not be null
    public BlockSessionHandler(
            final MetricsHolder metricsHolder,
            final VerificationConfig verificationConfig,
            final VerificationDataProvider verificationDataProvider,
            final AtomicLong lastVerifiedBlock,
            final ConcurrentLinkedDeque<Long> recentlyVerifiedBlocks,
            final ExecutorService executor,
            final BadBlockDumper badBlockDumper,
            final ActiveSessionsBuffer activeSessions,
            final VerificationOutbox outbox) {
        this.metricsHolder = metricsHolder;
        this.sessionHandlerMetrics = metricsHolder.sessionHandlerMetrics();
        this.verificationConfig = Objects.requireNonNull(verificationConfig);
        this.verificationDataProvider = Objects.requireNonNull(verificationDataProvider);
        this.lastVerifiedBlock = Objects.requireNonNull(lastVerifiedBlock);
        this.recentlyVerifiedBlocks = Objects.requireNonNull(recentlyVerifiedBlocks);
        this.executor = Objects.requireNonNull(executor);
        this.badBlockDumper = Objects.requireNonNull(badBlockDumper);
        this.activeSessions = Objects.requireNonNull(activeSessions);
        this.outbox = Objects.requireNonNull(outbox);
        this.nextUniqueSessionIdentifier = new AtomicLong(0);
    }

    /// Process the supplied [BlockItems] based on the source.
    /// Items supplied here must be validated beforehand.
    ///
    /// @param blockItems the block items to process, must be validated beforehand
    /// @param blockSource the source the items were received from
    public void processBlockItems(final BlockItems blockItems, final BlockSource blockSource) {
        switch (blockSource) {
            case PUBLISHER -> processPublisherLiveItems(blockItems);
            case BACKFILL -> processBackfilledItems(blockItems);
            case null, default ->
                LOGGER.log(INFO, "Received block items from unknown or unsupported source: {0}", blockSource);
        }
    }

    /// Process the reception of live blocks from the publisher. Publisher supplied [BlockItems] can only
    /// be received in series, this means that it is safe to assume changes made in this invocation will
    /// be visible in the next one, but it also means that publisher supplied items will not race.
    /// The publisher guarantees that when a block starts, items received will be in order. It cannot,
    /// however, guarantee that a block will finish. A batch that starts a block cancels the active
    /// publisher session of a block that never finished, as we can safely assume the publisher has moved
    /// on, and counts it as superseded, unless the session had already produced its result on its own.
    /// The batch then starts a new session, admitted before it receives any items, so the buffer
    /// protects it until it has received the end of its block, even a block that comes in one batch. Any
    /// other batch goes to the active publisher session; a batch arriving when there is none is
    /// disregarded.
    ///
    /// @param blockItems the publisher supplied block items to process
    private void processPublisherLiveItems(final BlockItems blockItems) {
        final BlockVerificationSession session;
        if (blockItems.isStartOfNewBlock()) {
            final BlockVerificationSession superseded = activeSessions.activePublisherSession();
            if (superseded != null) {
                final boolean cancelled = superseded.cancel();
                if (cancelled) {
                    sessionHandlerMetrics.verificationSessionsSuperseded().increment();
                }
            }
            session = startNewSession(blockItems, BlockSource.PUBLISHER);
            activeSessions.admit(session);
        } else {
            session = activeSessions.activePublisherSession();
        }
        if (session != null) {
            if (blockItems.isEndOfBlock()) {
                // mark before offering, so the session never sees the ending batch
                // while still considered incomplete
                session.markEndOfBlockReceived();
            }
            session.getBlockItemsDeque().offer(blockItems);
        }
    }

    /// Process the reception of backfilled blocks. Backfilled blocks always come complete in a single batch of
    /// [BlockItems]. The session for the block is marked complete before it is admitted, so an eviction
    /// reports a cancelled complete block.
    ///
    /// @param blockItems to process
    private void processBackfilledItems(final BlockItems blockItems) {
        final BlockVerificationSession session = startNewSession(blockItems, BlockSource.BACKFILL);
        session.markEndOfBlockReceived();
        activeSessions.admit(session);
        session.getBlockItemsDeque().offer(blockItems);
    }

    /// Start a new session and increment the blocks received metric.
    ///
    /// @param blockItems the first block items of the block to verify
    /// @param blockSource the source of the block
    /// @return the started session
    private BlockVerificationSession startNewSession(final BlockItems blockItems, final BlockSource blockSource) {
        final BlockVerificationSession session = createSession(blockItems, blockSource);
        session.start();
        sessionHandlerMetrics.verificationBlocksReceived().increment();
        return session;
    }

    /// Create a new [CompletableVerificationSession].
    ///
    /// @param blockItems the first block items of the block to verify
    /// @param blockSource the source of the block
    /// @return a new, not yet started session
    private CompletableVerificationSession createSession(final BlockItems blockItems, final BlockSource blockSource) {
        return new CompletableVerificationSession(
                nextUniqueSessionIdentifier.getAndIncrement(),
                blockItems.blockNumber(),
                metricsHolder,
                blockSource,
                verificationDataProvider,
                lastVerifiedBlock,
                recentlyVerifiedBlocks,
                executor,
                outbox,
                verificationConfig,
                badBlockDumper);
    }
}
