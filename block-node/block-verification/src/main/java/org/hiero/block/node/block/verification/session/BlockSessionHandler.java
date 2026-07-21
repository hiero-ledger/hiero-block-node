// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import static java.lang.System.Logger.Level.INFO;

import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.hiero.block.node.block.verification.BadBlockDumper;
import org.hiero.block.node.block.verification.VerificationConfig;
import org.hiero.block.node.block.verification.VerificationDataProvider;
import org.hiero.block.node.block.verification.metrics.MetricsHolder;
import org.hiero.block.node.block.verification.metrics.SessionHandlerMetrics;
import org.hiero.block.node.block.verification.session.BlockVerificationSession.SessionKey;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;

/// Handler for [BlockVerificationSession]s.
/// This handler is responsible for creating, managing, and canceling
/// [BlockVerificationSession]s.
/// The handler is also able to receive data from multiple sources and forward it to the correct session.
/// We have a limited number of sessions we can have running simultaneously, configurable via
/// [VerificationConfig#activeSessionsBufferSize()]. When the buffer fills up and a new session has to be started,
/// the session verifying the lowest block will be canceled to make room for the new one.
public final class BlockSessionHandler {
    /// Logger for the handler.
    private static final System.Logger LOGGER = System.getLogger(BlockSessionHandler.class.getName());
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
    /// All currently active sessions, keyed and ordered by [SessionKey].
    private final ConcurrentSkipListMap<SessionKey, BlockVerificationSession> activeSessions;
    /// Provider of the verification data, passed to created sessions.
    private final VerificationDataProvider verificationDataProvider;
    /// The session currently receiving live items from the publisher, if any.
    private final AtomicReference<BlockVerificationSession> activePublisherSession;
    /// Keys of sessions that have marked themselves finished and await graceful completion.
    private final ConcurrentSkipListSet<SessionKey> finishedSessions;
    /// Dumps failing block bytes to disk for diagnostics, passed to created sessions.
    private final BadBlockDumper badBlockDumper;

    /// Constructor.
    ///
    /// @param context the block node context, must not be null
    /// @param metricsHolder the holder for all verification metrics, must not be null
    /// @param verificationConfig the configuration for verification, must not be null
    /// @param verificationDataProvider provider of the verification data, must not be null
    /// @param lastVerifiedBlock the last successfully verified block, must not be null
    /// @param recentlyVerifiedBlocks the set of recently verified blocks, must not be null
    /// @param activeSessions the map to hold active sessions, must not be null
    /// @param executor the executor used to run sessions, must not be null
    /// @param badBlockDumper the bad block dumper for diagnostics, must not be null
    public BlockSessionHandler(
            final BlockNodeContext context,
            final MetricsHolder metricsHolder,
            final VerificationConfig verificationConfig,
            final VerificationDataProvider verificationDataProvider,
            final AtomicLong lastVerifiedBlock,
            final ConcurrentLinkedDeque<Long> recentlyVerifiedBlocks,
            final ConcurrentSkipListMap<SessionKey, BlockVerificationSession> activeSessions,
            final ExecutorService executor,
            final BadBlockDumper badBlockDumper) {
        this.context = Objects.requireNonNull(context);
        this.metricsHolder = Objects.requireNonNull(metricsHolder);
        this.sessionHandlerMetrics = metricsHolder.sessionHandlerMetrics();
        this.verificationDataProvider = Objects.requireNonNull(verificationDataProvider);
        this.verificationConfig = Objects.requireNonNull(verificationConfig);
        this.lastVerifiedBlock = Objects.requireNonNull(lastVerifiedBlock);
        this.recentlyVerifiedBlocks = Objects.requireNonNull(recentlyVerifiedBlocks);
        this.executor = Objects.requireNonNull(executor);
        this.activeSessions = Objects.requireNonNull(activeSessions);
        this.nextUniqueSessionIdentifier = new AtomicLong(0);
        this.activePublisherSession = new AtomicReference<>();
        this.finishedSessions = new ConcurrentSkipListSet<>();
        this.badBlockDumper = Objects.requireNonNull(badBlockDumper);
    }

    /// Process the supplied [BlockItems] based on the source.
    /// Items supplied here must be validated beforehand.
    /// Before processing, any finished sessions are gracefully completed.
    ///
    /// @param blockItems the block items to process, must be validated beforehand
    /// @param blockSource the source the items were received from
    public void processBlockItems(final BlockItems blockItems, final BlockSource blockSource) {
        completeFinishedSessions();
        switch (blockSource) {
            case PUBLISHER -> processPublisherLiveItems(blockItems);
            case BACKFILL -> processBackfilledItems(blockItems);
            case null, default ->
                LOGGER.log(INFO, "Received block items from unknown or unsupported source: {0}", blockSource);
        }
    }

    /// Attempt to complete finished sessions.
    /// Every session that has marked itself finished is asked to complete; when it
    /// does, it is removed from the finished set and the active sessions buffer.
    private void completeFinishedSessions() {
        for (final SessionKey candidate : finishedSessions) {
            final BlockVerificationSession sessionToComplete = activeSessions.get(candidate);
            if (sessionToComplete != null) {
                if (sessionToComplete.complete()) {
                    finishedSessions.remove(candidate);
                    activeSessions.remove(candidate);
                    activePublisherSession.compareAndSet(sessionToComplete, null);
                }
            }
        }
    }

    /// Process the reception of live blocks from the publisher. Publisher supplied [BlockItems] can only
    /// be received in series, this means that it is safe to assume changes made in this invocation will
    /// be visible in the next one, but it also means that publisher supplied items will not race.
    /// The publisher guarantees that when a block starts, items received will be in order. It cannot,
    /// however, guarantee that a block will finish. If a new block starts prematurely (this can be detected
    /// because we can follow along an active session), the current session must be canceled as we can safely
    /// assume we have moved on.
    ///
    /// @param blockItems the publisher supplied block items to process
    private void processPublisherLiveItems(final BlockItems blockItems) {
        BlockVerificationSession local = activePublisherSession.get();
        if (blockItems.isStartOfNewBlock()) {
            if (local != null) {
                local.cancel();
            }
            local = startNewSession(blockItems, BlockSource.PUBLISHER);
            activePublisherSession.set(local);
            activateSession(local);
        }
        // check if we have an active publisher session, if not, then disregard the items
        if (local != null) {
            local.getBlockItemsDeque().offer(blockItems);
        }
        if (blockItems.isEndOfBlock()) {
            // drop the reference to the active session, it is no longer needed
            activePublisherSession.set(null);
        }
    }

    /// Process the reception of backfilled blocks. Backfilled blocks always come complete in a single batch of
    /// [BlockItems]. We must simply start a session for the block we just received.
    ///
    /// @param blockItems to process
    private void processBackfilledItems(final BlockItems blockItems) {
        final BlockVerificationSession session = startNewSession(blockItems, BlockSource.BACKFILL);
        activateSession(session);
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
                context,
                verificationConfig,
                finishedSessions,
                badBlockDumper);
    }

    /// Activate a new session.
    /// This method will activate the session and potentially cancel the one verifying the lowest
    /// block if the buffer for maximum allowed sessions is full, unless that is the session that
    /// was just activated.
    ///
    /// @param session the session to activate
    private void activateSession(final BlockVerificationSession session) {
        activeSessions.put(session.sessionKey(), session);
        final SessionKey candidateSessionKey = activeSessions.firstKey();
        if (activeSessions.size() > verificationConfig.activeSessionsBufferSize()
                && candidateSessionKey != session.sessionKey()) {
            final BlockVerificationSession removed = activeSessions.remove(candidateSessionKey);
            if (removed != null) {
                removed.cancel();
                activePublisherSession.compareAndSet(removed, null);
            }
        }
    }
}
