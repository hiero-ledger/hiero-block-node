// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import static java.lang.System.Logger.Level.INFO;

import java.util.Objects;
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
/// [org.hiero.block.node.block.verification.verifier.BlockVerificationResult].
/// The handler is also able to receive data from multiple sources and forward it to the correct session.
/// We have a limited number of sessions we can have running simultaneously, configurable via
/// [VerificationConfig#activeSessionsBufferSize()]. When the buffer fills up and a new session has to be started,
/// the oldest one will be canceled to make room for the new one.
public final class BlockSessionHandler {
    private static final System.Logger LOGGER = System.getLogger(BlockSessionHandler.class.getName());
    private final BlockNodeContext context;
    private final MetricsHolder metricsHolder;
    private final SessionHandlerMetrics sessionHandlerMetrics;
    private final VerificationConfig verificationConfig;
    private final AtomicLong lastVerifiedBlock;
    private final ConcurrentSkipListSet<Long> recentlyVerifiedBlocks;
    private final AtomicLong nextUniqueSessionIdentifier;
    private final ExecutorService executor;
    private final ConcurrentSkipListMap<SessionKey, BlockVerificationSession> activeSessions;
    private final VerificationDataProvider verificationDataProvider;
    private final AtomicReference<BlockVerificationSession> activePublisherSession;
    private final ConcurrentSkipListSet<SessionKey> finishedSessions;
    private final BadBlockDumper badBlockDumper;

    /// Constructor.
    public BlockSessionHandler(
            final BlockNodeContext context,
            final MetricsHolder metricsHolder,
            final VerificationConfig verificationConfig,
            final VerificationDataProvider verificationDataProvider,
            final AtomicLong lastVerifiedBlock,
            final ConcurrentSkipListSet<Long> recentlyVerifiedBlocks,
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
        this.activeSessions = new ConcurrentSkipListMap<>();
        this.nextUniqueSessionIdentifier = new AtomicLong(0);
        this.activePublisherSession = new AtomicReference<>();
        this.finishedSessions = new ConcurrentSkipListSet<>();
        this.badBlockDumper = Objects.requireNonNull(badBlockDumper);
    }

    /// Process the supplied [BlockItems] based on the source.
    /// Items supplied here must be validated beforehand.
    public void processBlockItems(final BlockItems blockItems, final BlockSource blockSource) {
        completeFinishedSessions();
        switch (blockSource) {
            case PUBLISHER -> processPublisherLiveItems(blockItems);
            case BACKFILL -> processBackfilledItems(blockItems);
            case null, default ->
                LOGGER.log(INFO, "Received block items from unknown or unsupported source: {0}", blockSource);
        }
    }

    /// Attempt to complete finished sessions
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

    /// Start a new session.
    private BlockVerificationSession startNewSession(final BlockItems blockItems, final BlockSource blockSource) {
        final BlockVerificationSession session = createSession(blockItems, blockSource);
        session.start();
        sessionHandlerMetrics.verificationBlocksReceived().increment();
        return session;
    }

    /// Create a new [CompletableVerificationSession].
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
    /// This method will activate the session and potentially cancel the oldest one if the buffer for
    /// maximum allowed sessions is full.
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
