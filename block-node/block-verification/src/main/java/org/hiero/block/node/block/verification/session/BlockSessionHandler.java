// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import static java.lang.System.Logger.Level.INFO;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import org.hiero.block.node.block.verification.BadBlockDumper;
import org.hiero.block.node.block.verification.VerificationConfig;
import org.hiero.block.node.block.verification.VerificationDataProvider;
import org.hiero.block.node.block.verification.metrics.MetricsHolder;
import org.hiero.block.node.block.verification.metrics.SessionHandlerMetrics;
import org.hiero.block.node.block.verification.session.BlockVerificationSession.SessionKey;
import org.hiero.block.node.block.verification.session.eviction.EvictionPolicy;
import org.hiero.block.node.block.verification.session.eviction.EvictionSnapshot;
import org.hiero.block.node.block.verification.session.eviction.SessionSnapshot;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;

/// Handler for [BlockVerificationSession]s.
/// This handler is responsible for creating, managing, and canceling
/// [BlockVerificationSession]s.
/// The handler receives data through two entry points, one per delivery path:
/// live items from the publisher stream ([#processLiveItems(BlockItems)]) start
/// [SessionPriority#HIGH] sessions, whole blocks delivered at once
/// ([#processWholeBlock(BlockItems, BlockSource)]) start [SessionPriority#LOW]
/// sessions. Both entry points may be called concurrently from different threads.
///
/// We have a limited number of sessions we can have running simultaneously, configurable via
/// [VerificationConfig#activeSessionsBufferSize()]. When a new session pushes the buffer over that
/// limit, the [EvictionPolicy] selects the sessions to cancel and this handler cancels them.
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
    /// The policy that selects which sessions to evict when the buffer is over its limit.
    private final EvictionPolicy evictionPolicy;
    /// Serializes activation of sessions, so that a session is only visible to
    /// an eviction round while its owner holds the lock and the size check is exact.
    private final ReentrantLock activationLock;

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
    /// @param evictionPolicy the policy selecting sessions to evict, must not be null
    public BlockSessionHandler(
            final BlockNodeContext context,
            final MetricsHolder metricsHolder,
            final VerificationConfig verificationConfig,
            final VerificationDataProvider verificationDataProvider,
            final AtomicLong lastVerifiedBlock,
            final ConcurrentLinkedDeque<Long> recentlyVerifiedBlocks,
            final ConcurrentSkipListMap<SessionKey, BlockVerificationSession> activeSessions,
            final ExecutorService executor,
            final BadBlockDumper badBlockDumper,
            final EvictionPolicy evictionPolicy) {
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
        this.evictionPolicy = Objects.requireNonNull(evictionPolicy);
        this.activationLock = new ReentrantLock();
    }

    /// Process live [BlockItems] received from the publisher stream.
    /// Items supplied here must be validated beforehand.
    /// Before processing, any finished sessions are gracefully completed.
    /// Sessions started here are [SessionPriority#HIGH].
    ///
    /// @param blockItems the block items to process, must be validated beforehand, must not be null
    public void processLiveItems(final BlockItems blockItems) {
        Objects.requireNonNull(blockItems);
        completeFinishedSessions();
        processPublisherLiveItems(blockItems);
    }

    /// Process a complete block delivered at once, as a single batch of
    /// [BlockItems] that both starts and ends the block.
    /// Items supplied here must be validated beforehand.
    /// Before processing, any finished sessions are gracefully completed.
    /// Sessions started here are [SessionPriority#LOW].
    ///
    /// @param blockItems the complete block to process, must be validated beforehand, must not be null
    /// @param blockSource the source the block was received from, must not be null
    public void processWholeBlock(final BlockItems blockItems, final BlockSource blockSource) {
        Objects.requireNonNull(blockItems);
        Objects.requireNonNull(blockSource);
        completeFinishedSessions();
        processWholeBlockItems(blockItems, blockSource);
    }

    /// Attempt to complete finished sessions.
    /// Every session that has marked itself finished is asked to complete; when it
    /// does, it is removed from the finished set and the active sessions buffer.
    /// A finished session that is no longer in the buffer was evicted before it
    /// reported, there is nothing left to complete and only the key is dropped.
    private void completeFinishedSessions() {
        for (final SessionKey candidate : finishedSessions) {
            final BlockVerificationSession sessionToComplete = activeSessions.get(candidate);
            if (sessionToComplete == null) {
                finishedSessions.remove(candidate);
            } else if (sessionToComplete.complete()) {
                finishedSessions.remove(candidate);
                activeSessions.remove(candidate);
                activePublisherSession.compareAndSet(sessionToComplete, null);
            }
        }
        sessionHandlerMetrics.verificationActiveSessions().set(activeSessions.size());
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
            local = startNewSession(blockItems, BlockSource.PUBLISHER, SessionPriority.HIGH);
            activePublisherSession.set(local);
            if (blockItems.isEndOfBlock()) {
                // the batch carries the complete block, mark it complete before
                // activation makes the session visible for eviction by the
                // concurrent whole-block thread, so an eviction cancel reports
                // CANCELLED instead of CANCELLED_INCOMPLETE
                local.markEndOfBlockReceived();
            }
            activateSession(local);
        }
        // check if we have an active publisher session, if not, then disregard the items
        if (local != null) {
            if (blockItems.isEndOfBlock()) {
                // mark before offering so the session never observes the ending
                // batch in its deque while still considered incomplete
                local.markEndOfBlockReceived();
            }
            local.getBlockItemsDeque().offer(blockItems);
        }
        if (blockItems.isEndOfBlock()) {
            // drop the reference to the active session, it is no longer needed
            activePublisherSession.set(null);
        }
    }

    /// Process the reception of a whole block. Such blocks always come complete in a single batch of
    /// [BlockItems]. We must simply start a session for the block we just received.
    ///
    /// @param blockItems the complete block to process
    /// @param blockSource the source the block was received from
    private void processWholeBlockItems(final BlockItems blockItems, final BlockSource blockSource) {
        final BlockVerificationSession session = startNewSession(blockItems, blockSource, SessionPriority.LOW);
        // a whole block always arrives complete in a single batch, mark it
        // complete before activation makes the session visible for eviction by
        // the concurrent publisher thread, so an eviction cancel reports
        // CANCELLED instead of CANCELLED_INCOMPLETE
        session.markEndOfBlockReceived();
        activateSession(session);
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

    /// Create a new [CompletableVerificationSession].
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
                finishedSessions,
                badBlockDumper);
    }

    /// Activate a new session.
    /// The session is added to the active sessions buffer. If that pushes the buffer over its
    /// limit, the eviction policy is asked which sessions to cancel and they are cancelled here.
    /// Adding, checking and evicting happen under the activation lock, so the two delivery
    /// threads never observe each other's half activated sessions and the size check is exact.
    /// The lock is never held while blocking: the policy is a pure selection over a snapshot.
    ///
    /// @param session the session to activate
    private void activateSession(final BlockVerificationSession session) {
        activationLock.lock();
        try {
            activeSessions.put(session.sessionKey(), session);
            if (activeSessions.size() > verificationConfig.activeSessionsBufferSize()) {
                evictUnderLock(session.sessionKey());
            }
        } finally {
            activationLock.unlock();
        }
        sessionHandlerMetrics.verificationActiveSessions().set(activeSessions.size());
    }

    /// Run one eviction round. Must be called with the activation lock held.
    ///
    /// The session that was just activated is protected, and so is the publisher session
    /// still receiving live items, if any: evicting it would report `CANCELLED_INCOMPLETE`,
    /// which the publisher treats as already handled, and the block would be lost.
    /// Every victim the policy selects is checked again before it is cancelled: it must not
    /// have reported its result already and it must still be in the buffer, since a session
    /// can complete between the snapshot and the cancellation.
    ///
    /// @param currentKey the key of the session that was just activated
    private void evictUnderLock(final SessionKey currentKey) {
        final int sizeBeforeEviction = activeSessions.size();
        final int limit = verificationConfig.activeSessionsBufferSize();
        final EvictionSnapshot snapshot = snapshot(currentKey, limit);
        final List<SessionKey> victims = evictionPolicy.selectVictims(snapshot);
        final List<SessionKey> evicted = new ArrayList<>();
        int evictedHigh = 0;
        int evictedLow = 0;
        for (final SessionKey victimKey : victims) {
            if (finishedSessions.contains(victimKey)) {
                // the session has already reported its result, it is reaped on the next round
                LOGGER.log(INFO, "Skipping eviction of already finished session {0}", victimKey);
            } else {
                final BlockVerificationSession removed = activeSessions.remove(victimKey);
                if (removed == null) {
                    // completed and reaped by the other delivery thread in the meantime
                    LOGGER.log(INFO, "Skipping eviction of session {0}, no longer active", victimKey);
                } else {
                    final boolean cancelled = removed.cancel();
                    activePublisherSession.compareAndSet(removed, null);
                    if (cancelled) {
                        sessionHandlerMetrics
                                .verificationSessionsEvicted(removed.priority())
                                .increment();
                        evicted.add(victimKey);
                        if (removed.priority() == SessionPriority.HIGH) {
                            evictedHigh++;
                        } else {
                            evictedLow++;
                        }
                    }
                }
            }
        }
        final int sizeAfterEviction = activeSessions.size();
        final String message = "Active sessions buffer over limit {0} with {1} sessions, evicted {2} high and {3} low"
                + " priority sessions {4}, {5} sessions remain";
        LOGGER.log(INFO, message, limit, sizeBeforeEviction, evictedHigh, evictedLow, evicted, sizeAfterEviction);
        if (sizeAfterEviction > limit) {
            final String overshoot = "Active sessions buffer remains over limit {0} with {1} sessions, the remaining"
                    + " sessions are protected or are expected to complete on their own";
            LOGGER.log(INFO, overshoot, limit, sizeAfterEviction);
        }
    }

    /// Take an immutable snapshot of the active sessions buffer for the eviction policy.
    /// The active publisher session is read after the buffer so that a session which
    /// became the active publisher session during the iteration is still protected.
    ///
    /// @param currentKey the key of the session that was just activated
    /// @param limit the buffer limit
    /// @return the snapshot
    private EvictionSnapshot snapshot(final SessionKey currentKey, final int limit) {
        final List<SessionSnapshot> sessions = new ArrayList<>(activeSessions.size());
        for (final BlockVerificationSession session : activeSessions.values()) {
            sessions.add(new SessionSnapshot(
                    session.sessionKey(), session.priority(), session.blockSource(), session.isEndOfBlockReceived()));
        }
        final Set<SessionKey> protectedKeys = new HashSet<>();
        protectedKeys.add(currentKey);
        final BlockVerificationSession publisherSession = activePublisherSession.get();
        if (publisherSession != null && !publisherSession.isEndOfBlockReceived()) {
            protectedKeys.add(publisherSession.sessionKey());
        }
        return new EvictionSnapshot(
                sessions,
                lastVerifiedBlock.get(),
                verificationConfig.firstOrderedBlock(),
                verificationConfig.allSourcesRequireOrdering(),
                limit,
                protectedKeys);
    }
}
