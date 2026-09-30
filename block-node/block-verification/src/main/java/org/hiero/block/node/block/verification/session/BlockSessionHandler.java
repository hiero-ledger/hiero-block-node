// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.INFO;

import java.util.List;
import java.util.Objects;
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
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.metrics.LongCounter;

/// Handler for [BlockVerificationSession]s.
/// This handler is responsible for creating, managing, and canceling
/// [BlockVerificationSession]s.
/// The handler is also able to receive data from multiple sources and forward it to the correct session.
///
/// Active sessions live in two lanes, see [SessionLane]: a high priority lane for the
/// sessions started from the publisher's live stream and a low priority lane for the
/// sessions started from any other channel. The lanes share one limit, configurable
/// via [VerificationConfig#activeSessionsBufferSize()], which applies to their
/// combined count. When a new session is activated and the
/// combined count exceeds the limit, room is made by a [SessionEvictionPolicy]: the
/// policy selects sessions from an immutable [ActiveSessionsSnapshot], and this handler
/// removes, cancels and accounts for them, once per admission. Eviction runs under a
/// lock so that the two ingress threads never evict at the same time; everything else
/// is lock free, as before.
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
    /// The high priority lane: active sessions started from the publisher's live stream, ordered by [SessionKey].
    private final ConcurrentSkipListMap<SessionKey, BlockVerificationSession> highPrioritySessions;
    /// The low priority lane: active sessions started from any other channel, ordered by [SessionKey].
    private final ConcurrentSkipListMap<SessionKey, BlockVerificationSession> lowPrioritySessions;
    /// The policy that selects the sessions to evict when the lanes hold more sessions than allowed.
    private final SessionEvictionPolicy evictionPolicy;
    /// Guards the eviction so that the two ingress threads never evict at the same time.
    private final ReentrantLock evictionLock;
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
    /// @param highPrioritySessions the map to hold the high priority lane, must not be null
    /// @param lowPrioritySessions the map to hold the low priority lane, must not be null
    /// @param evictionPolicy the policy that selects sessions to evict, must not be null
    /// @param executor the executor used to run sessions, must not be null
    /// @param badBlockDumper the bad block dumper for diagnostics, must not be null
    public BlockSessionHandler(
            final BlockNodeContext context,
            final MetricsHolder metricsHolder,
            final VerificationConfig verificationConfig,
            final VerificationDataProvider verificationDataProvider,
            final AtomicLong lastVerifiedBlock,
            final ConcurrentLinkedDeque<Long> recentlyVerifiedBlocks,
            final ConcurrentSkipListMap<SessionKey, BlockVerificationSession> highPrioritySessions,
            final ConcurrentSkipListMap<SessionKey, BlockVerificationSession> lowPrioritySessions,
            final SessionEvictionPolicy evictionPolicy,
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
        this.highPrioritySessions = Objects.requireNonNull(highPrioritySessions);
        this.lowPrioritySessions = Objects.requireNonNull(lowPrioritySessions);
        this.evictionPolicy = Objects.requireNonNull(evictionPolicy);
        this.evictionLock = new ReentrantLock();
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
    /// does, it is removed from the finished set and from its lane. A finished key
    /// whose session is in no lane belongs to a session that was evicted after it
    /// finished; only the key is left, so it is dropped.
    private void completeFinishedSessions() {
        for (final SessionKey candidate : finishedSessions) {
            final BlockVerificationSession inHighPriorityLane = highPrioritySessions.get(candidate);
            if (inHighPriorityLane != null) {
                completeFinishedSession(candidate, inHighPriorityLane, highPrioritySessions);
            } else {
                final BlockVerificationSession inLowPriorityLane = lowPrioritySessions.get(candidate);
                if (inLowPriorityLane != null) {
                    completeFinishedSession(candidate, inLowPriorityLane, lowPrioritySessions);
                } else {
                    finishedSessions.remove(candidate);
                }
            }
        }
        sessionHandlerMetrics.verificationActiveSessions().set(activeSessionCount());
    }

    /// Complete one finished session and, when it completes, remove it from the
    /// finished set and from its lane.
    ///
    /// @param key the key of the session
    /// @param session the session to complete
    /// @param lane the lane holding the session
    private void completeFinishedSession(
            final SessionKey key,
            final BlockVerificationSession session,
            final ConcurrentSkipListMap<SessionKey, BlockVerificationSession> lane) {
        if (session.complete()) {
            finishedSessions.remove(key);
            lane.remove(key);
            activePublisherSession.compareAndSet(session, null);
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
            if (blockItems.isEndOfBlock()) {
                // the batch carries the complete block, mark it complete before
                // activation makes the session visible for eviction by the
                // concurrent backfill thread, so an eviction cancel reports
                // CANCELLED instead of CANCELLED_INCOMPLETE
                local.markEndOfBlockReceived();
            }
            activateSession(local, SessionLane.HIGH_PRIORITY);
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

    /// Process the reception of backfilled blocks. Backfilled blocks always come complete in a single batch of
    /// [BlockItems]. We must simply start a session for the block we just received.
    ///
    /// @param blockItems to process
    private void processBackfilledItems(final BlockItems blockItems) {
        final BlockVerificationSession session = startNewSession(blockItems, BlockSource.BACKFILL);
        // a backfilled block always arrives complete in a single batch, mark it
        // complete before activation makes the session visible for eviction by
        // the concurrent publisher thread, so an eviction cancel reports
        // CANCELLED instead of CANCELLED_INCOMPLETE
        session.markEndOfBlockReceived();
        activateSession(session, SessionLane.LOW_PRIORITY);
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

    /// Activate a new session in its lane.
    /// When the combined count of both lanes then exceeds the configured limit,
    /// room is made under the eviction lock, see [#makeRoom()].
    ///
    /// @param session the session to activate
    /// @param lane the lane the session belongs to
    private void activateSession(final BlockVerificationSession session, final SessionLane lane) {
        laneOf(lane).put(session.sessionKey(), session);
        if (isOverLimit()) {
            evictionLock.lock();
            try {
                makeRoom();
            } finally {
                evictionLock.unlock();
            }
        }
        sessionHandlerMetrics.verificationActiveSessions().set(activeSessionCount());
    }

    /// Make room in the lanes, called with the eviction lock held, once per admission.
    /// Finished sessions are reaped first, because sessions may have finished
    /// while waiting for the lock, and the count is checked again before the
    /// policy is consulted: the other ingress thread may already have made room.
    /// Every session the policy selects is then evicted when it is still present.
    /// One pass pays for one admission: the admission added one session, and the
    /// pass removes the selected session, or finds that the count is already
    /// within the limit, or finds that the selected session was reaped meanwhile,
    /// which lowered the count as well. An admission in flight on the other
    /// ingress thread pays for itself in its own pass, so the time spent under the
    /// lock is bounded by one reap, one policy consultation and the evictions it
    /// selected.
    private void makeRoom() {
        completeFinishedSessions();
        if (isOverLimit()) {
            final List<SessionKey> selected = evictionPolicy.selectForEviction(snapshot());
            for (final SessionKey key : selected) {
                evict(key);
            }
        }
    }

    /// Evict one session: remove it from its lane, cancel it, and account for it.
    /// A session that is in no lane any more was reaped by the other ingress
    /// thread since the snapshot was taken, which lowered the count already, and
    /// is left alone.
    ///
    /// @param key the key of the session to evict
    private void evict(final SessionKey key) {
        final BlockVerificationSession fromHighPriorityLane = highPrioritySessions.remove(key);
        if (fromHighPriorityLane != null) {
            finishEviction(key, fromHighPriorityLane, SessionLane.HIGH_PRIORITY);
        } else {
            final BlockVerificationSession fromLowPriorityLane = lowPrioritySessions.remove(key);
            if (fromLowPriorityLane != null) {
                finishEviction(key, fromLowPriorityLane, SessionLane.LOW_PRIORITY);
            }
        }
    }

    /// Cancel a session that was just removed from its lane and do the bookkeeping.
    /// Cancelling runs the session's result handling on this thread, so the
    /// cancellation notification has been sent and the key has been added to the
    /// finished set when the call returns; the key is dropped again because the
    /// session is no longer in any lane. A session whose cancellation returns
    /// false had already produced its result on its own and is not counted as
    /// evicted.
    ///
    /// @param key the key of the removed session
    /// @param session the removed session
    /// @param lane the lane it was removed from
    private void finishEviction(final SessionKey key, final BlockVerificationSession session, final SessionLane lane) {
        final boolean cancelled = session.cancel();
        finishedSessions.remove(key);
        activePublisherSession.compareAndSet(session, null);
        if (cancelled) {
            evictedSessions(lane).increment();
            LOGGER.log(
                    DEBUG,
                    "Evicted the session for block {0} from the {1} lane, the active sessions buffer is over its limit of {2}",
                    key.blockNumber(),
                    lane,
                    verificationConfig.activeSessionsBufferSize());
        }
    }

    /// Take an immutable view of both lanes for the eviction policy.
    ///
    /// @return the snapshot
    private ActiveSessionsSnapshot snapshot() {
        final BlockVerificationSession active = activePublisherSession.get();
        final SessionKey activeKey;
        if (active != null) {
            activeKey = active.sessionKey();
        } else {
            activeKey = null;
        }
        return new ActiveSessionsSnapshot(
                highPrioritySessions.keySet(), lowPrioritySessions.keySet(), activeKey, lastVerifiedBlock.get());
    }

    /// The combined number of sessions in both lanes.
    ///
    /// @return the combined count
    private int activeSessionCount() {
        return highPrioritySessions.size() + lowPrioritySessions.size();
    }

    /// Whether the combined count exceeds the configured limit.
    ///
    /// @return true when over the limit
    private boolean isOverLimit() {
        return activeSessionCount() > verificationConfig.activeSessionsBufferSize();
    }

    /// The map holding a lane.
    ///
    /// @param lane the lane
    /// @return its map
    private ConcurrentSkipListMap<SessionKey, BlockVerificationSession> laneOf(final SessionLane lane) {
        return switch (lane) {
            case HIGH_PRIORITY -> highPrioritySessions;
            case LOW_PRIORITY -> lowPrioritySessions;
        };
    }

    /// The evictions counter of a lane.
    ///
    /// @param lane the lane
    /// @return its counter
    private LongCounter.Measurement evictedSessions(final SessionLane lane) {
        return switch (lane) {
            case HIGH_PRIORITY -> sessionHandlerMetrics.verificationSessionsEvictedHighPriority();
            case LOW_PRIORITY -> sessionHandlerMetrics.verificationSessionsEvictedLowPriority();
        };
    }
}
