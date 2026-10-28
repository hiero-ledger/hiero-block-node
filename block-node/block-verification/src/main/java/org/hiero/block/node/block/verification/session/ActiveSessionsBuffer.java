// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import static java.lang.System.Logger.Level.DEBUG;

import java.util.NavigableSet;
import java.util.Objects;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import org.hiero.block.node.block.verification.VerificationConfig;
import org.hiero.block.node.block.verification.metrics.SessionHandlerMetrics;
import org.hiero.block.node.block.verification.session.BlockVerificationSession.SessionKey;
import org.hiero.metrics.LongCounter;

/// The active sessions buffer: a helper of the [BlockSessionHandler] that holds the
/// sessions the handler has started and not cleared yet, in two lanes (see
/// [SessionLane]) that share one limit, [VerificationConfig#activeSessionsBufferSize()].
/// Every admission that takes the combined count over the limit evicts the session
/// the [SessionEvictionPolicy] selects, so the count is above the limit only while
/// admissions are in flight, by at most one per ingress thread, and back at it once
/// they have made room. An admission never waits for room.
///
/// The buffer is passive. The handler decides what happens to its sessions and when,
/// and the buffer gives it the means to do so: [#admit(BlockVerificationSession)] adds
/// a session to the lane its block's source calls for and, when that takes the
/// combined count over the limit, evicts what the [SessionEvictionPolicy] selects;
/// [#activePublisherSession()] finds the session the publisher's next batch belongs to;
/// [#clearCompletedSessions()] removes the sessions that are complete. Sessions never
/// change the buffer themselves.
///
/// The handler calls the buffer from both ingress threads. Eviction runs under a lock,
/// held only for work in memory, so the two threads never evict at the same time.
public final class ActiveSessionsBuffer {
    /// Logger for the buffer.
    private static final System.Logger LOGGER = System.getLogger(ActiveSessionsBuffer.class.getName());
    /// The configuration for verification, source of the limit.
    private final VerificationConfig verificationConfig;
    /// The policy that selects the session to evict when the lanes hold more sessions than the limit.
    private final SessionEvictionPolicy evictionPolicy;
    /// The last successfully verified block, read for the snapshot handed to the policy.
    private final AtomicLong lastVerifiedBlock;
    /// The metrics of the buffer: the active sessions gauge and the evictions counters.
    private final SessionHandlerMetrics sessionHandlerMetrics;
    /// The high priority lane: active sessions of blocks from the publisher, ordered by [SessionKey].
    private final ConcurrentSkipListMap<SessionKey, BlockVerificationSession> highPrioritySessions;
    /// The low priority lane: active sessions of blocks from any other source, ordered by [SessionKey].
    private final ConcurrentSkipListMap<SessionKey, BlockVerificationSession> lowPrioritySessions;
    /// The latest admitted publisher session, until it is cleared or evicted; see [#activePublisherSession()].
    private final AtomicReference<BlockVerificationSession> latestPublisherSession;
    /// Guards the eviction so that the two ingress threads never evict at the same time.
    private final ReentrantLock evictionLock;

    /// Constructor.
    ///
    /// @param verificationConfig the configuration for verification, must not be null
    /// @param evictionPolicy the policy that selects sessions to evict, must not be null
    /// @param lastVerifiedBlock the last successfully verified block, must not be null
    /// @param sessionHandlerMetrics the metrics to record the buffer's size and evictions in, must not be null
    public ActiveSessionsBuffer(
            final VerificationConfig verificationConfig,
            final SessionEvictionPolicy evictionPolicy,
            final AtomicLong lastVerifiedBlock,
            final SessionHandlerMetrics sessionHandlerMetrics) {
        this.verificationConfig = Objects.requireNonNull(verificationConfig);
        this.evictionPolicy = Objects.requireNonNull(evictionPolicy);
        this.lastVerifiedBlock = Objects.requireNonNull(lastVerifiedBlock);
        this.sessionHandlerMetrics = Objects.requireNonNull(sessionHandlerMetrics);
        this.highPrioritySessions = new ConcurrentSkipListMap<>();
        this.lowPrioritySessions = new ConcurrentSkipListMap<>();
        this.latestPublisherSession = new AtomicReference<>();
        this.evictionLock = new ReentrantLock();
    }

    /// Admit a started session to the lane its block's source calls for: the high
    /// priority lane for the publisher, the low priority lane for any other source. A
    /// publisher session also becomes the latest publisher session. When the combined
    /// count then exceeds the limit, the eviction policy is consulted under the
    /// eviction lock, see [#makeRoom()]. The admission never waits for room; the
    /// session is added before the policy is consulted, so it may itself be the one
    /// evicted.
    ///
    /// @param session the started session to admit, must not be null
    public void admit(final BlockVerificationSession session) {
        final SessionLane lane =
                switch (session.blockSource()) {
                    case PUBLISHER -> {
                        latestPublisherSession.set(session);
                        yield SessionLane.HIGH_PRIORITY;
                    }
                    case BACKFILL, HISTORY, UNKNOWN -> SessionLane.LOW_PRIORITY;
                };
        laneOf(lane).put(session.sessionKey(), session);
        if (isOverLimit()) {
            evictionLock.lock();
            try {
                makeRoom();
            } finally {
                evictionLock.unlock();
            }
        }
        sessionHandlerMetrics.verificationActiveSessions().set(size());
    }

    /// The session the publisher's next batch belongs to: the latest admitted publisher
    /// session, until it has received the end of its block. The eviction policy never
    /// selects it. A session that has received its whole block but is still being
    /// verified is not the active one: a new block does not cancel it and no further
    /// batch is added to it.
    ///
    /// @return the active publisher session, or null when there is none
    public BlockVerificationSession activePublisherSession() {
        final BlockVerificationSession latest = latestPublisherSession.get();
        final BlockVerificationSession result;
        if (latest != null && !latest.isEndOfBlockReceived()) {
            result = latest;
        } else {
            result = null;
        }
        return result;
    }

    /// Clear the complete sessions: every session whose result has been handled leaves
    /// its lane, and stops being the latest publisher session if it was.
    public void clearCompletedSessions() {
        clearCompleted(highPrioritySessions);
        clearCompleted(lowPrioritySessions);
        sessionHandlerMetrics.verificationActiveSessions().set(size());
    }

    /// Take an immutable view of both lanes for the eviction policy. The lanes are
    /// copied before the active publisher session is resolved: a publisher session in
    /// the copy that is still active by then is the latest one, so it is protected, and
    /// one that is not active any more has received the end of its block, so evicting
    /// it reports a complete block. Resolving it first would let a session admitted
    /// in between appear in the copy without its protection.
    ///
    /// @return the snapshot
    ActiveSessionsSnapshot snapshot() {
        final NavigableSet<SessionKey> highPriorityKeys = new TreeSet<>(highPrioritySessions.keySet());
        final NavigableSet<SessionKey> lowPriorityKeys = new TreeSet<>(lowPrioritySessions.keySet());
        final BlockVerificationSession active = activePublisherSession();
        final SessionKey activeKey;
        if (active != null) {
            activeKey = active.sessionKey();
        } else {
            activeKey = null;
        }
        return new ActiveSessionsSnapshot(highPriorityKeys, lowPriorityKeys, activeKey, lastVerifiedBlock.get());
    }

    /// The combined number of sessions in both lanes.
    ///
    /// @return the combined count
    int size() {
        return highPrioritySessions.size() + lowPrioritySessions.size();
    }

    /// Clear the complete sessions of one lane.
    ///
    /// @param lane the lane to clear
    private void clearCompleted(final ConcurrentSkipListMap<SessionKey, BlockVerificationSession> lane) {
        for (final BlockVerificationSession session : lane.values()) {
            if (session.isComplete()) {
                lane.remove(session.sessionKey());
                latestPublisherSession.compareAndSet(session, null);
            }
        }
    }

    /// Make room in the lanes, called with the eviction lock held, once per admission
    /// over the limit. Complete sessions are cleared first, because sessions may have
    /// completed while waiting for the lock, and the count is checked again before the
    /// policy is consulted: the other ingress thread may already have made room. The
    /// session the policy selects is then evicted when it is still present. The policy
    /// selects a session whenever the lanes hold one other than the active publisher
    /// session, so every pass that finds the buffer over its limit takes one session
    /// out: by evicting it, or because the other ingress thread cleared it after the
    /// snapshot was taken, which lowered the count already. A pass never loops and never
    /// waits: the time spent under the lock is bounded by one clearing, one snapshot,
    /// one policy consultation and at most one eviction, and an admission in flight on
    /// the other ingress thread runs its own pass.
    private void makeRoom() {
        clearCompleted(highPrioritySessions);
        clearCompleted(lowPrioritySessions);
        if (isOverLimit()) {
            final SessionKey selected = evictionPolicy.selectForEviction(snapshot());
            if (selected != null) {
                evict(selected);
            }
        }
    }

    /// Evict one session: remove it from its lane, cancel it, and account for it.
    /// A session that is in no lane any more was cleared by the other ingress thread
    /// since the snapshot was taken, which lowered the count already, and is left
    /// alone.
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

    /// Cancel a session that was just removed from its lane and count it. Cancelling
    /// runs the session's result handling on this thread, which sends the
    /// cancellation. A session whose cancellation returns false had already produced
    /// its result on its own and is not counted as evicted.
    ///
    /// @param key the key of the removed session
    /// @param session the removed session
    /// @param lane the lane it was removed from
    private void finishEviction(final SessionKey key, final BlockVerificationSession session, final SessionLane lane) {
        final boolean cancelled = session.cancel();
        latestPublisherSession.compareAndSet(session, null);
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

    /// Whether the combined count exceeds the configured limit.
    ///
    /// @return true when over the limit
    private boolean isOverLimit() {
        return size() > verificationConfig.activeSessionsBufferSize();
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
