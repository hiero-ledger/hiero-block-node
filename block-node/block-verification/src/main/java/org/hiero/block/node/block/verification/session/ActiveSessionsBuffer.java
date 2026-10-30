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
/// sessions the handler has started and that have not been cleared yet, in two lanes
/// (see [SessionLane]) that share one limit,
/// [VerificationConfig#activeSessionsBufferSize()]. An admission first clears the
/// sessions that are complete; when the buffer is still full, it evicts the session
/// the [SessionEvictionPolicy] selects among the sessions already held, and only then
/// adds the new session, so the combined count never exceeds the limit and a session
/// is never evicted by its own admission. An admission never waits for room.
///
/// The buffer is passive. The handler decides what happens to its sessions and when,
/// and the buffer gives it the means to do so: [#admit(BlockVerificationSession)]
/// clears the sessions that are complete, makes room when the buffer is still full,
/// by evicting what the [SessionEvictionPolicy] selects, and adds a session to the
/// lane its block's source calls for; [#activePublisherSession()] finds the session
/// the publisher's next batch belongs to. Sessions never change the buffer
/// themselves.
///
/// The handler calls the buffer from both ingress threads. The lanes change only
/// within an admission, which runs under the eviction lock with its clearing, its
/// eviction and its addition, so the two threads never change the lanes at the same
/// time, and the lanes stay as the snapshot for the eviction policy shows them until
/// the eviction. Complete sessions stay in their lanes until the next admission clears
/// them, and the gauge is set on every admission. The lock is held for work in memory
/// only: cancelling an evicted session runs its result handling on the admitting
/// thread, and its failure notification is sent without waiting, see
/// [VerificationOutbox][org.hiero.block.node.block.verification.VerificationOutbox].
/// Only finding the active publisher session takes no lock: it reads the latest
/// publisher session, which only changes under the lock.
public final class ActiveSessionsBuffer {
    /// Logger for the buffer.
    private static final System.Logger LOGGER = System.getLogger(ActiveSessionsBuffer.class.getName());
    /// The configuration for verification, source of the limit.
    private final VerificationConfig verificationConfig;
    /// The policy that selects the session to evict when an admission finds the lanes full.
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
    /// Guards each admission, with its clearing, its eviction and its addition, so that
    /// the two ingress threads never change the lanes at the same time.
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
    /// publisher session also becomes the latest publisher session. The whole admission
    /// runs under the eviction lock: the complete sessions are cleared first, then, when
    /// the buffer is still full, room is made, see [#makeRoom()], and only then is the
    /// session added. The policy chooses among the sessions already held, so a session
    /// is never evicted by its own admission, and the combined count never exceeds the
    /// limit. The active sessions gauge is set
    /// before the lock is released, so it never shows a count taken halfway through an
    /// admission. An admission on the other ingress thread waits until this one has
    /// added its session. The admission never waits for room.
    ///
    /// @param session the started session to admit, must not be null
    public void admit(final BlockVerificationSession session) {
        evictionLock.lock();
        try {
            clearCompletedSessions();
            if (isFull()) {
                makeRoom();
            }
            final SessionLane lane =
                    switch (session.blockSource()) {
                        case PUBLISHER -> {
                            latestPublisherSession.set(session);
                            yield SessionLane.HIGH_PRIORITY;
                        }
                        case BACKFILL, HISTORY, UNKNOWN -> SessionLane.LOW_PRIORITY;
                    };
            laneOf(lane).put(session.sessionKey(), session);
            sessionHandlerMetrics.verificationActiveSessions().set(size());
        } finally {
            evictionLock.unlock();
        }
    }

    /// The session the publisher's next batch belongs to: the latest admitted publisher
    /// session, until it has received the end of its block, or until an admission
    /// clears it once it is complete. The eviction policy never selects it. A session
    /// that has received its whole block but is still being verified is not the active
    /// one: a new block does not cancel it and no further batch is added to it.
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

    /// Clear the complete sessions, called first by every admission with the eviction
    /// lock held, the only place sessions are cleared: every session whose result has
    /// been handled leaves its lane, and stops being the latest publisher session if
    /// it was.
    private void clearCompletedSessions() {
        clearCompleted(highPrioritySessions);
        clearCompleted(lowPrioritySessions);
    }

    /// Take an immutable view of both lanes for the eviction policy. It is taken with
    /// the eviction lock held, before the session being admitted is added, so it holds
    /// only sessions already in the buffer, and no session joins or leaves the lanes
    /// while it is taken; only the active publisher session can still receive the end
    /// of its block meanwhile. The lanes are copied before the active publisher session
    /// is resolved: a publisher session in the copy that is still active by then is
    /// protected, and one that is not active any more has received the end of its
    /// block, so evicting it reports a complete block.
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

    /// Make room for a session about to be admitted to a full buffer, called with the
    /// eviction lock held once the admission has cleared the complete sessions and the
    /// buffer is still full. The policy selects a session whenever the lanes hold one
    /// other than the active publisher session, which a full buffer always does, and
    /// no session joins or leaves the lanes between the snapshot and the eviction, so
    /// every pass evicts the session the policy selects. A pass never loops and never
    /// waits: it adds one snapshot, one policy consultation and one eviction to the
    /// admission's time under the lock.
    private void makeRoom() {
        final SessionKey selected = evictionPolicy.selectForEviction(snapshot());
        if (selected != null) {
            evict(selected);
        }
    }

    /// Evict one session: remove it from its lane, cancel it, and account for it. The
    /// policy selects from a snapshot taken under the eviction lock, which is still
    /// held, so the selected session is always in one of the lanes.
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
                    "Evicted the session for block {0} from the {1} lane, the active sessions buffer is full at its limit of {2}",
                    key.blockNumber(),
                    lane,
                    verificationConfig.activeSessionsBufferSize());
        }
    }

    /// Whether the combined count has reached the configured limit, so that another
    /// session can only be admitted once room is made.
    ///
    /// @return true when the buffer is full
    private boolean isFull() {
        return size() >= verificationConfig.activeSessionsBufferSize();
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
