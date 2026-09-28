// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.hiero.block.node.block.verification.VerificationConfig;
import org.hiero.block.node.block.verification.metrics.SessionHandlerMetrics;
import org.hiero.block.node.block.verification.session.BlockVerificationSession.SessionKey;
import org.hiero.block.node.block.verification.session.eviction.EvictionPolicy;
import org.hiero.block.node.block.verification.session.eviction.EvictionSnapshot;
import org.hiero.block.node.block.verification.session.eviction.SessionSnapshot;

/// The bounded buffer of running verification sessions.
///
/// Sessions enter through [#activate(BlockVerificationSession)] and leave in
/// one of two ways: on their own, through [#remove(SessionKey)] invoked by the
/// session once its result has been handled, or by eviction, when an
/// activation pushes the buffer over its limit and the [EvictionPolicy]
/// selects them.
///
/// Threading: `activate` may be called concurrently from the two delivery
/// threads and is serialized by a lock that is held only for in-memory work:
/// adding the session, checking the size, taking the snapshot, running the
/// policy, guarding and removing the victims. Cancelling the victims, which
/// runs their result handling inline, happens after the lock is released.
/// `remove`, `size` and `contains` never take the lock and may run on any
/// thread. No other lock is ever taken by this class.
///
/// Protection: a high priority session that has not yet received the batch
/// ending its block is never evicted. Its cancellation would be reported as
/// an incomplete cancellation, which the publisher does not act on, so the
/// block would be lost. This is the single definition of protection, used
/// both to build the protected keys handed to the policy and to guard every
/// victim before it is removed. Low priority sessions are never protected.
///
/// The limit is a target, not a hard bound: when every session over the limit
/// is protected or is expected to complete on its own, the buffer overshoots
/// until the next activation runs the policy again.
public final class ActiveSessionsBuffer {
    /// Logger for the buffer.
    private static final System.Logger LOGGER = System.getLogger(ActiveSessionsBuffer.class.getName());
    /// All currently active sessions, keyed and ordered by [SessionKey].
    private final ConcurrentSkipListMap<SessionKey, BlockVerificationSession> sessions;
    /// Serializes activations, so that at most one eviction round runs at a time
    /// and the size check is exact against concurrent activations.
    private final ReentrantLock lock;
    /// The maximum number of sessions the buffer aims to hold.
    private final int limit;
    /// The first block number that requires strict ordering.
    private final long firstOrderedBlock;
    /// Whether sources other than the publisher are ordered.
    private final boolean allSourcesRequireOrdering;
    /// The last successfully verified block, shared with the sessions.
    private final AtomicLong lastVerifiedBlock;
    /// The policy that selects which sessions to evict when the buffer is over its limit.
    private final EvictionPolicy policy;
    /// Metrics recorded by this buffer: the size gauge and the evicted counters.
    private final SessionHandlerMetrics metrics;

    /// Constructor.
    ///
    /// @param verificationConfig the configuration for verification, source of the limit
    ///     and the ordering settings, must not be null
    /// @param lastVerifiedBlock the last successfully verified block, must not be null
    /// @param policy the policy selecting sessions to evict, must not be null
    /// @param metrics the metrics recorded by this buffer, must not be null
    public ActiveSessionsBuffer(
            final VerificationConfig verificationConfig,
            final AtomicLong lastVerifiedBlock,
            final EvictionPolicy policy,
            final SessionHandlerMetrics metrics) {
        Objects.requireNonNull(verificationConfig);
        this.limit = verificationConfig.activeSessionsBufferSize();
        this.firstOrderedBlock = verificationConfig.firstOrderedBlock();
        this.allSourcesRequireOrdering = verificationConfig.allSourcesRequireOrdering();
        this.lastVerifiedBlock = Objects.requireNonNull(lastVerifiedBlock);
        this.policy = Objects.requireNonNull(policy);
        this.metrics = Objects.requireNonNull(metrics);
        this.sessions = new ConcurrentSkipListMap<>();
        this.lock = new ReentrantLock();
    }

    /// Activate a started session.
    ///
    /// The session is added to the buffer. A session that produced its result
    /// before it became visible here is removed right away: its own removal
    /// found nothing to remove. Otherwise, if the buffer is now over its limit,
    /// one eviction round runs: the policy selects victims over a snapshot of
    /// the buffer, every victim is checked again before it is removed, and the
    /// removed sessions are cancelled once the lock is released. Activation
    /// never throws because of an eviction round: any failure in it is logged
    /// and no session is evicted.
    ///
    /// @param session the started session to activate, must not be null
    public void activate(final BlockVerificationSession session) {
        Objects.requireNonNull(session);
        final SessionKey key = session.sessionKey();
        final int sizeBefore;
        final EvictionRound round;
        lock.lock();
        try {
            sessions.put(key, session);
            if (session.isFinished()) {
                sessions.remove(key);
            }
            sizeBefore = sessions.size();
            if (sizeBefore > limit) {
                round = selectAndRemoveVictimsUnderLock();
            } else {
                round = EvictionRound.EMPTY;
            }
        } finally {
            lock.unlock();
        }
        if (sizeBefore > limit) {
            cancelAndReport(sizeBefore, round);
        }
        updateGauge();
    }

    /// Remove a session from the buffer, if present.
    /// Invoked by every session once its result has been handled, and safe to
    /// invoke for a session that was already evicted.
    ///
    /// @param key the key of the session to remove, must not be null
    public void remove(final SessionKey key) {
        Objects.requireNonNull(key);
        sessions.remove(key);
        updateGauge();
    }

    /// The number of sessions currently in the buffer.
    ///
    /// @return the current size
    public int size() {
        return sessions.size();
    }

    /// Whether a session is currently in the buffer.
    ///
    /// @param key the key of the session, must not be null
    /// @return `true` if the session is in the buffer
    public boolean contains(final SessionKey key) {
        Objects.requireNonNull(key);
        return sessions.containsKey(key);
    }

    /// A session is protected from eviction while it is high priority and has
    /// not received the batch ending its block.
    ///
    /// @param session the session to check
    /// @return `true` if the session must not be evicted
    static boolean isProtected(final BlockVerificationSession session) {
        return session.priority() == SessionPriority.HIGH && !session.isEndOfBlockReceived();
    }

    /// Run the selection and removal part of one eviction round. Must be
    /// called with the lock held. This is the single safety boundary of a
    /// round: an unexpected failure anywhere in it is logged and results in
    /// no eviction, so that the activation always returns normally.
    ///
    /// @return the sessions removed from the buffer, to be cancelled once the lock is released
    private EvictionRound selectAndRemoveVictimsUnderLock() {
        EvictionRound result;
        try {
            final EvictionSnapshot snapshot = snapshotUnderLock();
            final List<SessionKey> selected = policy.selectVictims(snapshot);
            final List<BlockVerificationSession> removed = new ArrayList<>(selected.size());
            final Iterator<SessionKey> victimKeys = selected.iterator();
            // a session may leave on its own while the round runs, so stop as soon as the buffer is within its limit
            while (victimKeys.hasNext() && sessions.size() > limit) {
                final SessionKey victimKey = victimKeys.next();
                final BlockVerificationSession victim = sessions.get(victimKey);
                // a victim that has left, or is about to leave, on its own is simply skipped
                if (victim != null && !victim.isFinished()) {
                    if (isProtected(victim)) {
                        LOGGER.log(
                                WARNING, "Eviction policy selected the protected session {0}, it is kept", victimKey);
                    } else {
                        sessions.remove(victimKey);
                        removed.add(victim);
                    }
                }
            }
            result = new EvictionRound(selected.size(), removed);
        } catch (final RuntimeException e) {
            final String message = "Eviction round failed with %d sessions in a buffer of %d, no session evicted"
                    .formatted(sessions.size(), limit);
            LOGGER.log(WARNING, message, e);
            result = EvictionRound.EMPTY;
        }
        return result;
    }

    /// Take an immutable snapshot of the buffer for the policy. Must be called
    /// with the lock held. Sessions that have already produced their result
    /// are leaving on their own and are left out.
    ///
    /// @return the snapshot
    private EvictionSnapshot snapshotUnderLock() {
        final List<SessionSnapshot> snapshots = new ArrayList<>(sessions.size());
        final Set<SessionKey> protectedKeys = new HashSet<>();
        for (final BlockVerificationSession session : sessions.values()) {
            if (!session.isFinished()) {
                snapshots.add(new SessionSnapshot(session.sessionKey(), session.priority(), session.blockSource()));
                if (isProtected(session)) {
                    protectedKeys.add(session.sessionKey());
                }
            }
        }
        return new EvictionSnapshot(
                snapshots, lastVerifiedBlock.get(), firstOrderedBlock, allSourcesRequireOrdering, limit, protectedKeys);
    }

    /// Cancel the sessions removed by a round, count the ones actually
    /// cancelled and report the round. Runs without the lock: cancelling a
    /// session runs its result handling inline. A removed session that
    /// produced its result in the meantime reports `false` from its
    /// cancellation and is not counted, its result is handled normally.
    ///
    /// @param sizeBefore the size of the buffer when the round started
    /// @param round the outcome of the selection and removal
    private void cancelAndReport(final int sizeBefore, final EvictionRound round) {
        final List<BlockVerificationSession> evicted =
                new ArrayList<>(round.removed().size());
        for (final BlockVerificationSession victim : round.removed()) {
            if (victim.cancel()) {
                metrics.verificationSessionsEvicted(victim.priority()).increment();
                evicted.add(victim);
            }
        }
        final List<SessionKey> evictedKeys =
                evicted.stream().map(BlockVerificationSession::sessionKey).toList();
        final long evictedHigh = countByPriority(evicted, SessionPriority.HIGH);
        final long evictedLow = countByPriority(evicted, SessionPriority.LOW);
        final int sizeAfter = sessions.size();
        final String message;
        if (sizeAfter > limit) {
            message = "Active sessions buffer over its limit of {0} with {1} sessions: the policy selected {2}, evicted"
                    + " {3} high and {4} low priority sessions {5}, {6} sessions remain, the remaining sessions are"
                    + " protected or will complete on their own";
        } else {
            message = "Active sessions buffer over its limit of {0} with {1} sessions: the policy selected {2}, evicted"
                    + " {3} high and {4} low priority sessions {5}, {6} sessions remain";
        }
        LOGGER.log(INFO, message, limit, sizeBefore, round.selected(), evictedHigh, evictedLow, evictedKeys, sizeAfter);
    }

    /// Count the sessions of the given priority.
    private static long countByPriority(final List<BlockVerificationSession> sessions, final SessionPriority priority) {
        return sessions.stream()
                .filter(session -> session.priority() == priority)
                .count();
    }

    /// Publish the current size of the buffer to the gauge.
    private void updateGauge() {
        metrics.verificationActiveSessions().set(sessions.size());
    }

    /// The outcome of the selection and removal part of an eviction round.
    ///
    /// @param selected the number of victims the policy selected
    /// @param removed the sessions actually removed from the buffer, in selection order
    private record EvictionRound(int selected, List<BlockVerificationSession> removed) {
        /// A round that removed nothing.
        private static final EvictionRound EMPTY = new EvictionRound(0, List.of());
    }
}
