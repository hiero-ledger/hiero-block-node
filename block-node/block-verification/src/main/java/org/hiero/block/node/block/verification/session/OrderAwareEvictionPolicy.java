// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import org.hiero.block.node.block.verification.VerificationConfig;
import org.hiero.block.node.block.verification.session.BlockVerificationSession.SessionKey;

/// The eviction policy the active sessions buffer applies in production. It
/// always selects a session, so that the buffer never stays over its limit, and
/// it prefers a session that cannot complete on its own.
///
/// Given the last verified block, every session in the buffer is in one of three
/// states:
/// - finishing: it does not wait for order, because its block is at most one
///   ahead of the last verified block, below the first ordered block, or in the
///   low priority lane while not all sources require ordering. It reports its
///   result on its own.
/// - progressing: it waits for order, and every block from the one after the
///   last verified block up to its own is held by a session of either lane. It
///   is released as the run below it verifies.
/// - stuck: it waits for order above the first gap, the lowest block after the
///   last verified block that no session holds. It waits for a block nobody is
///   working on and can hold its place indefinitely.
///
/// The best candidate is the stuck session with the highest block. Successes are
/// released in order, so evicting a stuck session never holds up a session that
/// could otherwise be released: everything below the first gap is unaffected,
/// and everything above it is stuck already. The highest one leaves no other
/// session behind it but, possibly, the active publisher session, and for the
/// publisher it is the newest block, the smallest rewind for a resend. When
/// sessions of both lanes hold the highest stuck block, the low priority one is
/// selected.
///
/// When no session is stuck, every session would complete on its own, yet the
/// buffer has to make room, so the policy selects the first backup candidate of:
/// 1. The low priority session with the highest block whose eviction opens no gap
///    below a high priority session: its block is at or below the last verified
///    block, another session holds the same block, or no high priority session
///    holds a higher block.
/// 1. The low priority session with the highest block. A low priority session
///    always goes before a high priority one: backfill fetches the block again on
///    a later gap scan, while a publisher block given up has to be resent on the
///    live stream. At this point high priority sessions wait on every low priority
///    session, so the ones above the evicted block stay stuck until backfill
///    supplies it again, and while the buffer stays full that block can be
///    evicted again on its next arrival.
/// 1. The high priority session at the backup position, once the low priority
///    lane is empty, counted from the highest block down: the configured limit
///    divided by 11, and at least the third session. A snapshot taken while
///    sessions were being cleared can hold fewer, and the lowest session is
///    selected then. The position keeps clear of both ends of the lane. The
///    lowest session is the next to complete, and evicting it holds up every
///    session above it. The highest sessions are the blocks the publisher has
///    just sent, and evicting them asks for the same blocks again on every
///    admission. A session about one eleventh of the way down leaves a short run
///    above the gap it opens, and a long run below it that keeps draining while
///    the publisher resends the evicted block. When the session at the position
///    is the active publisher session, the next one up is selected, or the next
///    one down when none is above it. That happens once the publisher resends a
///    block this step evicted, since the resent block lands back on the same
///    position: the next one up opens the gap above the block being received, so
///    that block and every block below it still complete in order, and it is the
///    block the publisher sends next.
///
/// The policy selects nothing only when the snapshot holds no session but the
/// active publisher session, the high priority session still receiving its block
/// items. That session is never selected, because the publisher does not resend
/// a block that ends incomplete. Every selected session has therefore received
/// its complete block, and its eviction reports [SessionFailureType#CANCELLED].
/// The session just admitted is treated like any other: a block that arrives
/// stuck far ahead of the rest is selected on arrival.
public final class OrderAwareEvictionPolicy implements SessionEvictionPolicy {
    /// The prime the configured limit is divided by to find the backup position.
    /// It is fixed, not configurable: with the default limit of 101 the backup
    /// position is the ninth session from the highest block down.
    private static final int BACKUP_POSITION_DIVISOR = 11;
    /// The lowest backup position: the third session from the highest block down.
    private static final int MINIMUM_BACKUP_POSITION = 3;
    /// The configuration for verification, source of the ordering settings.
    private final VerificationConfig verificationConfig;
    /// The position of the high priority backup candidate, counted from one at the
    /// session with the highest block down.
    private final int backupPosition;

    /// Constructor.
    ///
    /// @param verificationConfig the configuration for verification, must not be null
    public OrderAwareEvictionPolicy(final VerificationConfig verificationConfig) {
        this.verificationConfig = verificationConfig;
        this.backupPosition = Math.max(
                MINIMUM_BACKUP_POSITION, verificationConfig.activeSessionsBufferSize() / BACKUP_POSITION_DIVISOR);
    }

    /// {@inheritDoc}
    /// ---
    /// Selects the best candidate, the stuck session with the highest block, or,
    /// when no session is stuck, the first backup candidate, see the class
    /// documentation.
    @Override
    public SessionKey selectForEviction(final ActiveSessionsSnapshot snapshot) {
        final Map<Long, Integer> sessionsPerBlock = sessionsPerBlock(snapshot);
        final SessionKey bestCandidate = bestCandidate(snapshot, sessionsPerBlock);
        final SessionKey result;
        if (bestCandidate != null) {
            result = bestCandidate;
        } else {
            result = backupCandidate(snapshot, sessionsPerBlock);
        }
        return result;
    }

    /// Count the sessions of both lanes that hold each block.
    ///
    /// @param snapshot the view of the buffer
    /// @return the number of sessions per held block
    private static Map<Long, Integer> sessionsPerBlock(final ActiveSessionsSnapshot snapshot) {
        final Map<Long, Integer> sessionsPerBlock = new HashMap<>();
        for (final SessionKey key : snapshot.highPrioritySessions()) {
            sessionsPerBlock.merge(key.blockNumber(), 1, Integer::sum);
        }
        for (final SessionKey key : snapshot.lowPrioritySessions()) {
            sessionsPerBlock.merge(key.blockNumber(), 1, Integer::sum);
        }
        return sessionsPerBlock;
    }

    /// Find the best candidate: the stuck session with the highest block, the low
    /// priority one on a tie, never the active publisher session.
    ///
    /// @param snapshot the view of the buffer
    /// @param sessionsPerBlock the number of sessions per held block
    /// @return the best candidate, or null when no session other than the active one is stuck
    private SessionKey bestCandidate(final ActiveSessionsSnapshot snapshot, final Map<Long, Integer> sessionsPerBlock) {
        final long firstGap = firstGap(snapshot.lastVerifiedBlock(), sessionsPerBlock);
        final SessionKey stuckHighPriority =
                highestStuck(snapshot.highPrioritySessions(), snapshot.activeHighPrioritySession(), firstGap, true);
        final SessionKey stuckLowPriority = highestStuck(
                snapshot.lowPrioritySessions(), null, firstGap, verificationConfig.allSourcesRequireOrdering());
        final SessionKey result;
        if (stuckHighPriority == null) {
            result = stuckLowPriority;
        } else if (stuckLowPriority == null) {
            result = stuckHighPriority;
        } else if (stuckLowPriority.blockNumber() >= stuckHighPriority.blockNumber()) {
            result = stuckLowPriority;
        } else {
            result = stuckHighPriority;
        }
        return result;
    }

    /// Find the first gap: the lowest block after the last verified block that no
    /// session of either lane holds. Before any block is verified, the search starts
    /// at block 0.
    ///
    /// @param lastVerifiedBlock the last verified block of the snapshot
    /// @param sessionsPerBlock the number of sessions per held block
    /// @return the first gap
    private static long firstGap(final long lastVerifiedBlock, final Map<Long, Integer> sessionsPerBlock) {
        long firstGap = lastVerifiedBlock + 1;
        while (sessionsPerBlock.containsKey(firstGap)) {
            firstGap++;
        }
        return firstGap;
    }

    /// Find the stuck session with the highest key in one lane.
    ///
    /// @param lane the keys of the lane, ascending
    /// @param excluded a key never to select, may be null
    /// @param firstGap the first gap
    /// @param laneAwaitsOrder whether sessions of this lane are subject to ordering at all
    /// @return the highest stuck key of the lane other than the excluded one, or null when there is none
    private SessionKey highestStuck(
            final NavigableSet<SessionKey> lane,
            final SessionKey excluded,
            final long firstGap,
            final boolean laneAwaitsOrder) {
        SessionKey result = null;
        if (laneAwaitsOrder) {
            final Iterator<SessionKey> descending = lane.descendingIterator();
            while (result == null && descending.hasNext()) {
                final SessionKey key = descending.next();
                if (!key.equals(excluded) && isStuck(key.blockNumber(), firstGap)) {
                    result = key;
                }
            }
        }
        return result;
    }

    /// Decide whether a session for the given block, in a lane subject to ordering,
    /// is stuck: its block is above the first gap, which also puts it more than one
    /// ahead of the last verified block, and at or above the first ordered block, as
    /// the ordering stage requires before it makes a session wait.
    ///
    /// @param blockNumber the block of the session
    /// @param firstGap the first gap
    /// @return true when the session is stuck
    private boolean isStuck(final long blockNumber, final long firstGap) {
        return blockNumber > firstGap && blockNumber >= verificationConfig.firstOrderedBlock();
    }

    /// Find the first backup candidate, for when no session is stuck: the highest low
    /// priority session whose eviction opens no gap below a high priority session,
    /// else the highest low priority session, else the high priority session at the
    /// backup position.
    ///
    /// @param snapshot the view of the buffer
    /// @param sessionsPerBlock the number of sessions per held block
    /// @return the backup candidate, or null when the snapshot holds no session but the active one
    private SessionKey backupCandidate(
            final ActiveSessionsSnapshot snapshot, final Map<Long, Integer> sessionsPerBlock) {
        final SessionKey lowPriorityOpeningNoGap = highestLowPriorityOpeningNoGap(snapshot, sessionsPerBlock);
        final SessionKey result;
        if (lowPriorityOpeningNoGap != null) {
            result = lowPriorityOpeningNoGap;
        } else if (!snapshot.lowPrioritySessions().isEmpty()) {
            result = snapshot.lowPrioritySessions().last();
        } else {
            result = highPriorityAtBackupPosition(snapshot);
        }
        return result;
    }

    /// Find the low priority session with the highest key whose eviction opens no gap
    /// below a high priority session: its block is at or below the last verified
    /// block, another session holds the same block, or no high priority session holds
    /// a higher block.
    ///
    /// @param snapshot the view of the buffer
    /// @param sessionsPerBlock the number of sessions per held block
    /// @return the highest such low priority key, or null when there is none
    private static SessionKey highestLowPriorityOpeningNoGap(
            final ActiveSessionsSnapshot snapshot, final Map<Long, Integer> sessionsPerBlock) {
        final NavigableSet<SessionKey> highPriority = snapshot.highPrioritySessions();
        final long highestHighPriorityBlock;
        if (highPriority.isEmpty()) {
            highestHighPriorityBlock = Long.MIN_VALUE;
        } else {
            highestHighPriorityBlock = highPriority.last().blockNumber();
        }
        SessionKey result = null;
        final Iterator<SessionKey> descending = snapshot.lowPrioritySessions().descendingIterator();
        while (result == null && descending.hasNext()) {
            final SessionKey key = descending.next();
            final long blockNumber = key.blockNumber();
            if (blockNumber <= snapshot.lastVerifiedBlock()
                    || sessionsPerBlock.get(blockNumber) > 1
                    || blockNumber > highestHighPriorityBlock) {
                result = key;
            }
        }
        return result;
    }

    /// Find the high priority session at the backup position, counted from one at the
    /// session with the highest block down, or the lowest session when the lane holds
    /// fewer sessions than that. When that session is the active publisher session,
    /// the next session up is selected, or the next one down when none is above it.
    ///
    /// @param snapshot the view of the buffer
    /// @return the key at the backup position or next to it, or null when the lane holds
    ///     no session other than the active one
    private SessionKey highPriorityAtBackupPosition(final ActiveSessionsSnapshot snapshot) {
        final NavigableSet<SessionKey> highPriority = snapshot.highPrioritySessions();
        SessionKey atPosition = null;
        int position = 0;
        final Iterator<SessionKey> descending = highPriority.descendingIterator();
        while (position < backupPosition && descending.hasNext()) {
            atPosition = descending.next();
            position++;
        }
        final SessionKey result;
        if (atPosition == null || !atPosition.equals(snapshot.activeHighPrioritySession())) {
            result = atPosition;
        } else {
            final SessionKey nextUp = highPriority.higher(atPosition);
            if (nextUp != null) {
                result = nextUp;
            } else {
                result = highPriority.lower(atPosition);
            }
        }
        return result;
    }
}
