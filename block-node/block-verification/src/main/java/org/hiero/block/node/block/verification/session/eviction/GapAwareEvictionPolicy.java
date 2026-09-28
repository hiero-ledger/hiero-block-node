// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session.eviction;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import org.hiero.block.node.block.verification.session.BlockVerificationSession.SessionKey;
import org.hiero.block.node.block.verification.session.OrderingRules;
import org.hiero.block.node.block.verification.session.SessionPriority;

/// The default [EvictionPolicy].
///
/// Guiding principle: only sessions that wait on something external are ever
/// evicted. A session that is not subject to ordering, or whose block is at
/// or below the next expected block, leaves the buffer on its own as soon as
/// hashing and proof verification complete. Evicting such a session frees
/// nothing durable and only costs a resend (publisher) or a re-fetch
/// (backfill). Eviction exists to remove sessions that are, or will be,
/// parked in the ordering stage waiting for an earlier block that has not
/// arrived. Those are the *waiting* sessions, and the highest block among
/// them is the *top* of the waiting range.
///
/// Victims are always taken from the top of the waiting range downwards,
/// because the highest waiting block is the one furthest from being released
/// and the one the fewest other sessions depend on, in three tiers:
///
/// 1. A [SessionPriority#LOW] session at the top of the waiting range: no
///    other session depends on it.
/// 2. The highest [SessionPriority#HIGH] session that is not protected.
/// 3. The highest remaining [SessionPriority#LOW] session: it fills a gap a
///    higher session waits for, so it goes last.
///
/// Protected keys are never selected. Selection stops as soon as the buffer
/// is back within its limit. When only non waiting or protected sessions
/// remain, fewer victims than needed are returned and the caller tolerates a
/// transient overshoot.
public final class GapAwareEvictionPolicy implements EvictionPolicy {
    /// Highest block number first, then the newer session (higher unique id) first.
    private static final Comparator<SessionSnapshot> HIGHEST_FIRST = Comparator.comparingLong(
                    SessionSnapshot::blockNumber)
            .thenComparingLong(snapshot -> snapshot.key().uniqueId())
            .reversed();

    /// {@inheritDoc}
    @Override
    public List<SessionKey> selectVictims(final EvictionSnapshot snapshot) {
        Objects.requireNonNull(snapshot);
        final List<SessionSnapshot> remaining = new ArrayList<>(snapshot.sessions());
        remaining.sort(HIGHEST_FIRST);
        final List<SessionKey> victims = new ArrayList<>();
        boolean searching = true;
        while (searching && remaining.size() > snapshot.limit()) {
            final SessionSnapshot victim = nextVictim(remaining, snapshot);
            if (victim == null) {
                searching = false;
            } else {
                victims.add(victim.key());
                remaining.remove(victim);
            }
        }
        return victims;
    }

    /// Select the next victim among the remaining sessions.
    ///
    /// @param remaining the sessions not yet selected, sorted highest block first
    /// @param snapshot the snapshot the selection runs against
    /// @return the next victim, or `null` when no remaining session is waiting
    ///     or every waiting session is protected
    private static SessionSnapshot nextVictim(final List<SessionSnapshot> remaining, final EvictionSnapshot snapshot) {
        final List<SessionSnapshot> waiting = remaining.stream()
                .filter(session -> isWaiting(session, snapshot))
                .toList();
        final SessionSnapshot result;
        if (waiting.isEmpty()) {
            result = null;
        } else {
            // the list is sorted highest first, so the first waiting session marks the top of the waiting range,
            // protected sessions included: a low priority session below a protected top still fills its gap
            final long top = waiting.getFirst().blockNumber();
            final List<SessionSnapshot> eligible = waiting.stream()
                    .filter(session -> !snapshot.protectedKeys().contains(session.key()))
                    .toList();
            result = firstMatch(
                            eligible,
                            session -> session.priority() == SessionPriority.LOW && session.blockNumber() == top)
                    .or(() -> firstMatch(eligible, session -> session.priority() == SessionPriority.HIGH))
                    .or(() -> firstMatch(eligible, session -> session.priority() == SessionPriority.LOW))
                    .orElse(null);
        }
        return result;
    }

    /// The first session matching the predicate, in list order.
    private static Optional<SessionSnapshot> firstMatch(
            final List<SessionSnapshot> sessions, final Predicate<SessionSnapshot> predicate) {
        return sessions.stream().filter(predicate).findFirst();
    }

    /// A session is waiting when it is, or will be, parked in the ordering
    /// stage waiting for an earlier block.
    private static boolean isWaiting(final SessionSnapshot session, final EvictionSnapshot snapshot) {
        return OrderingRules.mustAwaitOrder(
                session.blockNumber(),
                session.source(),
                snapshot.nextExpectedBlock(),
                snapshot.firstOrderedBlock(),
                snapshot.allSourcesRequireOrdering());
    }
}
