// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session.eviction;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
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
/// parked in the ordering stage waiting for a block that has not arrived.
/// Those are the "stuck" sessions.
///
/// Among stuck sessions, victims are chosen in tiers, always the highest
/// block number first, because the highest stuck block is the one furthest
/// from being releasable and the one the fewest other sessions depend on:
///
/// 1. [SessionPriority#LOW] sessions that are not filling a gap some other
///    stuck session waits for.
/// 2. [SessionPriority#HIGH] sessions.
/// 3. [SessionPriority#LOW] sessions that do fill such a gap.
///
/// Protected keys are never selected, with one exception: a low priority
/// session so far ahead that it cannot be released before the whole buffer
/// turns over (`block > nextExpected + limit`) is treated as junk and evicts
/// itself instead of forcing a higher priority eviction. The exception is
/// disabled while the last verified block is unknown, so a node starting
/// mid chain can seed it with its first success.
///
/// Selection stops as soon as the buffer is back within its limit. When only
/// non-stuck or protected sessions remain, fewer victims than needed are
/// returned and the caller tolerates a transient overshoot.
public final class GapAwareEvictionPolicy implements EvictionPolicy {
    /// Sentinel for "no stuck session present"; valid block numbers are never negative.
    private static final long NO_STUCK_BLOCK = -1L;
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
            final SessionSnapshot victim = selectNextVictim(remaining, snapshot);
            if (victim == null) {
                searching = false;
            } else {
                victims.add(victim.key());
                remaining.remove(victim);
            }
        }
        return victims;
    }

    /// Select the next victim among the remaining sessions, or `null` if no
    /// session is eligible.
    ///
    /// @param remaining the sessions not yet selected, sorted highest block first
    /// @param snapshot the snapshot the selection runs against
    /// @return the next victim, or `null` when only non-stuck or protected sessions remain
    private SessionSnapshot selectNextVictim(final List<SessionSnapshot> remaining, final EvictionSnapshot snapshot) {
        final long nextExpected = snapshot.nextExpectedBlock();
        final long maxStuckBlock = maxStuckBlock(remaining, snapshot, nextExpected);
        SessionSnapshot tierOne = null;
        SessionSnapshot tierTwo = null;
        SessionSnapshot tierThree = null;
        for (final SessionSnapshot candidate : remaining) {
            if (isEligible(candidate, snapshot, nextExpected)) {
                final boolean needed = isNeeded(candidate, nextExpected, maxStuckBlock);
                final int tier =
                        switch (candidate.priority()) {
                            case LOW -> needed ? 3 : 1;
                            case HIGH -> 2;
                        };
                if (tier == 1 && tierOne == null) {
                    tierOne = candidate;
                } else if (tier == 2 && tierTwo == null) {
                    tierTwo = candidate;
                } else if (tier == 3 && tierThree == null) {
                    tierThree = candidate;
                }
            }
        }
        final SessionSnapshot victim;
        if (tierOne != null) {
            victim = tierOne;
        } else if (tierTwo != null) {
            victim = tierTwo;
        } else {
            victim = tierThree;
        }
        return victim;
    }

    /// A session is eligible for eviction when it is stuck and either not
    /// protected or so far ahead that protection does not apply.
    private boolean isEligible(
            final SessionSnapshot candidate, final EvictionSnapshot snapshot, final long nextExpected) {
        final boolean result;
        if (isStuck(candidate, snapshot, nextExpected)) {
            final boolean isProtected = snapshot.protectedKeys().contains(candidate.key());
            result = !isProtected || isOutsideWindow(candidate, snapshot, nextExpected);
        } else {
            result = false;
        }
        return result;
    }

    /// A session is stuck when it is, or will be, parked in the ordering
    /// stage waiting for an earlier block.
    private boolean isStuck(final SessionSnapshot candidate, final EvictionSnapshot snapshot, final long nextExpected) {
        return OrderingRules.mustAwaitOrder(
                candidate.blockNumber(),
                candidate.source(),
                nextExpected,
                snapshot.firstOrderedBlock(),
                snapshot.allSourcesRequireOrdering());
    }

    /// A low priority session is outside the window when the last verified
    /// block is known and the session's block cannot be released before the
    /// whole buffer has turned over at least once.
    private boolean isOutsideWindow(
            final SessionSnapshot candidate, final EvictionSnapshot snapshot, final long nextExpected) {
        return candidate.priority() == SessionPriority.LOW
                && snapshot.lastVerifiedBlock() >= 0
                && candidate.blockNumber() > nextExpected + snapshot.limit();
    }

    /// A session is needed when some stuck session with a higher block
    /// depends on it, i.e. its block lies in the gap between the next
    /// expected block and the highest stuck block.
    private boolean isNeeded(final SessionSnapshot candidate, final long nextExpected, final long maxStuckBlock) {
        return maxStuckBlock != NO_STUCK_BLOCK
                && candidate.blockNumber() >= nextExpected
                && candidate.blockNumber() < maxStuckBlock;
    }

    /// The highest block among the remaining stuck sessions.
    ///
    /// @return the block number, or [#NO_STUCK_BLOCK] when no remaining session is stuck
    private long maxStuckBlock(
            final List<SessionSnapshot> remaining, final EvictionSnapshot snapshot, final long nextExpected) {
        long result = NO_STUCK_BLOCK;
        for (final SessionSnapshot candidate : remaining) {
            if (result == NO_STUCK_BLOCK && isStuck(candidate, snapshot, nextExpected)) {
                // remaining is sorted highest first, so the first stuck session is the maximum
                result = candidate.blockNumber();
            }
        }
        return result;
    }
}
