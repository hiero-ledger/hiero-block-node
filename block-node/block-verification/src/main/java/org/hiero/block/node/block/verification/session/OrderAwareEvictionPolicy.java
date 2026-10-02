// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.OptionalLong;
import org.hiero.block.node.block.verification.VerificationConfig;
import org.hiero.block.node.block.verification.session.BlockVerificationSession.SessionKey;

/// The eviction policy the session handler applies in production.
///
/// The policy protects two things: the sessions the ordered stream of successes
/// depends on, and the high priority session that is still receiving its block
/// items (the block the publisher is currently streaming). A session "awaits
/// order" when it would park at the ordering stage given its block number, its
/// lane and the last verified block, that is when its block is more than one
/// ahead of the last verified block, at or above the first ordered block, and
/// its lane is subject to ordering (the high priority lane always is, because it
/// carries the publisher's blocks, which the ordering stage always orders; the
/// low priority lane only when all sources require ordering). A block is
/// "needed" when some session awaits order above it: every block between the
/// last verified block and the highest awaiting block is needed.
///
/// The selection, in order:
/// 1. In the low priority lane, the lowest session when it is stale (its block
///    is at or below the last verified block); otherwise the highest session
///    when nobody needs it (it is the highest awaiting session, or nothing
///    awaits order at all).
/// 2. In the high priority lane, the highest session other than the one that is
///    still receiving its block items. Blocks in this lane arrive in ascending
///    order from the publisher, so the highest one is the farthest from being
///    released and its resend costs the publisher the smallest rewind; the
///    session still being fed is never selected because the publisher does not
///    resend a block that ends incomplete.
/// 3. Nothing.
///
/// The just admitted session is not exempt: when it is the highest awaiting
/// session or a stale one, it is exactly the session nobody needs. A session
/// nobody needs can only be found in the low priority lane or as the highest
/// complete high priority session, so with any session besides the one being
/// fed the policy always selects one, and the buffer never grows without bound
/// for any delivery order.
public final class OrderAwareEvictionPolicy implements SessionEvictionPolicy {
    /// The configuration for verification, source of the ordering settings.
    private final VerificationConfig verificationConfig;

    /// Constructor.
    ///
    /// @param verificationConfig the configuration for verification, must not be null
    public OrderAwareEvictionPolicy(final VerificationConfig verificationConfig) {
        this.verificationConfig = Objects.requireNonNull(verificationConfig);
    }

    /// {@inheritDoc}
    /// ---
    /// Selects at most one session per call, see the class documentation for
    /// the order of preference.
    @Override
    public List<SessionKey> selectForEviction(final ActiveSessionsSnapshot snapshot) {
        Objects.requireNonNull(snapshot);
        final OptionalLong highestAwaitingOrder = highestAwaitingOrder(snapshot);
        final SessionKey fromLowPriorityLane = selectFromLowPriorityLane(snapshot, highestAwaitingOrder);
        final List<SessionKey> result;
        if (fromLowPriorityLane != null) {
            result = List.of(fromLowPriorityLane);
        } else {
            final SessionKey fromHighPriorityLane = selectFromHighPriorityLane(snapshot);
            if (fromHighPriorityLane != null) {
                result = List.of(fromHighPriorityLane);
            } else {
                result = List.of();
            }
        }
        return result;
    }

    /// Find the highest block among the sessions of both lanes that await
    /// order, ignoring the high priority session that is still receiving its
    /// items.
    ///
    /// @param snapshot the view of the buffer
    /// @return the highest awaiting block, empty when no session awaits order
    private OptionalLong highestAwaitingOrder(final ActiveSessionsSnapshot snapshot) {
        final long lastVerified = snapshot.lastVerifiedBlock();
        final OptionalLong inHighPriorityLane = highestAwaitingOrder(
                snapshot.highPrioritySessions(), snapshot.activeHighPrioritySession(), lastVerified, true);
        final OptionalLong inLowPriorityLane = highestAwaitingOrder(
                snapshot.lowPrioritySessions(), null, lastVerified, verificationConfig.allSourcesRequireOrdering());
        final OptionalLong result;
        if (inHighPriorityLane.isPresent() && inLowPriorityLane.isPresent()) {
            result = OptionalLong.of(Math.max(inHighPriorityLane.getAsLong(), inLowPriorityLane.getAsLong()));
        } else if (inHighPriorityLane.isPresent()) {
            result = inHighPriorityLane;
        } else {
            result = inLowPriorityLane;
        }
        return result;
    }

    /// Find the highest block among the sessions of one lane that await order.
    ///
    /// @param lane the keys of the lane, ascending
    /// @param excluded a key to ignore, may be null
    /// @param lastVerified the last verified block
    /// @param laneAwaitsOrder whether sessions of this lane are subject to ordering at all
    /// @return the highest awaiting block of the lane, empty when there is none
    private OptionalLong highestAwaitingOrder(
            final NavigableSet<SessionKey> lane,
            final SessionKey excluded,
            final long lastVerified,
            final boolean laneAwaitsOrder) {
        OptionalLong result = OptionalLong.empty();
        if (laneAwaitsOrder) {
            final Iterator<SessionKey> descending = lane.descendingIterator();
            while (result.isEmpty() && descending.hasNext()) {
                final SessionKey key = descending.next();
                if (!key.equals(excluded) && awaitsOrder(key.blockNumber(), lastVerified)) {
                    result = OptionalLong.of(key.blockNumber());
                }
            }
        }
        return result;
    }

    /// Decide whether a session for the given block would park at the ordering
    /// stage, mirroring the condition of the ordering stage: more than one ahead
    /// of the last verified block and at or above the first ordered block.
    ///
    /// @param blockNumber the block of the session
    /// @param lastVerified the last verified block
    /// @return true when the session would await order
    private boolean awaitsOrder(final long blockNumber, final long lastVerified) {
        return blockNumber > lastVerified + 1 && blockNumber >= verificationConfig.firstOrderedBlock();
    }

    /// Decide whether some awaiting session depends on the given block.
    ///
    /// @param blockNumber the block to test
    /// @param lastVerified the last verified block
    /// @param highestAwaitingOrder the highest awaiting block, empty when none awaits
    /// @return true when the block lies strictly between the last verified block and the
    ///     highest awaiting block
    private static boolean isNeeded(
            final long blockNumber, final long lastVerified, final OptionalLong highestAwaitingOrder) {
        return highestAwaitingOrder.isPresent()
                && lastVerified < blockNumber
                && blockNumber < highestAwaitingOrder.getAsLong();
    }

    /// Step 1: select from the low priority lane.
    ///
    /// @param snapshot the view of the buffer
    /// @param highestAwaitingOrder the highest awaiting block over both lanes
    /// @return the lowest session when it is stale, else the highest session when nobody
    ///     needs it, else null
    private static SessionKey selectFromLowPriorityLane(
            final ActiveSessionsSnapshot snapshot, final OptionalLong highestAwaitingOrder) {
        final NavigableSet<SessionKey> lane = snapshot.lowPrioritySessions();
        final long lastVerified = snapshot.lastVerifiedBlock();
        final SessionKey result;
        if (lane.isEmpty()) {
            result = null;
        } else if (lane.first().blockNumber() <= lastVerified) {
            result = lane.first();
        } else if (!isNeeded(lane.last().blockNumber(), lastVerified, highestAwaitingOrder)) {
            result = lane.last();
        } else {
            result = null;
        }
        return result;
    }

    /// Step 2: select from the high priority lane.
    ///
    /// @param snapshot the view of the buffer
    /// @return the highest high priority session other than the one still receiving items,
    ///     or null when there is none
    private static SessionKey selectFromHighPriorityLane(final ActiveSessionsSnapshot snapshot) {
        SessionKey result = null;
        final Iterator<SessionKey> descending = snapshot.highPrioritySessions().descendingIterator();
        while (result == null && descending.hasNext()) {
            final SessionKey key = descending.next();
            if (!key.equals(snapshot.activeHighPrioritySession())) {
                result = key;
            }
        }
        return result;
    }
}
