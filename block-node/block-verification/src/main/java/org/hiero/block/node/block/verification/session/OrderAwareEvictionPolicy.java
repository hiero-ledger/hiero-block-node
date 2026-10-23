// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import java.util.HashSet;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import org.hiero.block.node.block.verification.VerificationConfig;
import org.hiero.block.node.block.verification.session.BlockVerificationSession.SessionKey;

/// The eviction policy the active sessions buffer applies in production: it
/// evicts only a session that is stuck, never a session that completes on its
/// own.
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
/// The policy selects the stuck session with the highest block. Successes are
/// released in order, so evicting a stuck session never holds up a session that
/// could otherwise be released: everything below the first gap is unaffected,
/// and everything above it is stuck already. The highest one leaves no other
/// session behind it but, possibly, the active publisher session, and for the
/// publisher it is the newest block, the smallest rewind for a resend. The high
/// priority session still receiving its block items is never selected, because
/// the publisher does not resend a block that ends incomplete. When sessions of
/// both lanes hold the highest stuck block, the low priority one is selected.
/// The session just admitted is treated like any other: the next expected block
/// is finishing and always stays, while a block that arrives stuck far ahead of
/// the rest is selected on arrival.
///
/// When no session is stuck, the policy selects nothing: every session completes
/// on its own, and the buffer stays over its limit until they do.
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
    /// Selects the stuck session with the highest block, or nothing when no
    /// session is stuck, see the class documentation.
    @Override
    public SessionKey selectForEviction(final ActiveSessionsSnapshot snapshot) {
        final long firstGap = firstGap(snapshot);
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
    /// @param snapshot the view of the buffer
    /// @return the first gap
    private static long firstGap(final ActiveSessionsSnapshot snapshot) {
        final Set<Long> heldBlocks = new HashSet<>();
        for (final SessionKey key : snapshot.highPrioritySessions()) {
            heldBlocks.add(key.blockNumber());
        }
        for (final SessionKey key : snapshot.lowPrioritySessions()) {
            heldBlocks.add(key.blockNumber());
        }
        long firstGap = snapshot.lastVerifiedBlock() + 1;
        while (heldBlocks.contains(firstGap)) {
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
}
