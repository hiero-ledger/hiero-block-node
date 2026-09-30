// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import java.util.Collections;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.TreeSet;
import org.hiero.block.node.block.verification.session.BlockVerificationSession.SessionKey;

/// An immutable view of the active sessions buffer, taken at the moment an
/// eviction has to be decided and handed to the [SessionEvictionPolicy].
///
/// Both key sets are defensive, unmodifiable copies in ascending key order. The
/// snapshot carries no session objects, so a policy can be exercised with
/// synthetic keys alone.
///
/// @param highPrioritySessions keys of the sessions in the high priority lane, ascending, never null
/// @param lowPrioritySessions keys of the sessions in the low priority lane, ascending, never null
/// @param activeHighPrioritySession key of the high priority session that is still receiving its
///     block items (the block the publisher is streaming), or null when there is none
/// @param lastVerifiedBlock the last successfully verified block at the time of the
///     snapshot, negative when no block has been verified yet
public record ActiveSessionsSnapshot(
        NavigableSet<SessionKey> highPrioritySessions,
        NavigableSet<SessionKey> lowPrioritySessions,
        SessionKey activeHighPrioritySession,
        long lastVerifiedBlock) {
    /// Copies both key sets so that the snapshot is immutable and independent
    /// of the live lanes.
    public ActiveSessionsSnapshot {
        highPrioritySessions =
                Collections.unmodifiableNavigableSet(new TreeSet<>(Objects.requireNonNull(highPrioritySessions)));
        lowPrioritySessions =
                Collections.unmodifiableNavigableSet(new TreeSet<>(Objects.requireNonNull(lowPrioritySessions)));
    }

    /// The combined number of sessions in both lanes.
    ///
    /// @return the combined count
    public int size() {
        return highPrioritySessions.size() + lowPrioritySessions.size();
    }
}
