// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session.eviction;

import java.util.List;
import java.util.Set;
import org.hiero.block.common.utils.Preconditions;
import org.hiero.block.node.block.verification.session.BlockVerificationSession.SessionKey;

/// Everything an [EvictionPolicy] may look at when choosing victims: an
/// immutable view of the active sessions buffer together with the ordering
/// state and settings.
///
/// @param sessions all active sessions, sorted by [SessionKey]
/// @param lastVerifiedBlock the last successfully verified block, `-1` when not yet known
/// @param firstOrderedBlock the first block number that requires strict ordering
/// @param allSourcesRequireOrdering whether sources other than the publisher are ordered
/// @param limit the maximum number of sessions the buffer may hold
/// @param protectedKeys keys of sessions a policy must not select, e.g. the
///     session that was just activated
public record EvictionSnapshot(
        List<SessionSnapshot> sessions,
        long lastVerifiedBlock,
        long firstOrderedBlock,
        boolean allSourcesRequireOrdering,
        int limit,
        Set<SessionKey> protectedKeys) {
    /// Compact constructor, defensively copies the collections and validates the limit.
    public EvictionSnapshot {
        sessions = List.copyOf(sessions);
        protectedKeys = Set.copyOf(protectedKeys);
        Preconditions.requirePositive(limit, "The active sessions buffer limit must be positive");
    }

    /// The next block number expected to verify in order.
    /// @return the last verified block plus one
    public long nextExpectedBlock() {
        return lastVerifiedBlock + 1;
    }
}
