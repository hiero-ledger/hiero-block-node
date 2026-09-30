// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import org.hiero.block.node.block.verification.session.BlockVerificationSession.SessionKey;

/// A scripted eviction policy for handler tests. It records every snapshot it
/// receives and returns whatever the supplied selection function returns.
final class RecordingEvictionPolicy implements SessionEvictionPolicy {
    /// Every snapshot received, in order.
    private final List<ActiveSessionsSnapshot> snapshots;
    /// The selection to return for a snapshot.
    private final Function<ActiveSessionsSnapshot, List<SessionKey>> selection;

    /// Constructor.
    ///
    /// @param selection the selection to return for a snapshot, must not be null
    RecordingEvictionPolicy(final Function<ActiveSessionsSnapshot, List<SessionKey>> selection) {
        this.selection = Objects.requireNonNull(selection);
        this.snapshots = new ArrayList<>();
    }

    /// The naive test policy: the lowest key over both lanes, mirroring the former rule.
    ///
    /// @return a policy selecting the lowest key, or nothing when both lanes are empty
    static RecordingEvictionPolicy lowestKeyFirst() {
        return new RecordingEvictionPolicy(RecordingEvictionPolicy::selectLowestKey);
    }

    /// A policy that always selects the given keys.
    ///
    /// @param keys the keys to select on every call
    /// @return the policy
    static RecordingEvictionPolicy always(final SessionKey... keys) {
        final List<SessionKey> fixed = List.of(keys);
        return new RecordingEvictionPolicy(new Function<>() {
            @Override
            public List<SessionKey> apply(final ActiveSessionsSnapshot snapshot) {
                return fixed;
            }
        });
    }

    /// Every snapshot received so far, in order.
    ///
    /// @return the snapshots
    List<ActiveSessionsSnapshot> snapshots() {
        return snapshots;
    }

    /// {@inheritDoc}
    @Override
    public List<SessionKey> selectForEviction(final ActiveSessionsSnapshot snapshot) {
        snapshots.add(snapshot);
        return selection.apply(snapshot);
    }

    /// Select the lowest key over both lanes.
    private static List<SessionKey> selectLowestKey(final ActiveSessionsSnapshot snapshot) {
        final SessionKey lowestHighPriority = snapshot.highPrioritySessions().isEmpty()
                ? null
                : snapshot.highPrioritySessions().first();
        final SessionKey lowestLowPriority = snapshot.lowPrioritySessions().isEmpty()
                ? null
                : snapshot.lowPrioritySessions().first();
        final List<SessionKey> result;
        if (lowestHighPriority == null && lowestLowPriority == null) {
            result = List.of();
        } else if (lowestHighPriority == null) {
            result = List.of(lowestLowPriority);
        } else if (lowestLowPriority == null) {
            result = List.of(lowestHighPriority);
        } else if (lowestHighPriority.compareTo(lowestLowPriority) <= 0) {
            result = List.of(lowestHighPriority);
        } else {
            result = List.of(lowestLowPriority);
        }
        return result;
    }
}
