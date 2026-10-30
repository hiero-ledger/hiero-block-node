// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.hiero.block.node.block.verification.session.BlockVerificationSession.SessionKey;

/// A recording wrapper around an eviction policy, for buffer and handler tests.
///
/// It records every snapshot it is handed and every key the wrapped policy
/// selects, and returns that selection unchanged: the tests observe the
/// production policy at work instead of a selection rule of their own, so they
/// cannot drift from what the node does.
final class RecordingEvictionPolicy implements SessionEvictionPolicy {
    /// The policy whose selections are recorded and returned.
    private final SessionEvictionPolicy delegate;
    /// Every snapshot received, in order.
    private final List<ActiveSessionsSnapshot> snapshots;
    /// Every selection returned, in order; null where the wrapped policy selected nothing.
    private final List<SessionKey> selections;

    /// Constructor.
    ///
    /// @param delegate the policy whose selections are recorded, must not be null
    private RecordingEvictionPolicy(final SessionEvictionPolicy delegate) {
        this.delegate = Objects.requireNonNull(delegate);
        this.snapshots = new ArrayList<>();
        this.selections = new ArrayList<>();
    }

    /// Record what the given policy is handed and what it selects.
    ///
    /// @param delegate the policy to wrap, must not be null
    /// @return the recording wrapper
    static RecordingEvictionPolicy recording(final SessionEvictionPolicy delegate) {
        return new RecordingEvictionPolicy(delegate);
    }

    /// Every snapshot received so far, in order.
    ///
    /// @return the snapshots
    List<ActiveSessionsSnapshot> snapshots() {
        return snapshots;
    }

    /// Every selection returned so far, in order; null where the wrapped policy
    /// selected nothing.
    ///
    /// @return the selections
    List<SessionKey> selections() {
        return selections;
    }

    /// {@inheritDoc}
    @Override
    public SessionKey selectForEviction(final ActiveSessionsSnapshot snapshot) {
        snapshots.add(snapshot);
        final SessionKey selected = delegate.selectForEviction(snapshot);
        selections.add(selected);
        return selected;
    }
}
