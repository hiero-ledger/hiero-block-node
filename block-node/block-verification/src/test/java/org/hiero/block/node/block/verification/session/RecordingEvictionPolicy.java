// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import org.hiero.block.node.block.verification.session.BlockVerificationSession.SessionKey;

/// A recording wrapper around an eviction policy, for buffer and handler tests.
///
/// It records every snapshot it is handed and every key the wrapped policy
/// selects, and returns that selection unchanged: the tests observe the
/// production policy at work instead of a selection rule of their own, so they
/// cannot drift from what the node does. An optional hook receives each selection
/// before the buffer evicts it, so a test can reproduce what the other ingress
/// thread may do in that window, such as clearing the selected session once it
/// has completed.
final class RecordingEvictionPolicy implements SessionEvictionPolicy {
    /// The policy whose selections are recorded and returned.
    private final SessionEvictionPolicy delegate;
    /// Receives each selection, null included, before it is returned to the buffer.
    private final Consumer<SessionKey> afterSelection;
    /// Every snapshot received, in order.
    private final List<ActiveSessionsSnapshot> snapshots;
    /// Every selection returned, in order; null where the wrapped policy selected nothing.
    private final List<SessionKey> selections;

    /// Constructor.
    ///
    /// @param delegate the policy whose selections are recorded, must not be null
    /// @param afterSelection receives each selection before it is returned, must not be null
    private RecordingEvictionPolicy(final SessionEvictionPolicy delegate, final Consumer<SessionKey> afterSelection) {
        this.delegate = Objects.requireNonNull(delegate);
        this.afterSelection = Objects.requireNonNull(afterSelection);
        this.snapshots = new ArrayList<>();
        this.selections = new ArrayList<>();
    }

    /// Record what the given policy is handed and what it selects.
    ///
    /// @param delegate the policy to wrap, must not be null
    /// @return the recording wrapper
    static RecordingEvictionPolicy recording(final SessionEvictionPolicy delegate) {
        return new RecordingEvictionPolicy(delegate, RecordingEvictionPolicy::ignoreSelection);
    }

    /// Record what the given policy is handed and what it selects, and hand each
    /// selection to the given hook before the buffer evicts it.
    ///
    /// @param delegate the policy to wrap, must not be null
    /// @param afterSelection receives each selection, null included, must not be null
    /// @return the recording wrapper
    static RecordingEvictionPolicy recording(
            final SessionEvictionPolicy delegate, final Consumer<SessionKey> afterSelection) {
        return new RecordingEvictionPolicy(delegate, afterSelection);
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
        afterSelection.accept(selected);
        return selected;
    }

    /// The hook of a plain recording wrapper, which only records.
    ///
    /// @param selected the selection, ignored
    private static void ignoreSelection(final SessionKey selected) {
        // a plain recording wrapper has nothing to do with the selection
    }
}
