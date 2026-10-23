// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import org.hiero.block.node.block.verification.session.BlockVerificationSession.SessionKey;

/// Chooses the active session to give up when the active sessions buffer holds
/// more sessions than its limit.
///
/// The [ActiveSessionsBuffer] consults the policy at most once per admission, and
/// only when that admission takes the combined count of both lanes over the
/// configured limit; each such admission evicts at most the one session the
/// policy selects. A policy is a pure function of the snapshot it receives: it
/// selects a key and never touches a session. Removal, cancellation and
/// bookkeeping are the buffer's job, which also checks that the selected session
/// is still present and still running before counting an eviction.
@FunctionalInterface
public interface SessionEvictionPolicy {
    /// Select the session to evict from the given view of the buffer.
    ///
    /// @param snapshot the view of the buffer at the moment of the decision, never null
    /// @return the key of the session to evict, or null when the policy finds nothing
    ///     it is willing to evict, in which case the buffer stays over its limit
    SessionKey selectForEviction(ActiveSessionsSnapshot snapshot);
}
