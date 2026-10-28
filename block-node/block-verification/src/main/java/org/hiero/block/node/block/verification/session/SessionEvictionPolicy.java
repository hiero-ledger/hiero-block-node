// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import org.hiero.block.node.block.verification.session.BlockVerificationSession.SessionKey;

/// Chooses the active session to give up when the active sessions buffer holds
/// more sessions than its limit.
///
/// The [ActiveSessionsBuffer] consults the policy at most once per admission, and
/// only when that admission takes the combined count of both lanes over the
/// configured limit; each such admission evicts the one session the policy
/// selects. The buffer must not stay over its limit, so a policy selects a
/// session whenever the snapshot holds one it may give up: any session but the
/// active publisher session, which is still receiving its block. A policy is a
/// pure function of the snapshot it receives: it selects a key and never touches
/// a session. Removal, cancellation and bookkeeping are the buffer's job, which
/// also checks that the selected session is still present and still running
/// before counting an eviction.
@FunctionalInterface
public interface SessionEvictionPolicy {
    /// Select the session to evict from the given view of the buffer.
    ///
    /// @param snapshot the view of the buffer at the moment of the decision, never null
    /// @return the key of the session to evict, never the active publisher session; null only when the
    ///     snapshot holds no other session
    SessionKey selectForEviction(ActiveSessionsSnapshot snapshot);
}
