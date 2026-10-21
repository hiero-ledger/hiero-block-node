// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import java.util.List;
import org.hiero.block.node.block.verification.session.BlockVerificationSession.SessionKey;

/// Chooses which active sessions to give up when the active sessions buffer
/// holds more sessions than it may.
///
/// The [ActiveSessionsBuffer] consults the policy at most once per admission, and
/// only when that admission takes the combined count of both lanes over the
/// configured limit. A policy is a pure function of the snapshot it receives: it
/// selects keys and never touches a session. Removal, cancellation and bookkeeping
/// are the buffer's job, which also checks that a selected session is still
/// present and still running before counting an eviction.
@FunctionalInterface
public interface SessionEvictionPolicy {
    /// Select the sessions to evict from the given view of the buffer.
    ///
    /// @param snapshot the view of the buffer at the moment of the decision, never null
    /// @return the keys of the sessions to evict, in the order to evict them; an
    ///     empty list when the policy finds nothing it is willing to evict
    List<SessionKey> selectForEviction(ActiveSessionsSnapshot snapshot);
}
