// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import java.util.List;
import org.hiero.block.node.block.verification.session.BlockVerificationSession.SessionKey;

/// Chooses which active sessions to give up when the active sessions buffer
/// holds more sessions than it may.
///
/// The session handler consults the policy only when the combined count of both
/// lanes exceeds the configured limit, and consults it again while the count is
/// still over the limit after the selected sessions were evicted. A policy is a
/// pure function of the snapshot it receives: it selects keys and never touches
/// a session. Removal, cancellation and bookkeeping are the handler's job, which
/// also checks that a selected session is still present and still running
/// before counting an eviction.
@FunctionalInterface
public interface SessionEvictionPolicy {
    /// Select the sessions to evict from the given view of the buffer.
    ///
    /// @param snapshot the view of the buffer at the moment of the decision, never null
    /// @return the keys of the sessions to evict, in the order to evict them; an
    ///     empty list when the policy finds nothing it is willing to evict
    List<SessionKey> selectForEviction(ActiveSessionsSnapshot snapshot);
}
