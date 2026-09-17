// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session.eviction;

import java.util.List;
import org.hiero.block.node.block.verification.session.BlockVerificationSession.SessionKey;

/// A policy that chooses which active verification sessions to evict when
/// the active sessions buffer is over its limit.
///
/// A policy is a pure selector: it receives an immutable [EvictionSnapshot]
/// and returns the keys of the sessions to evict, in eviction order. It has
/// no side effects and never blocks. Removal, cancellation, the safety guards
/// and the metrics are the responsibility of the caller, so every policy is
/// trivially testable by handing it fabricated snapshots.
///
/// Policies run on a messaging handler thread while the activation lock is
/// held, so they must be cheap, ideally linear in the number of sessions.
public interface EvictionPolicy {
    /// Select the sessions to evict.
    ///
    /// The caller evicts the returned keys in order. A policy should return
    /// enough victims to bring the buffer back within its limit, but it may
    /// return fewer (or none) when every remaining session is protected or
    /// is expected to complete on its own.
    ///
    /// @param snapshot the immutable view of the buffer to select from, never null
    /// @return the keys to evict, in order; never null, may be empty, never
    ///     contains a protected key unless the policy documents an exception
    List<SessionKey> selectVictims(EvictionSnapshot snapshot);
}
