// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

/// The priority of a [BlockVerificationSession], derived from the delivery
/// path the block came in on.
///
/// The priority decides which sessions yield first when the active sessions
/// buffer is over its limit. It is deliberately NOT the
/// [org.hiero.block.node.spi.blockmessaging.BlockSource]: the source says who
/// produced the block, the priority says which ring buffer delivered it.
public enum SessionPriority {
    /// Started from the live block items ring, i.e. the publisher stream.
    /// Protected: only evicted when no lower priority session can make room.
    HIGH,
    /// Started from a whole-block delivery, i.e. a backfilled block
    /// notification today and the unvalidated blocks ring in the future.
    /// Evicted first when room must be made.
    LOW
}
