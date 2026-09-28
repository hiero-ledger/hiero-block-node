// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

/// Which delivery path a session was started from. Decides which sessions
/// yield first when the active sessions buffer is over its limit. This is not
/// the [org.hiero.block.node.spi.blockmessaging.BlockSource]: it records the
/// path the block came in on, not who produced the block.
public enum SessionPriority {
    /// Started from the live items ring buffer (publisher stream). Never
    /// evicted while still receiving its block; once complete it yields only
    /// after the low priority sessions at the top of the waiting range.
    HIGH,
    /// Started from a whole block delivered at once (a backfilled block today,
    /// the unvalidated blocks ring buffer tomorrow). Never protected: it yields
    /// first whenever it is at the top of the waiting range.
    LOW
}
