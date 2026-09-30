// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

/// The lane of the active sessions buffer a session belongs to.
///
/// A lane stands for the priority of the channel a session's block came in
/// through. The high priority lane holds the sessions started from the live
/// stream of the publisher; the low priority lane holds the sessions started
/// from every other channel (backfill today). The lane therefore coincides with
/// the block source today. The lanes share one combined limit, and the eviction
/// policy treats them differently: the high priority lane is kept as long as the
/// low priority lane can make room. The lane is a session handler concept only;
/// it is not part of the plugin SPI.
public enum SessionLane {
    /// Sessions kept whenever possible: today the sessions started from the live block items stream of the publisher.
    HIGH_PRIORITY,
    /// Sessions given up first when room is needed: today the sessions started from the backfill notifications.
    LOW_PRIORITY
}
