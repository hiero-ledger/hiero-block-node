// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

/// The lane of the active sessions buffer a session belongs to.
///
/// A lane stands for the priority of the channel a session's block came in
/// through. The high priority lane holds the sessions started from the live
/// stream of the publisher; the low priority lane holds the sessions started
/// from every other channel (backfill today). The lane therefore coincides with
/// the block source today. The lanes share one combined limit. The eviction
/// policy favours the high priority lane in three ways: the publisher session
/// still receiving its block is never given up; when sessions of both lanes hold
/// the highest stuck block, the low priority one goes first; and when no session
/// is stuck, no high priority session goes while the low priority lane holds a
/// session. The lane is an active sessions buffer concept only; it is not part
/// of the plugin SPI.
public enum SessionLane {
    /// Sessions started from the live block items stream of the publisher; kept
    /// on a tie with the low priority lane.
    HIGH_PRIORITY,
    /// Sessions started from every other channel, today the backfill
    /// notifications; given up first on a tie with the high priority lane.
    LOW_PRIORITY
}
