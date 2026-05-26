// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.blockmessaging;

import com.hedera.pbj.runtime.io.buffer.Bytes;

/// Emitted by the live-state plugin when its in-memory state advances.
///
/// Two transitions emit this notification:
///
///  - `VERIFIED` — a verified block was applied to the mutable state and
///    promoted to the latest immutable copy. Other plugins MAY now safely
///    issue binary state queries against the new metadata.
///  - `SNAPSHOT` — a durable snapshot of the latest immutable state was
///    written to disk. Consumers MAY use this as a checkpoint signal.
///
/// `roundNumber` SHALL be sourced from the `RoundHeader` block item of the
/// applied block — never from approximation.
public record StateUpdateNotification(
        StateUpdateType type, long blockNumber, long roundNumber, Bytes stateRootHash, long stateSize) {

    public enum StateUpdateType {
        /// A verified block was applied and promoted to the latest immutable state.
        VERIFIED,
        /// A snapshot of the latest immutable state was written to disk.
        SNAPSHOT
    }
}