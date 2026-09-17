// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session.eviction;

import java.util.Objects;
import org.hiero.block.node.block.verification.session.BlockVerificationSession.SessionKey;
import org.hiero.block.node.block.verification.session.SessionPriority;
import org.hiero.block.node.spi.blockmessaging.BlockSource;

/// An immutable view of one active verification session, safe to hand to an
/// [EvictionPolicy].
///
/// @param key the composite key of the session
/// @param priority the priority of the session
/// @param source the source of the block the session verifies
/// @param endOfBlockReceived whether the batch ending the block has been received
public record SessionSnapshot(
        SessionKey key, SessionPriority priority, BlockSource source, boolean endOfBlockReceived) {
    /// Compact constructor, validates the reference components.
    public SessionSnapshot {
        Objects.requireNonNull(key);
        Objects.requireNonNull(priority);
        Objects.requireNonNull(source);
    }

    /// The number of the block the session verifies.
    /// @return the block number
    public long blockNumber() {
        return key.blockNumber();
    }
}
