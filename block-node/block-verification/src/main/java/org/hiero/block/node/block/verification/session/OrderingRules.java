// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import org.hiero.block.node.block.verification.VerificationConfig;
import org.hiero.block.node.spi.blockmessaging.BlockSource;

/// The ordering rules: whether the successful result of a block has to wait for its
/// turn before it is released. Successes are released strictly in block order, each
/// one right after the last verified block. The [ResultOrderingManager] applies these
/// rules to every successful result, and the [OrderAwareEvictionPolicy] applies them
/// to every session it is handed, so the two always agree on which sessions wait.
///
/// A block waits when all of the following hold:
/// - a block has been verified already. Before the first success, the ordering stage
///   adopts the first block that reaches it as the last verified block, so nothing
///   waits until then;
/// - it is ahead of the next expected block, the one right after the last verified
///   block;
/// - it is at or above [VerificationConfig#firstOrderedBlock()];
/// - its source is subject to ordering: the publisher always is, every other source
///   only when [VerificationConfig#allSourcesRequireOrdering()] is `true`.
///
/// The rules are pure functions of the configuration and their inputs.
public final class OrderingRules {
    /// Not instantiable, see the class documentation.
    private OrderingRules() {}

    /// Whether the successful result of a block from the given source waits for its turn.
    ///
    /// @param verificationConfig the configuration for verification, source of the ordering settings
    /// @param blockNumber the block of the result
    /// @param source the source of the block
    /// @param lastVerifiedBlock the last verified block, negative when no block has been verified yet
    /// @return true when the result has to wait for its turn
    public static boolean waitsForOrder(
            final VerificationConfig verificationConfig,
            final long blockNumber,
            final BlockSource source,
            final long lastVerifiedBlock) {
        return waitsForOrder(
                verificationConfig, blockNumber, requiresOrdering(verificationConfig, source), lastVerifiedBlock);
    }

    /// Whether a session of the given lane waits for its turn once its block verifies.
    /// The high priority lane holds the blocks of the publisher, the low priority lane
    /// those of every other source.
    ///
    /// @param verificationConfig the configuration for verification, source of the ordering settings
    /// @param blockNumber the block of the session
    /// @param lane the lane of the session
    /// @param lastVerifiedBlock the last verified block, negative when no block has been verified yet
    /// @return true when the session's result has to wait for its turn
    public static boolean waitsForOrder(
            final VerificationConfig verificationConfig,
            final long blockNumber,
            final SessionLane lane,
            final long lastVerifiedBlock) {
        return waitsForOrder(
                verificationConfig, blockNumber, requiresOrdering(verificationConfig, lane), lastVerifiedBlock);
    }

    /// Whether the blocks of the given source are subject to ordering: the publisher's
    /// always are, every other source's only when all sources require ordering.
    ///
    /// @param verificationConfig the configuration for verification, source of the ordering settings
    /// @param source the source of the blocks
    /// @return true when the blocks of the source are released in order
    public static boolean requiresOrdering(final VerificationConfig verificationConfig, final BlockSource source) {
        return switch (source) {
            case PUBLISHER -> true;
            case BACKFILL, HISTORY, UNKNOWN -> verificationConfig.allSourcesRequireOrdering();
        };
    }

    /// Whether the sessions of the given lane are subject to ordering: the high priority
    /// lane's always are, the low priority lane's only when all sources require
    /// ordering.
    ///
    /// @param verificationConfig the configuration for verification, source of the ordering settings
    /// @param lane the lane of the sessions
    /// @return true when the sessions of the lane are released in order
    public static boolean requiresOrdering(final VerificationConfig verificationConfig, final SessionLane lane) {
        return switch (lane) {
            case HIGH_PRIORITY -> true;
            case LOW_PRIORITY -> verificationConfig.allSourcesRequireOrdering();
        };
    }

    /// Whether a block waits for its turn, once it is known whether it is subject to
    /// ordering at all.
    ///
    /// @param verificationConfig the configuration for verification, source of the first ordered block
    /// @param blockNumber the block
    /// @param subjectToOrdering whether the block's source or lane is subject to ordering
    /// @param lastVerifiedBlock the last verified block, negative when no block has been verified yet
    /// @return true when the block has to wait for its turn
    private static boolean waitsForOrder(
            final VerificationConfig verificationConfig,
            final long blockNumber,
            final boolean subjectToOrdering,
            final long lastVerifiedBlock) {
        return subjectToOrdering
                && lastVerifiedBlock >= 0
                && blockNumber >= verificationConfig.firstOrderedBlock()
                && blockNumber > lastVerifiedBlock + 1;
    }
}
