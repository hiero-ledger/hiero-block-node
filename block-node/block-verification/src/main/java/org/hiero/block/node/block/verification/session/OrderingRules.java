// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import org.hiero.block.node.spi.blockmessaging.BlockSource;

/// The rules that decide whether a successfully verified block must await
/// its turn before its result is propagated.
///
/// These rules are shared between the ordering stage
/// ([ResultOrderingManager]), which applies them to a result that is ready,
/// and the eviction policy, which applies them to every active session in
/// order to predict which sessions are, or will be, parked waiting for an
/// earlier block. Keeping a single definition guarantees the two can never
/// disagree.
public final class OrderingRules {
    /// Private constructor to prevent instantiation.
    private OrderingRules() {}

    /// Decide whether a block from the given source is subject to strict
    /// ordering at all.
    ///
    /// @param blockNumber the number of the block
    /// @param source the source of the block
    /// @param firstOrderedBlock the first block number that requires strict ordering
    /// @param allSourcesRequireOrdering whether sources other than the publisher are ordered
    /// @return `true` if the block must be released strictly in order
    public static boolean requiresOrdering(
            final long blockNumber,
            final BlockSource source,
            final long firstOrderedBlock,
            final boolean allSourcesRequireOrdering) {
        return blockNumber >= firstOrderedBlock && (source == BlockSource.PUBLISHER || allSourcesRequireOrdering);
    }

    /// Decide whether a block must wait for an earlier block before its
    /// result can be released.
    ///
    /// @param blockNumber the number of the block
    /// @param source the source of the block
    /// @param nextExpectedBlock the next block number expected to verify in order
    /// @param firstOrderedBlock the first block number that requires strict ordering
    /// @param allSourcesRequireOrdering whether sources other than the publisher are ordered
    /// @return `true` if the block is subject to ordering and is ahead of the next expected block
    public static boolean mustAwaitOrder(
            final long blockNumber,
            final BlockSource source,
            final long nextExpectedBlock,
            final long firstOrderedBlock,
            final boolean allSourcesRequireOrdering) {
        return requiresOrdering(blockNumber, source, firstOrderedBlock, allSourcesRequireOrdering)
                && blockNumber > nextExpectedBlock;
    }
}
