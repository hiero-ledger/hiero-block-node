// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.blockmessaging;

import com.hedera.pbj.runtime.io.buffer.Bytes;

/// Notification that a block was persisted, or failed to persist, by a block provider.
///
/// @param blockNumber the block number this notification is for
/// @param succeeded `true` if the block was persisted successfully
/// @param blockProviderPriority the priority of the provider that persisted the block
/// @param blockSource the source of the block
/// @param blockRootHash the verified root hash of the block, or [Bytes#EMPTY] when the provider does not
///     have it (for example when the block is persisted by a provider that never saw its verification);
///     a `null` is treated as [Bytes#EMPTY]
public record PersistedNotification(
        long blockNumber, boolean succeeded, int blockProviderPriority, BlockSource blockSource, Bytes blockRootHash) {

    public PersistedNotification {
        blockRootHash = blockRootHash == null ? Bytes.EMPTY : blockRootHash;
    }

    /// Creates a notification without a block root hash.
    public PersistedNotification(
            final long blockNumber,
            final boolean succeeded,
            final int blockProviderPriority,
            final BlockSource blockSource) {
        this(blockNumber, succeeded, blockProviderPriority, blockSource, Bytes.EMPTY);
    }
}
