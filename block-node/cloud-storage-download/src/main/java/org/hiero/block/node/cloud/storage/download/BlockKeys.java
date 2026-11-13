// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.download;

/// Object keys of blocks and markers in the bucket the consensus nodes upload blocks to.
///
/// Format: `{prefix}/{nodeId}/AAAA/BBBB/CCCC/DDDD/EEE.blk.zstd`, where the 19-digit
/// zero-padded block number is split as 4/4/4/4/3 digits. The digit layout must stay
/// identical to the one the expanded cloud storage plugin uploads with.
final class BlockKeys {
    /// Extension of a block object.
    private static final String BLOCK_EXTENSION = ".blk.zstd";
    /// Extension of the marker object that tells a block is complete in the bucket.
    /// Provisional: the marker rule is not confirmed yet.
    private static final String MARKER_EXTENSION = ".sig";

    private BlockKeys() {}

    /// Builds the key of a block object.
    ///
    /// @param prefix the object key prefix, `null` or empty for none
    /// @param nodeId the ID of the consensus node that uploaded the block
    /// @param blockNumber the block number
    /// @return the object key of the block
    static String blockKey(final String prefix, final long nodeId, final long blockNumber) {
        return baseKey(prefix, nodeId, blockNumber) + BLOCK_EXTENSION;
    }

    /// Builds the key of the marker object of a block.
    ///
    /// @param prefix the object key prefix, `null` or empty for none
    /// @param nodeId the ID of the consensus node that uploaded the block
    /// @param blockNumber the block number
    /// @return the object key of the marker
    static String markerKey(final String prefix, final long nodeId, final long blockNumber) {
        return baseKey(prefix, nodeId, blockNumber) + MARKER_EXTENSION;
    }

    private static String baseKey(final String prefix, final long nodeId, final long blockNumber) {
        final long seg1 = blockNumber / 1_000_000_000_000_000L;
        final long seg2 = blockNumber / 100_000_000_000L % 10_000L;
        final long seg3 = blockNumber / 10_000_000L % 10_000L;
        final long seg4 = blockNumber / 1_000L % 10_000L;
        final long seg5 = blockNumber % 1_000L;
        final String path = String.format("%d/%04d/%04d/%04d/%04d/%03d", nodeId, seg1, seg2, seg3, seg4, seg5);
        return (prefix == null || prefix.isEmpty()) ? path : prefix + "/" + path;
    }
}
