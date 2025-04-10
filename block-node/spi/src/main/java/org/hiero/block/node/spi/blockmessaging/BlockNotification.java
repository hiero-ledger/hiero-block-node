// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.blockmessaging;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Simple record block notifications.
 *
 * @param blockNumber the block number this notification is for
 * @param type        the type of notification
 * @param blockHash   the hash of the block, if the type is BLOCK_VERIFIED
 * @param block       the block, if the type is BLOCK_VERIFIED
 */
public record BlockNotification(
        long blockNumber, @NonNull Type type, Bytes blockHash, org.hiero.hapi.block.node.BlockUnparsed block) {
    /**
     * Enum representing the type of block notification.
     */
    public enum Type {
        BLOCK_VERIFIED,
        BLOCK_FAILED_VERIFICATION,
        BLOCK_PERSISTED
    }

    /**
     * Constructor for BlockNotification. Validates optional parameters based on the type of notification.
     *
     * @param blockNumber the block number this notification is for
     * @param type        the type of notification
     * @param blockHash   the hash of the block, if the type is BLOCK_VERIFIED
     * @param block       the block, if the type is BLOCK_VERIFIED
     */
    public BlockNotification {
        if (type == Type.BLOCK_VERIFIED) {
            if (blockHash == null || block == null) {
                throw new IllegalArgumentException("blockHash and block must be non-null for BLOCK_VERIFIED");
            }
        } else {
            if (blockHash != null || block != null) {
                throw new IllegalArgumentException("blockHash and block must be null for " + type);
            }
        }
    }
}
