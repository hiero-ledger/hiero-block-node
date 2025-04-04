// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.blockmessaging;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Simple record block notifications.
 *
 * @param blockNumber the block number this notification is for
 * @param type the type of notification
 * @param blockHash the hash of the block, if available otherwise null
 */
public record BlockNotification(long blockNumber, @NonNull Type type, Bytes blockHash) {
    /**
     * Enum representing the type of block notification.
     */
    public enum Type {
        BLOCK_VERIFIED,
        BLOCK_FAILED_VERIFICATION,
        BLOCK_PERSISTED
    }
}
