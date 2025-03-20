// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.plugins.blockmessaging;

/**
 * Simple record block notifications.
 *
 * @param blockNumber the block number this notification is for
 * @param type the type of notification
 */
public record BlockNotification(long blockNumber, Type type) {
    /**
     * Enum representing the type of block notification.
     */
    public enum Type {
        BLOCK_VERIFIED,
        BLOCK_FAILED_VERIFICATION,
        BLOCK_PERSISTED
    }
}
