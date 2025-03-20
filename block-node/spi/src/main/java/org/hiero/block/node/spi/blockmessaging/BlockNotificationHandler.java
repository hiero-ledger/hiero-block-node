// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.blockmessaging;

/**
 * Interface for handling block notifications.
 */
public interface BlockNotificationHandler {
    /**
     * Handle a block notification. Always called on handler thread. Each registered handler will have its own virtual
     * thread.
     *
     * @param notification the block notification to handle
     */
    void handleBlockNotification(BlockNotification notification);
}
