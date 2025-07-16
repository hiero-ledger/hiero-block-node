// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.blockmessaging;

/**
 * Interface for handling block notifications.
 */
public interface BlockNotificationHandler {
    /**
     * Handle a block verification notification. Always called on handler thread. Each registered handler will have its
     * own virtual thread.
     *
     * @param notification the block verification notification to handle
     */
    default void handleVerification(VerificationNotification notification) {}

    /**
     * Handle a block persisted notification. Always called on handler thread. Each registered handler will have its
     * own virtual thread.
     *
     * @param notification the block persisted notification to handle
     */
    default void handlePersisted(PersistedNotification notification) {}

    /**
     * Handle a backfilled block notification. Always called on handler thread. Each registered handler will have its
     * own virtual thread.
     *
     * @param notification the backfilled block notification to handle
     */
    default void handleBackfilled(BackfilledBlockNotification notification) {}
}
