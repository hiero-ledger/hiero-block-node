// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.blockmessaging;

/**
 * A notification indicating a change in publisher status.
 */
public record PublisherStatusUpdateNotification(UpdateType type, int activePublishers) {
    /**
     * An enum representing the type of publisher status update.
     */
    public enum UpdateType {
        /**
         * This type indicates that a publisher has connected.
         */
        PUBLISHER_CONNECTED,
        /**
         * This type indicates that a publisher has disconnected.
         */
        PUBLISHER_DISCONNECTED,
        /**
         * This type indicates that a publisher unavailability timeout has
         * occurred. This happens when no publishers are connected and active
         * for a configured period of time.
         */
        PUBLISHER_UNAVAILABILITY_TIMEOUT,
    }
}
