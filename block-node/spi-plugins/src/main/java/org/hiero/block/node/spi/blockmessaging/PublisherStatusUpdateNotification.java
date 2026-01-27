// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.blockmessaging;

/**
 * A notification indicating a change in publisher status.
 */
public record PublisherStatusUpdateNotification(UpdateType type, int activePublishers) {

    public enum UpdateType {
        PUBLISHER_CONNECTED,
        PUBLISHER_DISCONNECTED,
    }
}
