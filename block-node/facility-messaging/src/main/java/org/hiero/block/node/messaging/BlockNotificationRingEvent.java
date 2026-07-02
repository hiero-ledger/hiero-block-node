// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.messaging;

import org.hiero.block.node.spi.blockmessaging.BlockNotification;

/// Simple mutable container for a [BlockNotification]. The notifications ring buffer is made up
/// of these events. These notifications are published to downstream subscribers through the LMAX
/// Disruptor.
public final class BlockNotificationRingEvent {
    /// The notification to be published to downstream subscribers through the LMAX Disruptor.
    private BlockNotification notification;

    /// Sets the given notification to be published to downstream subscribers through the LMAX
    /// Disruptor.
    ///
    /// @param notification to set
    public void set(final BlockNotification notification) {
        this.notification = notification;
    }

    /// Gets the notification of the event from the LMAX Disruptor on the consumer side.
    ///
    /// @return the notification of the event
    public BlockNotification get() {
        return notification;
    }
}
