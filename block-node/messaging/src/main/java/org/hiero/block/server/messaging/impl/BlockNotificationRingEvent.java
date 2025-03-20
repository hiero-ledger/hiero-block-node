// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.messaging.impl;

import org.hiero.block.server.plugins.blockmessaging.BlockNotification;

/**
 * Simple mutable container for batch of block items. The ring buffer is made up of these events.
 */
public class BlockNotificationRingEvent {
    /** The value to be published to downstream subscribers through the LMAX Disruptor. */
    private BlockNotification val;

    /** Constructor for the BlockNotificationRingEvent class. */
    public BlockNotificationRingEvent() {}

    /**
     * Sets the given value to be published to downstream subscribers through the LMAX Disruptor.
     * The value must not be null and the method is thread-safe.
     *
     * @param val the value to set
     */
    public void set(final BlockNotification val) {
        this.val = val;
    }

    /**
     * Gets the value of the event from the LMAX Disruptor on the consumer side. The method is
     * thread-safe.
     *
     * @return the value of the event
     */
    public BlockNotification get() {
        return val;
    }
}
