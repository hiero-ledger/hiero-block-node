// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.messaging;

import org.hiero.block.node.spi.blockmessaging.UnvalidatedBlockNotification;

/**
 * Simple mutable container for an unvalidated block: a whole block that is not verified yet and does not come from the
 * live publisher. The unvalidated block ring buffer is made up of these events.
 */
public final class UnvalidatedBlockRingEvent {
    /** The unvalidated block notification to be published to downstream subscribers through the LMAX Disruptor. */
    private UnvalidatedBlockNotification notification;

    /**
     * Sets the given notification to be published to downstream subscribers through the LMAX Disruptor.
     *
     * @param notification the notification to set
     */
    public void set(final UnvalidatedBlockNotification notification) {
        this.notification = notification;
    }

    /**
     * Gets the notification of the event from the LMAX Disruptor on the consumer side.
     *
     * @return the notification of the event
     */
    public UnvalidatedBlockNotification get() {
        return notification;
    }
}
