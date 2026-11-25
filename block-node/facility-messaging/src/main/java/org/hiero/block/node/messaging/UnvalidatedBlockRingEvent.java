// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.messaging;

import org.hiero.block.node.spi.blockmessaging.BackfilledBlockNotification;

/**
 * Simple mutable container for an unvalidated block: a whole block that is not verified yet and does not come from the
 * live publisher, for now a block fetched by backfill. The unvalidated block ring buffer is made up of these events.
 */
public final class UnvalidatedBlockRingEvent {
    /** The backfilled block notification to be published to downstream subscribers through the LMAX Disruptor. */
    private BackfilledBlockNotification backfilledBlockNotification;

    /**
     * Sets the given notification to be published to downstream subscribers through the LMAX Disruptor.
     *
     * @param notification the notification to set
     */
    public void set(final BackfilledBlockNotification notification) {
        this.backfilledBlockNotification = notification;
    }

    /**
     * Gets the notification of the event from the LMAX Disruptor on the consumer side.
     *
     * @return the notification of the event
     */
    public BackfilledBlockNotification get() {
        return backfilledBlockNotification;
    }
}
