// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.messaging;

import org.hiero.block.node.spi.blockmessaging.BlockItems;

/**
 * Simple mutable container for a BlockNotification. The ring buffer is made up of these events.
 */
public class BlockItemBatchRingEvent {
    /** The value to be published to downstream subscribers through the LMAX Disruptor. */
    private BlockItems blockItems;

    /** Constructor for the BlockItemBatchRingEvent class. */
    public BlockItemBatchRingEvent() {}

    /**
     * Sets the given value to be published to downstream subscribers through the LMAX Disruptor.
     * The value must not be null and the method is thread-safe.
     *
     * @param blockItems the value to set
     */
    public void set(final BlockItems blockItems) {
        this.blockItems = blockItems;
    }

    /**
     * Gets the value of the event from the LMAX Disruptor on the consumer side. The method is
     * thread-safe.
     *
     * @return the value of the event
     */
    public BlockItems get() {
        return blockItems;
    }

    /**
     * toString method to provide a string representation of the BlockItemBatchRingEvent for debugging.
     *
     * @return Debug string representation of the BlockItemBatchRingEvent
     */
    @Override
    public String toString() {
        return "BlockItemBatchRingEvent{" + blockItems + '}';
    }
}
