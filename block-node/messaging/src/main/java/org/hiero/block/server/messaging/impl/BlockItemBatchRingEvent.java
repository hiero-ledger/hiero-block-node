// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.messaging.impl;

import com.hedera.hapi.block.BlockItemUnparsed;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Simple mutable container for a BlockNotification. The ring buffer is made up of these events.
 */
public class BlockItemBatchRingEvent {
    /** The value to be published to downstream subscribers through the LMAX Disruptor. */
    private List<BlockItemUnparsed> val;

    /** Constructor for the BlockItemBatchRingEvent class. */
    public BlockItemBatchRingEvent() {}

    /**
     * Sets the given value to be published to downstream subscribers through the LMAX Disruptor.
     * The value must not be null and the method is thread-safe.
     *
     * @param val the value to set
     */
    public void set(final List<BlockItemUnparsed> val) {
        this.val = val;
    }

    /**
     * Gets the value of the event from the LMAX Disruptor on the consumer side. The method is
     * thread-safe.
     *
     * @return the value of the event
     */
    public List<BlockItemUnparsed> get() {
        return val;
    }

    /**
     * toString method to provide a string representation of the BlockItemBatchRingEvent for debugging.
     *
     * @return Debug string representation of the BlockItemBatchRingEvent
     */
    @Override
    public String toString() {
        return "BlockItemBatchRingEvent{"
                + (val == null || val.isEmpty()
                        ? "empty"
                        : val.stream().map(BlockItemUnparsed::toString).collect(Collectors.joining(", ")))
                + '}';
    }
}
