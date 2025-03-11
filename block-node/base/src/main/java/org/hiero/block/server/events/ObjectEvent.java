// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.events;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * The ObjectEvent class defines a simple object event used to publish data to downstream
 * subscribers through the LMAX Disruptor RingBuffer.
 *
 * @param <T> the type of the data to publish
 */
public class ObjectEvent<T> {

    /** Constructor for the ObjectEvent class. */
    public ObjectEvent() {}

    private T val;

    /**
     * Sets the given value to be published to downstream subscribers through the LMAX Disruptor.
     * The value must not be null and the method is thread-safe.
     *
     * @param val the value to set
     */
    public void set(@NonNull final T val) {
        this.val = val;
    }

    /**
     * Gets the value of the event from the LMAX Disruptor on the consumer side. The method is
     * thread-safe.
     *
     * @return the value of the event
     */
    @NonNull
    public T get() {
        return val;
    }
}
