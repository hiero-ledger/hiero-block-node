// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.events;

import com.lmax.disruptor.EventHandler;

/**
 * Use this interface to combine the contract for handling block node events
 *
 * @param <V> the type of the event value
 */
public interface BlockNodeEventHandler<V> extends EventHandler<V> {

    /**
     * Use this method to check if the underlying event handler is timed out.
     *
     * @return true if the timeout has expired, false otherwise
     */
    default boolean isTimeoutExpired() {
        return false;
    }

    /**
     * Use this method to unsubscribe from the event handler.
     */
    default void unsubscribe() {}
}
