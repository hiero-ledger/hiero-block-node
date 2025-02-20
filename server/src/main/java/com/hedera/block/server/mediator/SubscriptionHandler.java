// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.mediator;

import com.hedera.block.server.events.BlockNodeEventHandler;
import com.hedera.block.server.events.ObjectEvent;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * The SubscriptionHandler interface defines the contract for subscribing and unsubscribing
 * downstream consumers to the stream of events.
 *
 * @param <V> the type of the subscription events
 */
public interface SubscriptionHandler<V> {

    /**
     * Subscribes the given handler to the stream of events.
     *
     * @param handler the handler to subscribe
     */
    void subscribe(@NonNull final BlockNodeEventHandler<ObjectEvent<V>> handler);

    /**
     * Subscribes the given handler to the stream of events and returns a poller to poll the events.
     *
     * @param handler the handler to subscribe
     * @return the poller to poll the events
     */
    Poller<ObjectEvent<V>> subscribePoller(@NonNull final BlockNodeEventHandler<ObjectEvent<V>> handler);

    /**
     * Unsubscribes the given handler from the stream of events.
     *
     * @param handler the handler to unsubscribe
     */
    void unsubscribe(@NonNull final BlockNodeEventHandler<ObjectEvent<V>> handler);

    /**
     * Checks if the given handler is subscribed to the stream of events.
     *
     * @param handler the handler to check
     * @return true if the handler is subscribed, false otherwise
     */
    boolean isSubscribed(@NonNull final BlockNodeEventHandler<ObjectEvent<V>> handler);

    /** Unsubscribes all the expired handlers from the stream of events. */
    void unsubscribeAllExpired();
}
