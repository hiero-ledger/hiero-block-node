// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.mediator;

import java.util.Optional;

/**
 * The Poller interface defines the contract for polling the next event from the stream of events.
 *
 * @param <V> the type of the polled event
 */
public interface Poller<V> {

    /**
     * Polls the next event from the stream of events.
     *
     * @return the next event
     * @throws Exception if an error occurs while polling the event
     */
    Optional<V> poll() throws Exception;

    boolean exceedsThreshold();
}
