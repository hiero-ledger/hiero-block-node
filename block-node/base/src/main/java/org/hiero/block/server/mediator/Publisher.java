// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.mediator;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * The Publisher interface defines the contract for publishing data emitted by the producer to
 * downstream subscribers.
 *
 * @param <U> the type of data to publish
 */
public interface Publisher<U> {

    /**
     * Publishes the given data to the downstream subscribers.
     *
     * @param data the data emitted by an upstream producer to publish to downstream subscribers.
     */
    void publish(@NonNull final U data);
}
