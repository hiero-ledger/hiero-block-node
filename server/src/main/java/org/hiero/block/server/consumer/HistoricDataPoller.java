// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.consumer;

import java.util.Optional;

/**
 * Interface for polling historic data.
 *
 * @param <V> the type of the historic data
 */
public interface HistoricDataPoller<V> {
    /**
     * Initializes the poller with the given block number.
     *
     * @param blockNumber the block number
     */
    void init(long blockNumber);

    /**
     * Polls the historic data.
     *
     * @return the historic data
     * @throws Exception if an error occurs
     */
    Optional<V> poll() throws Exception;
}
