// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.blockmessaging;

public record PersistedNotification(long startBlockNumber, long endBlockNumber, int blockProviderPriority) {
    /**
     * Constructor for PersistedNotification. Validates the start and end block numbers.
     *
     * @param startBlockNumber the starting block number
     * @param endBlockNumber   the ending block number
     */
    public PersistedNotification {
        if (startBlockNumber > endBlockNumber) {
            throw new IllegalArgumentException("startBlockNumber must be less than or equal to endBlockNumber");
        }
    }
}
