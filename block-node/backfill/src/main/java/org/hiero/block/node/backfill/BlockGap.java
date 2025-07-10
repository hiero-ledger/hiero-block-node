// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

public record BlockGap(long startBlockNumber, long endBlockNumber) {

    /**
     * Constructs a new BlockGap.
     *
     * @param startBlockNumber the starting block number of the gap, includes the first block in the gap
     * @param endBlockNumber   the ending block number of the gap, includes the last block in the gap
     */
    public BlockGap {
        if (startBlockNumber < 0) {
            throw new IllegalArgumentException("Start block number must be non-negative");
        }
        if (endBlockNumber < startBlockNumber) {
            throw new IllegalArgumentException("End block number must be greater than or equal to start block number");
        }
    }
}
