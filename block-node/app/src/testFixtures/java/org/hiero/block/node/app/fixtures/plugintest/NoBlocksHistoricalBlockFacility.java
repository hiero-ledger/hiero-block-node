// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.plugintest;

import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;

/**
 * A {@link HistoricalBlockFacility} that does not provide any blocks, for testing.
 */
public class NoBlocksHistoricalBlockFacility implements HistoricalBlockFacility {
    @Override
    public BlockAccessor block(long blockNumber) {
        return null;
    }

    @Override
    public long oldestBlockNumber() {
        return UNKNOWN_BLOCK_NUMBER;
    }

    @Override
    public long latestBlockNumber() {
        return UNKNOWN_BLOCK_NUMBER;
    }
}
