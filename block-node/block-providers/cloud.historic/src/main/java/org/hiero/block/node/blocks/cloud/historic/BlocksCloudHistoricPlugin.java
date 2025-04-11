// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.cloud.historic;

import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.BlockProviderPlugin;
import org.hiero.block.node.spi.historicalblocks.BlockRangeSet;

/**
 * This is a plugin for the block provider that uses cloud storage for historic blocks. It should be very similar to the
 * files historic plugin but is designed to store blocks into S3 compatible cloud storage.
 */
public class BlocksCloudHistoricPlugin implements BlockProviderPlugin {

    /**
     * {@inheritDoc}
     */
    @Override
    public int defaultPriority() {
        return 2000;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BlockAccessor block(long blockNumber) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    public BlockRangeSet availableBlocks() {
        return BlockRangeSet.EMPTY;
    }
}
