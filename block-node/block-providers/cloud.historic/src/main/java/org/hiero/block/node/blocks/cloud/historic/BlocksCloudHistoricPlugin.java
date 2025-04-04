// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.cloud.historic;

import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.BlockProviderPlugin;

/**
 * This is a plugin for the block provider that uses cloud storage for historic blocks. It should be very similar to the
 * files historic plugin but is designed to store blocks into S3 compatible cloud storage.
 */
public class BlocksCloudHistoricPlugin implements BlockProviderPlugin {

    @Override
    public int defaultPriority() {
        return 2000;
    }

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
