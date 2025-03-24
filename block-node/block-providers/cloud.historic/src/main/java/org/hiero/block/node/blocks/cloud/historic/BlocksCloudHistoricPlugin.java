// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.cloud.historic;

import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.BlockProviderPlugin;

/**  */
public class BlocksCloudHistoricPlugin implements BlockProviderPlugin {
    @Override
    public String name() {
        return "Cloud Historic";
    }

    @Override
    public int defaultPriority() {
        return 0;
    }

    @Override
    public BlockAccessor block(long blockNumber, Runnable deleteBlockCallback) {
        return null;
    }

    @Override
    public long latestBlockNumber() {
        return 0;
    }
}
