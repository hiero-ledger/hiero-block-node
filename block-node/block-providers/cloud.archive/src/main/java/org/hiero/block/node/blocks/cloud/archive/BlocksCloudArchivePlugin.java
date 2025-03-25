// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.cloud.archive;

import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.BlockProviderPlugin;

/**  */
public class BlocksCloudArchivePlugin implements BlockProviderPlugin {

    @Override
    public String name() {
        return "Cloud Archive";
    }

    @Override
    public int defaultPriority() {
        return 0;
    }

    @Override
    public BlockAccessor block(long blockNumber) {
        return null;
    }

    @Override
    public long latestBlockNumber() {
        return 0;
    }
}
