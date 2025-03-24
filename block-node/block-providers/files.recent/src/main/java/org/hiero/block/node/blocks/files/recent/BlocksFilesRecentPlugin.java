// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.recent;

import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.BlockItemHandler;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.BlockProviderPlugin;

/**  */
public class BlocksFilesRecentPlugin implements BlockProviderPlugin, BlockItemHandler {
    private BlockNodeContext context;
    // ==== BlockProviderPlugin Methods ================================================================================

    /**
     * {@inheritDoc}
     */
    @Override
    public String name() {
        return "Files Recent";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(BlockNodeContext context) {
        this.context = context;
        context.blockMessaging().registerBlockItemHandler(this, false,
                "BlocksFilesRecent");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start() {
        BlockProviderPlugin.super.start();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int defaultPriority() {
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BlockAccessor block(long blockNumber, Runnable deleteBlockCallback) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long latestBlockNumber() {
        return 0;
    }

    // ==== BlockItemHandler Methods ===================================================================================

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleBlockItemsReceived(BlockItems blockItems) {

    }
}
