// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.cloud.archive;

import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.BlockProviderPlugin;

/**
 * This a block provider plugin that stores blocks in a cloud archive. It designed to be read only. It intentionally
 * does not provide read access to the blocks. It does this so it can be implemented with services like AWS S3 Glacier
 * Deep Archive where reading back is slow(hours/days), asynchronous and very expensive. But they provide a
 * cost-effective storage solution for disaster recovery and long term storage.
 */
public class BlocksCloudArchivePlugin implements BlockProviderPlugin {

    /**
     * {@inheritDoc}
     * <p>
     * Returns Integer.MAX_VALUE because this plugin is designed to be a read only plugin and hence should only
     * be checked for read if all other providers have failed.
     */
    @Override
    public int defaultPriority() {
        return Integer.MAX_VALUE;
    }

    /**
     * {@inheritDoc}
     * <p>
     * This method always returns null. This is because this plugin does not provide any blocks. It is a read only
     * plugin that is used to archive block only.
     */
    @Override
    public BlockAccessor block(long blockNumber) {
        return null;
    }

    /**
     * {@inheritDoc}
     * <p>
     * This method always returns UNKNOWN_BLOCK_NUMBER. This is because this plugin does not provide any blocks. It is a
     * read only plugin that is used to archive block only.
     */
    @Override
    public long oldestBlockNumber() {
        return UNKNOWN_BLOCK_NUMBER;
    }

    /**
     * {@inheritDoc}
     * <p>
     * This method always returns UNKNOWN_BLOCK_NUMBER. This is because this plugin does not provide any blocks. It is a
     * read only plugin that is used to archive block only.
     */
    @Override
    public long latestBlockNumber() {
        return UNKNOWN_BLOCK_NUMBER;
    }
}
