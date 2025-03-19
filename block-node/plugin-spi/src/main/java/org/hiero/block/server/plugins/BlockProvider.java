package org.hiero.block.server.plugins;

import com.hedera.hapi.block.stream.Block;

public interface BlockProvider {

    public Block getBlock(long blockNumber);
    public long getMinimumAvailableBlockNumber();
    public long getMaximumAvailableBlockNumber();
}
