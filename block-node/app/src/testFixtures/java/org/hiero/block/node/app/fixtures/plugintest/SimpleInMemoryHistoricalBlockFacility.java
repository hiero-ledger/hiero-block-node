// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.plugintest;

import static org.hiero.block.node.app.fixtures.blocks.BlockItemUtils.toBlockItems;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.block.node.spi.blockmessaging.BlockItemHandler;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;

/**
 * Implementation of {@link HistoricalBlockFacility} that stores blocks in memory. It don't persist blocks on disk and
 * ignores verified vs unverified. So all blocks are stored in memory that are sent to messaging facility.
 */
public class SimpleInMemoryHistoricalBlockFacility implements HistoricalBlockFacility, BlockItemHandler {
    private final ConcurrentHashMap<Long, Block> blockStorage = new ConcurrentHashMap<>();
    private final AtomicLong oldestBlockNumber = new AtomicLong(UNKNOWN_BLOCK_NUMBER);
    private final AtomicLong latestBlockNumber = new AtomicLong(UNKNOWN_BLOCK_NUMBER);
    private final AtomicLong currentBlockNumber = new AtomicLong(UNKNOWN_BLOCK_NUMBER);
    private final List<BlockItems> partialBlock = new ArrayList<>();

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleBlockItemsReceived(BlockItems blockItems) {
        if (blockItems.isStartOfNewBlock()) {
            if (!partialBlock.isEmpty()) {
                throw new RuntimeException("Something went wrong, partitionedBlock is not empty. So we never got a end "
                        + "block for current block");
            }
            currentBlockNumber.set(blockItems.newBlockNumber());
        }
        partialBlock.add(blockItems);
        if (blockItems.isEndOfBlock()) {
            List<BlockItem> bi = new ArrayList<>();
            for (BlockItems items : partialBlock) {
                bi.addAll(toBlockItems(items.blockItems()));
            }
            Block block = new Block(bi);
            blockStorage.put(currentBlockNumber.get(), block);
            latestBlockNumber.set(currentBlockNumber.get());
            currentBlockNumber.set(UNKNOWN_BLOCK_NUMBER);
            partialBlock.clear();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BlockAccessor block(long blockNumber) {
        Block block = blockStorage.get(blockNumber);
        return block == null ? null : () -> block;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long oldestBlockNumber() {
        return oldestBlockNumber.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long latestBlockNumber() {
        return latestBlockNumber.get();
    }
}
