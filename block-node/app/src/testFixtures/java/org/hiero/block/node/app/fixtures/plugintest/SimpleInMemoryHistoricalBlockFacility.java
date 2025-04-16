// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.plugintest;

import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.hiero.block.node.app.fixtures.blocks.BlockItemUtils.toBlockItems;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.block.node.spi.blockmessaging.BlockItemHandler;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.BlockRangeSet;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;

/**
 * Implementation of {@link HistoricalBlockFacility} that stores blocks in memory. It don't persist blocks on disk and
 * ignores verified vs unverified. So all blocks are stored in memory that are sent to messaging facility.
 */
public class SimpleInMemoryHistoricalBlockFacility implements HistoricalBlockFacility, BlockItemHandler {
    private final ConcurrentHashMap<Long, Block> blockStorage = new ConcurrentHashMap<>();
    private final SimpleBlockRangeSet availableBlocks = new SimpleBlockRangeSet();
    private final AtomicLong currentBlockNumber = new AtomicLong(UNKNOWN_BLOCK_NUMBER);
    private final List<BlockItems> partialBlock = new ArrayList<>();
    private final AtomicBoolean delayResponses = new AtomicBoolean(false);

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
            availableBlocks.add(currentBlockNumber.get());
            currentBlockNumber.set(UNKNOWN_BLOCK_NUMBER);
            partialBlock.clear();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BlockAccessor block(final long blockNumber) {
        final Block block = blockStorage.get(blockNumber);
        while (delayResponses.get()) parkNanos(500_000L);
        return block == null ? null : new BlockAccessor() {
            @Override
            public long blockNumber() {
                return blockNumber;
            }

            @Override
            public Block block() {
                return block;
            }
        };
    }

    public void setDelayResponses() {
        delayResponses.compareAndSet(false, true);
    }

    public void clearDelayResponses() {
        delayResponses.compareAndSet(true, false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BlockRangeSet availableBlocks() {
        return availableBlocks;
    }
}
