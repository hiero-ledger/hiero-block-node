// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.plugintest;

import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.hiero.block.node.app.fixtures.blocks.BlockItemUtils.toBlockItems;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.block.node.app.fixtures.blocks.MinimalBlockAccessor;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.BlockItemHandler;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
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
    private final AtomicBoolean disablePlugin = new AtomicBoolean(false);
    private SimpleBlockRangeSet availableBlocksOverride;
    private BlockNodeContext blockNodeContext;

    @Override
    public void init(BlockNodeContext context, ServiceBuilder serviceBuilder) {
        this.blockNodeContext = context;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleBlockItemsReceived(BlockItems blockItems) {
        handleBlockItemsReceived(blockItems, true);
    }

    public void handleBlockItemsReceived(
            BlockItems blockItems, final boolean sendNotification, int priority, BlockSource source) {
        if (!disablePlugin.get()) {
            if (blockItems.isStartOfNewBlock()) {
                if (!partialBlock.isEmpty()) {
                    throw new RuntimeException(
                            "Something went wrong, partitionedBlock is not empty. So we never got a end "
                                    + "block for current block");
                }
                currentBlockNumber.set(blockItems.blockNumber());
            }
            partialBlock.add(blockItems);
            if (blockItems.isEndOfBlock()) {
                final long blockNumber = currentBlockNumber.getAndSet(UNKNOWN_BLOCK_NUMBER);
                List<BlockItem> bi = new ArrayList<>();
                for (BlockItems items : partialBlock) {
                    bi.addAll(toBlockItems(items.blockItems()));
                }
                Block block = new Block(bi);
                blockStorage.put(blockNumber, block);
                availableBlocks.add(blockNumber);
                partialBlock.clear();
                // send block persisted message
                if (sendNotification) {
                    blockNodeContext
                            .blockMessaging()
                            .sendBlockPersisted(new PersistedNotification(blockNumber, true, priority, source));
                }
            }
        }
    }

    public void handleBlockItemsReceived(BlockItems blockItems, final boolean sendNotification) {
        handleBlockItemsReceived(blockItems, sendNotification, 2000, BlockSource.UNKNOWN);
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public BlockAccessor block(final long blockNumber) {
        final Block block = blockStorage.get(blockNumber);
        while (delayResponses.get()) parkNanos(500_000L);
        return block == null ? null : new MinimalBlockAccessor(blockNumber, block);
    }

    public void setDelayResponses() {
        delayResponses.compareAndSet(false, true);
    }

    public void clearDelayResponses() {
        delayResponses.compareAndSet(true, false);
    }

    public void setDisablePlugin() {
        disablePlugin.compareAndSet(false, true);
    }

    public void clearDisablePlugin() {
        disablePlugin.compareAndSet(true, false);
    }

    /**
     * Sets a temporary override for the available blocks. The idea here is to
     * allow tests to set a specific set of available blocks which will be
     * returned by the {@link #availableBlocks()} method until cleared.
     * These temporary available blocks will not be updated if new blocks are
     * added to the facility. Once cleared, the facility will return the
     * original available blocks set, which are always updated. An example usage
     * here would be to simulate a condition where a check for available blocks
     * gives a specific set of blocks, but then later when we ask the facility
     * to give us the available blocks, it will return what is in the original
     * available blocks set. This simulates the condition where the check might
     * become stale or outdated and what we get in terms of accessible blocks
     * is what is currently available in the facility.
     */
    public void setTemporaryAvailableBlocks(@NonNull final SimpleBlockRangeSet availableBlocks) {
        this.availableBlocksOverride = Objects.requireNonNull(availableBlocks);
    }

    /**
     * Clears the temporary available blocks override set by the
     * {@link #setTemporaryAvailableBlocks(SimpleBlockRangeSet)}
     * @see #setTemporaryAvailableBlocks(SimpleBlockRangeSet) for details.
     */
    public void clearTemporaryAvailableBlocks() {
        this.availableBlocksOverride = null;
    }

    /**
     * {@inheritDoc}
     * <p>
     * If a temporary override is set using
     * {@link #setTemporaryAvailableBlocks(SimpleBlockRangeSet)}, then that
     * override will be returned until cleared with
     * {@link #clearTemporaryAvailableBlocks()}. Any updates to the available
     * blocks in the facility will not affect the temporary override. They will
     * be reflected however in the original available blocks set (accessible
     * when there is no override).
     */
    @Override
    public BlockRangeSet availableBlocks() {
        return availableBlocksOverride == null ? availableBlocks : availableBlocksOverride;
    }
}
