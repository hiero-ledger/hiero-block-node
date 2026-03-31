// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.plugintest;

import com.hedera.hapi.block.stream.Block;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.hiero.block.node.app.fixtures.blocks.BlockUtils;
import org.hiero.block.node.app.fixtures.blocks.MinimalBlockAccessor;
import org.hiero.block.node.spi.ApplicationStateFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.BlockRangeSet;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;

public class VerificationHandlingHistoricalBlockFacility implements HistoricalBlockFacility, BlockNotificationHandler {
    private final SimpleBlockRangeSet availableBlocks = new SimpleBlockRangeSet();
    private final ConcurrentNavigableMap<Long, Block> blockStorage = new ConcurrentSkipListMap<>();
    private BlockNodeContext context;

    @Override
    public String name() {
        return VerificationHandlingHistoricalBlockFacility.class.getSimpleName();
    }

    @Override
    public void init(
            final BlockNodeContext context,
            final ServiceBuilder serviceBuilder,
            final ApplicationStateFacility applicationStateFacility) {
        this.context = context;
    }

    @Override
    public void start() {
        context.blockMessaging().registerBlockNotificationHandler(this, true, name());
    }

    @Override
    public void stop() {
        context.blockMessaging().unregisterBlockNotificationHandler(this);
    }

    @Override
    public void handleVerification(final VerificationNotification notification) {
        if (notification.success()) {
            blockStorage.put(notification.blockNumber(), BlockUtils.toBlock(notification.block()));
            availableBlocks.add(notification.blockNumber());
            context.blockMessaging()
                    .sendBlockPersisted(
                            new PersistedNotification(notification.blockNumber(), true, 2_000, notification.source()));
        }
    }

    @Override
    public BlockAccessor block(final long blockNumber) {
        final Block block = blockStorage.get(blockNumber);
        return block == null ? null : new MinimalBlockAccessor(blockNumber, block);
    }

    @Override
    public BlockRangeSet availableBlocks() {
        return availableBlocks;
    }
}
