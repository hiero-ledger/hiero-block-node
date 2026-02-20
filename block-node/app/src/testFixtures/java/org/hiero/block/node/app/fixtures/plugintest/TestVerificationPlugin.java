// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.plugintest;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.BlockItemHandler;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;

public class TestVerificationPlugin implements BlockNodePlugin, BlockNotificationHandler, BlockItemHandler {
    private final Set<Long> blocksToFail = new LinkedHashSet<>();
    private BlockNodeContext context;
    private VerificationSession currentSession;
    private BlockSource blockSource = BlockSource.PUBLISHER;

    @Override
    public String name() {
        return TestVerificationPlugin.class.getSimpleName();
    }

    @Override
    public void init(final BlockNodeContext context, final ServiceBuilder serviceBuilder) {
        this.context = Objects.requireNonNull(context);
    }

    @Override
    public void start() {
        context.blockMessaging().registerBlockNotificationHandler(this, false, name());
        context.blockMessaging().registerBlockItemHandler(this, true, name());
    }

    @Override
    public void handleBlockItemsReceived(final BlockItems blockItems) {
        if (blockItems.isStartOfNewBlock()) {
            final long blockNumber = blockItems.blockNumber();
            currentSession = new VerificationSession(blockNumber, blocksToFail.remove(blockNumber), blockSource);
        }
        if (currentSession.processBlockItems(blockItems)) {
            context.blockMessaging().sendBlockVerification(currentSession.completeSession());
        }
    }

    @Override
    public void stop() {
        context.blockMessaging().unregisterBlockNotificationHandler(this);
        context.blockMessaging().unregisterBlockItemHandler(this);
    }

    public void setBlockSource(final BlockSource blockSource) {
        this.blockSource = Objects.requireNonNull(blockSource);
    }

    public void failBlocks(long... blockNumbers) {
        for (final long number : blockNumbers) {
            blocksToFail.add(number);
        }
    }

    private static final String NULL_SOURCE_MESSAGE =
            "BlockSource must be set before starting a verification session in the TestVerificationPlugin";

    private static final class VerificationSession {
        private final long blockNumber;
        private final boolean shouldFail;
        private final BlockSource blockSource;
        private final List<BlockItemUnparsed> blockItems;

        private VerificationSession(final long blockNumber, final boolean shouldFail, final BlockSource blockSource) {
            assertNotNull(blockSource, NULL_SOURCE_MESSAGE);
            this.blockNumber = blockNumber;
            this.shouldFail = shouldFail;
            this.blockSource = blockSource;
            this.blockItems = new ArrayList<>();
        }

        private boolean processBlockItems(final BlockItems blockItems) {
            this.blockItems.addAll(blockItems.blockItems());
            return blockItems.isEndOfBlock();
        }

        private VerificationNotification completeSession() {
            if (shouldFail) {
                return new VerificationNotification(false, blockNumber, null, null, blockSource);
            } else {
                // @todo() add a way to either generate a block hash, or be able to add a custom block hash for a given
                //   block. This is based on needs.
                return new VerificationNotification(
                        true, blockNumber, null, new BlockUnparsed(blockItems), blockSource);
            }
        }
    }
}
