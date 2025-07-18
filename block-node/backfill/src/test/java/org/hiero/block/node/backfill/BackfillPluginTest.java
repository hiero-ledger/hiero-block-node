// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder;
import org.hiero.block.node.app.fixtures.plugintest.PluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.SimpleBlockRangeSet;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.app.fixtures.server.BlockNodeMock;
import org.hiero.block.node.spi.blockmessaging.BackfilledBlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class BackfillPluginTest extends PluginTestBase<BackfillPlugin> {

    /** The historical block facility. */
    private final SimpleInMemoryHistoricalBlockFacility historicalBlockFacility;

    private final BlockNodeMock blockNodeMock;

    public BackfillPluginTest() {
        // we will create a BN Mock with port number 8081 and blocks from 0 to 400
        final SimpleInMemoryHistoricalBlockFacility secondBNBlockFacility = new SimpleInMemoryHistoricalBlockFacility();
        for (int i = 0; i < 400; i++) {
            final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createSimpleBlockUnparsedWithNumber(i);
            secondBNBlockFacility.handleBlockItemsReceived(new BlockItems(List.of(block), i), false);
        }

        this.historicalBlockFacility = new SimpleInMemoryHistoricalBlockFacility();
        this.blockNodeMock = new BlockNodeMock(8081, secondBNBlockFacility);

        start(
                new BackfillPlugin(),
                this.historicalBlockFacility,
                Map.of(
                        "backfill.blockNodeSourcesPath",
                        "/Users/user/Projects/hiero-block-node/block-node/backfill/src/test/resources/block-nodes.json",
                        "backfill.fetchBatchSize",
                        "5",
                        "backfill.coolDownTimeBetweenBatchesMs",
                        "100",
                        "backfill.initialDelayMs",
                        "1000"));
    }

    @Test
    @DisplayName("Backfill Happy Test")
    void testBackfillPlugin() throws InterruptedException {
        // insert a GAP in the historical block facility
        final SimpleBlockRangeSet temporaryAvailableBlocks = new SimpleBlockRangeSet();
        temporaryAvailableBlocks.add(10, 20);
        this.historicalBlockFacility.setTemporaryAvailableBlocks(temporaryAvailableBlocks);
        CountDownLatch countDownLatch =
                new CountDownLatch((int) (temporaryAvailableBlocks.max() - temporaryAvailableBlocks.min()));

        this.blockMessaging.registerBlockNotificationHandler(
                new BlockNotificationHandler() {
                    @Override
                    public void handleBackfilled(BackfilledBlockNotification notification) {
                        blockNodeContext
                                .blockMessaging()
                                .sendBlockVerification(new VerificationNotification(
                                        true,
                                        notification.blockNumber(),
                                        Bytes.wrap("123"),
                                        notification.block(),
                                        BlockSource.BACKFILL));
                    }
                },
                false,
                "test-backfill-handler");

        this.blockMessaging.registerBlockNotificationHandler(
                new BlockNotificationHandler() {
                    @Override
                    public void handleVerification(VerificationNotification notification) {
                        blockNodeContext
                                .blockMessaging()
                                .sendBlockPersisted(new PersistedNotification(
                                        notification.blockNumber(),
                                        notification.blockNumber(),
                                        10,
                                        notification.source()));
                        countDownLatch.countDown();
                    }
                },
                false,
                "test-backfill-handler");

        boolean backfillSuccess =
                countDownLatch.await(5, TimeUnit.MINUTES); // Wait until countDownLatch.countDown() is called

        // Continue with your assertions or test logic/BlockItems blockItems = mock(BlockItems.class);
        assertEquals(true, backfillSuccess);
        assertEquals(0, countDownLatch.getCount(), "Count down latch should be 0 after backfill");
    }
}
