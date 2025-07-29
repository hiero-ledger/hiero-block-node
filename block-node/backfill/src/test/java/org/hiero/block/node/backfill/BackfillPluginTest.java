// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder;
import org.hiero.block.node.app.fixtures.plugintest.PluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.SimpleBlockRangeSet;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.app.fixtures.server.TestBlockNodeServer;
import org.hiero.block.node.spi.blockmessaging.BackfilledBlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class BackfillPluginTest extends PluginTestBase<BackfillPlugin, BlockingExecutor> {

    /** The historical block facility. */
    private final SimpleInMemoryHistoricalBlockFacility historicalBlockFacility;

    private final TestBlockNodeServer testBlockNodeServer;

    private final List<TestBlockNodeServer> blockNodeServerMocks = new ArrayList();

    private final Map<String, String> defaultConfig = Map.of(
            "backfill.blockNodeSourcesPath",
            getClass().getClassLoader().getResource("block-nodes.json").getFile(),
            "backfill.fetchBatchSize",
            "5",
            "backfill.delayBetweenBatchesMs",
            "100",
            "backfill.initialDelayMs",
            "1000");

    public BackfillPluginTest() {
        super(new BlockingExecutor(new LinkedBlockingQueue<>()));
        // we will create a BN Mock with port number 8081 and blocks from 0 to 400
        final SimpleInMemoryHistoricalBlockFacility secondBNBlockFacility = new SimpleInMemoryHistoricalBlockFacility();
        for (int i = 0; i < 400; i++) {
            final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createSimpleBlockUnparsedWithNumber(i);
            secondBNBlockFacility.handleBlockItemsReceived(new BlockItems(List.of(block), i), false);
        }

        this.historicalBlockFacility = new SimpleInMemoryHistoricalBlockFacility();
        this.testBlockNodeServer = new TestBlockNodeServer(8081, secondBNBlockFacility);

        // we create another BN Server Mock that is available at port 8082 but has no useful blocks
        final SimpleInMemoryHistoricalBlockFacility emptyBNBlockFacility = new SimpleInMemoryHistoricalBlockFacility();
        for (int i = 10000; i < 10001; i++) {
            final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createSimpleBlockUnparsedWithNumber(i);
            emptyBNBlockFacility.handleBlockItemsReceived(new BlockItems(List.of(block), i), false);
        }
        blockNodeServerMocks.add(new TestBlockNodeServer(8082, emptyBNBlockFacility));
    }

    @Test
    @DisplayName("Backfill Happy Test")
    void testBackfillPlugin() throws InterruptedException {

        String blockNodeSourcesPath =
                getClass().getClassLoader().getResource("block-nodes.json").getFile();

        start(
                new BackfillPlugin(),
                this.historicalBlockFacility,
                Map.of(
                        "backfill.blockNodeSourcesPath",
                        blockNodeSourcesPath,
                        "backfill.fetchBatchSize",
                        "5",
                        "backfill.delayBetweenBatchesMs",
                        "100",
                        "backfill.initialDelaySeconds",
                        "5"));

        // insert a GAP in the historical block facility
        final SimpleBlockRangeSet temporaryAvailableBlocks = new SimpleBlockRangeSet();
        temporaryAvailableBlocks.add(10, 20);
        this.historicalBlockFacility.setTemporaryAvailableBlocks(temporaryAvailableBlocks);
        CountDownLatch countDownLatch = new CountDownLatch(10); // 0 to 9 inclusive, so 10 blocks

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

        // Verify sent verifications
        assertEquals(
                10,
                blockMessaging.getSentPersistedNotifications().size(),
                "Should have sent 11 persisted notifications");
        assertEquals(
                10,
                blockMessaging.getSentVerificationNotifications().size(),
                "Should have sent 11 verification notifications");

        plugin.stop();
    }

    @Test
    @DisplayName("Priority 1 BN is unavailable, fallback to 2nd priority BN")
    void testSecondarySourceBakcfill() throws InterruptedException {
        // lets re-initialize the plugin with a different config
        String blockNodeSourcesPath =
                getClass().getClassLoader().getResource("block-nodes-2.json").getFile();

        start(
                new BackfillPlugin(),
                this.historicalBlockFacility,
                Map.of(
                        "backfill.blockNodeSourcesPath",
                        blockNodeSourcesPath,
                        "backfill.fetchBatchSize",
                        "5",
                        "backfill.delayBetweenBatchesMs",
                        "100",
                        "backfill.initialDelaySeconds",
                        "5",
                        "backfill.initialRetryDelayMs",
                        "500"));

        // insert a GAP in the historical block facility
        final SimpleBlockRangeSet temporaryAvailableBlocks = new SimpleBlockRangeSet();
        temporaryAvailableBlocks.add(10, 20);
        this.historicalBlockFacility.setTemporaryAvailableBlocks(temporaryAvailableBlocks);
        CountDownLatch countDownLatch = new CountDownLatch(10); // 0 to 9 inclusive, so 10 blocks

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

        // Verify sent verifications
        assertEquals(
                10,
                blockMessaging.getSentPersistedNotifications().size(),
                "Should have sent 11 persisted notifications");
        assertEquals(
                10,
                blockMessaging.getSentVerificationNotifications().size(),
                "Should have sent 11 verification notifications");
    }

    @Test
    @DisplayName("Backfill found no available block-nodes, should not backfill")
    void testBackfillNoAvailableBlockNodes() throws InterruptedException {
        // lets re-initialize the plugin with a different config
        String blockNodeSourcesPath =
                getClass().getClassLoader().getResource("block-nodes-3.json").getFile();

        start(
                new BackfillPlugin(),
                this.historicalBlockFacility,
                Map.of(
                        "backfill.blockNodeSourcesPath",
                        blockNodeSourcesPath,
                        "backfill.fetchBatchSize",
                        "5",
                        "backfill.delayBetweenBatchesMs",
                        "100",
                        "backfill.initialDelaySeconds",
                        "5",
                        "backfill.initialRetryDelayMs",
                        "500"));

        // insert a GAP in the historical block facility
        final SimpleBlockRangeSet temporaryAvailableBlocks = new SimpleBlockRangeSet();
        temporaryAvailableBlocks.add(10, 20);
        this.historicalBlockFacility.setTemporaryAvailableBlocks(temporaryAvailableBlocks);

        // give 10 seconds to allow processing to finish...
        TimeUnit.SECONDS.sleep(10);

        assertEquals(
                0, blockMessaging.getSentPersistedNotifications().size(), "Should have sent 0 persisted notifications");
        assertEquals(
                0,
                blockMessaging.getSentVerificationNotifications().size(),
                "Should have sent 0 verification notifications");
    }
}
