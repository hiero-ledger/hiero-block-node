// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.node.app.fixtures.async.BlockingSerialExecutor;
import org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder;
import org.hiero.block.node.app.fixtures.plugintest.PluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.SimpleBlockRangeSet;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.app.fixtures.server.TestBlockNodeServer;
import org.hiero.block.node.backfill.client.BackfillSource;
import org.hiero.block.node.backfill.client.BackfillSourceConfig;
import org.hiero.block.node.spi.blockmessaging.BackfilledBlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.NewestBlockKnownToNetworkNotification;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BackfillPluginTest extends PluginTestBase<BackfillPlugin, BlockingSerialExecutor> {

    /** TempDir for the current test */
    private final Path testTempDir;

    /**
     * The historical block facility.
     */
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

    public BackfillPluginTest(@TempDir final Path tempDir) {
        super(new BlockingSerialExecutor(new LinkedBlockingQueue<>()));
        this.testTempDir = Objects.requireNonNull(tempDir);
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

        // register the backfill handler
        registerBackfillHandler();
        // register the verification handler
        registerVerificationHandler(countDownLatch);

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

        // Set up a backfill source with two nodes, one primary and one secondary
        // The primary node is at port 8082 and the secondary node is at port 8081
        BackfillSourceConfig backfillSourceConfig = BackfillSourceConfig.newBuilder()
                .address("localhost")
                .port(8082)
                .priority(1)
                .build();
        BackfillSourceConfig secondaryBackfillSourceConfig = BackfillSourceConfig.newBuilder()
                .address("localhost")
                .port(8081)
                .priority(2)
                .build();
        BackfillSource backfillSource = BackfillSource.newBuilder()
                .nodes(backfillSourceConfig, secondaryBackfillSourceConfig)
                .build();
        // Create a temporary file for the backfill source configuration
        String backfillSourcePath = testTempDir + "/backfill-source-2.json";
        createTestConfig(backfillSource, backfillSourcePath);

        start(
                new BackfillPlugin(),
                this.historicalBlockFacility,
                Map.of(
                        "backfill.blockNodeSourcesPath",
                        backfillSourcePath,
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

        // register the backfill handler
        registerBackfillHandler();
        // register the verification handler
        registerVerificationHandler(countDownLatch);

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
        // let's create a config with a non-existing block-node source
        String blockNodeSourcesPath = testTempDir + "/backfill-source-3.json";
        BackfillSourceConfig backfillSourceConfig = BackfillSourceConfig.newBuilder()
                .address("non-existing-block-node.example.node") // non-existing address
                .port(80840) // non-existing port
                .priority(1)
                .build();
        BackfillSource backfillSource =
                BackfillSource.newBuilder().nodes(backfillSourceConfig).build();
        // Create a temporary file for the backfill source configuration
        createTestConfig(backfillSource, blockNodeSourcesPath);

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

    @Test
    @DisplayName("Backfill On-Demand Happy path test")
    void testBackfillOnDemand() throws InterruptedException {
        // Create a test configuration
        BackfillSourceConfig config = BackfillSourceConfig.newBuilder()
                .address("localhost")
                .port(40844)
                .priority(1)
                .build();
        BackfillSource backfillSource =
                BackfillSource.newBuilder().nodes(config).build();
        // Create a temporary file for the backfill source configuration
        String backfillSourcePath = testTempDir + "/backfill-source-4.json";
        createTestConfig(backfillSource, backfillSourcePath);
        // using the same port, start a BN mock that has blocks from 0 to 50
        final SimpleInMemoryHistoricalBlockFacility mockServer = new SimpleInMemoryHistoricalBlockFacility();
        for (int i = 0; i < 51; i++) {
            final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createSimpleBlockUnparsedWithNumber(i);
            mockServer.handleBlockItemsReceived(new BlockItems(List.of(block), i), false);
        }
        TestBlockNodeServer testBlockNodeServer = new TestBlockNodeServer(config.port(), mockServer);
        // start block-node with blocks from 0 to 30
        start(
                new BackfillPlugin(),
                this.historicalBlockFacility,
                Map.of(
                        "backfill.blockNodeSourcesPath",
                        backfillSourcePath,
                        "backfill.fetchBatchSize",
                        "10",
                        "backfill.delayBetweenBatchesMs",
                        "100",
                        "backfill.initialDelaySeconds",
                        "5"));

        // Insert some blocks on the historical block facility
        // insert a GAP in the historical block facility
        final SimpleBlockRangeSet temporaryAvailableBlocks = new SimpleBlockRangeSet();
        temporaryAvailableBlocks.add(0, 30);
        this.historicalBlockFacility.setTemporaryAvailableBlocks(temporaryAvailableBlocks);
        // We will backfill blocks from 31 to 50 inclusive, so we expect 20 blocks to be backfilled
        CountDownLatch countDownLatch = new CountDownLatch(20); // from 31 to 50 inclusive, so 20 blocks
        // register the backfill handler
        registerBackfillHandler();
        // register the verification handler
        registerVerificationHandler(countDownLatch);
        // Trigger the backfill on-demand by sending a NewestBlockKnownToNetworkNotification
        // to the block messaging system
        NewestBlockKnownToNetworkNotification newestBlockNotification = new NewestBlockKnownToNetworkNotification(50L);
        this.blockMessaging.sendNewestBlockKnownToNetwork(newestBlockNotification);
        // Wait for the backfill to complete
        boolean backfillSuccess =
                countDownLatch.await(5, TimeUnit.MINUTES); // Wait until countDownLatch.countDown() is called

        // assertions
        assertTrue(backfillSuccess);
        assertEquals(0, countDownLatch.getCount(), "Count down latch should be 0 after backfill");

        // Verify sent verifications
        assertEquals(
                20,
                blockMessaging.getSentPersistedNotifications().size(),
                "Should have sent 20 persisted notifications");
        assertEquals(
                20,
                blockMessaging.getSentVerificationNotifications().size(),
                "Should have sent 20 verification notifications");

        plugin.stop();
    }

    @Test
    @DisplayName("Backfill on-demand while historical (autonomous) backfill is running")
    void testBackfillOnDemandWhileAutonomousBackfillRunning() throws InterruptedException {
        // Prepare a backfill source configuration
        // Create a test configuration
        BackfillSourceConfig config = BackfillSourceConfig.newBuilder()
                .address("localhost")
                .port(40845)
                .priority(1)
                .build();
        BackfillSource backfillSource =
                BackfillSource.newBuilder().nodes(config).build();
        // Create a temporary file for the backfill source configuration
        String backfillSourcePath = testTempDir + "/backfill-source-5.json";
        createTestConfig(backfillSource, backfillSourcePath);
        // using the same port, start a BN mock that has blocks from 0 to 1000
        final SimpleInMemoryHistoricalBlockFacility mockServer = new SimpleInMemoryHistoricalBlockFacility();
        for (int i = 0; i < 1001; i++) {
            final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createSimpleBlockUnparsedWithNumber(i);
            mockServer.handleBlockItemsReceived(new BlockItems(List.of(block), i), false);
        }
        TestBlockNodeServer testBlockNodeServer = new TestBlockNodeServer(config.port(), mockServer);

        // start block-node with blocks from 800 to 900
        // needs to backfilled from 0 to 799 autonomously
        // and then on-demand from 901 to 1000
        start(
                new BackfillPlugin(),
                this.historicalBlockFacility,
                Map.of(
                        "backfill.blockNodeSourcesPath",
                        backfillSourcePath,
                        "backfill.fetchBatchSize",
                        "50",
                        "backfill.delayBetweenBatchesMs",
                        "100",
                        "backfill.initialDelaySeconds",
                        "5"));

        // Insert some blocks on the historical block facility
        // insert a GAP in the historical block facility
        final SimpleBlockRangeSet temporaryAvailableBlocks = new SimpleBlockRangeSet();
        temporaryAvailableBlocks.add(800, 900);
        this.historicalBlockFacility.setTemporaryAvailableBlocks(temporaryAvailableBlocks);
        // 3 latches, one for making sure the autonomous backfill is mid-way through
        // one for the total backfill from 0 to 799
        // and one for the on-demand backfill from 901 to 1000
        CountDownLatch latch1 = new CountDownLatch(180); // mid-way through the autonomous backfill
        CountDownLatch latchHistorical = new CountDownLatch(800); // historical blocks from 0 to 799
        CountDownLatch latchLive = new CountDownLatch(100); // live blocks from 901 to 1000

        // register the backfill handler
        registerBackfillHandler();
        // register the verification handler
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
                        latch1.countDown();
                        if (notification.blockNumber() < 800) {
                            latchHistorical.countDown(); // Count down for historical blocks
                        } else if (notification.blockNumber() >= 900) {
                            latchLive.countDown(); // Count down for live blocks
                        }
                    }
                },
                false,
                "test-backfill-handler");

        boolean startOnDemand = latch1.await(1, TimeUnit.MINUTES); // Wait until latch1.countDown() is called
        assertTrue(startOnDemand, "Should have started on-demand backfill while autonomous backfill is running");
        // Trigger the on-demand backfill by sending a NewestBlockKnownToNetworkNotification
        NewestBlockKnownToNetworkNotification newestBlockNotification =
                new NewestBlockKnownToNetworkNotification(1000L);
        this.blockMessaging.sendNewestBlockKnownToNetwork(newestBlockNotification);
        // Wait for the backfill to complete
        boolean backfillSuccess = latchHistorical.await(2, TimeUnit.MINUTES); // Wait until latch2.countDown() is called
        assertTrue(backfillSuccess, "Should have completed the backfill successfully");
        assertEquals(0, latchHistorical.getCount(), "Count down latch should be 0 after backfill");

        // Wait for the on-demand backfill to complete
        boolean onDemandSuccess = latchLive.await(2, TimeUnit.MINUTES); // Wait until latch3.countDown() is called
        assertTrue(onDemandSuccess, "Should have completed the on-demand backfill successfully");

        // Verify sent verifications
        assertEquals(
                900,
                blockMessaging.getSentPersistedNotifications().size(),
                "Should have sent 900 persisted notifications");
        assertEquals(
                900,
                blockMessaging.getSentVerificationNotifications().size(),
                "Should have sent 900 verification notifications");
        // Stop the plugin
        plugin.stop();
    }

    @Test
    @DisplayName("Backfill Autonomous, GAP available within 2 different backfill sources")
    void testBackfillPartialAvaiableSourcesForGap() throws  InterruptedException {

        // Set up 2 backfill sources with two nodes, one primary and one secondary
        // primary node will have blocks from 0 to 100
        // secondary node will have blocks from 50 to 150
        BackfillSourceConfig backfillSourceConfig = BackfillSourceConfig.newBuilder()
                .address("localhost")
                .port(40841)
                .priority(1)
                .build();
        BackfillSourceConfig secondaryBackfillSourceConfig = BackfillSourceConfig.newBuilder()
                .address("localhost")
                .port(40842)
                .priority(2)
                .build();
        BackfillSource backfillSource = BackfillSource.newBuilder()
                .nodes(backfillSourceConfig, secondaryBackfillSourceConfig)
                .build();
        // Create a temporary file for the backfill source configuration
        String backfillSourcePath = testTempDir + "/backfill-source-7.json";
        createTestConfig(backfillSource, backfillSourcePath);
        // create storage for source 1
        final SimpleInMemoryHistoricalBlockFacility storage1 = new SimpleInMemoryHistoricalBlockFacility();
        for (int i = 0; i < 101; i++) {
            final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createSimpleBlockUnparsedWithNumber(i);
            storage1.handleBlockItemsReceived(new BlockItems(List.of(block), i), false);
        }
        // create storage for source 2
        final SimpleInMemoryHistoricalBlockFacility storage2 = new SimpleInMemoryHistoricalBlockFacility();
        for (int i = 50; i < 151; i++) {
            final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createSimpleBlockUnparsedWithNumber(i);
            storage2.handleBlockItemsReceived(new BlockItems(List.of(block), i), false);
        }
        // start block-node mocks
        TestBlockNodeServer testBlockNodeServer1 = new TestBlockNodeServer(backfillSourceConfig.port(), storage1);
        TestBlockNodeServer testBlockNodeServer2 = new TestBlockNodeServer(secondaryBackfillSourceConfig.port(), storage2);

        // start with plugin configuration
        start(
                new BackfillPlugin(),
                this.historicalBlockFacility,
                Map.of(
                        "backfill.blockNodeSourcesPath",
                        backfillSourcePath,
                        "backfill.fetchBatchSize",
                        "200",
                        "backfill.delayBetweenBatchesMs",
                        "100",
                        "backfill.initialDelaySeconds",
                        "5"));

        // Insert blocks from 125 to 175 in the historical block facility
        final SimpleBlockRangeSet temporaryAvailableBlocks = new SimpleBlockRangeSet();
        temporaryAvailableBlocks.add(125, 175);
        this.historicalBlockFacility.setTemporaryAvailableBlocks(temporaryAvailableBlocks);

        CountDownLatch backfillLatch = new CountDownLatch(125); // 0 to 124 inclusive, so 125 blocks

        // register the backfill handler
        registerBackfillHandler();
        // register the verification handler
        registerVerificationHandler(backfillLatch);

        boolean backfillSuccess =
                backfillLatch.await(2, TimeUnit.MINUTES); // Wait until countDownLatch.countDown() is called

        // Continue with your assertions or test logic/BlockItems blockItems = mock(BlockItems.class);
        assertTrue(backfillSuccess);
        assertEquals(0, backfillLatch.getCount(), "Count down latch should be 0 after backfill");
        // Verify sent verifications
        assertEquals(
                125,
                blockMessaging.getSentPersistedNotifications().size(),
                "Should have sent 125 persisted notifications");
        assertEquals(
                125,
                blockMessaging.getSentVerificationNotifications().size(),
                "Should have sent 125 verification notifications");
        // Stop the plugin
        plugin.stop();





    }

    private void createTestConfig(BackfillSource backfillSource, String configPath) {
        String jsonString = BackfillSource.JSON.toJSON(backfillSource);
        // Write the JSON string to the specified file path
        try {
            java.nio.file.Files.write(java.nio.file.Paths.get(configPath), jsonString.getBytes());
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to write config to file: " + configPath, e);
        }
    }

    private void registerBackfillHandler() {
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
    }

    private void registerVerificationHandler(CountDownLatch countDownLatch) {
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
    }
}
