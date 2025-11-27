// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
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
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BackfillPluginTest extends PluginTestBase<BackfillPlugin, BlockingExecutor, ScheduledBlockingExecutor> {

    /** TempDir for the current test */
    private final Path testTempDir;

    private List<TestBlockNodeServer> testBlockNodeServers;

    public BackfillPluginTest(@TempDir final Path tempDir) {
        super(
                new BlockingExecutor(new LinkedBlockingQueue<>()),
                new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
        this.testTempDir = Objects.requireNonNull(tempDir);
    }

    @BeforeEach
    void setup() {
        testBlockNodeServers = new ArrayList<>();
    }

    @AfterEach
    void cleanup() {
        // stop any started test block node servers
        if (testBlockNodeServers != null) {
            for (TestBlockNodeServer server : testBlockNodeServers) {
                if (server != null) {
                    server.stop();
                }
            }
        }

        plugin.stop();
    }

    @Test
    @DisplayName("Historical Backfill - Autonomous Happy Test")
    void testBackfillPlugin() throws InterruptedException {

        // Block Node sources
        String blockNodeSourcesPath =
                getClass().getClassLoader().getResource("block-nodes.json").getFile();
        // BN 1
        final HistoricalBlockFacility historicalBlockFacilityForServer = getHistoricalBlockFacility(0, 400);
        testBlockNodeServers.add(new TestBlockNodeServer(40801, historicalBlockFacilityForServer));

        // Config Override
        Map<String, String> configOverride = BackfillConfigBuilder.NewBuilder()
                .backfillSourcePath(blockNodeSourcesPath)
                .fetchBatchSize(100)
                .initialDelay(500) // start quickly
                .build();

        // create a historical block facility for the plugin (should have a GAP)
        final HistoricalBlockFacility historicalBlockFacilityForPlugin = getHistoricalBlockFacility(200, 400);

        // start the plugin
        start(new BackfillPlugin(), historicalBlockFacilityForPlugin, configOverride);

        // expected blocks to backfill
        int expectedBlocksToBackfill = 200; // from 0 to 199 inclusive, so 200 blocks

        CountDownLatch countDownLatch = new CountDownLatch(expectedBlocksToBackfill);
        // register the backfill handler
        registerDefaultTestBackfillHandler();
        // register the verification handler
        registerDefaultTestVerificationHandler(countDownLatch);

        boolean backfillSuccess =
                countDownLatch.await(5, TimeUnit.MINUTES); // Wait until countDownLatch.countDown() is called

        // Continue with your assertions or test logic/BlockItems blockItems = mock(BlockItems.class);
        //        assertEquals(true, backfillSuccess);
        assertEquals(0, countDownLatch.getCount(), "Count down latch should be 0 after backfill");

        // Verify sent verifications
        assertEquals(
                expectedBlocksToBackfill,
                blockMessaging.getSentPersistedNotifications().size(),
                "Should have sent 11 persisted notifications");
        assertEquals(
                expectedBlocksToBackfill,
                blockMessaging.getSentVerificationNotifications().size(),
                "Should have sent 11 verification notifications");
    }

    @Test
    @DisplayName("Recent Backfill - Autonomous Happy Test")
    void testBackfillPluginRecentAutonomous() throws InterruptedException {

        // Block Node sources
        String blockNodeSourcesPath =
                getClass().getClassLoader().getResource("block-nodes.json").getFile();

        // BN 1
        final HistoricalBlockFacility historicalBlockFacilityForServer = getHistoricalBlockFacility(0, 400);
        testBlockNodeServers.add(new TestBlockNodeServer(40801, historicalBlockFacilityForServer));

        // Config Override
        Map<String, String> configOverride = BackfillConfigBuilder.NewBuilder()
                .backfillSourcePath(blockNodeSourcesPath)
                .fetchBatchSize(100)
                .initialDelay(500) // start quickly
                .build();

        // create a historical block facility for the plugin (should have a GAP)
        final HistoricalBlockFacility historicalBlockFacilityForPlugin = getHistoricalBlockFacility(0, 200);

        // start the plugin
        start(new BackfillPlugin(), historicalBlockFacilityForPlugin, configOverride);

        // expected blocks to backfill
        int expectedBlocksToBackfill = 200; // from 201 to 400 inclusive, so 200 blocks

        CountDownLatch countDownLatch = new CountDownLatch(expectedBlocksToBackfill);
        // register the backfill handler
        registerDefaultTestBackfillHandler();
        // register the verification handler
        registerDefaultTestVerificationHandler(countDownLatch);

        boolean backfillSuccess =
                countDownLatch.await(5, TimeUnit.MINUTES); // Wait until countDownLatch.countDown() is called

        // Continue with your assertions or test logic/BlockItems blockItems = mock(BlockItems.class);
        //        assertEquals(true, backfillSuccess);
        assertEquals(0, countDownLatch.getCount(), "Count down latch should be 0 after backfill");

        // Verify sent verifications
        assertEquals(
                expectedBlocksToBackfill,
                blockMessaging.getSentPersistedNotifications().size(),
                "Should have sent 200 persisted notifications");
        assertEquals(
                expectedBlocksToBackfill,
                blockMessaging.getSentVerificationNotifications().size(),
                "Should have sent 200 verification notifications");
    }

    @Test
    @DisplayName("Historical Backfill - Priority 1 BN is unavailable, fallback to 2nd priority BN")
    void testSecondarySourceBackfill() throws InterruptedException {

        // Block Node sources
        // Set up a backfill source with two nodes, one primary and one secondary
        // The primary node is at port 8082 and the secondary node is at port 40800
        BackfillSourceConfig backfillSourceConfig = BackfillSourceConfig.newBuilder()
                .address("localhost")
                .port(8082)
                .priority(1)
                .build();
        BackfillSourceConfig secondaryBackfillSourceConfig = BackfillSourceConfig.newBuilder()
                .address("localhost")
                .port(40801)
                .priority(2)
                .build();
        BackfillSource backfillSource = BackfillSource.newBuilder()
                .nodes(backfillSourceConfig, secondaryBackfillSourceConfig)
                .build();
        String backfillSourcePath = testTempDir + "/backfill-source-2.json";
        createTestBlockNodeSourcesFile(backfillSource, backfillSourcePath);

        // BN 2
        final HistoricalBlockFacility historicalBlockFacilityForServer = getHistoricalBlockFacility(0, 400);
        testBlockNodeServers.add(
                new TestBlockNodeServer(secondaryBackfillSourceConfig.port(), historicalBlockFacilityForServer));

        // Config Override
        Map<String, String> configOverride = BackfillConfigBuilder.NewBuilder()
                .backfillSourcePath(backfillSourcePath)
                .maxRetries(2)
                .initialDelay(500) // start quickly
                .build();

        // create a historical block facility for the plugin (should have a GAP)
        final HistoricalBlockFacility historicalBlockFacilityForPlugin = getHistoricalBlockFacility(10, 400);

        // start the plugin
        start(new BackfillPlugin(), historicalBlockFacilityForPlugin, configOverride);

        // insert a GAP in the historical block facility
        CountDownLatch countDownLatch = new CountDownLatch(10); // 0 to 9 inclusive, so 10 blocks

        // register the backfill handler
        registerDefaultTestBackfillHandler();
        // register the verification handler
        registerDefaultTestVerificationHandler(countDownLatch);

        boolean backfillSuccess =
                countDownLatch.await(5, TimeUnit.MINUTES); // Wait until countDownLatch.countDown() is called

        // Continue with your assertions or test logic/BlockItems blockItems = mock(BlockItems.class);
        //        assertEquals(true, backfillSuccess);
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
    @DisplayName("Backfill - No available block-nodes, should not backfill")
    void testBackfillNoAvailableBlockNodes() throws InterruptedException {
        // Block Node sources
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
        createTestBlockNodeSourcesFile(backfillSource, blockNodeSourcesPath);

        // create a historical block facility for the plugin (should have a GAP)
        final HistoricalBlockFacility historicalBlockFacility = getHistoricalBlockFacility(10, 20);
        final Map<String, String> configOverride = BackfillConfigBuilder.NewBuilder()
                .backfillSourcePath(blockNodeSourcesPath)
                .maxRetries(1)
                .build();
        // start the plugin
        start(new BackfillPlugin(), historicalBlockFacility, configOverride);

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
    @DisplayName("Recent Backfill - On-Demand External Trigger Happy path")
    void testBackfillOnDemand() throws InterruptedException {
        // Block Node sources
        BackfillSourceConfig config = BackfillSourceConfig.newBuilder()
                .address("localhost")
                .port(40844)
                .priority(1)
                .build();
        BackfillSource backfillSource =
                BackfillSource.newBuilder().nodes(config).build();
        // Create a temporary file for the backfill source configuration
        String backfillSourcePath = testTempDir + "/backfill-source-4.json";
        createTestBlockNodeSourcesFile(backfillSource, backfillSourcePath);
        // using the same port, start a BN mock that has blocks from 0 to 50
        final HistoricalBlockFacility blockNodeServerBlockFacility = getHistoricalBlockFacility(0, 50);
        testBlockNodeServers.add(new TestBlockNodeServer(config.port(), blockNodeServerBlockFacility));

        // config override for test
        final Map<String, String> configOverride = BackfillConfigBuilder.NewBuilder()
                .backfillSourcePath(backfillSourcePath)
                .build();

        // create a historical block facility for the plugin (should have a GAP)
        final HistoricalBlockFacility historicalBlockFacility = getHistoricalBlockFacility(0, 30);

        // start block-node with blocks from 0 to 30
        start(new BackfillPlugin(), historicalBlockFacility, configOverride);

        // We will backfill blocks from 31 to 50 inclusive, so we expect 20 blocks to be backfilled
        CountDownLatch countDownLatch = new CountDownLatch(20); // from 31 to 50 inclusive, so 20 blocks
        // register the backfill handler
        registerDefaultTestBackfillHandler();
        // register the verification handler
        registerDefaultTestVerificationHandler(countDownLatch);
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
    }

    @Test
    @DisplayName("Recent Backfill - On-Demand No External Trigger Happy path")
    void testBackfillOnDemandRecent() throws InterruptedException {
        // Block Node sources
        BackfillSourceConfig config = BackfillSourceConfig.newBuilder()
                .address("localhost")
                .port(40844)
                .priority(1)
                .build();
        BackfillSource backfillSource =
                BackfillSource.newBuilder().nodes(config).build();
        // Create a temporary file for the backfill source configuration
        String backfillSourcePath = testTempDir + "/backfill-source-4.json";
        createTestBlockNodeSourcesFile(backfillSource, backfillSourcePath);
        // using the same port, start a BN mock that has blocks from 0 to 50
        final HistoricalBlockFacility blockNodeServerBlockFacility = getHistoricalBlockFacility(0, 50);
        testBlockNodeServers.add(new TestBlockNodeServer(config.port(), blockNodeServerBlockFacility));

        // config override for test
        final Map<String, String> configOverride = BackfillConfigBuilder.NewBuilder()
                .backfillSourcePath(backfillSourcePath)
                .build();

        // create a historical block facility for the plugin (should have a GAP)
        final HistoricalBlockFacility historicalBlockFacility = getHistoricalBlockFacility(0, 30);

        // start block-node with blocks from 0 to 30
        start(new BackfillPlugin(), historicalBlockFacility, configOverride);

        // We will backfill blocks from 31 to 50 inclusive, so we expect 20 blocks to be backfilled
        CountDownLatch countDownLatch = new CountDownLatch(20); // from 31 to 50 inclusive, so 20 blocks
        // register the backfill handler
        registerDefaultTestBackfillHandler();
        // register the verification handler
        registerDefaultTestVerificationHandler(countDownLatch);

        // No external trigger the backfill on-demand should receive NewestBlockKnownToNetworkNotification from
        // LiveStreamPublisherManager
        NewestBlockKnownToNetworkNotification newestBlockNotification = new NewestBlockKnownToNetworkNotification(-1L);
        this.blockMessaging.sendNewestBlockKnownToNetwork(newestBlockNotification);

        // LiveStreamPublisherManager
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
    }

    @Test
    @DisplayName("Historical & Recent Backfill - On-demand while historical (autonomous) backfill is running")
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
        createTestBlockNodeSourcesFile(backfillSource, backfillSourcePath);

        // BN mock server
        HistoricalBlockFacility blockFacilityServer = getHistoricalBlockFacility(0, 200);
        testBlockNodeServers.add(new TestBlockNodeServer(config.port(), blockFacilityServer));

        // config override for test
        final Map<String, String> configOverride = BackfillConfigBuilder.NewBuilder()
                .backfillSourcePath(backfillSourcePath)
                .initialDelay(100) // start quickly
                .scanInterval(500000) // scan every 500 seconds
                .build();

        // create a historical block facility for the plugin (should have a GAP)
        final HistoricalBlockFacility historicalBlockFacility = getHistoricalBlockFacility(50, 100);

        // start block-node with blocks from 50 to 100
        // needs to backfilled from 0 to 50 autonomously
        // and then on-demand from 101 to 200
        start(new BackfillPlugin(), historicalBlockFacility, configOverride);

        // 3 latches, one for making sure the autonomous backfill is mid-way through
        // one for the total backfill from 0 to 200
        // and one for the on-demand backfill from 501 to 200
        CountDownLatch latch1 = new CountDownLatch(10); // mid-way through the autonomous backfill
        CountDownLatch latchHistorical = new CountDownLatch(50); // historical blocks from 0 to 49
        CountDownLatch latchLive = new CountDownLatch(100); // live blocks from 101 to 200

        // register the backfill handler
        registerDefaultTestBackfillHandler();
        // register the verification handler
        this.blockMessaging.registerBlockNotificationHandler(
                new BlockNotificationHandler() {
                    @Override
                    public void handleVerification(VerificationNotification notification) {
                        blockNodeContext
                                .blockMessaging()
                                .sendBlockPersisted(new PersistedNotification(
                                        notification.blockNumber(), true, 10, notification.source()));
                        latch1.countDown();
                        if (notification.blockNumber() < 50) {
                            latchHistorical.countDown(); // Count down for historical blocks
                        } else if (notification.blockNumber() >= 100) {
                            latchLive.countDown(); // Count down for live blocks
                        }
                    }
                },
                false,
                "test-backfill-handler");

        boolean startAutonomous = latch1.await(1, TimeUnit.MINUTES); // Wait until latch1.countDown() is called
        assertTrue(startAutonomous, "Should have started on-demand backfill while autonomous backfill is running");
        // Trigger the on-demand backfill by sending a NewestBlockKnownToNetworkNotification
        NewestBlockKnownToNetworkNotification newestBlockNotification = new NewestBlockKnownToNetworkNotification(200L);
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
                150,
                blockMessaging.getSentPersistedNotifications().size(),
                "Should have sent 150 persisted notifications");
        assertEquals(
                150,
                blockMessaging.getSentVerificationNotifications().size(),
                "Should have sent 150 verification notifications");
    }

    @Test
    @DisplayName(
            "Historical & Recent Backfill - On-demand while historical (autonomous) backfill is running no ext trigger")
    void testBackfillOnDemandWhileAutonomousBackfillRunningRecent() throws InterruptedException {
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
        createTestBlockNodeSourcesFile(backfillSource, backfillSourcePath);

        // BN mock server
        HistoricalBlockFacility blockFacilityServer = getHistoricalBlockFacility(0, 200);
        testBlockNodeServers.add(new TestBlockNodeServer(config.port(), blockFacilityServer));

        // config override for test
        final Map<String, String> configOverride = BackfillConfigBuilder.NewBuilder()
                .backfillSourcePath(backfillSourcePath)
                .initialDelay(100) // start quickly
                .scanInterval(500000) // scan every 500 seconds
                .build();

        // create a historical block facility for the plugin (should have a GAP)
        final HistoricalBlockFacility historicalBlockFacility = getHistoricalBlockFacility(50, 100);

        // start block-node with blocks from 50 to 100
        // needs to backfilled from 0 to 50 autonomously
        // and then on-demand from 101 to 200
        start(new BackfillPlugin(), historicalBlockFacility, configOverride);

        // 3 latches, one for making sure the autonomous backfill is mid-way through
        // one for the total backfill from 0 to 200
        // and one for the on-demand backfill from 501 to 200
        CountDownLatch latch1 = new CountDownLatch(10); // mid-way through the autonomous backfill
        CountDownLatch latchHistorical = new CountDownLatch(50); // historical blocks from 0 to 49
        CountDownLatch latchLive = new CountDownLatch(100); // live blocks from 101 to 200

        // register the backfill handler
        registerDefaultTestBackfillHandler();
        // register the verification handler
        this.blockMessaging.registerBlockNotificationHandler(
                new BlockNotificationHandler() {
                    @Override
                    public void handleVerification(VerificationNotification notification) {
                        blockNodeContext
                                .blockMessaging()
                                .sendBlockPersisted(new PersistedNotification(
                                        notification.blockNumber(), true, 10, notification.source()));
                        latch1.countDown();
                        if (notification.blockNumber() < 50) {
                            latchHistorical.countDown(); // Count down for historical blocks
                        } else if (notification.blockNumber() >= 100) {
                            latchLive.countDown(); // Count down for live blocks
                        }
                    }
                },
                false,
                "test-backfill-handler");

        boolean startAutonomous = latch1.await(1, TimeUnit.MINUTES); // Wait until latch1.countDown() is called
        assertTrue(startAutonomous, "Should have started on-demand backfill while autonomous backfill is running");

        // No external trigger the backfill on-demand should receive NewestBlockKnownToNetworkNotification from
        // LiveStreamPublisherManager
        NewestBlockKnownToNetworkNotification newestBlockNotification = new NewestBlockKnownToNetworkNotification(-1L);
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
                150,
                blockMessaging.getSentPersistedNotifications().size(),
                "Should have sent 150 persisted notifications");
        assertEquals(
                150,
                blockMessaging.getSentVerificationNotifications().size(),
                "Should have sent 150 verification notifications");
    }

    @Test
    @DisplayName("Historical & Recent Backfill - Autonomous, GAP available within 2 different backfill sources")
    void testBackfillPartialAvailableSourcesForGap() throws InterruptedException {

        // Set up 2 backfill sources with two nodes, one primary and one secondary
        // primary node will have blocks from 0 to 100
        // secondary node will have blocks from 50 to 150
        BackfillSourceConfig backfillSourceConfig = BackfillSourceConfig.newBuilder()
                .address("localhost")
                .port(40841)
                .priority(2)
                .build();
        BackfillSourceConfig secondaryBackfillSourceConfig = BackfillSourceConfig.newBuilder()
                .address("localhost")
                .port(40842)
                .priority(1)
                .build();
        BackfillSource backfillSource = BackfillSource.newBuilder()
                .nodes(backfillSourceConfig, secondaryBackfillSourceConfig)
                .build();
        // Create a temporary file for the backfill source configuration
        String backfillSourcePath = testTempDir + "/backfill-source-7.json";
        createTestBlockNodeSourcesFile(backfillSource, backfillSourcePath);
        // create storage for source 1
        final SimpleInMemoryHistoricalBlockFacility storage1 = getHistoricalBlockFacility(0, 101);

        // create storage for source 2
        final SimpleInMemoryHistoricalBlockFacility storage2 = getHistoricalBlockFacility(50, 150);

        // start block-node mocks
        testBlockNodeServers.add(new TestBlockNodeServer(backfillSourceConfig.port(), storage1));
        testBlockNodeServers.add(new TestBlockNodeServer(secondaryBackfillSourceConfig.port(), storage2));

        // config override
        Map<String, String> config = BackfillConfigBuilder.NewBuilder()
                .backfillSourcePath(backfillSourcePath)
                .initialDelay(100) // start quickly
                .scanInterval(500000) // scan every 500 seconds
                .build();

        // create a historical block facility for the plugin (should have a GAP from 0 to 124)
        final SimpleInMemoryHistoricalBlockFacility historicalBlockFacility = getHistoricalBlockFacility(125, 150);

        // start with plugin configuration
        start(new BackfillPlugin(), historicalBlockFacility, config);

        CountDownLatch backfillLatch = new CountDownLatch(125); // 0 to 124 inclusive, so 125 blocks

        // register the backfill handler
        registerDefaultTestBackfillHandler();
        // register the verification handler
        this.blockMessaging.registerBlockNotificationHandler(
                new BlockNotificationHandler() {
                    @Override
                    public void handleVerification(VerificationNotification notification) {
                        blockNodeContext
                                .blockMessaging()
                                .sendBlockPersisted(new PersistedNotification(
                                        notification.blockNumber(), true, 10, notification.source()));
                        backfillLatch.countDown();

                        // Add the block number to the historical block facility's available blocks
                        // since next iteration we will only fill remaining blocks
                        SimpleBlockRangeSet newRange =
                                ((SimpleBlockRangeSet) historicalBlockFacility.availableBlocks());
                        newRange.add(notification.blockNumber());
                        historicalBlockFacility.setTemporaryAvailableBlocks(newRange);
                    }
                },
                false,
                "test-backfill-handler");

        boolean backfillSuccess =
                backfillLatch.await(2, TimeUnit.MINUTES); // Wait until countDownLatch.countDown() is called

        // Continue with your assertions or test logic/BlockItems blockItems = mock(BlockItems.class);
        //        assertTrue(backfillSuccess);
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
    }

    @Test
    @DisplayName(
            "Historical & Recent Backfill - Autonomous, GAP available within 2 different backfill sources no ext trigger")
    void testBackfillPartialAvailableSourcesForRecentGap() throws InterruptedException {

        // Set up 2 backfill sources with two nodes, one primary and one secondary
        // primary node will have blocks from 0 to 100
        // secondary node will have blocks from 50 to 150
        BackfillSourceConfig backfillSourceConfig = BackfillSourceConfig.newBuilder()
                .address("localhost")
                .port(40841)
                .priority(2)
                .build();
        BackfillSourceConfig secondaryBackfillSourceConfig = BackfillSourceConfig.newBuilder()
                .address("localhost")
                .port(40842)
                .priority(1)
                .build();
        BackfillSource backfillSource = BackfillSource.newBuilder()
                .nodes(backfillSourceConfig, secondaryBackfillSourceConfig)
                .build();
        // Create a temporary file for the backfill source configuration
        String backfillSourcePath = testTempDir + "/backfill-source-7.json";
        createTestBlockNodeSourcesFile(backfillSource, backfillSourcePath);
        // create storage for source 1
        final SimpleInMemoryHistoricalBlockFacility storage1 = getHistoricalBlockFacility(0, 100);

        // create storage for source 2
        final SimpleInMemoryHistoricalBlockFacility storage2 = getHistoricalBlockFacility(50, 150);

        // start block-node mocks
        testBlockNodeServers.add(new TestBlockNodeServer(backfillSourceConfig.port(), storage1));
        testBlockNodeServers.add(new TestBlockNodeServer(secondaryBackfillSourceConfig.port(), storage2));

        // config override
        Map<String, String> config = BackfillConfigBuilder.NewBuilder()
                .backfillSourcePath(backfillSourcePath)
                .initialDelay(100) // start quickly
                .scanInterval(500000) // scan every 500 seconds
                .build();

        // create a historical block facility for the plugin (should have a GAP from 0 to 62, and 88 to 125)
        final SimpleInMemoryHistoricalBlockFacility historicalBlockFacility = getHistoricalBlockFacility(62, 87);

        // start with plugin configuration
        start(new BackfillPlugin(), historicalBlockFacility, config);

        CountDownLatch backfillLatch = new CountDownLatch(125); // 0 to 124 inclusive, so 125 blocks

        // register the backfill handler
        registerDefaultTestBackfillHandler();
        // register the verification handler
        this.blockMessaging.registerBlockNotificationHandler(
                new BlockNotificationHandler() {
                    @Override
                    public void handleVerification(VerificationNotification notification) {
                        blockNodeContext
                                .blockMessaging()
                                .sendBlockPersisted(new PersistedNotification(
                                        notification.blockNumber(), true, 10, notification.source()));
                        backfillLatch.countDown();

                        // Add the block number to the historical block facility's available blocks
                        // since next iteration we will only fill remaining blocks
                        SimpleBlockRangeSet newRange =
                                ((SimpleBlockRangeSet) historicalBlockFacility.availableBlocks());
                        newRange.add(notification.blockNumber());
                        historicalBlockFacility.setTemporaryAvailableBlocks(newRange);
                    }
                },
                false,
                "test-backfill-handler");

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
    }

    private void createTestBlockNodeSourcesFile(BackfillSource backfillSource, String configPath) {
        String jsonString = BackfillSource.JSON.toJSON(backfillSource);
        // Write the JSON string to the specified file path
        try {
            java.nio.file.Files.write(java.nio.file.Paths.get(configPath), jsonString.getBytes());
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to write config to file: " + configPath, e);
        }
    }

    private SimpleInMemoryHistoricalBlockFacility getHistoricalBlockFacility(long startBlock, long endBlock) {
        // Create a new historical block facility with the specified start and end blocks
        SimpleInMemoryHistoricalBlockFacility historicalBlockFacility = new SimpleInMemoryHistoricalBlockFacility();
        for (long i = startBlock; i <= endBlock; i++) {
            final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createSimpleBlockUnparsedWithNumber(i);
            historicalBlockFacility.handleBlockItemsReceived(new BlockItems(List.of(block), i), false);
        }
        return historicalBlockFacility;
    }

    private void registerDefaultTestBackfillHandler() {
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

    private void registerDefaultTestVerificationHandler(CountDownLatch countDownLatch) {
        this.blockMessaging.registerBlockNotificationHandler(
                new BlockNotificationHandler() {
                    @Override
                    public void handleVerification(VerificationNotification notification) {
                        blockNodeContext
                                .blockMessaging()
                                .sendBlockPersisted(new PersistedNotification(
                                        notification.blockNumber(), true, 10, notification.source()));
                        countDownLatch.countDown();
                    }
                },
                false,
                "test-backfill-handler");
    }

    private static class BackfillConfigBuilder {

        // Fields with default values
        private String backfillSourcePath;
        private int fetchBatchSize = 10;
        private int delayBetweenBatches = 100;
        private int initialDelay = 500;
        private int initialRetryDelay = 500;
        private int maxRetries = 3;
        private int scanIntervalMs = 60000; // 60 seconds
        private long startBlock = 0L;
        private long endBlock = -1L; // -1 means no end block, backfill until the latest block
        private int perBlockProcessingTimeout = 500; // half second
        private int noActivityNotificationIntervalSeconds = 1;

        private BackfillConfigBuilder() {
            // private to force use of NewBuilder()
        }

        public static BackfillConfigBuilder NewBuilder() {
            return new BackfillConfigBuilder();
        }

        public BackfillConfigBuilder backfillSourcePath(String path) {
            this.backfillSourcePath = path;
            return this;
        }

        public BackfillConfigBuilder fetchBatchSize(int value) {
            this.fetchBatchSize = value;
            return this;
        }

        public BackfillConfigBuilder delayBetweenBatches(int value) {
            this.delayBetweenBatches = value;
            return this;
        }

        public BackfillConfigBuilder initialDelay(int value) {
            this.initialDelay = value;
            return this;
        }

        public BackfillConfigBuilder initialRetryDelay(int value) {
            this.initialRetryDelay = value;
            return this;
        }

        public BackfillConfigBuilder maxRetries(int value) {
            this.maxRetries = value;
            return this;
        }

        public BackfillConfigBuilder scanInterval(int value) {
            this.scanIntervalMs = value;
            return this;
        }

        public BackfillConfigBuilder startBlock(long value) {
            this.startBlock = value;
            return this;
        }

        public BackfillConfigBuilder endBlock(long value) {
            this.endBlock = value;
            return this;
        }

        public BackfillConfigBuilder perBlockProcessingTimeout(int value) {
            this.perBlockProcessingTimeout = value;
            return this;
        }

        public BackfillConfigBuilder noActivityNotificationIntervalSeconds(int value) {
            this.noActivityNotificationIntervalSeconds = value;
            return this;
        }

        public Map<String, String> build() {
            if (backfillSourcePath == null || backfillSourcePath.isBlank()) {
                throw new IllegalStateException("backfillSourcePath is required");
            }

            Map backfillConfig = new HashMap(Map.of(
                    "backfill.blockNodeSourcesPath", backfillSourcePath,
                    "backfill.fetchBatchSize", String.valueOf(fetchBatchSize),
                    "backfill.delayBetweenBatches", String.valueOf(delayBetweenBatches),
                    "backfill.initialDelay", String.valueOf(initialDelay),
                    "backfill.initialRetryDelay", String.valueOf(initialRetryDelay),
                    "backfill.maxRetries", String.valueOf(maxRetries),
                    "backfill.scanInterval", String.valueOf(scanIntervalMs),
                    "backfill.startBlock", String.valueOf(startBlock),
                    "backfill.endBlock", String.valueOf(endBlock),
                    "backfill.perBlockProcessingTimeout", String.valueOf(perBlockProcessingTimeout)));

            backfillConfig.put(
                    "producer.noActivityNotificationIntervalSeconds",
                    String.valueOf(noActivityNotificationIntervalSeconds));

            return backfillConfig;
        }
    }
}
