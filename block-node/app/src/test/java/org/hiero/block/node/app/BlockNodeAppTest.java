// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPairGenerator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.hiero.block.api.BlockNodeVersions;
import org.hiero.block.api.BlockNodeVersions.PluginVersion;
import org.hiero.block.api.BlockRange;
import org.hiero.block.api.RangedAddressBookHistory;
import org.hiero.block.api.RosterEntry;
import org.hiero.block.api.TssData;
import org.hiero.block.api.TssRoster;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.app.state.ApplicationStateConfig;
import org.hiero.block.node.base.ranges.ConcurrentLongRangeSet;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.ServiceLoaderFunction;
import org.hiero.block.node.spi.blockmessaging.AddressBookHistoryNotification;
import org.hiero.block.node.spi.blockmessaging.ApplicationStateNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.AvailableBlocksNotification;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.StoredBlocksNotification;
import org.hiero.block.node.spi.blockmessaging.TssDataNotification;
import org.hiero.block.node.spi.health.HealthFacility.State;
import org.hiero.block.node.spi.historicalblocks.BlockProviderPlugin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.MockitoAnnotations;

/**
 * Unit tests for the BlockNodeApp class.
 */
class BlockNodeAppTest {
    BlockNodePlugin plugin1;
    BlockNodePlugin plugin2;
    BlockProviderPlugin providerPlugin1;
    BlockProviderPlugin providerPlugin2;

    private BlockNodeApp blockNodeApp;
    private TestBlockMessagingFacility mockBlockMessagingFacility;

    /**
     * Create a mocked plugin of the given class.
     *
     * @param num The instance number of the plugin to create. This is used to differentiate different instances of the
     *            plugin.
     * @param pluginClass The class of the plugin to create. This is used to create the plugin.
     * @param <T> The type of the plugin to create. This is used to create the plugin.
     * @return The mocked plugin instance.
     */
    private static <T extends BlockNodePlugin> T createMockedPlugin(int num, Class<T> pluginClass) {
        T plugin = mock(pluginClass);
        when(plugin.name()).thenReturn(pluginClass.getSimpleName() + " " + num);
        return plugin;
    }

    @BeforeEach
    void setUp() throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        MockitoAnnotations.openMocks(this);
        // minimal plugin mocks
        plugin1 = createMockedPlugin(1, BlockNodePlugin.class);
        plugin2 = createMockedPlugin(2, BlockNodePlugin.class);
        providerPlugin1 = createMockedPlugin(1, BlockProviderPlugin.class);
        when(providerPlugin1.availableBlocks()).thenReturn(new ConcurrentLongRangeSet(0, 10));
        when(providerPlugin1.defaultPriority()).thenReturn(1);
        providerPlugin2 = createMockedPlugin(2, BlockProviderPlugin.class);
        when(providerPlugin2.availableBlocks()).thenReturn(new ConcurrentLongRangeSet(20, 30));
        when(providerPlugin2.defaultPriority()).thenReturn(2);
        // mock the messaging facility
        mockBlockMessagingFacility = spy(new TestBlockMessagingFacility());
        // create custom service loader function
        ServiceLoaderFunction serviceLoaderFunction = new ServiceLoaderFunction() {
            @SuppressWarnings("unchecked")
            @Override
            public <C> Stream<? extends C> loadServices(Class<C> serviceClass) {
                if (serviceClass == BlockNodePlugin.class) {
                    return Stream.of(plugin1, plugin2).map(service -> (C) service);
                } else if (serviceClass == BlockProviderPlugin.class) {
                    return Stream.of(providerPlugin1, providerPlugin2).map(service -> (C) service);
                } else if (serviceClass == BlockMessagingFacility.class) {
                    return Stream.of(mockBlockMessagingFacility).map(service -> (C) service);
                }
                return super.loadServices(serviceClass);
            }
        };
        // now we can create the BlockNodeApp instance
        blockNodeApp = spy(new BlockNodeApp(serviceLoaderFunction, false));
    }

    @AfterEach
    void cleanup() {
        for (String file : List.of(
                "build/tmp/data/block/node/tss-bootstrap-roster.json",
                "build/resources/test/data/config/rsa-bootstrap-roster.json",
                "build/tmp/data/block/node/rsa-address-book-history.json",
                "build/tmp/data/block/node/block-ranges.json",
                "build/tmp/data/block/node/block-ranges.json.tmp")) {
            try {
                Files.deleteIfExists(Path.of(file));
            } catch (Exception e) {
                // ignore
            }
        }
    }

    @Test
    @DisplayName("Test BlockNodeApp Initialization")
    void testInitialization() {
        assertNotNull(blockNodeApp);
        assertEquals(State.STARTING, blockNodeApp.blockNodeState());
    }

    @Test
    @DisplayName("Test BlockNodeApp Shutdown")
    void testShutdown() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {}));
        assertEquals(State.STARTING, blockNodeApp.blockNodeState());
        blockNodeApp.start();
        assertEquals(State.RUNNING, blockNodeApp.blockNodeState());
        blockNodeApp.shutdown("TestClass", "TestReason");
        // check status is set to SHUTTING_DOWN
        assertEquals(State.SHUTTING_DOWN, blockNodeApp.blockNodeState());
    }

    @Test
    @DisplayName("Test BlockNodeApp Start")
    void testStart() {
        blockNodeApp.start();
        // check plugins have been started
        verify(plugin1, times(1)).init(any(), any());
        verify(plugin2, times(1)).init(any(), any());
        verify(providerPlugin1, times(1)).init(any(), any());
        verify(providerPlugin2, times(1)).init(any(), any());
        // check plugins have been started
        verify(plugin1, times(1)).start();
        verify(plugin2, times(1)).start();
        verify(providerPlugin1, times(1)).start();
        verify(providerPlugin2, times(1)).start();
        // check messaging facility has been started
        verify(mockBlockMessagingFacility, times(1)).start();
        // check health facility status is set to RUNNING
        assertEquals(State.RUNNING, blockNodeApp.blockNodeContext.serverHealth().blockNodeState());
    }

    @Test
    @DisplayName("Test main method")
    void testMain() throws IOException {
        // Attempts to start the app with some test configuration (see app-test.properties)
        assertDoesNotThrow(() -> BlockNodeApp.main(new String[] {}));
    }

    /**
     * This test aims to insure the independence of plugins by starting them in varying order.
     * Validate that starting plugins in parallel works
     */
    @Test
    @DisplayName("Test plugin startup in parallel")
    void testPluginStartupParallel() throws IOException {
        final ServiceLoaderFunction serviceLoaderFunction = new ServiceLoaderFunction();
        // Case 1: Test in parallel
        final BlockNodeApp blockNodeApp = new BlockNodeApp(serviceLoaderFunction, false);
        assertNotNull(blockNodeApp);
        startBlockNode(blockNodeApp);
    }

    /**
     * This test aims to insure the independence of plugins by starting them in varying order.
     * Test in ServiceLoader Order to make sure plugins load correctly
     */
    @Test
    @DisplayName("Test plugin startup in ServiceLoader order")
    void testPluginStartupInOrder() throws IOException {
        final ServiceLoaderFunction serviceLoaderFunction = new ServiceLoaderFunction();

        // Case 2: Start plugins in ServiceLoader order
        final BlockNodeApp blockNodeApp = new BlockNodeApp(serviceLoaderFunction, false) {
            @Override
            protected void startPlugins(List<BlockNodePlugin> plugins) {
                for (BlockNodePlugin plugin : plugins) {
                    plugin.start();
                }
            }
        };
        assertNotNull(blockNodeApp);
        startBlockNode(blockNodeApp);
    }

    /**
     * This test aims to insure the independence of plugins by starting them in varying order.
     * Test in reverse ServiceLoader Order to make sure plugins load correctly
     * This should identify any dependencies on ServiceLoader order
     */
    @Test
    @DisplayName("Test plugin startup in reverse order")
    void testPluginStartupReverseOrder() throws IOException {
        final ServiceLoaderFunction serviceLoaderFunction = new ServiceLoaderFunction();

        // Case 3: Test in reverse order returned by the service loader.
        final BlockNodeApp blockNodeApp = new BlockNodeApp(serviceLoaderFunction, false) {
            @Override
            protected void startPlugins(List<BlockNodePlugin> plugins) {
                for (BlockNodePlugin plugin : plugins.reversed()) {
                    plugin.start();
                }
            }
        };
        assertNotNull(blockNodeApp);
        startBlockNode(blockNodeApp);
    }

    /**
     * This test aims to insure the independence of plugins by starting them in varying order.
     * Use {@code Collections.shuffle()} to test a few more permutations to introduce some controlled randomness.
     * as this greatly increases the unit test time.
     */
    @Test
    @DisplayName("Test plugin startup in shuffled order")
    void testPluginStartupIndependence() throws IOException {
        final int SHUFFLE_COUNT = 100;
        final ServiceLoaderFunction serviceLoaderFunction = new ServiceLoaderFunction();

        BlockNodeApp blockNodeApp;
        // Case 4: Test in reverse order returned by the service loader.
        for (int i = 0; i < SHUFFLE_COUNT; i++) {
            blockNodeApp = new BlockNodeApp(serviceLoaderFunction, false) {
                @Override
                protected void startPlugins(List<BlockNodePlugin> plugins) {
                    final List<BlockNodePlugin> shuffledPlugins = new ArrayList<>(plugins);
                    Collections.shuffle(shuffledPlugins);
                    for (BlockNodePlugin plugin : shuffledPlugins) {
                        plugin.start();
                    }
                }
            };
            assertNotNull(blockNodeApp);
            startBlockNode(blockNodeApp);
        }
    }

    /**
     * Test the BlockNodeVersions.
     */
    @Test
    @DisplayName("Test BlockNodeVersions")
    void testBlockNodeVersions() throws IOException {
        final ServiceLoaderFunction serviceLoaderFunction = new ServiceLoaderFunction();
        final BlockNodeApp blockNodeApp = new BlockNodeApp(serviceLoaderFunction, false);
        final BlockNodeVersions blockNodeVersions = blockNodeApp.blockNodeContext.blockNodeVersions();

        final SemanticVersion blockNodeVersion = blockNodeVersions.blockNodeVersion();
        assertNotNull(blockNodeVersion);
        // This will need to be changed to 1 at some point
        assertEquals(0, blockNodeVersion.major());

        // test the stream protocol version
        final SemanticVersion streamProtocolVersion = blockNodeVersions.streamProtoVersion();
        assertNotNull(streamProtocolVersion);
        assertEquals(0, streamProtocolVersion.major());
        assertTrue(streamProtocolVersion.minor() > 70);

        // In dev, the plugins should have the same SemVer as the BlockNodeApp
        final List<PluginVersion> pluginVersions = blockNodeVersions.installedPluginVersions();
        for (PluginVersion pluginVersion : pluginVersions) {
            assertNotNull(pluginVersion.pluginId());

            final SemanticVersion softwareVersion = pluginVersion.pluginSoftwareVersion();
            assertNotNull(softwareVersion);
            assertEquals(blockNodeVersion, pluginVersion.pluginSoftwareVersion());
            // just to be sure
            assertEquals(blockNodeVersion.major(), softwareVersion.major());
            assertEquals(blockNodeVersion.minor(), softwareVersion.minor());
            assertEquals(blockNodeVersion.patch(), softwareVersion.patch());

            // every plugin should have at least one provided service
            final List<String> features = pluginVersion.pluginFeatureNames();
            // plugins default to an empty list of features
            assertTrue(pluginVersion.pluginFeatureNames().isEmpty());
        }
    }

    protected void startBlockNode(BlockNodeApp blockNodeApp) {
        assertDoesNotThrow(blockNodeApp::start);
        assertEquals(State.RUNNING, blockNodeApp.blockNodeState());
        blockNodeApp.shutdown("BlockNodeTestApp", "testPluginStartupIndependence");
        assertEquals(State.SHUTTING_DOWN, blockNodeApp.blockNodeState());
    }

    private static class TestPlugin implements BlockNodePlugin, ApplicationStateNotificationHandler {
        private final AtomicInteger notificationCount = new AtomicInteger(0);
        private volatile CountDownLatch latch = new CountDownLatch(0);

        @Override
        public String name() {
            return "TestPlugin";
        }

        volatile TssData lastTssData;
        volatile RangedAddressBookHistory lastAddressBookHistory;
        volatile List<BlockRange> lastStoredBlocks;
        volatile List<BlockRange> lastAvailableBlocks;

        /** Call before the action under test to set how many notifications are expected. */
        void expectContextUpdates(final int count) {
            notificationCount.set(0);
            latch = new CountDownLatch(count);
        }

        /**
         * Blocks until the expected number of notifications arrived, or the timeout elapses.
         */
        void awaitContextUpdates(final long timeoutSeconds) throws InterruptedException {
            assertTrue(
                    latch.await(timeoutSeconds, TimeUnit.SECONDS),
                    "ApplicationStateNotificationHandler was not called within " + timeoutSeconds + "s");
        }

        /**
         * Blocks until stored blocks satisfy the condition, or the timeout elapses.
         *
         * @return the stored blocks list when condition is met, or last known value if timeout elapses
         */
        List<BlockRange> awaitStoredBlocks(final long timeoutSeconds, final Predicate<List<BlockRange>> condition)
                throws InterruptedException {
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
            while (System.nanoTime() < deadline) {
                final List<BlockRange> current = lastStoredBlocks;
                if (current != null && condition.test(current)) {
                    return current;
                }
                Thread.sleep(50);
            }
            return lastStoredBlocks;
        }

        int getContextUpdated() {
            return notificationCount.get();
        }

        @Override
        public void handleTssDataUpdate(final TssDataNotification notification) {
            lastTssData = notification.tssData();
            notificationCount.incrementAndGet();
            latch.countDown();
        }

        @Override
        public void handleAddressBookHistoryUpdate(final AddressBookHistoryNotification notification) {
            lastAddressBookHistory = notification.rangedAddressBookHistory();
            notificationCount.incrementAndGet();
            latch.countDown();
        }

        @Override
        public void handleStoredBlocksUpdate(final StoredBlocksNotification notification) {
            lastStoredBlocks = notification.storedBlocks();
            notificationCount.incrementAndGet();
            latch.countDown();
        }

        @Override
        public void handleAvailableBlocksUpdate(final AvailableBlocksNotification notification) {
            lastAvailableBlocks = notification.availableBlocks();
            notificationCount.incrementAndGet();
            latch.countDown();
        }
    }

    /**
     * When plugins register on two different ports the app uses a single WebServer with a named socket for each port.
     */
    @Test
    @DisplayName("Two-port mode: single WebServer with named socket for the second port")
    void twoPortModeUsesSingleWebServerWithNamedSocket() throws IOException {
        final ServiceLoaderFunction twoPortLoader = new ServiceLoaderFunction() {
            @SuppressWarnings("unchecked")
            @Override
            public <C> Stream<? extends C> loadServices(Class<C> serviceClass) {
                if (serviceClass == BlockMessagingFacility.class) {
                    return Stream.of(new TestBlockMessagingFacility()).map(s -> (C) s);
                }
                if (serviceClass == BlockNodePlugin.class) {
                    BlockNodePlugin publisherPlugin = new BlockNodePlugin() {
                        @Override
                        public String name() {
                            return "TestPublisher";
                        }

                        @Override
                        public void init(BlockNodeContext context, ServiceBuilder serviceBuilder) {
                            serviceBuilder.registerHttpService("/pub", 40840, rules -> {});
                        }
                    };
                    BlockNodePlugin consumerPlugin = new BlockNodePlugin() {
                        @Override
                        public String name() {
                            return "TestConsumer";
                        }

                        @Override
                        public void init(BlockNodeContext context, ServiceBuilder serviceBuilder) {
                            serviceBuilder.registerHttpService("/cons", 40940, rules -> {});
                        }
                    };
                    return Stream.of(publisherPlugin, consumerPlugin).map(s -> (C) s);
                }
                if (serviceClass == BlockProviderPlugin.class) {
                    return Stream.empty();
                }
                return super.loadServices(serviceClass);
            }
        };
        final BlockNodeApp twoPortApp = new BlockNodeApp(twoPortLoader, false);
        assertNotNull(twoPortApp.serviceBuilder, "A single WebServer must be created even in two-port mode");
        assertEquals(2, twoPortApp.portsEnabled.size(), "Two distinct ports must be tracked");
    }

    /**
     * When all plugins register on the same port the app creates a single WebServer with no named sockets.
     */
    @Test
    @DisplayName("Single-port mode: one WebServer with no extra sockets when all plugins use the same port")
    void singlePortModeSamePortValueUsesSingleWebServer() throws IOException {
        final ServiceLoaderFunction singlePortLoader = new ServiceLoaderFunction() {
            @SuppressWarnings("unchecked")
            @Override
            public <C> Stream<? extends C> loadServices(Class<C> serviceClass) {
                if (serviceClass == BlockMessagingFacility.class) {
                    return Stream.of(new TestBlockMessagingFacility()).map(s -> (C) s);
                }
                if (serviceClass == BlockNodePlugin.class) {
                    BlockNodePlugin plugin1 = new BlockNodePlugin() {
                        @Override
                        public String name() {
                            return "TestPlugin1";
                        }

                        @Override
                        public void init(BlockNodeContext context, ServiceBuilder serviceBuilder) {
                            serviceBuilder.registerHttpService("/svc1", 40840, rules -> {});
                        }
                    };
                    BlockNodePlugin plugin2 = new BlockNodePlugin() {
                        @Override
                        public String name() {
                            return "TestPlugin2";
                        }

                        @Override
                        public void init(BlockNodeContext context, ServiceBuilder serviceBuilder) {
                            serviceBuilder.registerHttpService("/svc2", 40840, rules -> {});
                        }
                    };
                    return Stream.of(plugin1, plugin2).map(s -> (C) s);
                }
                if (serviceClass == BlockProviderPlugin.class) {
                    return Stream.empty();
                }
                return super.loadServices(serviceClass);
            }
        };
        final BlockNodeApp singlePortApp = new BlockNodeApp(singlePortLoader, false);
        assertNotNull(singlePortApp.serviceBuilder, "A single WebServer must be created");
        assertEquals(
                1,
                singlePortApp.portsEnabled.size(),
                "Only one port must be tracked when all plugins use the same port");
    }

    /// build a `TssData` object from individual fields from the `TssBootstrapConfig`
    ///
    /// @param ledgerId The ledgerId Bytes
    /// @param wrapsVerificationKey The wrapsVerificationKey Bytes
    /// @param nodeId The node id
    /// @param weight The weight
    /// @param schnorrPublicKey The schnorrPublicKey Bytes
    /// @param validFromBlock The block from which this TssData is valid
    /// @param rosterValidFromBlock The block from which this TssRoster is valid
    /// @return a `TssData` object
    private TssData buildTssData(
            Bytes ledgerId,
            Bytes wrapsVerificationKey,
            long nodeId,
            long weight,
            Bytes schnorrPublicKey,
            long validFromBlock,
            long rosterValidFromBlock) {
        RosterEntry rosterEntry = RosterEntry.newBuilder()
                .nodeId(nodeId)
                .weight(weight)
                .schnorrPublicKey(schnorrPublicKey)
                .build();
        TssRoster tssRoster = TssRoster.newBuilder().rosterEntries(rosterEntry).build();
        return TssData.newBuilder()
                .ledgerId(ledgerId)
                .wrapsVerificationKey(wrapsVerificationKey)
                .currentRoster(tssRoster)
                .validFromBlock(validFromBlock)
                .build();
    }

    /**
     * Startup dispatch ordering: a handler registered before {@link BlockNodeApp#start()}
     * (i.e. during plugin {@code init()}, which real plugins do) must still receive the notifications
     * that the application state facility dispatches while loading its state in {@code start()}.
     *
     * <p>This is not obvious: {@link ApplicationStateNotificationHandler} is non-gating, and the
     * messaging facility starts a non-gating processor at the ring buffer's <em>current</em> cursor,
     * so any event already in the ring is skipped. It works only because
     * {@link BlockNodeApp#start()} starts the messaging facility (which drains the
     * pre-registered handler list) <em>before</em> the application state facility publishes anything.
     * If that order is ever swapped, every plugin silently misses its startup state and this test fails.
     */
    @Test
    @DisplayName("handler registered before start receives startup-load notifications")
    void preRegisteredHandlerReceivesStartupLoadNotifications() throws Exception {
        final ServiceLoaderFunction serviceLoaderFunction = new ServiceLoaderFunction();
        final BlockNodeApp app = new BlockNodeApp(serviceLoaderFunction, false);
        final ApplicationStateConfig cfg =
                app.blockNodeContext.configuration().getConfigData(ApplicationStateConfig.class);

        // Persist TSS data and an RSA address book so loadApplicationState() has state to dispatch.
        final Path tssPath = cfg.tssBootstrapFilePath();
        Files.createDirectories(tssPath.getParent());
        final TssData tssData =
                buildTssData(Bytes.fromHex("0a0b0c"), Bytes.fromHex("0d0e0f"), 7, 3, Bytes.fromHex("101112"), 42, 0);
        Files.write(tssPath, TssData.JSON.toBytes(tssData).toByteArray());
        Files.deleteIfExists(cfg.rsaBootstrapFilePath());
        writeRsaBootstrapFile(cfg.rsaBootstrapFilePath());

        // Register BEFORE the facility is started, exactly as a plugin does from init().
        final TestPlugin testPlugin = new TestPlugin();
        app.blockNodeContext
                .blockMessaging()
                .registerApplicationStateNotificationHandler(testPlugin, false, testPlugin.name());
        testPlugin.expectContextUpdates(2);

        app.start();

        try {
            testPlugin.awaitContextUpdates(5);
            assertNotNull(testPlugin.lastTssData, "Pre-registered handler must receive the loaded TSS data");
            assertEquals(tssData.ledgerId(), testPlugin.lastTssData.ledgerId());
            assertNotNull(
                    testPlugin.lastAddressBookHistory,
                    "Pre-registered handler must receive the loaded address book history");
            assertEquals(1, testPlugin.lastAddressBookHistory.addressBooks().size());
        } finally {
            app.shutdown("BlockNodeAppTest", "test complete");
        }
    }

    /**
     * Startup dispatch for block providers: a provider reports its available blocks from {@code init()}
     * (as {@code BlockFileRecentPlugin} and {@code BlockFileHistoricPlugin} do), and a plugin registers
     * its handler from its own {@code init()}. Both happen in the constructor's init loop, before the
     * messaging facility is started, so the notification is published into a ring buffer with no
     * handler attached. The handler must still learn the startup block state once the application
     * state facility starts.
     */
    @Test
    @DisplayName("blocks reported during provider init reach a handler registered during init")
    void blocksReportedDuringProviderInitReachHandlerRegisteredDuringInit() throws Exception {
        final BlockProviderPlugin provider = createMockedPlugin(1, BlockProviderPlugin.class);
        when(provider.availableBlocks()).thenReturn(new ConcurrentLongRangeSet(0, 10));
        doAnswer(invocation -> {
                    final BlockNodeContext context = invocation.getArgument(0);
                    context.applicationStateFacility().updateAvailableBlocks();
                    return null;
                })
                .when(provider)
                .init(any(), any());
        final TestPlugin testPlugin = new TestPlugin() {
            @Override
            public void init(final BlockNodeContext context, final ServiceBuilder serviceBuilder) {
                context.blockMessaging().registerApplicationStateNotificationHandler(this, false, name());
            }
        };
        // one available-blocks and one stored-blocks notification
        testPlugin.expectContextUpdates(2);
        // real messaging facility, since the loss depends on its ring buffer start semantics
        final ServiceLoaderFunction serviceLoaderFunction = new ServiceLoaderFunction() {
            @SuppressWarnings("unchecked")
            @Override
            public <C> Stream<? extends C> loadServices(Class<C> serviceClass) {
                if (serviceClass == BlockProviderPlugin.class) {
                    return Stream.of(provider).map(service -> (C) service);
                } else if (serviceClass == BlockNodePlugin.class) {
                    return Stream.of(testPlugin).map(service -> (C) service);
                }
                return super.loadServices(serviceClass);
            }
        };
        final BlockNodeApp app = new BlockNodeApp(serviceLoaderFunction, false);
        app.start();

        try {
            testPlugin.awaitContextUpdates(5);
            assertEquals(List.of(new BlockRange(0L, 10L)), testPlugin.lastAvailableBlocks);
            assertEquals(List.of(new BlockRange(0L, 10L)), testPlugin.lastStoredBlocks);
        } finally {
            app.shutdown("BlockNodeAppTest", "test complete");
        }
    }

    /// Writes a valid single-book RSA bootstrap file; it needs a real RSA key to pass address book validation.
    private static void writeRsaBootstrapFile(final Path rsaPath) throws Exception {
        final KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(2048);
        final String hexKey =
                HexFormat.of().formatHex(kpg.generateKeyPair().getPublic().getEncoded());
        final NodeAddressBook book = NodeAddressBook.newBuilder()
                .nodeAddress(
                        NodeAddress.newBuilder().nodeId(1).rsaPubKey(hexKey).build())
                .build();
        Files.createDirectories(rsaPath.getParent());
        Files.write(rsaPath, NodeAddressBook.JSON.toBytes(book).toByteArray());
    }
}
