// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
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
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.hiero.block.api.BlockNodeVersions;
import org.hiero.block.api.BlockNodeVersions.PluginVersion;
import org.hiero.block.api.RosterEntry;
import org.hiero.block.api.TssData;
import org.hiero.block.api.TssRoster;
import org.hiero.block.node.app.config.state.ApplicationStateConfig;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.base.ranges.ConcurrentLongRangeSet;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceLoaderFunction;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
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
        when(plugin.configDataTypes()).thenReturn(List.of());
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
        try {
            Files.deleteIfExists(Path.of("build/tmp/data/block/node/app-state-data.json"));
        } catch (Exception e) {
            // ignore
        }
        // Remove the RSA file written by addressBookPersistenceRoundTrip so subsequent tests
        // start with a null address book rather than inheriting persisted state.
        try {
            Files.deleteIfExists(Path.of("build/resources/test/data/config/rsa-bootstrap-roster.json"));
        } catch (Exception e) {
            // ignore
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
                for (BlockNodePlugin plugin : loadedPlugins) {
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

    private static class TestPlugin implements BlockNodePlugin {
        private final AtomicInteger contextUpdated = new AtomicInteger(0);
        private volatile CountDownLatch latch = new CountDownLatch(0);

        /** Call before the action under test to set how many `onContextUpdate` calls are expected. */
        void expectContextUpdates(final int count) {
            latch = new CountDownLatch(count);
        }

        /**
         * Blocks until `onContextUpdate` has been called the expected number of times, or the
         * timeout elapses (in which case the test fails).
         */
        void awaitContextUpdates(final long timeoutSeconds) throws InterruptedException {
            assertTrue(
                    latch.await(timeoutSeconds, TimeUnit.SECONDS),
                    "onContextUpdate was not called within " + timeoutSeconds + "s");
        }

        int getContextUpdated() {
            return contextUpdated.get();
        }

        @Override
        public void onContextUpdate(final BlockNodeContext context) {
            contextUpdated.incrementAndGet();
            latch.countDown();
        }
    }

    /**
     * Test ApplicationStateFacility.
     */
    @Test
    @DisplayName("Test ApplicationStateFacility")
    void testApplicationStateFacility() throws IOException, InterruptedException {
        final ServiceLoaderFunction serviceLoaderFunction = new ServiceLoaderFunction();
        final BlockNodeApp blockNodeApp = new BlockNodeApp(serviceLoaderFunction, false);
        final TestPlugin testPlugin = new TestPlugin();

        // start the ApplicationStateFacility manually as blockNodeApp.start() is not being called
        blockNodeApp.startApplicationStateFacility();

        blockNodeApp.loadedPlugins.add(testPlugin);
        testPlugin.expectContextUpdates(1);

        blockNodeApp.updateTssData(null);
        blockNodeApp.updateTssData(
                buildTssData(Bytes.fromHex("040506"), Bytes.fromHex("010203"), 1, 2, Bytes.fromHex("070809"), 200, 50));
        TssData tssData =
                buildTssData(Bytes.fromHex("040506"), Bytes.fromHex("010203"), 1, 2, Bytes.fromHex("070809"), 100, 50);
        blockNodeApp.updateTssData(tssData);
        // wait for the ApplicationStateFacility scanner to pick up the update
        testPlugin.awaitContextUpdates(5);

        assertEquals(1, testPlugin.getContextUpdated());

        // stop the ApplicationStateFacility manually as blockNodeApp.shutdown() is not being called
        blockNodeApp.stopApplicationStateFacility();
    }

    /**
     * Test ApplicationStateFacility load failure.
     */
    @Test
    @DisplayName("should not fail on bad TssData file")
    void testApplicationStateFacilityBadFile() throws IOException, InterruptedException {
        final ServiceLoaderFunction serviceLoaderFunction = new ServiceLoaderFunction();
        final BlockNodeApp blockNodeApp = new BlockNodeApp(serviceLoaderFunction, false);
        final Path appStateDataFilePath = blockNodeApp
                .blockNodeContext
                .configuration()
                .getConfigData(ApplicationStateConfig.class)
                .dataFilePath();

        Files.deleteIfExists(appStateDataFilePath);
        Files.createFile(appStateDataFilePath);

        // start the ApplicationStateFacility manually as blockNodeApp.start() is not being called
        blockNodeApp.startApplicationStateFacility();

        assertNull(blockNodeApp.blockNodeContext.tssData());
    }

    /**
     * Test ApplicationStateFacility persistence.
     */
    @Test
    @DisplayName("should persist and load TssData")
    void testApplicationStateFacilityPersistence() throws IOException, InterruptedException {
        final ServiceLoaderFunction serviceLoaderFunction = new ServiceLoaderFunction();
        final BlockNodeApp blockNodeApp = new BlockNodeApp(serviceLoaderFunction, false);
        // start the ApplicationStateFacility manually as blockNodeApp.start() is not being called
        blockNodeApp.startApplicationStateFacility();
        // Register a test plugin so we can await the scanner's onContextUpdate callback.
        final TestPlugin testPlugin = new TestPlugin();
        blockNodeApp.loadedPlugins.add(testPlugin);
        testPlugin.expectContextUpdates(1);
        // update the tssData which should persist to disk
        TssData tssData =
                buildTssData(Bytes.fromHex("010203"), Bytes.fromHex("040506"), 1, 2, Bytes.fromHex("070809"), 50, 100);
        blockNodeApp.updateTssData(tssData);
        // wait for the scanner to process and persist the update
        testPlugin.awaitContextUpdates(5);

        // create a new BlockNodeApp which will load the persisted TssData
        final BlockNodeApp blockNodeApp2 = new BlockNodeApp(serviceLoaderFunction, false);
        // startApplicationStateFacility loads state synchronously before the scheduler starts,
        // so no additional waiting is needed after this call.
        blockNodeApp2.startApplicationStateFacility();

        TssData tssData1 = blockNodeApp2.blockNodeContext.tssData();
        assertNotNull(tssData1);
        assertEquals(tssData.ledgerId(), tssData1.ledgerId());
        assertEquals(tssData.wrapsVerificationKey(), tssData1.wrapsVerificationKey());

        RosterEntry roster = tssData.currentRoster().rosterEntries().getFirst();
        RosterEntry roster1 = tssData1.currentRoster().rosterEntries().getFirst();

        assertEquals(roster.nodeId(), roster1.nodeId());
        assertEquals(roster.weight(), roster1.weight());
        assertEquals(roster.schnorrPublicKey(), roster1.schnorrPublicKey());

        // stop the ApplicationStateFacility manually as shutdown() is not being called
        blockNodeApp2.stopApplicationStateFacility();
        // stop the ApplicationStateFacility manually as shutdown() is not being called
        blockNodeApp.stopApplicationStateFacility();
    }

    /**
     * validateAddressBook rejects books with no entries or only blank RSA keys.
     */
    @Test
    @DisplayName("validateAddressBook rejects empty book and all-blank RSA keys")
    void validateAddressBookRejectsInvalidBooks() {
        // empty node list
        assertThrows(
                IllegalStateException.class,
                () -> BlockNodeApp.validateAddressBook(
                        NodeAddressBook.newBuilder().build(), "test-empty"),
                "Empty address book must throw");

        // all entries have blank rsaPubKey
        final NodeAddressBook allBlank = NodeAddressBook.newBuilder()
                .nodeAddress(NodeAddress.newBuilder().nodeId(0).rsaPubKey("").build())
                .build();
        assertThrows(
                IllegalStateException.class,
                () -> BlockNodeApp.validateAddressBook(allBlank, "test-all-blank"),
                "Book with only blank RSA keys must throw");
    }

    /**
     * validateAddressBook accepts a book with at least one non-blank RSA key.
     */
    @Test
    @DisplayName("validateAddressBook accepts book with at least one non-blank RSA key")
    void validateAddressBookAcceptsValidBook() throws NoSuchAlgorithmException {
        final KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(2048);
        final KeyPair kp = kpg.generateKeyPair();
        final String hexKey = HexFormat.of().formatHex(kp.getPublic().getEncoded());
        final NodeAddressBook valid = NodeAddressBook.newBuilder()
                .nodeAddress(
                        NodeAddress.newBuilder().nodeId(0).rsaPubKey(hexKey).build())
                .build();
        assertDoesNotThrow(() -> BlockNodeApp.validateAddressBook(valid, "test-valid"));
    }

    /**
     * updateAddressBook with null input is a no-op — context stays unchanged.
     */
    @Test
    @DisplayName("updateAddressBook with null input is a no-op")
    void updateAddressBookNullIsNoOp() throws IOException {
        final ServiceLoaderFunction serviceLoaderFunction = new ServiceLoaderFunction();
        final BlockNodeApp app = new BlockNodeApp(serviceLoaderFunction, false);
        app.startApplicationStateFacility();
        final NodeAddressBook before = app.blockNodeContext.nodeAddressBook();

        app.updateAddressBook(null);

        assertEquals(before, app.blockNodeContext.nodeAddressBook(), "null update must not change the address book");
        app.stopApplicationStateFacility();
    }

    /**
     * updateAddressBook with an empty or all-blank-key book is rejected; context stays unchanged.
     */
    @Test
    @DisplayName("updateAddressBook rejects invalid books and leaves context unchanged")
    void updateAddressBookInvalidBookIsRejected() throws IOException {
        final ServiceLoaderFunction serviceLoaderFunction = new ServiceLoaderFunction();
        final BlockNodeApp app = new BlockNodeApp(serviceLoaderFunction, false);
        app.startApplicationStateFacility();

        final NodeAddressBook emptyBook = NodeAddressBook.newBuilder().build();
        app.updateAddressBook(emptyBook);
        assertNull(app.blockNodeContext.nodeAddressBook(), "Empty book must be rejected");

        final NodeAddressBook allBlank = NodeAddressBook.newBuilder()
                .nodeAddress(NodeAddress.newBuilder().nodeId(0).rsaPubKey("").build())
                .build();
        app.updateAddressBook(allBlank);
        assertNull(app.blockNodeContext.nodeAddressBook(), "All-blank-key book must be rejected");

        app.stopApplicationStateFacility();
    }

    /**
     * When the RSA bootstrap file does not exist the address book is null after startup.
     */
    @Test
    @DisplayName("loadApplicationState with missing RSA file leaves address book null")
    void loadApplicationStateMissingRsaFileAddressBookNull() throws IOException {
        final ServiceLoaderFunction serviceLoaderFunction = new ServiceLoaderFunction();
        final BlockNodeApp app = new BlockNodeApp(serviceLoaderFunction, false);
        final Path rsaPath = app.blockNodeContext
                .configuration()
                .getConfigData(ApplicationStateConfig.class)
                .rsaBootstrapFilePath();
        Files.deleteIfExists(rsaPath);

        app.startApplicationStateFacility();

        assertNull(app.blockNodeContext.nodeAddressBook(), "Missing RSA file must leave address book null");
        app.stopApplicationStateFacility();
    }

    /**
     * When the RSA bootstrap file exists but is corrupt, startApplicationStateFacility throws.
     */
    @Test
    @DisplayName("loadApplicationState with corrupt RSA file throws IllegalStateException")
    void loadApplicationStateCorruptRsaFileThrows() throws IOException {
        final ServiceLoaderFunction serviceLoaderFunction = new ServiceLoaderFunction();
        final BlockNodeApp app = new BlockNodeApp(serviceLoaderFunction, false);
        final Path rsaPath = app.blockNodeContext
                .configuration()
                .getConfigData(ApplicationStateConfig.class)
                .rsaBootstrapFilePath();
        Files.createDirectories(rsaPath.getParent());
        Files.write(rsaPath, new byte[] {(byte) 0xFF, (byte) 0xFE, 0x00});

        assertThrows(
                IllegalStateException.class,
                app::startApplicationStateFacility,
                "Corrupt RSA file must throw IllegalStateException");
        app.stopApplicationStateFacility();
    }

    /**
     * updateAddressBook queues the book for the scanner and context is updated on the next tick.
     */
    @Test
    @DisplayName("updateAddressBook queues book; scanner picks it up and notifies plugins")
    void updateAddressBookQueuesForScanner() throws IOException, InterruptedException, NoSuchAlgorithmException {
        final ServiceLoaderFunction serviceLoaderFunction = new ServiceLoaderFunction();
        final BlockNodeApp app = new BlockNodeApp(serviceLoaderFunction, false);
        final TestPlugin testPlugin = new TestPlugin();
        app.startApplicationStateFacility();
        app.loadedPlugins.add(testPlugin);

        final int updatesBeforeCall = testPlugin.getContextUpdated();
        testPlugin.expectContextUpdates(1);

        final KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(2048);
        final KeyPair kp = kpg.generateKeyPair();
        final String hexKey = HexFormat.of().formatHex(kp.getPublic().getEncoded());
        final NodeAddressBook book = NodeAddressBook.newBuilder()
                .nodeAddress(
                        NodeAddress.newBuilder().nodeId(1).rsaPubKey(hexKey).build())
                .build();
        app.updateAddressBook(book);

        // wait for the scanner to pick up the pending address book and call onContextUpdate
        testPlugin.awaitContextUpdates(5);

        assertEquals(
                updatesBeforeCall + 1,
                testPlugin.getContextUpdated(),
                "onContextUpdate must be called once for the address book update");
        assertNotNull(app.blockNodeContext.nodeAddressBook());
        assertEquals(1, app.blockNodeContext.nodeAddressBook().nodeAddress().size());
        assertEquals(
                hexKey,
                app.blockNodeContext.nodeAddressBook().nodeAddress().getFirst().rsaPubKey());

        app.stopApplicationStateFacility();
    }

    /**
     * Address book persisted by updateAddressBook is reloaded by a fresh BlockNodeApp.
     */
    @Test
    @DisplayName("address book is persisted and reloaded on next startup")
    void addressBookPersistenceRoundTrip() throws IOException, InterruptedException, NoSuchAlgorithmException {
        final ServiceLoaderFunction serviceLoaderFunction = new ServiceLoaderFunction();
        final BlockNodeApp app = new BlockNodeApp(serviceLoaderFunction, false);
        final Path rsaPath = app.blockNodeContext
                .configuration()
                .getConfigData(org.hiero.block.node.app.config.state.ApplicationStateConfig.class)
                .rsaBootstrapFilePath();

        app.startApplicationStateFacility();
        final TestPlugin testPlugin = new TestPlugin();
        app.loadedPlugins.add(testPlugin);
        testPlugin.expectContextUpdates(1);

        final KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(2048);
        final KeyPair kp = kpg.generateKeyPair();
        final String hexKey = HexFormat.of().formatHex(kp.getPublic().getEncoded());

        final NodeAddressBook book = NodeAddressBook.newBuilder()
                .nodeAddress(
                        NodeAddress.newBuilder().nodeId(7).rsaPubKey(hexKey).build())
                .build();
        app.updateAddressBook(book);
        // wait for scanner to process and persist the address book
        testPlugin.awaitContextUpdates(5);
        app.stopApplicationStateFacility();

        assertTrue(Files.exists(rsaPath), "RSA file must exist after persistence");

        // second app loads from persisted file
        final BlockNodeApp app2 = new BlockNodeApp(serviceLoaderFunction, false);
        app2.startApplicationStateFacility();

        final NodeAddressBook loaded = app2.blockNodeContext.nodeAddressBook();
        assertNotNull(loaded, "Persisted address book must be loaded on restart");
        assertEquals(1, loaded.nodeAddress().size());
        assertEquals(hexKey, loaded.nodeAddress().getFirst().rsaPubKey());

        app2.stopApplicationStateFacility();
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
        TssRoster tssRoster = TssRoster.newBuilder()
                .rosterEntries(rosterEntry)
                .validFromBlock(rosterValidFromBlock)
                .build();
        return TssData.newBuilder()
                .ledgerId(ledgerId)
                .wrapsVerificationKey(wrapsVerificationKey)
                .currentRoster(tssRoster)
                .validFromBlock(validFromBlock)
                .build();
    }
}
