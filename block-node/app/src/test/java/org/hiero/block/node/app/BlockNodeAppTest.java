// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hedera.hapi.node.base.SemanticVersion;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.hiero.block.api.BlockNodeVersions.PluginVersion;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.base.ranges.ConcurrentLongRangeSet;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceLoaderFunction;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.health.HealthFacility.State;
import org.hiero.block.node.spi.historicalblocks.BlockProviderPlugin;
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
     * @return The mocked plugin instance.
     * @param <T> The type of the plugin to create. This is used to create the plugin.
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

        SemanticVersion blockNodeVersion =
                blockNodeApp.blockNodeContext.blockNodeVersions().blockNodeVersion();
        assertNotNull(blockNodeVersion);
        // This will need to be changed to 1 at some point
        assertEquals(0, blockNodeVersion.major());

        // In dev, the plugins should have the same SemVer as the plugins
        List<PluginVersion> pluginVersions =
                blockNodeApp.blockNodeContext.blockNodeVersions().installedPluginVersions();
        for (PluginVersion pluginVersion : pluginVersions) {
            assertNotNull(pluginVersion.pluginId());

            SemanticVersion softwareVersion = pluginVersion.pluginSoftwareVersion();
            assertNotNull(softwareVersion);
            assertEquals(blockNodeVersion, pluginVersion.pluginSoftwareVersion());
            // just to be sure
            assertEquals(blockNodeVersion.major(), softwareVersion.major());
            assertEquals(blockNodeVersion.minor(), softwareVersion.minor());
            assertEquals(blockNodeVersion.patch(), softwareVersion.patch());
        }
    }

    protected void startBlockNode(BlockNodeApp blockNodeApp) {
        assertDoesNotThrow(blockNodeApp::start);
        assertEquals(State.RUNNING, blockNodeApp.blockNodeState());
        blockNodeApp.shutdown("BlockNodeTestApp", "testPluginStartupIndependence");
        assertEquals(State.SHUTTING_DOWN, blockNodeApp.blockNodeState());
    }
}
