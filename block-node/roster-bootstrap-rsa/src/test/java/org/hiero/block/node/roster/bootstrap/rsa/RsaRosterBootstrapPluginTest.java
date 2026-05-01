// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.roster.bootstrap.rsa;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.plugintest.PluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

/**
 * Unit tests for `RsaRosterBootstrapPlugin`.
 *
 * <p>File loading and persistence are now handled by {@code BlockNodeApp}. The plugin's
 * sole responsibility is to check whether the address book was already loaded
 * ({@code context.nodeAddressBook() != null}) and, if not, to fetch it from the Mirror Node.
 *
 * <p>Tests that simulate "BlockNodeApp loaded the file" pass a pre-built
 * {@code NodeAddressBook} via the {@code preloadedAddressBook} overload of
 * {@code PluginTestBase.start()}.
 */
class RsaRosterBootstrapPluginTest
        extends PluginTestBase<RsaRosterBootstrapPlugin, BlockingExecutor, ScheduledBlockingExecutor> {

    RsaRosterBootstrapPluginTest() {
        super(
                new BlockingExecutor(new LinkedBlockingQueue<>()),
                new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
    }

    /**
     * Override tearDown to gracefully handle cases where `start()` threw before
     * the base class could initialise `metricsRegistry` (e.g. exception tests).
     */
    @Override
    @AfterEach
    public void tearDown() throws IOException {
        testThreadPoolManager.shutdownNow();
        if (blockNodeContext != null && blockNodeContext.metricRegistry() != null) {
            blockNodeContext.metricRegistry().close();
        }
    }

    // -------------------------------------------------------------------------
    // Pre-loaded address book tests (simulates BlockNodeApp loading from file)
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("Pre-loaded address book (BlockNodeApp loaded from file)")
    class PreloadedAddressBook {

        @Test
        @DisplayName("Pre-loaded book is reflected in context after start()")
        void preloadedBookIsReflectedInContext() {
            final NodeAddressBook book = buildAddressBook(3);

            start(new RsaRosterBootstrapPlugin(),
                    new SimpleInMemoryHistoricalBlockFacility(),
                    null, null, Map.of(), book);

            final NodeAddressBook loaded = blockNodeContext.nodeAddressBook();
            assertNotNull(loaded);
            assertEquals(3, loaded.nodeAddress().size());
            assertEquals("hexkey0", loaded.nodeAddress().get(0).rsaPubKey());
        }

        @Test
        @DisplayName("Metrics are recorded when address book is pre-loaded")
        void metricsAreRecordedForPreloadedBook() {
            final NodeAddressBook book = buildAddressBook(4);

            start(new RsaRosterBootstrapPlugin(),
                    new SimpleInMemoryHistoricalBlockFacility(),
                    null, null, Map.of(), book);

            assertEquals(4, getMetricValue(RsaRosterBootstrapPlugin.METRIC_ROSTER_ENTRIES_LOADED));
            assertTrue(getMetricValue(RsaRosterBootstrapPlugin.METRIC_ROSTER_LOAD_DURATION_MS) >= 0);
        }

        @Test
        @DisplayName("onContextUpdate is called when updateAddressBook is invoked")
        void contextUpdateIsDeliveredViaUpdateAddressBook() {
            final NodeAddressBook book = buildAddressBook(2);

            start(new RsaRosterBootstrapPlugin(),
                    new SimpleInMemoryHistoricalBlockFacility(),
                    null, null, Map.of(), book);

            // updateAddressBook triggers onContextUpdate synchronously in PluginTestBase.
            final NodeAddressBook published = blockNodeContext.nodeAddressBook();
            assertFalse(published.nodeAddress().isEmpty());
            assertEquals(2, published.nodeAddress().size());
        }
    }

    // -------------------------------------------------------------------------
    // Mirror Node fallback tests
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("Mirror Node fallback")
    class MirrorNodeFallback {

        @Test
        @DisplayName("Unreachable Mirror Node (invalid URL) throws at start() when no book pre-loaded")
        void unreachableMirrorNodeThrows(@TempDir final Path tempDir) {
            // No preloaded address book — plugin must fetch from Mirror Node and fail
            assertThrows(IllegalStateException.class, () -> start(
                    new RsaRosterBootstrapPlugin(),
                    new SimpleInMemoryHistoricalBlockFacility(),
                    null,
                    Map.of(
                            "roster.bootstrap.mirrorNodeBaseUrl", "http://localhost:1",
                            "roster.bootstrap.mirrorNodeConnectTimeoutSeconds", "1",
                            "roster.bootstrap.mirrorNodeReadTimeoutSeconds", "1"),
                    Map.of(),
                    null));
        }
    }

    // -------------------------------------------------------------------------
    // Config registration
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("configDataTypes() includes BootstrapRosterConfig")
    void configDataTypesIncludesBootstrapRosterConfig() {
        assertTrue(new RsaRosterBootstrapPlugin()
                .configDataTypes()
                .contains(BootstrapRosterConfig.class));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static NodeAddressBook buildAddressBook(final int count) {
        final List<NodeAddress> addresses = new java.util.ArrayList<>();
        for (int i = 0; i < count; i++) {
            addresses.add(NodeAddress.newBuilder()
                    .nodeId(i)
                    .rsaPubKey("hexkey" + i)
                    .build());
        }
        return NodeAddressBook.newBuilder().nodeAddress(addresses).build();
    }
}
