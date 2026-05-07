// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.roster.bootstrap.rsa;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.plugintest.PluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/// Unit tests for `RsaRosterBootstrapPlugin`.
///
/// File loading and persistence are handled by `BlockNodeApp`. The plugin's
/// sole responsibility is to check whether the address book was already loaded
/// (`context.nodeAddressBook() != null`) and, if not, to fetch it from the Mirror Node.
///
/// Tests that simulate "BlockNodeApp loaded the file between init and start" use the
/// `doInit` / `simulatePreloadedAddressBook` / `doStart` pattern from `PluginTestBase`.
class RsaRosterBootstrapPluginTest
        extends PluginTestBase<RsaRosterBootstrapPlugin, BlockingExecutor, ScheduledBlockingExecutor> {

    RsaRosterBootstrapPluginTest() {
        super(
                new BlockingExecutor(new LinkedBlockingQueue<>()),
                new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
    }

    /// Override tearDown to gracefully handle cases where `start()` threw before
    /// the base class could initialise `metricsRegistry` (e.g. exception tests).
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

            doInit(new RsaRosterBootstrapPlugin(), new SimpleInMemoryHistoricalBlockFacility(), null, null, Map.of());
            simulatePreloadedAddressBook(book);
            doStart();

            final NodeAddressBook loaded = blockNodeContext.nodeAddressBook();
            assertNotNull(loaded);
            assertEquals(3, loaded.nodeAddress().size());
            assertEquals("hexkey0", loaded.nodeAddress().get(0).rsaPubKey());
        }

        @Test
        @DisplayName("Metrics are recorded when address book is pre-loaded")
        void metricsAreRecordedForPreloadedBook() {
            final NodeAddressBook book = buildAddressBook(4);

            doInit(new RsaRosterBootstrapPlugin(), new SimpleInMemoryHistoricalBlockFacility(), null, null, Map.of());
            simulatePreloadedAddressBook(book);
            doStart();

            assertEquals(4, getMetricValue(RsaRosterBootstrapPlugin.METRIC_ROSTER_ENTRIES_LOADED));
            assertTrue(getMetricValue(RsaRosterBootstrapPlugin.METRIC_ROSTER_LOAD_DURATION_MS) >= 0);
        }

        @Test
        @DisplayName("onContextUpdate is called before start() when address book is pre-loaded")
        void contextUpdateIsDeliveredBeforeStart() {
            final NodeAddressBook book = buildAddressBook(2);

            doInit(new RsaRosterBootstrapPlugin(), new SimpleInMemoryHistoricalBlockFacility(), null, null, Map.of());
            // simulatePreloadedAddressBook triggers onContextUpdate — plugin sees the book before start()
            simulatePreloadedAddressBook(book);
            doStart();

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
        void unreachableMirrorNodeThrows() {
            // No preloaded address book — plugin must fetch from Mirror Node and fail
            assertThrows(
                    IllegalStateException.class,
                    () -> start(
                            new RsaRosterBootstrapPlugin(),
                            new SimpleInMemoryHistoricalBlockFacility(),
                            Map.of(
                                    "roster.bootstrap.mirrorNodeBaseUrl", "http://localhost:1",
                                    "roster.bootstrap.mirrorNodeConnectTimeoutSeconds", "1",
                                    "roster.bootstrap.mirrorNodeReadTimeoutSeconds", "1")));
        }

        // TODO: Add tests for blank-key filtering, 0x-prefix stripping, and pagination using
        //  an embedded HTTP server (e.g. com.sun.net.httpserver.HttpServer or WireMock).
        //  The blank-key filter and 0x-strip logic in fetchFromMirrorNode() is verified at the
        //  MirrorNodeNodesResponse parsing layer; the relative-nextLink resolution is fixed in
        //  RsaRosterBootstrapPlugin.fetchFromMirrorNode() and covered by MirrorNodeNodesResponseTest.
    }

    // -------------------------------------------------------------------------
    // Mirror Node success paths — embedded HTTP server
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("Mirror Node success paths (embedded HTTP server)")
    class MirrorNodeEmbeddedServer {

        private HttpServer server;
        private int port;

        @BeforeEach
        void startServer() throws IOException {
            server = HttpServer.create(new InetSocketAddress(0), 0);
            port = server.getAddress().getPort();
            server.start();
        }

        @AfterEach
        void stopServer() {
            if (server != null) {
                server.stop(0);
            }
        }

        private Map<String, String> serverConfig() {
            return Map.of(
                    "roster.bootstrap.mirrorNodeBaseUrl",
                    "http://localhost:" + port,
                    "roster.bootstrap.mirrorNodeConnectTimeoutSeconds",
                    "5",
                    "roster.bootstrap.mirrorNodeReadTimeoutSeconds",
                    "5");
        }

        private void registerStaticHandler(final String path, final int statusCode, final String body) {
            server.createContext(path, exchange -> {
                final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(statusCode, bytes.length);
                try (var out = exchange.getResponseBody()) {
                    out.write(bytes);
                }
            });
        }

        @Test
        @DisplayName("Single-page response loads all nodes into the address book")
        void singlePageLoadsAllNodes() {
            registerStaticHandler(
                    "/api/v1/network/nodes",
                    200,
                    "{\"nodes\":[{\"node_id\":1,\"public_key\":\"aabbcc\"},{\"node_id\":2,\"public_key\":\"ddeeff\"}],\"links\":{\"next\":null}}");

            start(new RsaRosterBootstrapPlugin(), new SimpleInMemoryHistoricalBlockFacility(), serverConfig());

            final NodeAddressBook book = blockNodeContext.nodeAddressBook();
            assertNotNull(book);
            assertEquals(2, book.nodeAddress().size());
            assertEquals(1L, book.nodeAddress().get(0).nodeId());
            assertEquals("aabbcc", book.nodeAddress().get(0).rsaPubKey());
        }

        @Test
        @DisplayName("Paginated response collects nodes from all pages")
        void paginatedResponseCollectsAllNodes() {
            final AtomicInteger callCount = new AtomicInteger(0);
            server.createContext("/api/v1/network/nodes", exchange -> {
                final String body = callCount.getAndIncrement() == 0
                        ? "{\"nodes\":[{\"node_id\":1,\"public_key\":\"aabbcc\"}],\"links\":{\"next\":\"/api/v1/network/nodes?page=2\"}}"
                        : "{\"nodes\":[{\"node_id\":2,\"public_key\":\"ddeeff\"}],\"links\":{\"next\":null}}";
                final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, bytes.length);
                try (var out = exchange.getResponseBody()) {
                    out.write(bytes);
                }
            });

            start(new RsaRosterBootstrapPlugin(), new SimpleInMemoryHistoricalBlockFacility(), serverConfig());

            final NodeAddressBook book = blockNodeContext.nodeAddressBook();
            assertNotNull(book);
            assertEquals(2, book.nodeAddress().size());
            assertEquals(1L, book.nodeAddress().get(0).nodeId());
            assertEquals(2L, book.nodeAddress().get(1).nodeId());
        }

        @Test
        @DisplayName("Nodes with blank or null public_key are skipped; 0x prefix is stripped")
        void blankKeySkippedAndOxPrefixStripped() {
            registerStaticHandler(
                    "/api/v1/network/nodes",
                    200,
                    "{\"nodes\":["
                            + "{\"node_id\":1,\"public_key\":\"\"},"
                            + "{\"node_id\":2,\"public_key\":\"0xaabbcc\"},"
                            + "{\"node_id\":3,\"public_key\":null}"
                            + "],\"links\":{\"next\":null}}");

            start(new RsaRosterBootstrapPlugin(), new SimpleInMemoryHistoricalBlockFacility(), serverConfig());

            final NodeAddressBook book = blockNodeContext.nodeAddressBook();
            assertNotNull(book);
            assertEquals(1, book.nodeAddress().size());
            assertEquals(2L, book.nodeAddress().get(0).nodeId());
            assertEquals("aabbcc", book.nodeAddress().get(0).rsaPubKey());
        }

        @Test
        @DisplayName("Only active entries (timestamp.to=null) are collected; superseded entries stop pagination")
        void mixedActiveAndHistoricalEntriesOnlyLoadsActive() {
            // Single handler serves page 1 on first call and records subsequent calls.
            // Page 1 contains two active entries followed by one superseded (historical) entry.
            // The plugin must stop at the historical entry and never request page 2.
            final AtomicInteger callCount = new AtomicInteger(0);
            server.createContext("/api/v1/network/nodes", exchange -> {
                final int call = callCount.getAndIncrement();
                final String body;
                if (call == 0) {
                    // Page 1: two active (to=null), then one historical (to!=null) — signals end of active entries.
                    body = "{\"nodes\":["
                            + "{\"node_id\":0,\"public_key\":\"aabbcc\",\"timestamp\":{\"from\":\"1000.0\",\"to\":null}},"
                            + "{\"node_id\":1,\"public_key\":\"ddeeff\",\"timestamp\":{\"from\":\"900.0\",\"to\":null}},"
                            + "{\"node_id\":0,\"public_key\":\"oldkey\",\"timestamp\":{\"from\":\"800.0\",\"to\":\"900.0\"}}"
                            + "],\"links\":{\"next\":\"/api/v1/network/nodes?page=2\"}}";
                } else {
                    // Page 2 — should never be fetched.
                    body = "{\"nodes\":[],\"links\":{\"next\":null}}";
                }
                final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, bytes.length);
                try (var out = exchange.getResponseBody()) {
                    out.write(bytes);
                }
                exchange.close();
            });

            start(new RsaRosterBootstrapPlugin(), new SimpleInMemoryHistoricalBlockFacility(), serverConfig());

            final NodeAddressBook book = blockNodeContext.nodeAddressBook();
            assertNotNull(book);
            // Only the two active entries (node 0 and node 1) should be present.
            assertEquals(2, book.nodeAddress().size());
            assertEquals(0L, book.nodeAddress().get(0).nodeId());
            assertEquals("aabbcc", book.nodeAddress().get(0).rsaPubKey());
            assertEquals(1L, book.nodeAddress().get(1).nodeId());
            assertEquals("ddeeff", book.nodeAddress().get(1).rsaPubKey());
            assertEquals(1, callCount.get(), "Page 2 must not be fetched after a superseded entry stops iteration");
        }

        @Test
        @DisplayName("HTTP 500 triggers retry and succeeds on the next attempt")
        void http500TriggersRetryThenSucceeds() {
            final AtomicInteger callCount = new AtomicInteger(0);
            server.createContext("/api/v1/network/nodes", exchange -> {
                if (callCount.getAndIncrement() == 0) {
                    exchange.sendResponseHeaders(500, -1);
                } else {
                    final byte[] bytes =
                            "{\"nodes\":[{\"node_id\":1,\"public_key\":\"aabbcc\"}],\"links\":{\"next\":null}}"
                                    .getBytes(StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(200, bytes.length);
                    try (var out = exchange.getResponseBody()) {
                        out.write(bytes);
                    }
                }
                exchange.close();
            });

            start(new RsaRosterBootstrapPlugin(), new SimpleInMemoryHistoricalBlockFacility(), serverConfig());

            final NodeAddressBook book = blockNodeContext.nodeAddressBook();
            assertNotNull(book);
            assertEquals(1, book.nodeAddress().size());
            assertEquals("aabbcc", book.nodeAddress().get(0).rsaPubKey());
        }
    }

    // -------------------------------------------------------------------------
    // Config registration
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("configDataTypes() includes BootstrapRosterConfig")
    void configDataTypesIncludesBootstrapRosterConfig() {
        assertTrue(new RsaRosterBootstrapPlugin().configDataTypes().contains(BootstrapRosterConfig.class));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static NodeAddressBook buildAddressBook(final int count) {
        final List<NodeAddress> addresses = new java.util.ArrayList<>();
        for (int i = 0; i < count; i++) {
            addresses.add(
                    NodeAddress.newBuilder().nodeId(i).rsaPubKey("hexkey" + i).build());
        }
        return NodeAddressBook.newBuilder().nodeAddress(addresses).build();
    }
}
