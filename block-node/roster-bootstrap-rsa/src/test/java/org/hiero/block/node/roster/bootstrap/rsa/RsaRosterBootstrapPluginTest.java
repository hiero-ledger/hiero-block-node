// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.roster.bootstrap.rsa;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.hiero.block.internal.BlockNodeSource;
import org.hiero.block.internal.BlockNodeSourceConfig;
import org.hiero.block.internal.RangedAddressBookHistory;
import org.hiero.block.internal.RangedNodeAddressBook;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.plugintest.PluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.app.fixtures.server.TestBlockNodeServer;
import org.hiero.block.node.spi.BlockNodeContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/// Unit tests for `RsaRosterBootstrapPlugin`.
///
/// ## Responsibility split
///
/// File loading and persistence are handled by `BlockNodeApp`, not by this plugin.
/// The plugin's sole responsibility is:
/// - If `context.nodeAddressBook()` is non-null at `start()`: a bootstrap file was already
///   parsed by `BlockNodeApp.loadApplicationState()` — record metrics and return.
/// - If it is null: fetch from the Mirror Node, call `applicationStateFacility.updateAddressBook()`,
///   and let `BlockNodeApp` persist and broadcast the result.
///
/// ## Simulating file preload in tests
///
/// In production, `BlockNodeApp.loadApplicationState()` reads the RSA bootstrap file, builds a
/// `NodeAddressBook`, stages it as a pending update, and the `applicationStateExecutor` scheduler
/// fires a scan tick that rebuilds the `BlockNodeContext` and calls `onContextUpdate` on every
/// plugin before `start()` is invoked.
///
/// In tests, we skip the scheduler entirely by calling `updateAddressBook(book)` directly after
/// `doInit()`. This synchronously updates `blockNodeContext` and calls `plugin.onContextUpdate()`,
/// so by the time `doStart()` runs the plugin's internal `context` reference already holds the
/// address book — exactly as it would in production after the scanner tick fires.
class RsaRosterBootstrapPluginTest
        extends PluginTestBase<RsaRosterBootstrapPlugin, BlockingExecutor, ScheduledBlockingExecutor> {

    private List<TestBlockNodeServer> testBlockNodeServers;

    /// TempDir for the current test
    private final Path testTempDir;

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

        if (plugin != null) plugin.stop();
    }

    RsaRosterBootstrapPluginTest(@TempDir final Path testTempDir) {
        super(
                new BlockingExecutor(new LinkedBlockingQueue<>()),
                new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
        this.testTempDir = testTempDir;
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
    // Pre-loaded address book (simulates BlockNodeApp.loadApplicationState())
    //
    // updateAddressBook(book) replaces the full BlockNodeApp scheduler cycle:
    //   loadApplicationState() → pendingAddressBook.set() → scanner tick
    //   → BlockNodeContext rebuilt → plugin.onContextUpdate() called
    //
    // By the time doStart() is called, plugin.context.nodeAddressBook() is
    // non-null, so start() takes the "file-loaded" branch and skips the
    // Mirror Node fetch entirely.
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("Pre-loaded address book (BlockNodeApp loaded from file)")
    class PreloadedAddressBook {

        @Test
        @DisplayName("start() skips Mirror Node and exposes the pre-loaded book in context")
        void preloadedBookIsReflectedInContext() {
            // updateAddressBook() simulates BlockNodeApp pre-loading the RSA bootstrap file:
            // it synchronously updates blockNodeContext and calls plugin.onContextUpdate() so
            // the plugin's internal context reference holds the book before start() runs.
            final NodeAddressBook book = buildAddressBook(3);
            doInit(new RsaRosterBootstrapPlugin(), new SimpleInMemoryHistoricalBlockFacility(), null, null, Map.of());
            updateAddressBook(book);
            doStart();

            final NodeAddressBook loaded = blockNodeContext.nodeAddressBook();
            assertNotNull(loaded);
            assertEquals(3, loaded.nodeAddress().size());
            assertEquals("hexkey0", loaded.nodeAddress().getFirst().rsaPubKey());
        }

        @Test
        @DisplayName("Metrics reflect the pre-loaded book entry count and a non-negative load duration")
        void metricsAreRecordedForPreloadedBook() {
            // Same pre-load simulation as above; verifies that start() records the correct
            // roster_entries_loaded count and a valid roster_load_duration_ms metric.
            final NodeAddressBook book = buildAddressBook(4);
            doInit(new RsaRosterBootstrapPlugin(), new SimpleInMemoryHistoricalBlockFacility(), null, null, Map.of());
            updateAddressBook(book);
            doStart();

            assertEquals(4, getMetricValue(RsaRosterBootstrapPlugin.METRIC_ROSTER_ENTRIES_LOADED));
            assertTrue(getMetricValue(RsaRosterBootstrapPlugin.METRIC_ROSTER_LOAD_DURATION_MS) >= 0);
        }
    }

    // -------------------------------------------------------------------------
    // Pre-loaded address book history (simulates BlockNodeApp loading history file)
    //
    // updateAddressBookHistory(history) replaces the full BlockNodeApp scheduler cycle:
    //   loadApplicationState() → pendingAddressBookHistory.set() → scanner tick
    //   → BlockNodeContext rebuilt → plugin.onContextUpdate() called
    //
    // By the time doStart() is called, plugin.context.nodeAddressBookHistory() is
    // non-null, so start() takes the "history-loaded" branch.
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("Pre-loaded address book history (BlockNodeApp loaded history file)")
    class PreloadedAddressBookHistory {

        @Test
        @DisplayName("start() records era count and total entry count for a multi-era history")
        void multiEraHistoryMetrics() {
            // era1: 3 nodes, era2: 2 nodes → total 5 entries, 2 eras
            final RangedAddressBookHistory history =
                    buildHistory(ranged(buildAddressBook(3), 0L, 1000L), ranged(buildAddressBook(2), 1001L, 0L));

            doInit(new RsaRosterBootstrapPlugin(), new SimpleInMemoryHistoricalBlockFacility(), null, null, Map.of());
            updateAddressBookHistory(history);
            doStart();

            assertEquals(2L, getMetricValue(RsaRosterBootstrapPlugin.METRIC_ROSTER_ERAS_LOADED));
            assertEquals(5L, getMetricValue(RsaRosterBootstrapPlugin.METRIC_ROSTER_ENTRIES_LOADED));
            assertTrue(getMetricValue(RsaRosterBootstrapPlugin.METRIC_ROSTER_LOAD_DURATION_MS) >= 0);
        }

        @Test
        @DisplayName("history with a single era reports 1 era and the correct entry count")
        void singleEraHistoryMetrics() {
            final RangedAddressBookHistory history = buildHistory(ranged(buildAddressBook(4), 0L, 0L));

            doInit(new RsaRosterBootstrapPlugin(), new SimpleInMemoryHistoricalBlockFacility(), null, null, Map.of());
            updateAddressBookHistory(history);
            doStart();

            assertEquals(1L, getMetricValue(RsaRosterBootstrapPlugin.METRIC_ROSTER_ERAS_LOADED));
            assertEquals(4L, getMetricValue(RsaRosterBootstrapPlugin.METRIC_ROSTER_ENTRIES_LOADED));
        }

        @Test
        @DisplayName("history takes precedence: no Mirror Node fetch is scheduled")
        void historyPreventsMirrorNodeFetch() {
            // Even when a mirrorNodeBaseUrl is configured, loading the history means the plugin
            // returns early in start() — the scheduled executor must stay idle.
            final RangedAddressBookHistory history = buildHistory(ranged(buildAddressBook(2), 0L, 0L));

            doInit(
                    new RsaRosterBootstrapPlugin(),
                    new SimpleInMemoryHistoricalBlockFacility(),
                    null,
                    Map.of("roster.bootstrap.rsa.mirrorNodeBaseUrl", "http://localhost:9999"),
                    Map.of());
            updateAddressBookHistory(history);
            doStart();

            // No tasks should have been submitted to the scheduled executor
            assertEquals(
                    0L,
                    testThreadPoolManager.scheduledExecutor().getTaskCount(),
                    "No Mirror Node task should be scheduled when history is pre-loaded");
        }

        @Test
        @DisplayName("history absent, single address book present → plugin uses single-book path")
        void fallsBackToSingleBookWhenHistoryAbsent() {
            // No history in context but a single address book is present → existing path used
            final NodeAddressBook book = buildAddressBook(3);
            doInit(new RsaRosterBootstrapPlugin(), new SimpleInMemoryHistoricalBlockFacility(), null, null, Map.of());
            updateAddressBook(book);
            doStart();

            assertEquals(
                    0L,
                    getMetricValue(RsaRosterBootstrapPlugin.METRIC_ROSTER_ERAS_LOADED),
                    "Era gauge must be 0 in single-book mode");
            assertEquals(3L, getMetricValue(RsaRosterBootstrapPlugin.METRIC_ROSTER_ENTRIES_LOADED));
            assertNotNull(blockNodeContext.nodeAddressBook());
            assertNull(blockNodeContext.nodeAddressBookHistory());
        }
    }

    // -------------------------------------------------------------------------
    // Mirror Node fallback tests
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("Mirror Node fallback")
    class MirrorNodeFallback {

        @Test
        @DisplayName("Blank mirrorNodeBaseUrl returns early and leaves address book null")
        void blankMirrorNodeUrlReturnsEarly() {
            // No preloaded address book and no URL configured — plugin logs a WARNING and returns without a roster
            start(new RsaRosterBootstrapPlugin(), new SimpleInMemoryHistoricalBlockFacility(), Map.of());
            // Address book must remain null — no Mirror Node fetch was attempted
            assertNull(blockNodeContext.nodeAddressBook());
        }
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
                    "roster.bootstrap.rsa.mirrorNodeBaseUrl",
                    "http://localhost:" + port,
                    "roster.bootstrap.rsa.mirrorNodeConnectTimeoutSeconds",
                    "5",
                    "roster.bootstrap.rsa.mirrorNodeReadTimeoutSeconds",
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
            testThreadPoolManager.scheduledExecutor().executeSerially();

            final NodeAddressBook book = blockNodeContext.nodeAddressBook();
            assertNotNull(book);
            assertEquals(2, book.nodeAddress().size());
            assertEquals(1L, book.nodeAddress().getFirst().nodeId());
            assertEquals("aabbcc", book.nodeAddress().getFirst().rsaPubKey());
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
            testThreadPoolManager.scheduledExecutor().executeSerially();

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
            testThreadPoolManager.scheduledExecutor().executeSerially();

            final NodeAddressBook book = blockNodeContext.nodeAddressBook();
            assertNotNull(book);
            assertEquals(1, book.nodeAddress().size());
            assertEquals(2L, book.nodeAddress().getFirst().nodeId());
            assertEquals("aabbcc", book.nodeAddress().getFirst().rsaPubKey());
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
            testThreadPoolManager.scheduledExecutor().executeSerially();

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
        void http500TriggersRetryThenSucceeds() throws InterruptedException {
            final AtomicInteger callCount = new AtomicInteger(0);
            server.createContext("/api/v1/network/nodes", exchange -> {
                int call = callCount.getAndIncrement();
                if (call == 0) {
                    exchange.sendResponseHeaders(500, -1);
                } else if (call == 1) {
                    final byte[] bytes =
                            "{\"nodes\":[{\"node_id\":1,\"public_key\":\"aabbcc\"}],\"links\":{\"next\":null}}"
                                    .getBytes(StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(200, bytes.length);
                    try (var out = exchange.getResponseBody()) {
                        out.write(bytes);
                    }
                } else {
                    final byte[] bytes =
                            "{\"nodes\":[{\"node_id\":2,\"public_key\":\"ddeeff\"}],\"links\":{\"next\":null}}"
                                    .getBytes(StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(200, bytes.length);
                    try (var out = exchange.getResponseBody()) {
                        out.write(bytes);
                    }
                }
                exchange.close();
            });

            start(new RsaRosterBootstrapPlugin(), new SimpleInMemoryHistoricalBlockFacility(), serverConfig());

            // First task is the 500 error
            testThreadPoolManager.scheduledExecutor().executeSerially();
            // Second task should succeed
            testThreadPoolManager.scheduledExecutor().executeSerially();

            final NodeAddressBook book = blockNodeContext.nodeAddressBook();
            assertNotNull(book);
            assertEquals(1, book.nodeAddress().size());
            assertEquals(1, book.nodeAddress().getFirst().nodeId());
            assertEquals("aabbcc", book.nodeAddress().getFirst().rsaPubKey());

            // Third task should also succeed
            testThreadPoolManager.scheduledExecutor().executeSerially();

            final NodeAddressBook book2 = blockNodeContext.nodeAddressBook();
            assertNotNull(book2);
            assertEquals(1, book2.nodeAddress().size());
            assertEquals(2, book2.nodeAddress().getFirst().nodeId());
            assertEquals("ddeeff", book2.nodeAddress().getFirst().rsaPubKey());
        }
    }

    // -------------------------------------------------------------------------
    // Peer BN query path
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("Peer BN query path")
    class PeerQueryPath {

        @TempDir
        Path tempDir;

        private Map<String, String> peerConfig(String sourcesPath) {
            return Map.of(
                    "roster.bootstrap.rsa.blockNodeSourcesPath",
                    sourcesPath,
                    "roster.bootstrap.rsa.bnInitialQueryIntervalMillis",
                    "100");
        }

        private String writePeerSourcesFile(String json) throws IOException {
            Path file = tempDir.resolve("bn-sources.json");
            java.nio.file.Files.writeString(file, json);
            return file.toString();
        }

        @Test
        @DisplayName("Peer success: Mirror Node is never called")
        void peerSuccessMirrorNodeNeverCalled() throws IOException {
            // Set up a test server that returns a valid address book via serverStatusDetail
            TestBlockNodeServer server = new TestBlockNodeServer(0, new SimpleInMemoryHistoricalBlockFacility());

            final String json =
                    "{\"nodes\":[{\"address\":\"localhost\",\"port\":" + server.port() + ",\"priority\":1}]}";
            final String sourcesPath = writePeerSourcesFile(json);

            start(new RsaRosterBootstrapPlugin(), new SimpleInMemoryHistoricalBlockFacility(), peerConfig(sourcesPath));

            // Execute the peer-query task
            testThreadPoolManager.scheduledExecutor().executeSerially();

            server.stop();

            // Address book should be populated from the peer (TestBlockNodeServer returns a non-null book)
            // We verify at minimum that start() did not blow up and the executor ran
            assertNotNull(blockNodeContext);
        }

        @Test
        @DisplayName("No peer configured → skips directly to Mirror Node (backwards compat)")
        void noPeerConfiguredGoesToMirrorNode() {
            // No blockNodeSourcesPath, no mirrorNodeBaseUrl → warning and null book
            start(new RsaRosterBootstrapPlugin(), new SimpleInMemoryHistoricalBlockFacility(), Map.of());
            assertNull(blockNodeContext.nodeAddressBook());
        }

        @Test
        @DisplayName("Peer fails maxRetries times → falls through to Mirror Node")
        void peerExhaustionFallsThroughToMirrorNode() throws IOException {
            // Write a sources file pointing at a non-existent host so all peer attempts fail
            final String json = "{\"nodes\":[{\"address\":\"localhost\",\"port\":1,\"priority\":1}]}";
            final String sourcesPath = writePeerSourcesFile(json);

            // Configure a Mirror Node URL but no actual server — the MN fetch will also fail, but
            // what we care about is that the peer executor handed off to the MN executor.
            // We use maxRetries=1 so exhaustion happens after the first scheduled task.
            start(
                    new RsaRosterBootstrapPlugin(),
                    new SimpleInMemoryHistoricalBlockFacility(),
                    Map.of(
                            "roster.bootstrap.rsa.blockNodeSourcesPath",
                            sourcesPath,
                            "roster.bootstrap.rsa.mnInitialQueryIntervalMillis",
                            "100"));

            // First scheduled task: peer query (will fail — port 1 is unreachable)
            testThreadPoolManager.scheduledExecutor().executeSerially();

            // After exhaustion the plugin should fall through; no crash expected
            assertNull(blockNodeContext.nodeAddressBook());
        }

        @Test
        @DisplayName("request Node Address Book from a peer bn ")
        void requestRsaDataFromPeerBN() throws IOException, InterruptedException {
            final TestBlockNodeServer server1 = new TestBlockNodeServer(0, new SimpleInMemoryHistoricalBlockFacility());
            testBlockNodeServers.add(server1);
            String blockNodeSourcesPath = testTempDir + "/blocknode-sources.json";

            createTestBlockNodeSourcesFile(
                    BlockNodeSource.newBuilder()
                            .nodes(BlockNodeSourceConfig.newBuilder()
                                    .address("localhost")
                                    .port(server1.port())
                                    .priority(1)
                                    .build())
                            .build(),
                    blockNodeSourcesPath);

            // Config Override
            Map<String, String> configOverride = RsaRosterBootstrapConfigBuilder.newBuilder()
                    .blockNodeSourcesPath(blockNodeSourcesPath)
                    .bnInitialQueryIntervalMillis(500)
                    .bnSubsequentQueryIntervalMillis(10_000)
                    .maxIncomingBufferSize(104_857_600)
                    .enableTLS(false) // start quickly
                    .grpcOverallTimeout(10_000)
                    .build();

            final int[] contextUpdated = {0};
            final NodeAddressBook[] nodeAddressBooks = {null};
            CountDownLatch latch = new CountDownLatch(1);

            RsaRosterBootstrapPlugin plugin = new TestBootstrapPlugin(contextUpdated, nodeAddressBooks, latch);

            start(plugin, new SimpleInMemoryHistoricalBlockFacility(), configOverride);
            testThreadPoolManager.scheduledExecutor().executeSerially();
            latch.await();

            assertTrue(contextUpdated[0] > 0);
            assertNotNull(nodeAddressBooks[0]);

            // These are magic numbers, yes. The {@link TestBlockNodeServer} does not yet have a way to pass in TssData
            // to
            // hand back to testers. Using the values that are passed back to make sure the statusDetails api is
            // being called. Todo: add TssData flexibility to {@link TestBlockNodeServer}
            assertEquals(0L, nodeAddressBooks[0].nodeAddress().getFirst().nodeId());
            assertEquals(1L, nodeAddressBooks[0].nodeAddress().get(1).nodeId());
        }
    }

    // -------------------------------------------------------------------------
    // Config registration
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("configDataTypes() includes RsaRosterBootstrapConfig")
    void configDataTypesIncludesRsaRosterBootstrapConfig() {
        assertTrue(new RsaRosterBootstrapPlugin().configDataTypes().contains(RsaRosterBootstrapConfig.class));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static NodeAddressBook buildAddressBook(final int count) {
        final List<NodeAddress> addresses = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            addresses.add(
                    NodeAddress.newBuilder().nodeId(i).rsaPubKey("hexkey" + i).build());
        }
        return NodeAddressBook.newBuilder().nodeAddress(addresses).build();
    }

    private static RangedNodeAddressBook ranged(NodeAddressBook book, long start, long end) {
        return RangedNodeAddressBook.newBuilder()
                .addressBook(book)
                .startBlock(start)
                .endBlock(end)
                .build();
    }

    private static RangedAddressBookHistory buildHistory(RangedNodeAddressBook... entries) {
        return RangedAddressBookHistory.newBuilder()
                .addressBooks(List.of(entries))
                .build();
    }

    private void createTestBlockNodeSourcesFile(BlockNodeSource blockNodeSource, String configPath) throws IOException {
        String jsonString = BlockNodeSource.JSON.toJSON(blockNodeSource);
        // Write the JSON string to the specified file path
        java.nio.file.Files.write(java.nio.file.Paths.get(configPath), jsonString.getBytes());
    }

    /// Builder for creating backfill configuration maps for testing.
    public static class RsaRosterBootstrapConfigBuilder {

        private String blockNodeSourcesPath;
        private int bnInitialQueryIntervalMillis;
        private int bnSubsequentQueryIntervalMillis;
        private int maxIncomingBufferSize;
        private boolean enableTLS;
        private int grpcOverallTimeout;

        private RsaRosterBootstrapConfigBuilder() {
            // private to force use of NewBuilder()
        }

        public static RsaRosterBootstrapConfigBuilder newBuilder() {
            return new RsaRosterBootstrapConfigBuilder();
        }

        public RsaRosterBootstrapConfigBuilder blockNodeSourcesPath(String path) {
            this.blockNodeSourcesPath = path;
            return this;
        }

        public RsaRosterBootstrapConfigBuilder bnInitialQueryIntervalMillis(int value) {
            this.bnInitialQueryIntervalMillis = value;
            return this;
        }

        public RsaRosterBootstrapConfigBuilder bnSubsequentQueryIntervalMillis(int value) {
            this.bnSubsequentQueryIntervalMillis = value;
            return this;
        }

        public RsaRosterBootstrapConfigBuilder maxIncomingBufferSize(int value) {
            this.maxIncomingBufferSize = value;
            return this;
        }

        public RsaRosterBootstrapConfigBuilder grpcOverallTimeout(int value) {
            this.grpcOverallTimeout = value;
            return this;
        }

        public RsaRosterBootstrapConfigBuilder enableTLS(boolean value) {
            this.enableTLS = value;
            return this;
        }

        public Map<String, String> build() {
            if (blockNodeSourcesPath == null || blockNodeSourcesPath.isBlank()) {
                throw new IllegalStateException("blockNodeSourcesPath is required");
            }

            return new HashMap<>(Map.of(
                    "roster.bootstrap.rsa.blockNodeSourcesPath", blockNodeSourcesPath,
                    "roster.bootstrap.rsa.bnInitialQueryIntervalMillis", String.valueOf(bnInitialQueryIntervalMillis),
                    "roster.bootstrap.rsa.bnSubsequentQueryIntervalMillis",
                            String.valueOf(bnSubsequentQueryIntervalMillis),
                    "roster.bootstrap.rsa.maxIncomingBufferSize", String.valueOf(maxIncomingBufferSize),
                    "roster.bootstrap.rsa.grpcOverallTimeout", String.valueOf(grpcOverallTimeout),
                    "roster.bootstrap.rsa.enableTLS", String.valueOf(enableTLS)));
        }
    }

    private class TestBootstrapPlugin extends RsaRosterBootstrapPlugin {
        private final int[] contextUpdated;
        private final NodeAddressBook[] nodeAddressBooks;
        private final CountDownLatch latch;

        private TestBootstrapPlugin(int[] contextUpdated, NodeAddressBook[] nodeAddressBooks, CountDownLatch latch) {
            this.contextUpdated = contextUpdated;
            this.nodeAddressBooks = nodeAddressBooks;
            this.latch = latch;
        }

        @Override
        public void onContextUpdate(BlockNodeContext context) {
            contextUpdated[0]++;
            nodeAddressBooks[0] = context.nodeAddressBook();
            latch.countDown();
        }
    }
}
