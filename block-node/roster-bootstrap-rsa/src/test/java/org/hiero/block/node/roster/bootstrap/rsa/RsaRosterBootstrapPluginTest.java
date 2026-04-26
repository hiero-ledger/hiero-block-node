// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.roster.bootstrap.rsa;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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

/**
 * Unit tests for `RsaRosterBootstrapPlugin`.
 *
 * Uses `PluginTestBase` which synchronously calls `onContextUpdate()` when `updateAddressBook()`
 * is invoked, allowing assertions on `blockNodeContext.nodeAddressBook()` immediately after
 * `start()` returns.
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
    // File-loading tests
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("File-based loading")
    class FileLoading {

        @Test
        @DisplayName("Valid bootstrap file is loaded and published to context")
        void loadsValidBootstrapFile(@TempDir final Path tempDir) throws Exception {
            final Path filePath = tempDir.resolve("rsa-bootstrap-roster.pb");
            final NodeAddressBook book = buildAddressBook(3);
            writeAddressBook(book, filePath);

            start(new RsaRosterBootstrapPlugin(),
                    new SimpleInMemoryHistoricalBlockFacility(),
                    Map.of("roster.bootstrap.filePath", filePath.toString()));

            final NodeAddressBook loaded = blockNodeContext.nodeAddressBook();
            assertNotNull(loaded);
            assertEquals(3, loaded.nodeAddress().size());
            assertEquals("hexkey0", loaded.nodeAddress().get(0).rsaPubKey());
        }

        @Test
        @DisplayName("Bootstrap file with no usable RSA keys throws at start()")
        void emptyKeysBootstrapFileThrows(@TempDir final Path tempDir) throws Exception {
            final Path filePath = tempDir.resolve("rsa-bootstrap-roster.pb");
            // Write a book with entries that have blank RSA keys
            final NodeAddressBook book = NodeAddressBook.newBuilder()
                    .nodeAddress(List.of(NodeAddress.newBuilder().nodeId(0).rsaPubKey("").build()))
                    .build();
            writeAddressBook(book, filePath);

            assertThrows(IllegalStateException.class, () -> start(
                    new RsaRosterBootstrapPlugin(),
                    new SimpleInMemoryHistoricalBlockFacility(),
                    Map.of("roster.bootstrap.filePath", filePath.toString())));
        }

        @Test
        @DisplayName("Empty bootstrap file (zero entries) throws at start()")
        void emptyBootstrapFileThrows(@TempDir final Path tempDir) throws Exception {
            final Path filePath = tempDir.resolve("rsa-bootstrap-roster.pb");
            final NodeAddressBook book = NodeAddressBook.newBuilder()
                    .nodeAddress(List.of())
                    .build();
            writeAddressBook(book, filePath);

            assertThrows(IllegalStateException.class, () -> start(
                    new RsaRosterBootstrapPlugin(),
                    new SimpleInMemoryHistoricalBlockFacility(),
                    Map.of("roster.bootstrap.filePath", filePath.toString())));
        }

        @Test
        @DisplayName("Corrupt (non-protobuf) bootstrap file throws at start()")
        void corruptBootstrapFileThrows(@TempDir final Path tempDir) throws Exception {
            final Path filePath = tempDir.resolve("rsa-bootstrap-roster.pb");
            Files.write(filePath, new byte[] {0x00, 0x01, 0x02, (byte) 0xFF});

            assertThrows(IllegalStateException.class, () -> start(
                    new RsaRosterBootstrapPlugin(),
                    new SimpleInMemoryHistoricalBlockFacility(),
                    Map.of("roster.bootstrap.filePath", filePath.toString())));
        }

        @Test
        @DisplayName("onContextUpdate is called when updateAddressBook is invoked")
        void contextUpdateIsDelivered(@TempDir final Path tempDir) throws Exception {
            final Path filePath = tempDir.resolve("rsa-bootstrap-roster.pb");
            writeAddressBook(buildAddressBook(2), filePath);

            start(new RsaRosterBootstrapPlugin(),
                    new SimpleInMemoryHistoricalBlockFacility(),
                    Map.of("roster.bootstrap.filePath", filePath.toString()));

            // updateAddressBook triggers onContextUpdate synchronously in PluginTestBase.
            // Verify that the context reflects the published book.
            final NodeAddressBook published = blockNodeContext.nodeAddressBook();
            assertFalse(published.nodeAddress().isEmpty());
        }
    }

    // -------------------------------------------------------------------------
    // Mirror Node fallback tests
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("Mirror Node fallback")
    class MirrorNodeFallback {

        @Test
        @DisplayName("Unreachable Mirror Node (invalid URL) throws at start()")
        void unreachableMirrorNodeThrows(@TempDir final Path tempDir) {
            final Path filePath = tempDir.resolve("rsa-bootstrap-roster.pb");
            // File does not exist; MN URL is unreachable
            assertThrows(IllegalStateException.class, () -> start(
                    new RsaRosterBootstrapPlugin(),
                    new SimpleInMemoryHistoricalBlockFacility(),
                    Map.of(
                            "roster.bootstrap.filePath", filePath.toString(),
                            "roster.bootstrap.mirrorNodeBaseUrl", "http://localhost:1",
                            "roster.bootstrap.mirrorNodeConnectTimeoutSeconds", "1",
                            "roster.bootstrap.mirrorNodeReadTimeoutSeconds", "1")));
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

    private static void writeAddressBook(final NodeAddressBook book, final Path path) throws Exception {
        final Bytes encoded = NodeAddressBook.PROTOBUF.toBytes(book);
        Files.write(path, encoded.toByteArray());
    }
}
