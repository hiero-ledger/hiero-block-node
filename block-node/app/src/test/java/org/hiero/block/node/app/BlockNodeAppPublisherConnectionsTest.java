// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.ServiceEndpoint;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.hiero.block.api.NetworkConnection;
import org.hiero.block.api.RangedAddressBookHistory;
import org.hiero.block.api.RangedNodeAddressBook;
import org.hiero.block.node.spi.ServiceLoaderFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/// Tests that verify [BlockNodeApp#updateAddressBookHistory] correctly derives and propagates
/// publisher connections to [BlockNodeApp#knownPublishers].
class BlockNodeAppPublisherConnectionsTest {

    // ---- builders -------------------------------------------------------------------------------

    private static ServiceEndpoint ipEndpoint(final String hexIp, final int port) {
        return ServiceEndpoint.newBuilder()
                .ipAddressV4(Bytes.fromHex(hexIp))
                .port(port)
                .build();
    }

    private static NodeAddressBook bookWithEndpoints(final ServiceEndpoint... endpoints) {
        return NodeAddressBook.newBuilder()
                .nodeAddress(NodeAddress.newBuilder()
                        .nodeId(1L)
                        .serviceEndpoint(endpoints)
                        .build())
                .build();
    }

    /// Builds an address book with one node per supplied endpoint. Node ids are arbitrary and do
    /// not affect the derived connections, so two nodes carrying the same endpoint yield equal
    /// (duplicate) connections.
    private static NodeAddressBook bookWithNodes(final ServiceEndpoint... endpoints) {
        final List<NodeAddress> nodes = new ArrayList<>();
        long nodeId = 0;
        for (final ServiceEndpoint endpoint : endpoints) {
            nodes.add(NodeAddress.newBuilder()
                    .nodeId(nodeId++)
                    .serviceEndpoint(endpoint)
                    .build());
        }
        return NodeAddressBook.newBuilder().nodeAddress(nodes).build();
    }

    private static RangedNodeAddressBook era(final long start, final long end, final NodeAddressBook book) {
        return RangedNodeAddressBook.newBuilder()
                .addressBook(book)
                .startBlock(start)
                .endBlock(end)
                .build();
    }

    private static RangedAddressBookHistory historyOf(final RangedNodeAddressBook... eras) {
        return RangedAddressBookHistory.newBuilder().addressBooks(List.of(eras)).build();
    }

    @Nested
    @DisplayName("verify that updateAddressBookHistory propagates to knownPublishers")
    class TestKnownPublishersFromAddressBookUpdate {
        private BlockNodeApp app;

        @BeforeEach
        void setUp() throws IOException {
            app = new BlockNodeApp(new ServiceLoaderFunction(), false);
        }

        @Test
        @DisplayName("known publishers are derived from the newest era only")
        void newestEraChosen() {
            // Older era publishes 127.0.0.1; newer era (higher startBlock) publishes 127.0.0.2.
            final RangedAddressBookHistory history = historyOf(
                    era(0L, 99L, bookWithEndpoints(ipEndpoint("7F000001", 1))),
                    era(100L, -1L, bookWithEndpoints(ipEndpoint("7F000002", 2))));

            assertTrue(app.updateAddressBookHistory(history));

            final List<NetworkConnection> known = app.knownPublishers().activeEndpoints();
            assertEquals(1, known.size(), "only the newest era's endpoints are published");
            assertEquals("127.0.0.2", known.getFirst().remote().address());
        }

        @Test
        @DisplayName("newly derived connections are merged with the previously known publishers")
        void mergesWithExisting() {
            // First update seeds knownPublishers with 127.0.0.1.
            assertTrue(app.updateAddressBookHistory(
                    historyOf(era(0L, -1L, bookWithEndpoints(ipEndpoint("7F000001", 1))))));
            // Second update derives 127.0.0.2; both endpoints must be present after the merge.
            assertTrue(app.updateAddressBookHistory(
                    historyOf(era(10L, -1L, bookWithEndpoints(ipEndpoint("7F000002", 2))))));

            final List<NetworkConnection> known = app.knownPublishers().activeEndpoints();
            assertEquals(2, known.size());
            final List<String> addresses =
                    known.stream().map(c -> c.remote().address()).toList();
            assertTrue(addresses.contains("127.0.0.1"), "previously known publisher must be present");
            assertTrue(addresses.contains("127.0.0.2"), "newly derived publisher must be present");
        }

        @Test
        @DisplayName("a null history leaves knownPublishers unchanged")
        void nullHistoryLeavesKnownPublishersUnchanged() {
            assertFalse(app.updateAddressBookHistory(null));
            assertTrue(app.knownPublishers().activeEndpoints().isEmpty());
        }

        @Test
        @DisplayName("a history with no eras leaves knownPublishers unchanged")
        void emptyHistoryLeavesKnownPublishersUnchanged() {
            app.updateAddressBookHistory(RangedAddressBookHistory.newBuilder().build());
            assertTrue(app.knownPublishers().activeEndpoints().isEmpty());
        }

        /// Three successive address-book updates, each adding a new open-ended era whose node set
        /// overlaps the previous era. Each update must grow knownPublishers and must not introduce
        /// duplicate connections.
        ///
        /// This test verifies basic deduplication support, but does not fully exercise
        /// the possible deduplication scenarios. In particular it does not verify
        /// that publishers loaded from configuration with different category, schema, or
        /// protocol values are matched based only on remote address/port, and does not
        /// verify that when such matches are made, the correct value (from configuration)
        /// is the one retained.
        @Test
        @DisplayName("repeated overlapping updates grow knownPublishers without duplicates (expected-fail pre-dedup)")
        void repeatedOverlappingUpdatesHaveNoDuplicates() {
            assertTrue(app.knownPublishers().activeEndpoints().isEmpty(), "must start with no known publishers");

            // Each endpoint (distinct ip/port) becomes one node -> one connection; a shared endpoint
            // across eras therefore produces equal (duplicate) connections.
            final ServiceEndpoint endpoint1 = ipEndpoint("7F000001", 1);
            final ServiceEndpoint endpoint2 = ipEndpoint("7F000002", 2);
            final ServiceEndpoint endpoint3 = ipEndpoint("7F000003", 3);
            final ServiceEndpoint endpoint4 = ipEndpoint("7F000004", 4);
            final ServiceEndpoint endpoint5 = ipEndpoint("7F000005", 5);
            final ServiceEndpoint endpoint6 = ipEndpoint("7F000006", 6);
            final ServiceEndpoint endpoint7 = ipEndpoint("7F000007", 7);
            final ServiceEndpoint endpoint8 = ipEndpoint("7F000008", 8);
            final ServiceEndpoint endpoint9 = ipEndpoint("7F000009", 9);

            // era 1: 2 nodes
            final NodeAddressBook book1 = bookWithNodes(endpoint1, endpoint2);
            // era 2: 5 nodes, 2 (endpoint1, endpoint2) reused from era 1
            final NodeAddressBook book2 = bookWithNodes(endpoint1, endpoint2, endpoint3, endpoint4, endpoint5);
            // era 3: 8 nodes, 4 (endpoint2, endpoint3, endpoint4, endpoint5) reused from era 2
            final NodeAddressBook book3 = bookWithNodes(
                    endpoint2, endpoint3, endpoint4, endpoint5, endpoint6, endpoint7, endpoint8, endpoint9);

            // Era objects. The first era is open-ended in call 1, then bounded (endBlock == next
            // era's startBlock) in calls 2 and 3; likewise the second era for call 3.
            final RangedNodeAddressBook era1Open = era(0L, -1L, book1);
            final RangedNodeAddressBook era1Bounded = era(0L, 100L, book1);
            final RangedNodeAddressBook era2Open = era(100L, -1L, book2);
            final RangedNodeAddressBook era2Bounded = era(100L, 200L, book2);
            final RangedNodeAddressBook era3Open = era(200L, -1L, book3);

            // ---- call 1: single open-ended era with 2 nodes ----
            int previousSize = app.knownPublishers().activeEndpoints().size();
            assertTrue(app.updateAddressBookHistory(historyOf(era1Open)));
            List<NetworkConnection> known = app.knownPublishers().activeEndpoints();
            assertTrue(known.size() > previousSize, "call 1 must update knownPublishers");
            assertNoDuplicates(known);

            // ---- call 2: era 1 now bounded [0,100]; new open-ended era 2 [100,-1] with 5 nodes ----
            previousSize = known.size();
            assertTrue(app.updateAddressBookHistory(historyOf(era1Bounded, era2Open)));
            known = app.knownPublishers().activeEndpoints();
            assertTrue(known.size() > previousSize, "call 2 must update knownPublishers");
            assertNoDuplicates(known);

            // ---- call 3: era 2 now bounded [100,200]; new open-ended era 3 [200,-1] with 8 nodes ----
            previousSize = known.size();
            assertTrue(app.updateAddressBookHistory(historyOf(era1Bounded, era2Bounded, era3Open)));
            known = app.knownPublishers().activeEndpoints();
            assertTrue(known.size() > previousSize, "call 3 must update knownPublishers");
            assertNoDuplicates(known);
        }

        /// Asserts the supplied connection list contains no duplicate entries.
        private static void assertNoDuplicates(final List<NetworkConnection> connections) {
            assertEquals(
                    connections.size(),
                    new HashSet<>(connections).size(),
                    "knownPublishers must not contain duplicate entries");
        }
    }
}
