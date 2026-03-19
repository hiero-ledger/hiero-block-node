// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.mirrornode;

import static org.junit.jupiter.api.Assertions.*;

import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.pbj.runtime.io.stream.ReadableStreamingData;
import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.time.Instant;
import java.util.List;
import org.hiero.block.internal.AddressBookHistory;
import org.hiero.block.internal.DatedNodeAddressBook;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link GenerateTestnetAddressBookHistory} and the underlying
 * testnet genesis address book resource.
 */
class GenerateTestnetAddressBookHistoryTest {

    private static final String GENESIS_ADDRESS_BOOK_RESOURCE = "/testnet-genesis-address-book.proto.bin";
    private static final String TESTNET_GENESIS_TIMESTAMP = "2024-02-01T18:35:20.644859297Z";

    @Nested
    @DisplayName("Bundled genesis address book resource")
    class GenesisAddressBookResourceTests {

        @Test
        @DisplayName("Resource exists on classpath")
        void testResourceExists() {
            try (InputStream in = getClass().getResourceAsStream(GENESIS_ADDRESS_BOOK_RESOURCE)) {
                assertNotNull(in, "testnet-genesis-address-book.proto.bin should exist on classpath");
            } catch (Exception e) {
                fail("Failed to open resource: " + e.getMessage());
            }
        }

        @Test
        @DisplayName("Resource parses as valid NodeAddressBook")
        void testResourceParsesAsNodeAddressBook() throws Exception {
            try (InputStream in = getClass().getResourceAsStream(GENESIS_ADDRESS_BOOK_RESOURCE)) {
                NodeAddressBook addressBook = NodeAddressBook.PROTOBUF.parse(new ReadableStreamingData(in));
                assertNotNull(addressBook, "Parsed address book should not be null");
                assertFalse(addressBook.nodeAddress().isEmpty(), "Address book should have nodes");
            }
        }

        @Test
        @DisplayName("Address book contains exactly 7 testnet nodes")
        void testAddressBookHasSevenNodes() throws Exception {
            try (InputStream in = getClass().getResourceAsStream(GENESIS_ADDRESS_BOOK_RESOURCE)) {
                NodeAddressBook addressBook = NodeAddressBook.PROTOBUF.parse(new ReadableStreamingData(in));
                assertEquals(7, addressBook.nodeAddress().size(), "Testnet should have 7 nodes (0.0.3 through 0.0.9)");
            }
        }

        @Test
        @DisplayName("Node account IDs are 0.0.3 through 0.0.9")
        void testNodeAccountIds() throws Exception {
            try (InputStream in = getClass().getResourceAsStream(GENESIS_ADDRESS_BOOK_RESOURCE)) {
                NodeAddressBook addressBook = NodeAddressBook.PROTOBUF.parse(new ReadableStreamingData(in));
                List<Long> accountNums = addressBook.nodeAddress().stream()
                        .map(node -> node.nodeAccountId().accountNum())
                        .sorted()
                        .toList();
                assertEquals(List.of(3L, 4L, 5L, 6L, 7L, 8L, 9L), accountNums);
            }
        }

        @Test
        @DisplayName("All nodes have RSA public keys")
        void testAllNodesHavePublicKeys() throws Exception {
            try (InputStream in = getClass().getResourceAsStream(GENESIS_ADDRESS_BOOK_RESOURCE)) {
                NodeAddressBook addressBook = NodeAddressBook.PROTOBUF.parse(new ReadableStreamingData(in));
                for (var node : addressBook.nodeAddress()) {
                    assertNotNull(node.rsaPubKey(), "Node " + node.nodeId() + " should have an RSA public key");
                    assertFalse(
                            node.rsaPubKey().isEmpty(),
                            "Node " + node.nodeId() + " RSA public key should not be empty");
                }
            }
        }
    }

    @Nested
    @DisplayName("Address book history generation")
    class AddressBookHistoryGenerationTests {

        @Test
        @DisplayName("Produces valid AddressBookHistory JSON with single entry")
        void testProducesValidHistory() throws Exception {
            // Load the address book
            NodeAddressBook addressBook;
            try (InputStream in = getClass().getResourceAsStream(GENESIS_ADDRESS_BOOK_RESOURCE)) {
                addressBook = NodeAddressBook.PROTOBUF.parse(new ReadableStreamingData(in));
            }

            // Build the history (same logic as the command)
            Instant genesis = Instant.parse(TESTNET_GENESIS_TIMESTAMP);
            Timestamp timestamp = Timestamp.newBuilder()
                    .seconds(genesis.getEpochSecond())
                    .nanos(genesis.getNano())
                    .build();
            DatedNodeAddressBook datedBook = new DatedNodeAddressBook(timestamp, addressBook);
            AddressBookHistory history = new AddressBookHistory(List.of(datedBook));

            // Write to JSON
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (WritableStreamingData out = new WritableStreamingData(baos)) {
                AddressBookHistory.JSON.write(history, out);
            }

            // Verify output is non-empty
            byte[] jsonBytes = baos.toByteArray();
            assertTrue(jsonBytes.length > 0, "JSON output should not be empty");

            // Parse it back to verify round-trip
            AddressBookHistory parsed =
                    AddressBookHistory.JSON.parse(new ReadableStreamingData(new ByteArrayInputStream(jsonBytes)));
            assertNotNull(parsed, "Parsed history should not be null");
            assertEquals(1, parsed.addressBooks().size(), "History should have exactly one entry");
        }

        @Test
        @DisplayName("Genesis timestamp is correctly encoded")
        void testGenesisTimestampEncoding() throws Exception {
            NodeAddressBook addressBook;
            try (InputStream in = getClass().getResourceAsStream(GENESIS_ADDRESS_BOOK_RESOURCE)) {
                addressBook = NodeAddressBook.PROTOBUF.parse(new ReadableStreamingData(in));
            }

            Instant genesis = Instant.parse(TESTNET_GENESIS_TIMESTAMP);
            Timestamp timestamp = Timestamp.newBuilder()
                    .seconds(genesis.getEpochSecond())
                    .nanos(genesis.getNano())
                    .build();
            DatedNodeAddressBook datedBook = new DatedNodeAddressBook(timestamp, addressBook);
            AddressBookHistory history = new AddressBookHistory(List.of(datedBook));

            // Write and parse back
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (WritableStreamingData out = new WritableStreamingData(baos)) {
                AddressBookHistory.JSON.write(history, out);
            }
            AddressBookHistory parsed = AddressBookHistory.JSON.parse(
                    new ReadableStreamingData(new ByteArrayInputStream(baos.toByteArray())));

            Timestamp parsedTimestamp = parsed.addressBooks().getFirst().blockTimestampOrThrow();
            assertEquals(genesis.getEpochSecond(), parsedTimestamp.seconds(), "Seconds should match genesis");
            assertEquals(genesis.getNano(), parsedTimestamp.nanos(), "Nanos should match genesis");
        }

        @Test
        @DisplayName("Round-tripped history preserves node count")
        void testRoundTripPreservesNodeCount() throws Exception {
            NodeAddressBook addressBook;
            try (InputStream in = getClass().getResourceAsStream(GENESIS_ADDRESS_BOOK_RESOURCE)) {
                addressBook = NodeAddressBook.PROTOBUF.parse(new ReadableStreamingData(in));
            }

            Instant genesis = Instant.parse(TESTNET_GENESIS_TIMESTAMP);
            Timestamp timestamp = Timestamp.newBuilder()
                    .seconds(genesis.getEpochSecond())
                    .nanos(genesis.getNano())
                    .build();
            AddressBookHistory history =
                    new AddressBookHistory(List.of(new DatedNodeAddressBook(timestamp, addressBook)));

            // Write and parse back
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (WritableStreamingData out = new WritableStreamingData(baos)) {
                AddressBookHistory.JSON.write(history, out);
            }
            AddressBookHistory parsed = AddressBookHistory.JSON.parse(
                    new ReadableStreamingData(new ByteArrayInputStream(baos.toByteArray())));

            NodeAddressBook parsedBook = parsed.addressBooks().getFirst().addressBook();
            assertEquals(7, parsedBook.nodeAddress().size(), "Round-tripped address book should still have 7 nodes");
        }
    }
}
