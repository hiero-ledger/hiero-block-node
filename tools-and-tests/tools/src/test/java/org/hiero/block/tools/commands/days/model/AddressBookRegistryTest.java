// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands.days.model;

import static org.junit.jupiter.api.Assertions.*;

import com.hedera.hapi.node.addressbook.NodeCreateTransactionBody;
import com.hedera.hapi.node.addressbook.NodeDeleteTransactionBody;
import com.hedera.hapi.node.addressbook.NodeUpdateTransactionBody;
import com.hedera.hapi.node.base.AccountID;
import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.Transaction;
import com.hedera.hapi.node.transaction.TransactionBody;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.hedera.pbj.runtime.io.stream.ReadableStreamingData;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import org.hiero.block.tools.days.model.AddressBookRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class AddressBookRegistryTest {

    @Test
    public void testLoadGenesisAddressBook() throws ParseException {
        NodeAddressBook addressBook = AddressBookRegistry.loadGenesisAddressBook();
        assertNotNull(addressBook);
        assertFalse(addressBook.nodeAddress().isEmpty());
    }

    @Test
    public void testPublicKeyForNode() throws ParseException {
        NodeAddressBook addressBook = AddressBookRegistry.loadGenesisAddressBook();
        String publicKey = AddressBookRegistry.publicKeyForNode(addressBook, 0, 0, 11);
        assertNotNull(publicKey);
        assertTrue(publicKey.startsWith(
                "308201a2300d06092a864886f70d01010105000382018f003082018a02820181009bdd8e84fadaa35"));
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    public void testUpdateAddressBook() throws Exception {
        try (var in = new ReadableStreamingData(AddressBookRegistryTest.class.getResourceAsStream(
                "/2021-06-08T17_35_26.000831000Z-file-102-update-transaction-body.bin"))) {
            int numOfTransactions = in.readInt();
            List<TransactionBody> transactionBodies = new ArrayList<>(numOfTransactions);
            for (int i = 0; i < numOfTransactions; i++) {
                int len = in.readInt();
                Bytes tbBytes = in.readBytes(len);
                transactionBodies.add(TransactionBody.PROTOBUF.parse(tbBytes));
            }
            AddressBookRegistry addressBookRegistry = new AddressBookRegistry();
            String changes = addressBookRegistry.updateAddressBook(Instant.now(), transactionBodies);
            System.out.println("changes = " + changes);
            // check changes contains expected updates
            //            Node 16 added with key ac367eb1
            //            Node 17 added with key 914257bf
            //            Node 18 added with key be305ad6
            //            Node 19 added with key c99647ce
            //            Node 20 added with key 2b7643a0
            //            Node 21 added with key c21c0c95
            //            Node 22 added with key be2ab00f
            //            Node 23 added with key 8c0a53e9
            assertTrue(changes.contains("Node 16 added with key ac367eb1"));
            assertTrue(changes.contains("Node 17 added with key 914257bf"));
            assertTrue(changes.contains("Node 18 added with key be305ad6"));
            assertTrue(changes.contains("Node 19 added with key c99647ce"));
            assertTrue(changes.contains("Node 20 added with key 2b7643a0"));
            assertTrue(changes.contains("Node 21 added with key c21c0c95"));
            assertTrue(changes.contains("Node 22 added with key be2ab00f"));
            assertTrue(changes.contains("Node 23 added with key 8c0a53e9"));
            // check the new nodes are in new address book
            NodeAddressBook updatedAddressBook = addressBookRegistry.getCurrentAddressBook();
            assertEquals(24 - 3, updatedAddressBook.nodeAddress().size());
            var node16 = updatedAddressBook.nodeAddress().stream()
                    .filter(na -> AddressBookRegistry.getNodeAccountId(na) == 16)
                    .findFirst()
                    .orElseThrow();
            assertTrue(
                    node16.rsaPubKey().contains("ac367eb1"),
                    "Node 16 public key should contain ac367eb1, but was: " + node16.rsaPubKey());
        }
    }

    /**
     * Test that duplicate address books (same content) are not added to the registry.
     * This test verifies the fix for the reference equality bug where line 167 was using
     * `!=` instead of `.equals()`, causing duplicate entries with identical content.
     */
    @Test
    public void testNoDuplicateAddressBooks() throws Exception {

        try (var in = new ReadableStreamingData(AddressBookRegistryTest.class.getResourceAsStream(
                "/2021-06-08T17_35_26.000831000Z-file-102-update-transaction-body.bin"))) {
            int numOfTransactions = in.readInt();
            List<TransactionBody> transactionBodies = new ArrayList<>(numOfTransactions);
            for (int i = 0; i < numOfTransactions; i++) {
                int len = in.readInt();
                Bytes tbBytes = in.readBytes(len);
                transactionBodies.add(TransactionBody.PROTOBUF.parse(tbBytes));
            }

            AddressBookRegistry addressBookRegistry = new AddressBookRegistry();

            // Initial size: 1 (genesis)
            assertEquals(1, addressBookRegistry.getAddressBookCount(), "Should start with only genesis address book");

            // First update - should add new entry
            Instant time1 = Instant.parse("2021-06-08T17:35:26.000831000Z");
            String changes1 = addressBookRegistry.updateAddressBook(time1, transactionBodies);
            assertNotNull(changes1, "Should detect changes on first update");
            assertTrue(changes1.contains("Node 16 added"), "Should add new nodes");

            // Size should now be 2 (genesis + new)
            assertEquals(2, addressBookRegistry.getAddressBookCount(), "Should have 2 entries after first update");

            // Second update with SAME transactions - should NOT add duplicate entry
            Instant time2 = Instant.parse("2021-06-08T18:00:00.000000000Z");
            String changes2 = addressBookRegistry.updateAddressBook(time2, transactionBodies);
            assertNull(changes2, "Should NOT detect changes when content is identical");

            // Size should still be 2 (no duplicate added)
            assertEquals(
                    2,
                    addressBookRegistry.getAddressBookCount(),
                    "Should NOT add duplicate entry when content is identical");

            // Third update with empty transaction list - should NOT add entry
            Instant time3 = Instant.parse("2021-06-08T19:00:00.000000000Z");
            String changes3 = addressBookRegistry.updateAddressBook(time3, new ArrayList<>());
            assertNull(changes3, "Should NOT detect changes with empty transactions");

            // Size should still be 2
            assertEquals(
                    2, addressBookRegistry.getAddressBookCount(), "Should NOT add entry when no transactions provided");
        }
    }

    /**
     * Test that content equality is used, not reference equality.
     * This directly tests the bug where different object references with identical
     * content would be considered different.
     */
    @Test
    public void testContentEqualityNotReferenceEquality() throws ParseException {
        // Create two registries with same genesis book
        AddressBookRegistry registry1 = new AddressBookRegistry();
        AddressBookRegistry registry2 = new AddressBookRegistry();

        // Get genesis books from both registries
        NodeAddressBook book1 = registry1.getCurrentAddressBook();
        NodeAddressBook book2 = registry2.getCurrentAddressBook();

        // They should be different object references
        assertNotSame(book1, book2, "Should be different object instances");

        // But they should be equal in content
        assertEquals(book1, book2, "Should have equal content (proper equals() implementation)");

        // Verify both registries start with same size
        int count1 = registry1.toPrettyString().split("\n").length - 1;
        int count2 = registry2.toPrettyString().split("\n").length - 1;
        assertEquals(count1, count2, "Both registries should start with same number of entries");
    }

    /**
     * Test that getAddressBookForBlock() returns the most recent address book when
     * the block time is after all address book entries in the registry.
     *
     * This test verifies the fix for the bug where blocks with timestamps after all
     * address book entries were incorrectly returning the genesis address book (13 nodes)
     * instead of the most recent address book (e.g., 31 nodes).
     *
     * Bug scenario: When validating blocks from Nov 13, 2025 onwards with an address book
     * history that only goes up to Nov 12, 2025, the old code would fall back to genesis
     * instead of using the Nov 12, 2025 address book.
     */
    @Test
    public void testGetAddressBookForBlockAfterAllEntries() throws Exception {
        try (var in = new ReadableStreamingData(AddressBookRegistryTest.class.getResourceAsStream(
                "/2021-06-08T17_35_26.000831000Z-file-102-update-transaction-body.bin"))) {
            int numOfTransactions = in.readInt();
            List<TransactionBody> transactionBodies = new ArrayList<>(numOfTransactions);
            for (int i = 0; i < numOfTransactions; i++) {
                int len = in.readInt();
                Bytes tbBytes = in.readBytes(len);
                transactionBodies.add(TransactionBody.PROTOBUF.parse(tbBytes));
            }

            AddressBookRegistry addressBookRegistry = new AddressBookRegistry();

            NodeAddressBook genesisBook = addressBookRegistry.getCurrentAddressBook();
            int genesisNodeCount = genesisBook.nodeAddress().size();

            Instant updateTime = Instant.parse("2021-06-08T17:35:26.000831000Z");
            addressBookRegistry.updateAddressBook(updateTime, transactionBodies);

            NodeAddressBook updatedBook = addressBookRegistry.getCurrentAddressBook();
            int updatedNodeCount = updatedBook.nodeAddress().size();
            assertEquals(21, updatedNodeCount, "Updated address book should have 21 nodes");

            assertNotEquals(
                    genesisNodeCount, updatedNodeCount, "Genesis and updated books should have different node counts");

            Instant beforeUpdateTime = Instant.parse("2020-01-01T00:00:00.000000000Z");
            NodeAddressBook bookBeforeUpdate = addressBookRegistry.getAddressBookForBlock(beforeUpdateTime);
            assertEquals(
                    genesisNodeCount,
                    bookBeforeUpdate.nodeAddress().size(),
                    "Block before update should use genesis address book");

            NodeAddressBook bookAtUpdate = addressBookRegistry.getAddressBookForBlock(updateTime);
            assertEquals(
                    updatedNodeCount,
                    bookAtUpdate.nodeAddress().size(),
                    "Block at update time should use updated address book");

            Instant afterUpdateTime = Instant.parse("2022-01-01T00:00:00.000000000Z");
            NodeAddressBook bookAfterUpdate = addressBookRegistry.getAddressBookForBlock(afterUpdateTime);
            assertEquals(
                    updatedNodeCount,
                    bookAfterUpdate.nodeAddress().size(),
                    "Block after update should use updated address book");

            Instant wayAfterUpdateTime = Instant.parse("2025-11-13T00:00:00.000000000Z");
            NodeAddressBook bookWayAfter = addressBookRegistry.getAddressBookForBlock(wayAfterUpdateTime);
            assertEquals(
                    updatedNodeCount,
                    bookWayAfter.nodeAddress().size(),
                    "Block way after all entries should use MOST RECENT address book, not genesis");
            assertNotEquals(
                    genesisNodeCount,
                    bookWayAfter.nodeAddress().size(),
                    "Block way after all entries should NOT fall back to genesis");
        }
    }

    /**
     * Test that reloadFromFile replaces the registry contents with the saved file.
     */
    @Test
    public void testReloadFromFile(@TempDir Path tempDir) throws Exception {
        // Create a registry and add an address book update
        try (var in = new ReadableStreamingData(AddressBookRegistryTest.class.getResourceAsStream(
                "/2021-06-08T17_35_26.000831000Z-file-102-update-transaction-body.bin"))) {
            int numOfTransactions = in.readInt();
            List<TransactionBody> transactionBodies = new ArrayList<>(numOfTransactions);
            for (int i = 0; i < numOfTransactions; i++) {
                int len = in.readInt();
                Bytes tbBytes = in.readBytes(len);
                transactionBodies.add(TransactionBody.PROTOBUF.parse(tbBytes));
            }

            AddressBookRegistry registry1 = new AddressBookRegistry();
            registry1.updateAddressBook(Instant.parse("2021-06-08T17:35:26.000831000Z"), transactionBodies);
            assertEquals(2, registry1.getAddressBookCount(), "Registry should have 2 entries after update");

            // Save to file
            Path jsonFile = tempDir.resolve("addressBookHistory.json");
            registry1.saveAddressBookRegistryToJsonFile(jsonFile);

            // Create a fresh registry (only genesis) and reload from the saved file
            AddressBookRegistry registry2 = new AddressBookRegistry();
            assertEquals(1, registry2.getAddressBookCount(), "Fresh registry should have 1 entry");

            registry2.reloadFromFile(jsonFile);
            assertEquals(2, registry2.getAddressBookCount(), "Reloaded registry should have 2 entries");

            // Verify the current address book matches
            assertEquals(
                    registry1.getCurrentAddressBook().nodeAddress().size(),
                    registry2.getCurrentAddressBook().nodeAddress().size(),
                    "Reloaded registry should have same node count as original");
        }
    }

    @Test
    public void testReloadFromNonExistentFileThrows(@TempDir Path tempDir) {
        AddressBookRegistry registry = new AddressBookRegistry();
        Path nonExistent = tempDir.resolve("does-not-exist.json");
        assertThrows(UncheckedIOException.class, () -> registry.reloadFromFile(nonExistent));
    }

    @Test
    public void testReloadFromCorruptFileThrows(@TempDir Path tempDir) throws Exception {
        AddressBookRegistry registry = new AddressBookRegistry();
        Path corruptFile = tempDir.resolve("corrupt.json");
        Files.writeString(corruptFile, "this is not valid json");
        assertThrows(UncheckedIOException.class, () -> registry.reloadFromFile(corruptFile));
    }

    @Nested
    @DisplayName("Dynamic Address Book (DAB) Transaction Tests")
    class DabTransactionTests {

        /** Sample DER-encoded certificate bytes for testing (fake but realistic length). */
        private static final byte[] SAMPLE_CERT_BYTES = new byte[256];

        static {
            // Fill with non-zero bytes so hex encoding produces a meaningful string
            for (int i = 0; i < SAMPLE_CERT_BYTES.length; i++) {
                SAMPLE_CERT_BYTES[i] = (byte) (i & 0xFF);
            }
        }

        private static final String SAMPLE_CERT_HEX = HexFormat.of().formatHex(SAMPLE_CERT_BYTES);

        @Test
        @DisplayName("NODEUPDATE with gossip CA certificate rotates node key")
        public void testNodeUpdateCertificateRotation() throws ParseException {
            AddressBookRegistry registry = new AddressBookRegistry();
            NodeAddressBook genesisBook = registry.getCurrentAddressBook();
            // Pick first node from genesis book
            NodeAddress firstNode = genesisBook.nodeAddress().getFirst();
            long nodeAcctNum = AddressBookRegistry.getNodeAccountId(firstNode);
            String originalKey = firstNode.rsaPubKey();

            // Create a NODEUPDATE transaction that rotates the gossip CA certificate
            TransactionBody updateBody = TransactionBody.newBuilder()
                    .nodeUpdate(NodeUpdateTransactionBody.newBuilder()
                            .nodeId(firstNode.nodeId())
                            .gossipCaCertificate(Bytes.wrap(SAMPLE_CERT_BYTES))
                            .build())
                    .build();

            String changes = registry.updateAddressBook(Instant.now(), List.of(updateBody));
            assertNotNull(changes, "Should detect address book change from NODEUPDATE");
            assertTrue(changes.contains("node lifecycle (DAB)"), "Change source should be DAB");
            assertTrue(changes.contains("key changed"), "Should report key change");

            // Verify the node's key was updated
            NodeAddressBook updatedBook = registry.getCurrentAddressBook();
            String updatedKey = AddressBookRegistry.publicKeyForNode(updatedBook, 0, 0, nodeAcctNum);
            assertNotEquals(originalKey, updatedKey, "Key should have changed after NODEUPDATE");
            assertEquals(SAMPLE_CERT_HEX, updatedKey, "Key should be hex-encoded certificate");
        }

        @Test
        @DisplayName("NODEUPDATE updates description and account ID when provided")
        public void testNodeUpdatePartialFields() throws ParseException {
            AddressBookRegistry registry = new AddressBookRegistry();
            NodeAddressBook genesisBook = registry.getCurrentAddressBook();
            NodeAddress firstNode = genesisBook.nodeAddress().getFirst();

            TransactionBody updateBody = TransactionBody.newBuilder()
                    .nodeUpdate(NodeUpdateTransactionBody.newBuilder()
                            .nodeId(firstNode.nodeId())
                            .description("Updated node description")
                            .build())
                    .build();

            String changes = registry.updateAddressBook(Instant.now(), List.of(updateBody));
            // Description change alone doesn't change keys, so address book still "changes" via equals
            NodeAddressBook updatedBook = registry.getCurrentAddressBook();
            NodeAddress updatedNode = updatedBook.nodeAddress().stream()
                    .filter(n -> n.nodeId() == firstNode.nodeId())
                    .findFirst()
                    .orElseThrow();
            assertEquals("Updated node description", updatedNode.description());
            // Key should remain unchanged
            assertEquals(firstNode.rsaPubKey(), updatedNode.rsaPubKey());
        }

        @Test
        @DisplayName("NODEUPDATE for non-existent node returns unchanged book")
        public void testNodeUpdateNonExistentNode() throws ParseException {
            AddressBookRegistry registry = new AddressBookRegistry();
            int initialCount = registry.getAddressBookCount();

            TransactionBody updateBody = TransactionBody.newBuilder()
                    .nodeUpdate(NodeUpdateTransactionBody.newBuilder()
                            .nodeId(99999)
                            .gossipCaCertificate(Bytes.wrap(SAMPLE_CERT_BYTES))
                            .build())
                    .build();

            String changes = registry.updateAddressBook(Instant.now(), List.of(updateBody));
            assertNull(changes, "Should not detect changes when node not found");
            assertEquals(initialCount, registry.getAddressBookCount());
        }

        @Test
        @DisplayName("NODECREATE adds a new node to the address book")
        public void testNodeCreate() throws ParseException {
            AddressBookRegistry registry = new AddressBookRegistry();
            NodeAddressBook genesisBook = registry.getCurrentAddressBook();
            int initialNodeCount = genesisBook.nodeAddress().size();

            // Create a new node with account 0.0.50 (nodeId = 47)
            AccountID newAcctId = AccountID.newBuilder().accountNum(50).build();
            TransactionBody createBody = TransactionBody.newBuilder()
                    .nodeCreate(NodeCreateTransactionBody.newBuilder()
                            .accountId(newAcctId)
                            .gossipCaCertificate(Bytes.wrap(SAMPLE_CERT_BYTES))
                            .description("New test node")
                            .build())
                    .build();

            String changes = registry.updateAddressBook(Instant.now(), List.of(createBody));
            assertNotNull(changes, "Should detect address book change from NODECREATE");
            assertTrue(changes.contains("node lifecycle (DAB)"), "Change source should be DAB");
            assertTrue(changes.contains("Node 50 added"), "Should report node addition");

            NodeAddressBook updatedBook = registry.getCurrentAddressBook();
            assertEquals(initialNodeCount + 1, updatedBook.nodeAddress().size(), "Should have one more node");

            // Verify the new node's properties
            NodeAddress newNode = updatedBook.nodeAddress().stream()
                    .filter(n -> AddressBookRegistry.getNodeAccountId(n) == 50)
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("New node not found"));
            assertEquals(47, newNode.nodeId(), "nodeId should be accountNum - 3");
            assertEquals(SAMPLE_CERT_HEX, newNode.rsaPubKey());
            assertEquals("New test node", newNode.description());
            assertEquals("0.0.50", newNode.memo().asUtf8String());
        }

        @Test
        @DisplayName("NODECREATE replaces existing node with same account ID")
        public void testNodeCreateReplacesExisting() throws ParseException {
            AddressBookRegistry registry = new AddressBookRegistry();
            NodeAddressBook genesisBook = registry.getCurrentAddressBook();
            // Get account number of the first node
            NodeAddress firstNode = genesisBook.nodeAddress().getFirst();
            long existingAcctNum = AddressBookRegistry.getNodeAccountId(firstNode);
            int initialNodeCount = genesisBook.nodeAddress().size();

            // Create a node with the same account ID — should replace
            AccountID existingAcctId =
                    AccountID.newBuilder().accountNum(existingAcctNum).build();
            TransactionBody createBody = TransactionBody.newBuilder()
                    .nodeCreate(NodeCreateTransactionBody.newBuilder()
                            .accountId(existingAcctId)
                            .gossipCaCertificate(Bytes.wrap(SAMPLE_CERT_BYTES))
                            .description("Replacement node")
                            .build())
                    .build();

            String changes = registry.updateAddressBook(Instant.now(), List.of(createBody));
            assertNotNull(changes);

            NodeAddressBook updatedBook = registry.getCurrentAddressBook();
            // Node count should be the same (replaced, not added)
            assertEquals(initialNodeCount, updatedBook.nodeAddress().size());
            // Verify the replacement
            String newKey = AddressBookRegistry.publicKeyForNode(updatedBook, 0, 0, existingAcctNum);
            assertEquals(SAMPLE_CERT_HEX, newKey);
        }

        @Test
        @DisplayName("NODEDELETE removes a node from the address book")
        public void testNodeDelete() throws ParseException {
            AddressBookRegistry registry = new AddressBookRegistry();

            // First, create a node with a known unique nodeId via NODECREATE
            AccountID newAcctId = AccountID.newBuilder().accountNum(50).build();
            TransactionBody createBody = TransactionBody.newBuilder()
                    .nodeCreate(NodeCreateTransactionBody.newBuilder()
                            .accountId(newAcctId)
                            .gossipCaCertificate(Bytes.wrap(SAMPLE_CERT_BYTES))
                            .description("Node to delete")
                            .build())
                    .build();
            registry.updateAddressBook(Instant.parse("2026-01-01T00:00:00Z"), List.of(createBody));
            NodeAddressBook bookAfterCreate = registry.getCurrentAddressBook();
            int nodeCountAfterCreate = bookAfterCreate.nodeAddress().size();

            // Now delete that node by its nodeId (50 - 3 = 47)
            TransactionBody deleteBody = TransactionBody.newBuilder()
                    .nodeDelete(
                            NodeDeleteTransactionBody.newBuilder().nodeId(47).build())
                    .build();

            String changes = registry.updateAddressBook(Instant.parse("2026-01-02T00:00:00Z"), List.of(deleteBody));
            assertNotNull(changes, "Should detect address book change from NODEDELETE");
            assertTrue(changes.contains("node lifecycle (DAB)"), "Change source should be DAB");
            assertTrue(changes.contains("removed"), "Should report node removal");

            NodeAddressBook updatedBook = registry.getCurrentAddressBook();
            assertEquals(nodeCountAfterCreate - 1, updatedBook.nodeAddress().size(), "Should have one fewer node");
            assertTrue(
                    updatedBook.nodeAddress().stream().noneMatch(n -> n.nodeId() == 47),
                    "Deleted node should not be present");
        }

        @Test
        @DisplayName("NODEDELETE for non-existent node returns unchanged book")
        public void testNodeDeleteNonExistentNode() throws ParseException {
            AddressBookRegistry registry = new AddressBookRegistry();
            int initialCount = registry.getAddressBookCount();

            TransactionBody deleteBody = TransactionBody.newBuilder()
                    .nodeDelete(
                            NodeDeleteTransactionBody.newBuilder().nodeId(99999).build())
                    .build();

            String changes = registry.updateAddressBook(Instant.now(), List.of(deleteBody));
            assertNull(changes, "Should not detect changes when node not found");
            assertEquals(initialCount, registry.getAddressBookCount());
        }

        @Test
        @DisplayName("filterToJustAddressBookTransactions includes DAB transactions")
        public void testFilterIncludesNodeLifecycleTransactions() throws ParseException {
            TransactionBody nodeUpdateBody = TransactionBody.newBuilder()
                    .nodeUpdate(NodeUpdateTransactionBody.newBuilder().nodeId(0).build())
                    .build();
            TransactionBody nodeCreateBody = TransactionBody.newBuilder()
                    .nodeCreate(NodeCreateTransactionBody.newBuilder()
                            .accountId(AccountID.newBuilder().accountNum(50).build())
                            .build())
                    .build();
            TransactionBody nodeDeleteBody = TransactionBody.newBuilder()
                    .nodeDelete(NodeDeleteTransactionBody.newBuilder().nodeId(0).build())
                    .build();

            // Wrap each in a Transaction with body set directly
            List<Transaction> transactions = List.of(
                    Transaction.newBuilder().body(nodeUpdateBody).build(),
                    Transaction.newBuilder().body(nodeCreateBody).build(),
                    Transaction.newBuilder().body(nodeDeleteBody).build());

            List<TransactionBody> filtered = AddressBookRegistry.filterToJustAddressBookTransactions(transactions);
            assertEquals(3, filtered.size(), "All three DAB transaction types should be included");
            assertTrue(filtered.get(0).hasNodeUpdate());
            assertTrue(filtered.get(1).hasNodeCreate());
            assertTrue(filtered.get(2).hasNodeDelete());
        }

        @Test
        @DisplayName("filterToJustAddressBookTransactions excludes non-address-book transactions")
        public void testFilterExcludesNonAddressBookTransactions() throws ParseException {
            // A transaction with no relevant fields (empty body)
            TransactionBody emptyBody = TransactionBody.newBuilder().build();
            List<Transaction> transactions =
                    List.of(Transaction.newBuilder().body(emptyBody).build());

            List<TransactionBody> filtered = AddressBookRegistry.filterToJustAddressBookTransactions(transactions);
            assertEquals(0, filtered.size(), "Non-address-book transactions should be excluded");
        }

        @Test
        @DisplayName("Multiple DAB transactions in same block are applied sequentially")
        public void testMultipleDabTransactionsInSameBlock() throws ParseException {
            AddressBookRegistry registry = new AddressBookRegistry();
            NodeAddressBook genesisBook = registry.getCurrentAddressBook();
            NodeAddress firstNode = genesisBook.nodeAddress().getFirst();
            int initialNodeCount = genesisBook.nodeAddress().size();

            // Transaction 1: Update first node's cert
            TransactionBody updateBody = TransactionBody.newBuilder()
                    .nodeUpdate(NodeUpdateTransactionBody.newBuilder()
                            .nodeId(firstNode.nodeId())
                            .gossipCaCertificate(Bytes.wrap(SAMPLE_CERT_BYTES))
                            .build())
                    .build();

            // Transaction 2: Create a new node
            AccountID newAcctId = AccountID.newBuilder().accountNum(100).build();
            TransactionBody createBody = TransactionBody.newBuilder()
                    .nodeCreate(NodeCreateTransactionBody.newBuilder()
                            .accountId(newAcctId)
                            .gossipCaCertificate(Bytes.wrap(SAMPLE_CERT_BYTES))
                            .description("Another new node")
                            .build())
                    .build();

            String changes = registry.updateAddressBook(Instant.now(), List.of(updateBody, createBody));
            assertNotNull(changes);

            NodeAddressBook updatedBook = registry.getCurrentAddressBook();
            // Should have original count + 1 new node
            assertEquals(initialNodeCount + 1, updatedBook.nodeAddress().size());
            // First node should have updated key
            long firstNodeAcct = AddressBookRegistry.getNodeAccountId(firstNode);
            assertEquals(SAMPLE_CERT_HEX, AddressBookRegistry.publicKeyForNode(updatedBook, 0, 0, firstNodeAcct));
            // New node should exist
            assertNotNull(updatedBook.nodeAddress().stream()
                    .filter(n -> AddressBookRegistry.getNodeAccountId(n) == 100)
                    .findFirst()
                    .orElse(null));
        }
    }
}
