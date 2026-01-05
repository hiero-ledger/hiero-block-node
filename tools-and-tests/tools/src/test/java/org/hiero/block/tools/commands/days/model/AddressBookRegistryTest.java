// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands.days.model;

import static org.junit.jupiter.api.Assertions.*;

import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.transaction.TransactionBody;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.hedera.pbj.runtime.io.stream.ReadableStreamingData;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.hiero.block.tools.days.model.AddressBookRegistry;
import org.junit.jupiter.api.Test;

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
            assertEquals(2, addressBookRegistry.getAddressBookCount(), "Should NOT add duplicate entry when content is identical");

            // Third update with empty transaction list - should NOT add entry
            Instant time3 = Instant.parse("2021-06-08T19:00:00.000000000Z");
            String changes3 = addressBookRegistry.updateAddressBook(time3, new ArrayList<>());
            assertNull(changes3, "Should NOT detect changes with empty transactions");

            // Size should still be 2
            assertEquals(2, addressBookRegistry.getAddressBookCount(), "Should NOT add entry when no transactions provided");
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

            assertNotEquals(genesisNodeCount, updatedNodeCount, "Genesis and updated books should have different node counts");

            Instant beforeUpdateTime = Instant.parse("2020-01-01T00:00:00.000000000Z");
            NodeAddressBook bookBeforeUpdate = addressBookRegistry.getAddressBookForBlock(beforeUpdateTime);
            assertEquals(genesisNodeCount, bookBeforeUpdate.nodeAddress().size(),
                "Block before update should use genesis address book");

            NodeAddressBook bookAtUpdate = addressBookRegistry.getAddressBookForBlock(updateTime);
            assertEquals(updatedNodeCount, bookAtUpdate.nodeAddress().size(),
                "Block at update time should use updated address book");

            Instant afterUpdateTime = Instant.parse("2022-01-01T00:00:00.000000000Z");
            NodeAddressBook bookAfterUpdate = addressBookRegistry.getAddressBookForBlock(afterUpdateTime);
            assertEquals(updatedNodeCount, bookAfterUpdate.nodeAddress().size(),
                "Block after update should use updated address book");

            Instant wayAfterUpdateTime = Instant.parse("2025-11-13T00:00:00.000000000Z");
            NodeAddressBook bookWayAfter = addressBookRegistry.getAddressBookForBlock(wayAfterUpdateTime);
            assertEquals(updatedNodeCount, bookWayAfter.nodeAddress().size(),
                "Block way after all entries should use MOST RECENT address book, not genesis");
            assertNotEquals(genesisNodeCount, bookWayAfter.nodeAddress().size(),
                "Block way after all entries should NOT fall back to genesis");
        }
    }
}
