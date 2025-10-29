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
}
