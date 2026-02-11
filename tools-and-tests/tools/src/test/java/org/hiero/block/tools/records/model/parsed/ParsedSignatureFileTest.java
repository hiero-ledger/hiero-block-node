// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.records.model.parsed;

import static org.hiero.block.tools.utils.TestBlocks.loadV6SignatureFiles;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.hedera.hapi.node.base.AccountID;
import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.ServiceEndpoint;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ParsedSignatureFile}.
 */
class ParsedSignatureFileTest {

    @Nested
    @DisplayName("isValid with AddressBook Tests")
    class IsValidWithAddressBookTests {

        @Test
        @DisplayName("Returns false when node is not in address book")
        void returnsFalseWhenNodeNotInAddressBook() {
            // Load a real signature file from node 3 (from V6 test data)
            List<ParsedSignatureFile> sigFiles = loadV6SignatureFiles();
            ParsedSignatureFile sigFileFromNode3 = sigFiles.stream()
                    .filter(sf -> sf.accountNum() == 3)
                    .findFirst()
                    .orElseThrow();

            // Create an address book that does NOT include node 3 (only nodes 100, 101)
            NodeAddressBook addressBookWithoutNode3 = createAddressBookWithNodes(100, 101);

            // Should return false (not throw) when node 3 is not in address book
            byte[] someHash = new byte[48];
            boolean result = sigFileFromNode3.isValid(someHash, addressBookWithoutNode3);

            assertFalse(result, "isValid should return false when node is not in address book");
        }

        @Test
        @DisplayName("Returns false when address book is empty")
        void returnsFalseWhenAddressBookEmpty() {
            // Load a real signature file
            List<ParsedSignatureFile> sigFiles = loadV6SignatureFiles();
            ParsedSignatureFile sigFile = sigFiles.get(0);

            // Create an empty address book
            NodeAddressBook emptyAddressBook = new NodeAddressBook(List.of());

            // Should return false (not throw) when address book is empty
            byte[] someHash = new byte[48];
            boolean result = sigFile.isValid(someHash, emptyAddressBook);

            assertFalse(result, "isValid should return false when address book is empty");
        }
    }

    /**
     * Creates a minimal address book with the specified node account numbers.
     */
    private static NodeAddressBook createAddressBookWithNodes(int... nodeAccountNums) {
        List<NodeAddress> addresses = new java.util.ArrayList<>();
        for (int i = 0; i < nodeAccountNums.length; i++) {
            int accountNum = nodeAccountNums[i];
            NodeAddress nodeAddress = NodeAddress.newBuilder()
                    .nodeId(i)
                    .nodeAccountId(AccountID.newBuilder()
                            .shardNum(0)
                            .realmNum(0)
                            .accountNum(accountNum)
                            .build())
                    .rsaPubKey("MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA" + accountNum)
                    .serviceEndpoint(List.of(ServiceEndpoint.newBuilder()
                            .ipAddressV4(Bytes.wrap(new byte[] {127, 0, 0, 1}))
                            .port(50211)
                            .build()))
                    .build();
            addresses.add(nodeAddress);
        }
        return new NodeAddressBook(addresses);
    }
}
