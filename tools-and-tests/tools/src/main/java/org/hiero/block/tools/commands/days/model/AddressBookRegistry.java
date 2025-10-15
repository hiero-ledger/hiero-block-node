// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands.days.model;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import com.hedera.hapi.node.base.AccountID;
import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.transaction.SignedTransaction;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.hedera.pbj.runtime.io.stream.ReadableStreamingData;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
/**
 * Registry of address books, starting with the Genesis address book.
 * New address books can be added as they are encountered in the record files.
 * The current address book is the most recently added address book.
 */
public class AddressBookRegistry {
    private final List<NodeAddressBook> addressBooks = new ArrayList<>();

    /**
     * Create a new AddressBookRegistry instance and load the Genesis address book.
     */
    public AddressBookRegistry() {
        try {
            addressBooks.add(loadGenesisAddressBook());
        } catch (ParseException e) {
            throw new RuntimeException("Error loading Genesis Address Book", e);
        }
    }

    /**
     * Get the current address book. Which is the most recently added address book.
     *
     * @return the current address book
     */
    public NodeAddressBook getCurrentAddressBook() {
        return addressBooks.getLast();
    }

    /**
     * Update the address book registry with any new address books found in the provided list of transactions.
     * This method should be called when processing a new block of transactions to ensure the address book is up to date.
     * <p>
     * There are two kinds of transactions that can update the address book:
     * <ul>
     *   <li>File append transactions that update the address book file(id: 0.0.102) with a new address book. Of type
     *   com.hedera.hapi.node.file.FileAppendTransactionBody . There can be one or more append transactions for a
     *   complete address book file contents update.</li>
     *   <li>Address book change transactions that add, update or remove nodes. These are types
     *   NodeCreateTransactionBody, NodeUpdateTransactionBody and NodeDeleteTransactionBody</li>
     * </ul>
     * This method should handle both types of transactions and update the address book accordingly.
     * </p>
     *
     * @param addressBookTransactions the list of transactions to check for address book updates
     */
    public void updateAddressBook(List<SignedTransaction> addressBookTransactions) {
        // TODO walk through the transactions, extract any file 0.0.102 updates or address book change transactions
        //  and apply them to create a new address book, then add it to the list.
    }

    // ==== Static utility methods for loading address books ====

    public static NodeAddressBook loadGenesisAddressBook() throws ParseException {
        try (var in = new ReadableStreamingData(Objects.requireNonNull(AddressBookRegistry.class
                .getClassLoader()
                .getResourceAsStream("mainnet-genesis-address-book.proto.bin")))) {
            return NodeAddressBook.PROTOBUF.parse(in);
        }
    }

    public static NodeAddressBook readAddressBook(byte[] bytes) throws ParseException {
        return NodeAddressBook.PROTOBUF.parse(Bytes.wrap(bytes));
    }

    /**
     * Get the public key for a node in the address book. This uses the memo field of the NodeAddress which contains
     * the node's shard, realm and number as a UTF-8 string.
     *
     * @param addressBook the address book to use to find the node
     * @param shard the shard number of the node
     * @param realm the realm number of the node
     * @param number the node number of the node
     * @return the public key for the node
     */
    public static String publicKeyForNode(
            final NodeAddressBook addressBook, final long shard, final long realm, final long number) {
        String nodeAddress = String.format("%d.%d.%d", shard, realm, number);
        return addressBook.nodeAddress().stream()
                .filter(na -> na.memo().asUtf8String().equals(nodeAddress))
                .findFirst()
                .orElseThrow()
                .rsaPubKey();
    }

    public static void main(String[] args) throws ParseException {
        System.out.println("Loading Genesis Address Book...");
        NodeAddressBook addressBook = loadGenesisAddressBook();
        System.out.println("Genesis Address Book loaded successfully.");
        for (NodeAddress nodeAddress : addressBook.nodeAddress()) {
            System.out.println("memo=" + nodeAddress.memo().asUtf8String() + " rsa_pub_key=" + nodeAddress.rsaPubKey());
        }

        // test public key lookup
        String publicKey = publicKeyForNode(addressBook, 0, 0, 11);
        System.out.println("publicKey=" + publicKey);
        if (!publicKey.startsWith(
                "308201a2300d06092a864886f70d01010105000382018f003082018a02820181009bdd8e84fadaa35")) {
            throw new RuntimeException("Unexpected public key");
        }
    }

    /**
     * Address book snapshot for October 2025 loaded from address-book-oct-2025.json at repository root.
     * If the JSON cannot be found or parsed at runtime, this constant will be an empty address book.
     */
    public static final NodeAddressBook OCT_2025 = loadOct2025();

    private static NodeAddressBook loadOct2025() {
        try {
            final var in = AddressBookRegistry.class.getClassLoader().getResourceAsStream("address-book-oct-2025.json");
            if (in == null) {
                return NodeAddressBook.DEFAULT; // Not available
            }
            try (in) {
                final Gson gson = new Gson();
                final AddressBookJson json = gson.fromJson(new java.io.InputStreamReader(in, StandardCharsets.UTF_8), AddressBookJson.class);
                final List<NodeAddress> nodes = new ArrayList<>();
                if (json != null && json.nodes != null) {
                    for (final AddressBookJson.Node n : json.nodes) {
                        final long accountNum = parseAccountNum(n.nodeAccountId);
                        final String memoStr = "0.0." + accountNum;
                        final String rsaHex = strip0x(n.publicKey);
                        final byte[] certHash = parseHexOrEmpty(n.nodeCertHash);

                        final AccountID accountID = AccountID.newBuilder()
                                .shardNum(0)
                                .realmNum(0)
                                .accountNum(accountNum)
                                .build();

                        final NodeAddress node = NodeAddress.newBuilder()
                                .memo(Bytes.wrap(memoStr.getBytes(StandardCharsets.UTF_8)))
                                .rsaPubKey(rsaHex)
                                .nodeId(n.nodeId)
                                .nodeAccountId(accountID)
                                .nodeCertHash(Bytes.wrap(certHash))
                                .build();
                        nodes.add(node);
                    }
                }
                return new NodeAddressBook(nodes);
            }
        } catch (Exception e) {
            // If anything goes wrong, return an empty book to avoid breaking callers.
            return NodeAddressBook.DEFAULT;
        }
    }

    private static String strip0x(String s) {
        if (s == null) return "";
        return s.startsWith("0x") || s.startsWith("0X") ? s.substring(2) : s;
    }

    private static long parseAccountNum(String account) {
        if (account == null || account.isBlank()) return 0L;
        // expects format like "0.0.23"
        final String[] parts = account.split("\\.");
        return Long.parseLong(parts[parts.length - 1]);
    }

    private static byte[] parseHexOrEmpty(String hex) {
        if (hex == null || hex.isBlank()) return new byte[0];
        final String s = strip0x(hex);
        try {
            return HexFormat.of().parseHex(s);
        } catch (Exception e) {
            return new byte[0];
        }
    }

    // Minimal JSON mapping classes
    private static final class AddressBookJson {
        List<Node> nodes;
        static final class Node {
            @SerializedName("node_id") long nodeId;
            @SerializedName("node_account_id") String nodeAccountId;
            @SerializedName("public_key") String publicKey;
            @SerializedName("node_cert_hash") String nodeCertHash;
        }
    }
}
