// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands.days.model;

import static org.hiero.block.tools.utils.TimeUtils.GENESIS_TIMESTAMP;
import static org.hiero.block.tools.utils.TimeUtils.toTimestamp;

import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.Transaction;
import com.hedera.hapi.node.transaction.SignedTransaction;
import com.hedera.hapi.node.transaction.TransactionBody;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.hedera.pbj.runtime.io.stream.ReadableStreamingData;
import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.hiero.block.internal.AddressBookHistory;
import org.hiero.block.internal.DatedNodeAddressBook;

/**
 * Registry of address books, starting with the Genesis address book.
 * New address books can be added as they are encountered in the record files.
 * The current address book is the most recently added address book.
 */
public class AddressBookRegistry {
    private final List<DatedNodeAddressBook> addressBooks = new ArrayList<>();
    // Maintain partial payloads for file id 2 only. Only completed parses for 0.0.102 are appended to addressBooks to
    // keep getCurrentAddressBook() aligned with authoritative book semantics.
    private ByteArrayOutputStream partialFileUpload = null;



    /**
     * Create a new AddressBookRegistry instance and load the Genesis address book.
     */
    public AddressBookRegistry() {
        try {
            addressBooks.add(new DatedNodeAddressBook(GENESIS_TIMESTAMP,loadGenesisAddressBook()));
        } catch (ParseException e) {
            throw new RuntimeException("Error loading Genesis Address Book", e);
        }
    }

    /**
     * Create a new AddressBookRegistry instance loading from JSON file.
     */
    public AddressBookRegistry(Path jsonFile) {
        try (var in = new ReadableStreamingData(Files.newInputStream(jsonFile))) {
            AddressBookHistory history = AddressBookHistory.JSON.parse(in);
            addressBooks.addAll(history.addressBooks());
        } catch (IOException | ParseException e) {
            throw new RuntimeException("Error loading Address Book History JSON file "+jsonFile, e);
        }
    }

    /**
     * Save the address book registry to a JSON file.
     *
     * @param file the path to the JSON file
     */
    public void saveAddressBookRegistryToJsonFile(Path file) {
        try(var out = new WritableStreamingData(Files.newOutputStream(file))) {
            AddressBookHistory history = new AddressBookHistory(addressBooks);
            AddressBookHistory.JSON.write(history, out);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the current address book. Which is the most recently added address book.
     *
     * @return the current address book
     */
    public NodeAddressBook getCurrentAddressBook() {
        return addressBooks.getLast().addressBook();
    }

    /**
     * Update the address book registry with any new address books found in the provided list of transactions.
     * This method should be called when processing a new block of transactions to ensure the address book is up to date.
     * <p>
     * There are two kinds of transactions that can update the address book:
     * <ul>
     *   <li>File update/append transactions that update the address book file (id: 0.0.102) with a new address book. Of
     *   types com.hedera.hapi.node.file.FileUpdateTransactionBody and FileAppendTransactionBody. There can be one or
     *   more append transactions for a complete address book file contents update.</li>
     *   <li>Address book change transactions that add, update or remove nodes. These are types
     *   NodeCreateTransactionBody, NodeUpdateTransactionBody and NodeDeleteTransactionBody</li>
     * </ul>
     * This method should handle both types of transactions and update the address book accordingly.
     * </p>
     *
     * @param addressBookTransactions the list of transactions to check for address book updates
     * @return a string describing the changes made to the address book, or a null string if no changes were made
     */
    @SuppressWarnings("DataFlowIssue")
    public String updateAddressBook(Instant blockInstant, List<TransactionBody> addressBookTransactions) {
        final NodeAddressBook currentBook = getCurrentAddressBook();
        NodeAddressBook newAddressBook = currentBook;
        // Walk through transactions in order, maintaining a buffer for 0.0.102. Only successful 0.0.102 parses produce
        // a
        // new version appended to addressBooks to align with authoritative address book semantics.
        for (final TransactionBody body : addressBookTransactions) {
            try {
                // Handle file-based updates for 0.0.102 only
                if ((body.hasFileUpdate() && body.fileUpdate().fileID().fileNum() == 102)
                        || (body.hasFileAppend() && body.fileAppend().fileID().fileNum() == 102)) {
                    if (partialFileUpload == null) partialFileUpload = new ByteArrayOutputStream();
                    if (body.hasFileUpdate()) {
                        body.fileUpdate().contents().writeTo(partialFileUpload);
                    } else { // append
                        body.fileAppend().contents().writeTo(partialFileUpload);
                    }
                    final byte[] contents = partialFileUpload.toByteArray();
                    // Ignore empty contents and try to parse a full NodeAddressBook from the accumulated bytes
                    if (contents.length > 0) {
                        try {
                            newAddressBook = readAddressBook(contents);
                            // Successfully parsed a new complete/valid book; reset partial accumulator
                            partialFileUpload = null;
                        } catch (final ParseException parseException) {
                            // Not yet a complete/valid book; keep accumulating across future appends
                            // Do nothing on failure.
                        }
                    }
                }
                // Ignore other transaction types (e.g., node lifecycle) in this registry; only file-based updates are
                // applied to compute new address book versions here.
            } catch (Exception e) {
                throw new RuntimeException("Error updating address book", e);
            }
        }
        if (newAddressBook != currentBook) {
            addressBooks.add(new DatedNodeAddressBook(toTimestamp(blockInstant),newAddressBook));
            // Update changes description
            return "Address Book Changed, via file update:\n" + addressBookChanges(currentBook, newAddressBook);
        }
        return null;
    }

    // ==== Static utility methods for loading address books ====

    /**
     * Filter a list of transactions to just those that are address book related. These are either file append
     * transactions to file 0.0.102 or node create/update/delete transactions.
     *
     * @param transactions the list of transactions to filter
     * @return a list of TransactionBody objects that are address book related
     * @throws ParseException if there is an error parsing a transaction
     */
    @SuppressWarnings("DataFlowIssue")
    public static List<TransactionBody> filterToJustAddressBookTransactions(List<Transaction> transactions)
            throws ParseException {
        List<TransactionBody> result = new ArrayList<>();
        for (Transaction t : transactions) {
            TransactionBody body;
            if (t.hasBody()) {
                body = t.body();
            } else if (t.bodyBytes().length() > 0) {
                body = TransactionBody.PROTOBUF.parse(t.bodyBytes());
            } else if (t.signedTransactionBytes().length() > 0) {
                final SignedTransaction st = SignedTransaction.PROTOBUF.parse(t.signedTransactionBytes());
                body = TransactionBody.PROTOBUF.parse(st.bodyBytes());
            } else {
                // no transaction body or signed bytes, ignore
                throw new ParseException("Transaction has no body or signed bytes");
            }
            // check if this is a file update/append to file 0.0.102
            if ((body.hasFileUpdate() && body.fileUpdate().hasFileID() && body.fileUpdate().fileID().fileNum() == 102)
                    || (body.hasFileAppend() && body.fileAppend().hasFileID() && body.fileAppend().fileID().fileNum() == 102)) {
                result.add(body);
            }
        }
        return result;
    }

    /**
     * Load the Genesis address book from the classpath resource "mainnet-genesis-address-book.proto.bin".
     *
     * @return the Genesis NodeAddressBook
     * @throws ParseException if there is an error parsing the address book
     */
    public static NodeAddressBook loadGenesisAddressBook() throws ParseException {
        try (var in = new ReadableStreamingData(Objects.requireNonNull(AddressBookRegistry.class
                .getClassLoader()
                .getResourceAsStream("mainnet-genesis-address-book.proto.bin")))) {
            return NodeAddressBook.PROTOBUF.parse(in);
        }
    }

    /**
     * Read an address book from a byte array.
     *
     * @param bytes the byte array containing the address book in protobuf format
     * @return the parsed NodeAddressBook
     * @throws ParseException if there is an error parsing the address book
     */
    public static NodeAddressBook readAddressBook(byte[] bytes) throws ParseException {
        return NodeAddressBook.PROTOBUF.parse(Bytes.wrap(bytes));
    }

    /**
     * Get the public key for a node in the address book. There are two ways the node ID is in the NodeAddress used at
     * different periods in the blockchain history. Early address books use the memo field of the NodeAddress which
     * contains the node's shard, realm and number as a UTF-8 string in the form "1.2.3". Later address books use the
     * nodeAccountId field of the NodeAddress which contains the account ID of the node, which includes the node id
     * number.
     *
     * @param addressBook the address book to use to find the node
     * @param shard the shard number of the node
     * @param realm the realm number of the node
     * @param number the node number of the node
     * @return the public key for the node
     */
    public static String publicKeyForNode(
            final NodeAddressBook addressBook, final long shard, final long realm, final long number) {
        // we assume shard and realm are always 0 for now, so we only use the number
        if (shard != 0 || realm != 0) {
            throw new IllegalArgumentException("Only shard 0 and realm 0 are supported");
        }
        return addressBook.nodeAddress().stream()
                .filter(na -> getNodeAccountId(na) == number)
                .findFirst()
                .orElseThrow()
                .rsaPubKey();
    }

    /**
     * Get the node ID for a node in the address book. First find the node in the address book using one of the two
     * ways node account numbers are in address book. Then get node ID from the nodeId field of the NodeAddress. If the
     * nodeId field is not set (older address books), derive it from the account number - 3.
     *
     * @param addressBook the address book to use to find the node
     * @param shard the shard number of the node
     * @param realm the realm number of the node
     * @param number the node number of the node
     * @return the node ID for the node
     */
    public static long nodeIdForNode(
        final NodeAddressBook addressBook, final long shard, final long realm, final long number) {
        // we assume shard and realm are always 0 for now, so we only use the number
        if (shard != 0 || realm != 0) {
            throw new IllegalArgumentException("Only shard 0 and realm 0 are supported");
        }
        final NodeAddress nodeAddress = addressBook.nodeAddress().stream()
            .filter(na -> getNodeAccountId(na) == number)
            .findFirst()
            .orElse(null);
        long addressBookNodeId = nodeAddress == null ? -1 : nodeAddress.nodeId();
        // For older address books where nodeId is not set, derive it from account number - 3
        return addressBookNodeId > 0 ? addressBookNodeId : number - 3;
    }

    /**
     * Get the node ID from a NodeAddress. The node ID can be found in one of three places:
     * <ul>
     *   <li>The nodeId field of the NodeAddress (if present)</li>
     *   <li>The nodeAccountId field of the NodeAddress (if present)</li>
     *   <li>The memo field of the NodeAddress (if present)</li>
     * </ul>
     *
     * @param nodeAddress the NodeAddress to get the node ID from
     * @return the node ID
     * @throws IllegalArgumentException if the NodeAddress does not have a node ID
     */
    @SuppressWarnings("DataFlowIssue")
    public static long getNodeAccountId(NodeAddress nodeAddress) {
        if (nodeAddress.hasNodeAccountId() && nodeAddress.nodeAccountId().hasAccountNum()) {
            return nodeAddress.nodeAccountId().accountNum();
        } else if (nodeAddress.memo().length() > 0) {
            final String memoStr = nodeAddress.memo().asUtf8String();
            return Long.parseLong(memoStr.substring(memoStr.lastIndexOf('.') + 1));
        } else {
            throw new IllegalArgumentException("NodeAddress has no nodeAccountId or memo: " + nodeAddress);
        }
    }

    /**
     * Compare two address books and return a string describing the changes between them.
     *
     * @param oldAddressBook the old address book
     * @param newAddressBook the new address book
     * @return a string describing the changes between the two address books
     */
    public static String addressBookChanges(
            final NodeAddressBook oldAddressBook, final NodeAddressBook newAddressBook) {
        final StringBuilder sb = new StringBuilder();
        final Map<Long, String> oldNodesIdToPubKey = new HashMap<>();
        for (var node : oldAddressBook.nodeAddress()) {
            oldNodesIdToPubKey.put(getNodeAccountId(node), node.rsaPubKey());
        }
        for (var node : newAddressBook.nodeAddress()) {
            final long nodeId = getNodeAccountId(node);
            final String oldPubKey = oldNodesIdToPubKey.get(nodeId);
            if (oldPubKey == null) {
                sb.append(String.format(
                        "   Node %d added with key %s%n",
                        nodeId, node.rsaPubKey().substring(70, 78)));
            } else if (!oldPubKey.equals(node.rsaPubKey())) {
                sb.append(String.format(
                        "   Node %d key changed from %s to %s%n",
                        nodeId, oldPubKey.substring(70, 78), node.rsaPubKey().substring(70, 78)));
            }
            oldNodesIdToPubKey.remove(nodeId);
        }
        for (var removedNodeId : oldNodesIdToPubKey.keySet()) {
            sb.append(String.format("   Node %d removed%n", removedNodeId));
        }
        return sb.toString();
    }
}
