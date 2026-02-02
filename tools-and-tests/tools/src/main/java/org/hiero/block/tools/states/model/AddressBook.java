// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.hiero.block.tools.states.utils.CryptoUtils;

/** A serializable collection of network addresses representing all nodes in the network. */
@SuppressWarnings("ForLoopReplaceableByForEach")
public class AddressBook {
    /** This version number should be used to handle compatibility issues that may arise from any future changes */
    private static final long VERSION = 1;

    /** the list of network addresses */
    List<Address> addresses;
    /** the cached total stake across all addresses */
    long totalStake;
    /** the cached per-member stakes array */
    long[] stakes;
    /** mapping from public key nickname to node ID */
    Map<String, Long> publicKeyToId;

    /** Creates an empty address book. */
    public AddressBook() {}

    /**
     * Creates an address book with the given data.
     *
     * @param addresses the list of network addresses
     * @param totalStake the cached total stake across all addresses
     * @param stakes the cached per-member stakes array
     * @param publicKeyToId mapping from public key nickname to node ID
     */
    public AddressBook(List<Address> addresses, long totalStake, long[] stakes, Map<String, Long> publicKeyToId) {
        this.addresses = addresses;
        this.totalStake = totalStake;
        this.stakes = stakes;
        this.publicKeyToId = publicKeyToId;
    }

    /**
     * Retrieves the address for a given node ID.
     *
     * @param id the node ID
     * @return the address, or {@code null} if the ID is out of range
     */
    public Address getAddress(long id) {
        if (id < 0 || id >= addresses.size()) {
            return null;
        }
        return addresses.get((int) id);
    }

    /**
     * Gets the number of addresses in this book.
     *
     * @return the number of addresses in this book
     */
    public int getSize() {
        return addresses.size();
    }

    /**
     * Gets the stake weight for a given node ID.
     *
     * @param id the node ID
     * @return the node's stake, or 0 if not found
     */
    public long getStake(long id) {
        Address addr = getAddress((int) id);
        return addr == null ? 0 : addr.stake();
    }

    /**
     * Gets the total stake across all nodes.
     *
     * @return the total stake across all nodes
     */
    public long getTotalStake() {
        if (totalStake == 0 && addresses != null) {
            long tmpTotalStake = 0;
            for (Address addr : addresses) {
                tmpTotalStake += addr.stake();
            }
            totalStake = tmpTotalStake;
        }
        return totalStake;
    }

    /**
     * Deserializes this address book from the given stream.
     *
     * @param inStream the stream to read from
     * @throws IOException if an I/O error occurs or the version is incompatible
     */
    public void copyFrom(DataInputStream inStream) throws IOException {
        // Discard the version number
        long version = inStream.readLong();
        if (version != VERSION) {
            throw new IOException("Incompatible AddressBook version: " + version);
        }

        addresses = new ArrayList<>();
        int size;
        size = inStream.readInt();
        for (int i = 0; i < size; i++) {
            addresses.add(Address.readAddress(inStream));
        }
        publicKeyToId = addressesToHashMap(addresses);
        totalStake = 0; // force recalculation on the next getTotalStake() call
        stakes = null; // force recalculation on the next getStakes() call
    }

    /**
     * Serializes this address book to the given stream.
     *
     * @param outStream the stream to write to
     * @throws IOException if an I/O error occurs
     */
    public void copyTo(DataOutputStream outStream) throws IOException {
        // Write the version number
        outStream.writeLong(VERSION);

        outStream.writeInt(addresses.size());
        for (int i = 0; i < addresses.size(); i++) {
            addresses.get(i).writeAddress(outStream);
        }
    }

    // create the hashMap and add all the current addresses
    private static Map<String, Long> addressesToHashMap(List<Address> addresses) {
        Map<String, Long> publicKeyToId = new HashMap<>();
        for (int i = 0; i < addresses.size(); i++) {
            publicKeyToId.put(addresses.get(i).nickname(), (long) i);
        }
        return publicKeyToId;
    }

    /**
     * Computes a SHA-384 hash over all addresses.
     *
     * @return the hash bytes
     */
    public byte[] getHash() {
        MessageDigest md = CryptoUtils.getMessageDigest();
        for (int i = 0; i < addresses.size(); i++) {
            addresses.get(i).updateHash(md);
        }
        return md.digest();
    }
}
