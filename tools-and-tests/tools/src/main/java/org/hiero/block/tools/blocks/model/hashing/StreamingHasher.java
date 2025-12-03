// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.model.hashing;

import static org.hiero.block.tools.blocks.model.hashing.HashingUtils.hashInternalNode;
import static org.hiero.block.tools.blocks.model.hashing.HashingUtils.hashLeaf;
import static org.hiero.block.tools.utils.Sha384.sha384Digest;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.LinkedList;
import java.util.List;

/**
 * A class that computes a Merkle tree root hash in a streaming fashion. It supports adding leaves one by one and
 * computes the root hash without storing the entire tree in memory. It uses SHA-384 as the hashing algorithm and
 * follows the prefixing scheme for leaves and internal nodes. Leaves can be optionally double-hashed before being
 * added to the tree.
 * <p>This is not thread safe, it is assumed use by single thread.</p>
 */
public class StreamingHasher implements Hasher {
    /** The hashing algorithm used for computing the hashes. */
    private final MessageDigest digest;
    /** A list to store intermediate hashes as we build the tree. */
    private final LinkedList<byte[]> hashList = new LinkedList<>();
    /** The count of leaves in the tree. */
    private long leafCount = 0;

    /** Create a new StreamingHasher with an empty state. */
    public StreamingHasher() {
        digest = sha384Digest();
    }

    /**
     * Create a StreamingHasher with an existing intermediate hashing state.
     * This allows resuming hashing from a previous state.
     *
     * @param intermediateHashingState the intermediate hashing state
     */
    public StreamingHasher(List<byte[]> intermediateHashingState) {
        this();
        this.hashList.addAll(intermediateHashingState);
    }

    /**
     * Save the current hashing state to a binary file.
     *
     * @param filePath the path to the file where the state will be saved
     * @throws Exception if an I/O error occurs
     */
    @Override
    public void save(Path filePath) throws Exception {
        try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(filePath))) {
            out.writeLong(leafCount);
            out.writeInt(hashList.size());
            for (byte[] hash : hashList) { // we know all hashes are 48 bytes
                out.write(hash);
            }
        }
    }

    /**
     * Load the hashing state from a binary file.
     *
     * @param filePath the path to the file from which the state will be loaded
     * @throws Exception if an I/O error occurs
     */
    @Override
    public void load(Path filePath) throws Exception {
        try (DataInputStream din = new DataInputStream(Files.newInputStream(filePath))) {
            leafCount = din.readLong();
            int hashCount = din.readInt();
            hashList.clear();
            for (int i = 0; i < hashCount; i++) {
                byte[] hash = new byte[48]; // SHA-384 produces 48-byte hashes
                din.readFully(hash);
                hashList.add(hash);
            }
        }
    }

    /**
     * Add a new leaf to the Merkle tree.
     *
     * @param data the data for the new leaf
     */
    @Override
    public void addLeaf(byte[] data) {
        final long i = leafCount;
        final byte[] e = hashLeaf(digest, data);
        hashList.add(e);
        for (long n = i; (n & 1L) == 1; n >>= 1) {
            final byte[] y = hashList.removeLast();
            final byte[] x = hashList.removeLast();
            hashList.add(hashInternalNode(digest, x, y));
        }
        leafCount++;
    }

    /**
     * Compute the Merkle tree root hash from the current state. This does not modify the internal state, so can be
     * called at any time and more leaves can be added afterward.
     *
     * @return the Merkle tree root hash
     */
    @Override
    public byte[] computeRootHash() {
        byte[] merkleRootHash = hashList.getLast();
        for (int i = hashList.size() - 2; i >= 0; i--) {
            merkleRootHash = hashInternalNode(digest, hashList.get(i), merkleRootHash);
        }
        return merkleRootHash;
    }

    /**
     * Get the current intermediate hashing state. This can be used to save the state and resume hashing later.
     *
     * @return the intermediate hashing state
     */
    public List<byte[]> intermediateHashingState() {
        return hashList;
    }

    /**
     * Get the number of leaves added to the tree so far.
     *
     * @return the number of leaves
     */
    @Override
    public long leafCount() {
        return leafCount;
    }
}
