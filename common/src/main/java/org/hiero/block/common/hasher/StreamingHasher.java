// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.common.hasher;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;

/**
 * A class that computes a Merkle tree root hash in a streaming fashion. It supports adding leaves one by one and
 * computes the root hash without storing the entire tree in memory. It hashes with {@link HashAlgorithm#SHA2_256}
 * and follows the prefixing scheme for leaves and internal nodes. Values that already are hashes,
 * such as block root hashes, can be added as nodes without being hashed again.
 * <p>This is not thread safe, it is assumed use by single thread.</p>
 */
public class StreamingHasher {
    /** The algorithm the hashes are computed with. */
    private static final HashAlgorithm ALGORITHM = HashAlgorithm.SHA2_256;
    /** The size of one node hash, the digest size of the algorithm. */
    private static final int HASH_SIZE = ALGORITHM.hashSize();
    /** The leaf data hashed for a null leaf: no bytes at all. */
    private static final byte[] EMPTY_LEAF_DATA = new byte[0];
    /** The reused digest for computing the hashes. */
    private final MessageDigest digest;
    /** A list to store intermediate hashes as we build the tree. */
    private final LinkedList<byte[]> hashList = new LinkedList<>();
    /** The count of leaves in the tree. */
    private long leafCount = 0;

    /**
     * Create a new StreamingHasher with an empty state.
     *
     * @throws NoSuchAlgorithmException if the Java runtime provides no implementation of the algorithm
     */
    public StreamingHasher() throws NoSuchAlgorithmException {
        this.digest = ALGORITHM.newDigest();
    }

    /**
     * Create a StreamingHasher with an existing intermediate hashing state.
     * This allows resuming hashing from a previous state. Every entry of the state is a subtree
     * root, so each must be exactly one SHA-256 digest; a state saved with another algorithm is
     * refused instead of being mixed into a tree of SHA-256 digests.
     *
     * @param intermediateHashingState the intermediate hashing state
     * @param leafCount the number of leaves the state was built from
     * @throws IllegalArgumentException if an entry of the state is not exactly one SHA-256 digest
     * @throws NoSuchAlgorithmException if the Java runtime provides no implementation of the algorithm
     */
    public StreamingHasher(final List<byte[]> intermediateHashingState, final long leafCount)
            throws NoSuchAlgorithmException {
        this();
        for (int i = 0; i < intermediateHashingState.size(); i++) {
            final byte[] hash = intermediateHashingState.get(i);
            if (hash.length != HASH_SIZE) {
                throw new IllegalArgumentException(
                        "Intermediate hashing state entry %d must be exactly %d bytes, got %d"
                                .formatted(i, HASH_SIZE, hash.length));
            }
        }
        this.hashList.addAll(intermediateHashingState);
        this.leafCount = leafCount;
    }

    /**
     * Add a new leaf to the Merkle tree. A null leaf has no data, so it is hashed as empty leaf
     * data: {@code hash(0x00)}.
     *
     * @param data the data for the new leaf, null for a leaf without data
     */
    public void addLeaf(final byte[] data) {
        final byte[] leafData;
        if (data == null) {
            leafData = EMPTY_LEAF_DATA;
        } else {
            leafData = data;
        }
        final long i = leafCount;
        final byte[] e = hashLeaf(leafData);
        hashList.add(e);
        for (long n = i; (n & 1L) == 1; n >>= 1) {
            final byte[] y = hashList.removeLast();
            final byte[] x = hashList.removeLast();
            hashList.add(hashInternalNode(x, y));
        }
        leafCount++;
    }

    /**
     * Add a pre-hashed node directly to the Merkle tree, bypassing leaf hashing.
     * Use this when the input is already a block root hash (e.g., building the all-previous-blocks tree),
     * matching the CN's {@code IncrementalStreamingHasher.addNodeByHash} behavior. The node is a
     * leaf of a tree of SHA-256 digests, so it must be exactly one SHA-256 digest; a hash of
     * another size is refused before the state changes.
     *
     * @param hash the pre-hashed node value to add
     * @throws IllegalArgumentException if the hash is not exactly one SHA-256 digest
     */
    public void addNodeByHash(final byte[] hash) {
        if (hash.length != HASH_SIZE) {
            throw new IllegalArgumentException(
                    "Node hash must be exactly %d bytes, got %d".formatted(HASH_SIZE, hash.length));
        } else {
            final long i = leafCount;
            hashList.add(hash);
            for (long n = i; (n & 1L) == 1; n >>= 1) {
                final byte[] y = hashList.removeLast();
                final byte[] x = hashList.removeLast();
                hashList.add(hashInternalNode(x, y));
            }
            leafCount++;
        }
    }

    /**
     * Compute the Merkle tree root hash from the current state. This does not modify the internal state, so can be
     * called at any time and more leaves can be added afterward. An empty tree has the root
     * {@link HashAlgorithm#emptyTreeHash()}.
     *
     * @return the Merkle tree root hash
     */
    public byte[] computeRootHash() {
        final byte[] result;
        if (hashList.isEmpty()) {
            result = ALGORITHM.emptyTreeHash().toByteArray();
        } else {
            byte[] merkleRootHash = hashList.getLast();
            for (int i = hashList.size() - 2; i >= 0; i--) {
                merkleRootHash = hashInternalNode(hashList.get(i), merkleRootHash);
            }
            result = merkleRootHash;
        }
        return result;
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
    public long leafCount() {
        return leafCount;
    }

    /**
     * Hash a leaf node with the leaf prefix.
     *
     * @param leafData the data of the leaf
     * @return the hash of the leaf node
     */
    private byte[] hashLeaf(final byte[] leafData) {
        digest.update(HashingUtilities.LEAF_PREFIX);
        return digest.digest(leafData);
    }

    /**
     * Hash an internal node by combining the hashes of its two children with the two children prefix.
     *
     * @param firstChild the hash of the first child
     * @param secondChild the hash of the second child
     * @return the hash of the internal node
     */
    private byte[] hashInternalNode(final byte[] firstChild, final byte[] secondChild) {
        digest.update(HashingUtilities.TWO_CHILDREN_NODE_PREFIX);
        digest.update(firstChild);
        return digest.digest(secondChild);
    }
}
