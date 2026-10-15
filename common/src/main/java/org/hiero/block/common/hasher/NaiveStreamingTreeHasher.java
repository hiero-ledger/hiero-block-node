// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.common.hasher;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.Objects;

/**
 * A naive implementation of {@link StreamingTreeHasher} that computes the root hash of a Merkle tree
 * using the streaming fold-up algorithm with domain-separated hashing, computing with
 * {@link HashAlgorithm#SHA2_256}.
 * <p>
 * The algorithm maintains a compact list of pending subtree roots. As each leaf is added,
 * whenever two siblings at the same height are complete, they are combined into an internal
 * node with the {@code 0x02} prefix. At finalization, remaining pending roots are folded
 * right-to-left.
 * <p>
 * An empty tree (no leaves added) returns the SHA-256 {@link HashAlgorithm#emptyTreeHash()},
 * matching the CN convention introduced in HAPI v0.72.
 */
public class NaiveStreamingTreeHasher implements StreamingTreeHasher {

    /** The algorithm the hashes are computed with. */
    private static final HashAlgorithm ALGORITHM = HashAlgorithm.SHA2_256;
    /** The size of one leaf hash, the digest size of the algorithm. */
    private static final int HASH_SIZE = ALGORITHM.hashSize();
    /** The pending subtree roots, one per open branch. */
    private final LinkedList<byte[]> hashList = new LinkedList<>();
    /** The number of leaves added so far. */
    private long leafCount = 0;
    /** Whether the root hash has been requested, after which no leaf is accepted. */
    private boolean rootHashRequested = false;

    @Override
    public void addLeaf(final ByteBuffer hash) throws NoSuchAlgorithmException {
        Objects.requireNonNull(hash);
        if (rootHashRequested) {
            throw new IllegalStateException("Root hash already requested");
        } else if (hash.remaining() < HASH_SIZE) {
            throw new IllegalArgumentException("Buffer has less than " + HASH_SIZE + " bytes remaining");
        } else {
            final byte[] bytes = new byte[HASH_SIZE];
            hash.get(bytes);
            hashList.add(bytes);
            // Fold-up: combine sibling pairs while the current position is odd
            for (long n = leafCount; (n & 1L) == 1; n >>= 1) {
                final byte[] right = hashList.removeLast();
                final byte[] left = hashList.removeLast();
                hashList.add(HashingUtilities.hashInternalNode(left, right));
            }
            leafCount++;
        }
    }

    @Override
    public Bytes rootHash() throws NoSuchAlgorithmException {
        rootHashRequested = true;
        final Bytes result;
        if (hashList.isEmpty()) {
            result = ALGORITHM.emptyTreeHash();
        } else {
            // Fold remaining pending roots right-to-left
            byte[] merkleRootHash = hashList.getLast();
            for (int i = hashList.size() - 2; i >= 0; i--) {
                merkleRootHash = HashingUtilities.hashInternalNode(hashList.get(i), merkleRootHash);
            }
            result = Bytes.wrap(merkleRootHash);
        }
        return result;
    }
}
