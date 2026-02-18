// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.common.hasher;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.concurrent.CompletableFuture;

/**
 * A naive implementation of {@link StreamingTreeHasher} that computes the root hash of a Merkle tree
 * using the streaming fold-up algorithm with domain-separated hashing.
 * <p>
 * The algorithm maintains a compact list of pending subtree roots. As each leaf is added,
 * whenever two siblings at the same height are complete, they are combined into an internal
 * node with the {@code 0x02} prefix. At finalization, remaining pending roots are folded
 * right-to-left.
 */
public class NaiveStreamingTreeHasher implements StreamingTreeHasher {
    /**
     * The hash returned for an empty tree (no leaves added). This is 48 zero bytes,
     * matching the CN's ZERO_BLOCK_HASH convention.
     */
    private static final byte[] EMPTY_HASH = new byte[HashingUtilities.HASH_SIZE];

    private final LinkedList<byte[]> hashList = new LinkedList<>();
    private long leafCount = 0;
    private boolean rootHashRequested = false;

    /**
     * Constructor for the {@link NaiveStreamingTreeHasher}.
     */
    public NaiveStreamingTreeHasher() {}

    @Override
    public void addLeaf(@NonNull final ByteBuffer hash) {
        if (rootHashRequested) {
            throw new IllegalStateException("Root hash already requested");
        }
        if (hash.remaining() < HASH_LENGTH) {
            throw new IllegalArgumentException("Buffer has less than " + HASH_LENGTH + " bytes remaining");
        }
        final byte[] bytes = new byte[HASH_LENGTH];
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

    @Override
    public CompletableFuture<Bytes> rootHash() {
        rootHashRequested = true;
        if (hashList.isEmpty()) {
            return CompletableFuture.completedFuture(Bytes.wrap(EMPTY_HASH));
        }
        // Fold remaining pending roots right-to-left
        byte[] merkleRootHash = hashList.getLast();
        for (int i = hashList.size() - 2; i >= 0; i--) {
            merkleRootHash = HashingUtilities.hashInternalNode(hashList.get(i), merkleRootHash);
        }
        return CompletableFuture.completedFuture(Bytes.wrap(merkleRootHash));
    }
}
