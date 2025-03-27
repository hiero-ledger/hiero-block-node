// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.common.hasher;

import static java.util.Objects.requireNonNull;
import static org.hiero.block.common.hasher.HashingUtilities.noThrowSha384HashOf;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

/**
 * A naive implementation of {@link StreamingTreeHasher} that computes the root hash of a perfect binary Merkle tree of
 * {@link ByteBuffer} leaves. Used to test the correctness of more efficient implementations.
 */
public class NaiveStreamingTreeHasher implements StreamingTreeHasher {
    private static final byte[] EMPTY_HASH = noThrowSha384HashOf(new byte[0]);

    private final List<byte[]> leafHashes = new ArrayList<>();
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
        leafHashes.add(bytes);
    }

    @Override
    public CompletableFuture<Bytes> rootHash() {
        rootHashRequested = true;
        if (leafHashes.isEmpty()) {
            return CompletableFuture.completedFuture(Bytes.wrap(EMPTY_HASH));
        }
        Queue<byte[]> hashes = new LinkedList<>(leafHashes);
        final int n = hashes.size();
        if ((n & (n - 1)) != 0) {
            final int paddedN = Integer.highestOneBit(n) << 1;
            while (hashes.size() < paddedN) {
                hashes.add(EMPTY_HASH);
            }
        }
        while (hashes.size() > 1) {
            final Queue<byte[]> newLeafHashes = new LinkedList<>();
            while (!hashes.isEmpty()) {
                final byte[] left = hashes.poll();
                final byte[] right = hashes.poll();
                // TODO this should be to use org.hiero.block.common.hasher.HashingUtilities.combine(byte[], byte[]) and
                // avoid copies
                final byte[] combined = new byte[left.length + requireNonNull(right).length];
                System.arraycopy(left, 0, combined, 0, left.length);
                System.arraycopy(right, 0, combined, left.length, right.length);
                newLeafHashes.add(noThrowSha384HashOf(combined));
            }
            hashes = newLeafHashes;
        }
        return CompletableFuture.completedFuture(Bytes.wrap(requireNonNull(hashes.poll())));
    }
}
