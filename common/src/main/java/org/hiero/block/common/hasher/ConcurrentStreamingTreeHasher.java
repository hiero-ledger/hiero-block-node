// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.common.hasher;

import static java.util.Objects.requireNonNull;
import static org.hiero.block.common.hasher.HashingUtilities.noThrowSha384HashOf;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.hiero.block.common.utils.Preconditions;

/**
 * A {@link StreamingTreeHasher} that computes the root hash of a perfect binary Merkle tree of {@link Bytes} leaves
 * using a concurrent algorithm that hashes leaves in parallel and combines the resulting hashes in parallel.
 * <p>
 * <b>Important:</b> This class is not thread-safe, and client code must not make concurrent calls to
 * {@link StreamingTreeHasher#addLeaf(ByteBuffer)} or {@link #rootHash()}.
 */
public class ConcurrentStreamingTreeHasher implements StreamingTreeHasher {
    /**
     * The default number of leaves to batch before combining the resulting hashes.
     */
    private static final int DEFAULT_HASH_COMBINE_BATCH_SIZE = 8;

    /**
     * The base {@link HashCombiner} that combines the hashes of the leaves of the tree, at height zero.
     */
    private final HashCombiner combiner = new HashCombiner(0);
    /**
     * The {@link ExecutorService} used to parallelize the hashing and combining of the leaves of the tree.
     */
    private final ExecutorService executorService;
    /**
     * The size of the batches of hashes to schedule for combination.
     * <p><b>Important:</b> This must be an even number so we can safely assume that any odd number
     * of scheduled hashes to combine can be padded with appropriately nested combination of hashes
     * whose descendants are all empty leaves.
     */
    private final int hashCombineBatchSize;

    /**
     * The number of leaves added to the tree.
     */
    private int numLeaves;
    /**
     * Set once before the root hash is requested, to the height of the tree implied by the number of leaves.
     */
    private int rootHeight;
    /**
     * Whether the tree has been finalized by requesting the root hash.
     */
    private boolean rootHashRequested = false;

    /**
     * Constructs a new {@link ConcurrentStreamingTreeHasher} with the given {@link ExecutorService}.
     *
     * @param executorService the executor service to use for parallelizing the hashing and combining of the tree
     */
    public ConcurrentStreamingTreeHasher(@NonNull final ExecutorService executorService) {
        this(executorService, DEFAULT_HASH_COMBINE_BATCH_SIZE);
    }

    /**
     * Constructs a new {@link ConcurrentStreamingTreeHasher} with the given {@link ExecutorService} and hash combine
     * batch size.
     *
     * @param executorService the executor service to use for parallelizing the hashing and combining of the tree
     * @param hashCombineBatchSize the size of the batches of hashes to schedule for combination
     * @throws IllegalArgumentException if the hash combine batch size is an odd number
     */
    public ConcurrentStreamingTreeHasher(
            @NonNull final ExecutorService executorService, final int hashCombineBatchSize) {
        this.executorService = requireNonNull(executorService);
        this.hashCombineBatchSize =
                Preconditions.requireEven(hashCombineBatchSize, "Hash combine batch size must be an even number");
        ;
    }

    @Override
    public void addLeaf(@NonNull final ByteBuffer hash) {
        requireNonNull(hash);
        if (rootHashRequested) {
            throw new IllegalStateException("Cannot add leaves after requesting the root hash");
        }
        if (hash.remaining() < HASH_LENGTH) {
            throw new IllegalArgumentException("Buffer has less than " + HASH_LENGTH + " bytes remaining");
        }
        numLeaves++;
        final byte[] bytes = new byte[HASH_LENGTH];
        hash.get(bytes);
        combiner.combine(bytes);
    }

    @Override
    public CompletableFuture<Bytes> rootHash() {
        rootHashRequested = true;
        rootHeight = rootHeightFor(numLeaves);
        return combiner.finalCombination();
    }

    @Override
    public Status status() {
        if (numLeaves == 0) {
            return Status.EMPTY;
        } else {
            final ArrayList<Bytes> rightmostHashes = new ArrayList<Bytes>();
            combiner.flushAvailable(rightmostHashes, rootHeightFor(numLeaves + 1));
            return new Status(numLeaves, rightmostHashes);
        }
    }

    /**
     * Computes the root hash of a perfect binary Merkle tree of {@link Bytes} leaves (padded on the right with
     * empty leaves to reach a power of two), given the penultimate status of the tree and the hash of the last
     * leaf added to the tree.
     *
     * @param penultimateStatus the penultimate status of the tree
     * @param lastLeafHash the last leaf hash added to the tree
     * @return the root hash of the tree
     */
    public static Bytes rootHashFrom(@NonNull final Status penultimateStatus, @NonNull final Bytes lastLeafHash) {
        requireNonNull(lastLeafHash);
        byte[] hash = lastLeafHash.toByteArray();
        final int rootHeight = rootHeightFor(penultimateStatus.numLeaves() + 1);
        for (int i = 0; i < rootHeight; i++) {
            final Bytes rightmostHash = penultimateStatus.rightmostHashes().get(i);
            if (rightmostHash.length() == 0) {
                hash = HashingUtilities.combine(hash, HashCombiner.EMPTY_HASHES[i]);
            } else {
                hash = HashingUtilities.combine(rightmostHash.toByteArray(), hash);
            }
        }
        return Bytes.wrap(hash);
    }

    private class HashCombiner {
        private static final ThreadLocal<MessageDigest> DIGESTS =
                ThreadLocal.withInitial(HashingUtilities::sha384DigestOrThrow);
        private static final int MAX_DEPTH = 24;
        private static final int MIN_TO_SCHEDULE = 16;

        private static final byte[][] EMPTY_HASHES = new byte[MAX_DEPTH][];

        static {
            EMPTY_HASHES[0] = noThrowSha384HashOf(new byte[0]);
            for (int i = 1; i < MAX_DEPTH; i++) {
                EMPTY_HASHES[i] = HashingUtilities.combine(EMPTY_HASHES[i - 1], EMPTY_HASHES[i - 1]);
            }
        }

        private final int height;

        private HashCombiner delegate;
        private List<byte[]> pendingHashes = new ArrayList<>();
        private CompletableFuture<Void> combination = CompletableFuture.completedFuture(null);

        private HashCombiner(final int height) {
            if (height >= MAX_DEPTH) {
                throw new IllegalArgumentException("Cannot combine hashes at height " + height);
            }
            this.height = height;
        }

        public void combine(@NonNull final byte[] hash) {
            pendingHashes.add(hash);
            if (pendingHashes.size() == hashCombineBatchSize) {
                schedulePendingWork();
            }
        }

        public CompletableFuture<Bytes> finalCombination() {
            if (height == rootHeight) {
                final byte[] rootHash = pendingHashes.isEmpty() ? EMPTY_HASHES[0] : pendingHashes.getFirst();
                return CompletableFuture.completedFuture(Bytes.wrap(rootHash));
            } else {
                if (!pendingHashes.isEmpty()) {
                    schedulePendingWork();
                }
                return combination.thenCompose(ignore -> delegate.finalCombination());
            }
        }

        public void flushAvailable(@NonNull final List<Bytes> rightmostHashes, final int stopHeight) {
            if (height < stopHeight) {
                final byte[] newPendingHash = pendingHashes.size() % 2 == 0 ? null : pendingHashes.removeLast();
                schedulePendingWork();
                combination.join();
                if (newPendingHash != null) {
                    pendingHashes.add(newPendingHash);
                    rightmostHashes.add(Bytes.wrap(newPendingHash));
                } else {
                    rightmostHashes.add(Bytes.EMPTY);
                }
                delegate.flushAvailable(rightmostHashes, stopHeight);
            }
        }

        private void schedulePendingWork() {
            if (delegate == null) {
                delegate = new HashCombiner(height + 1);
            }
            final CompletableFuture<List<byte[]>> pendingCombination;
            if (pendingHashes.size() < MIN_TO_SCHEDULE) {
                pendingCombination = CompletableFuture.completedFuture(combine(pendingHashes));
            } else {
                final List<byte[]> hashes = pendingHashes;
                pendingCombination = CompletableFuture.supplyAsync(() -> combine(hashes), executorService);
            }
            combination = combination.thenCombine(pendingCombination, (ignore, combined) -> {
                combined.forEach(delegate::combine);
                return null;
            });
            pendingHashes = new ArrayList<>();
        }

        private List<byte[]> combine(@NonNull final List<byte[]> hashes) {
            final List<byte[]> result = new ArrayList<>();
            final MessageDigest digest = DIGESTS.get();
            for (int i = 0, m = hashes.size(); i < m; i += 2) {
                final byte[] left = hashes.get(i);
                final byte[] right = i + 1 < m ? hashes.get(i + 1) : EMPTY_HASHES[height];
                digest.update(left);
                digest.update(right);
                result.add(digest.digest());
            }
            return result;
        }
    }

    private static int rootHeightFor(final int numLeaves) {
        final int numPerfectLeaves = containingPowerOfTwo(numLeaves);
        return numPerfectLeaves == 0 ? 0 : Integer.numberOfTrailingZeros(numPerfectLeaves);
    }

    private static int containingPowerOfTwo(final int n) {
        if ((n & (n - 1)) == 0) {
            return n;
        }
        return Integer.highestOneBit(n) << 1;
    }
}
