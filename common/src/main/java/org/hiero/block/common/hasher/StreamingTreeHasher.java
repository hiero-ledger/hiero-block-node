// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.common.hasher;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * Defines a streaming hash computation for a perfect binary Merkle tree of {@link Bytes} leaves; where the leaves
 * given before calling {@link #rootHash()} are right-padded with empty leaves as needed to ensure the final tree is
 * a perfect binary tree.
 */
public interface StreamingTreeHasher {
    /**
     * The length of the hash produced by this hasher.
     */
    int HASH_LENGTH = 48;

    /**
     * Adds a leaf hash to the implicit tree of items from the given buffer. The buffer's new position
     * will be the current position plus {@link #HASH_LENGTH}.
     * @param hash the leaf hash to add
     * @throws IllegalStateException if the root hash has already been requested
     * @throws IllegalArgumentException if the buffer does not have at least {@link #HASH_LENGTH} bytes remaining
     */
    void addLeaf(@NonNull ByteBuffer hash);

    /**
     * Returns a future that completes with the root hash of the tree of items. Once called, this hasher will not accept
     * any more leaf items.
     * @return a future that completes with the root hash of the tree of items
     */
    CompletableFuture<Bytes> rootHash();
}
