// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.common.hasher;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.nio.ByteBuffer;

/**
 * Defines a streaming hash computation for a binary Merkle tree of leaf hashes. Leaves are folded
 * as they arrive: whenever two sibling subtrees of the same height are complete they are combined
 * into their parent, and the open branches left when {@link #rootHash()} is called are folded
 * right to left into the root. Every leaf is one digest of {@link #algorithm()}; an empty tree
 * has the root {@link HashAlgorithm#emptyTreeHash()}.
 */
public interface StreamingTreeHasher {
    /**
     * Returns the algorithm this hasher computes with. Every leaf added to this hasher is one
     * digest of this algorithm, and so is the root hash.
     * @return the hash algorithm of this hasher
     */
    HashAlgorithm algorithm();

    /**
     * Adds a leaf hash to the implicit tree of items from the given buffer. The buffer's new position
     * will be the current position plus the digest size of {@link #algorithm()}.
     * @param hash the leaf hash to add
     * @throws IllegalStateException if the root hash has already been requested
     * @throws IllegalArgumentException if the buffer does not have at least one digest of bytes remaining
     */
    void addLeaf(ByteBuffer hash);

    /**
     * Returns the root hash of the tree of items. Once called, this hasher will not accept any more leaf items.
     * @return the root hash of the tree of items
     */
    Bytes rootHash();
}
