// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.common.hasher;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;

/**
 * Defines a streaming hash computation for a binary Merkle tree of leaf hashes. Leaves are folded
 * as they arrive: whenever two sibling subtrees of the same height are complete they are combined
 * into their parent, and the open branches left when {@link #rootHash()} is called are folded
 * right to left into the root. Every leaf is one SHA-256 digest; an empty tree has the root
 * {@link HashAlgorithm#emptyTreeHash()} of {@link HashAlgorithm#SHA2_256}.
 */
public interface StreamingTreeHasher {
    /**
     * Adds a leaf hash to the implicit tree of items from the given buffer. The buffer's new position
     * will be the current position plus the SHA-256 digest size.
     * @param hash the leaf hash to add
     * @throws IllegalStateException if the root hash has already been requested
     * @throws IllegalArgumentException if the buffer does not have at least one digest of bytes remaining
     * @throws NoSuchAlgorithmException if the Java runtime provides no implementation of the algorithm
     */
    void addLeaf(ByteBuffer hash) throws NoSuchAlgorithmException;

    /**
     * Returns the root hash of the tree of items. Once called, this hasher will not accept any more leaf items.
     * @return the root hash of the tree of items
     * @throws NoSuchAlgorithmException if the Java runtime provides no implementation of the algorithm
     */
    Bytes rootHash() throws NoSuchAlgorithmException;
}
