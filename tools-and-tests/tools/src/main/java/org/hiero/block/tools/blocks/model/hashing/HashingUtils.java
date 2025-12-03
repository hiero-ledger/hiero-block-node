// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.model.hashing;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.security.MessageDigest;
import java.util.Objects;

/**
 * Utility methods for Merkle tree hashing following the Block & State Merkle Tree Design.
 *
 * <p>This class provides domain-separated hashing for Merkle tree nodes using SHA-384.
 * Domain separation is achieved through single-byte prefixes that ensure leaf hashes
 * and internal node hashes occupy distinct hash spaces, preventing collision attacks.
 *
 * <h2>Prefix Scheme (from design doc)</h2>
 * <ul>
 *   <li>{@code 0x00} - Leaf node prefix: {@code hash(0x00 || leafData)}</li>
 *   <li>{@code 0x01} - Single-child internal node prefix: {@code hash(0x01 || childHash)}</li>
 *   <li>{@code 0x02} - Two-child internal node prefix: {@code hash(0x02 || leftHash || rightHash)}</li>
 * </ul>
 *
 * <p>This prefixing scheme protects against length extension attacks and second preimage
 * attacks by ensuring that internal nodes and leaf nodes cannot have the same hash.
 *
 * @see <a href="https://en.wikipedia.org/wiki/Length_extension_attack">Length Extension Attack</a>
 * @see <a href="https://en.wikipedia.org/wiki/Preimage_attack">Preimage Attack</a>
 */
public class HashingUtils {
    /** Prefix byte for leaf node hashes: {@code hash(0x00 || leafData)}. */
    public static final byte[] LEAF_PREFIX = new byte[] {0x00};

    /**
     * Prefix byte for single-child internal node hashes: {@code hash(0x01 || childHash)}.
     * Used when an internal node has only one child (the other is null/missing).
     */
    public static final byte[] SINGLE_CHILD_PREFIX = new byte[] {0x01};

    /**
     * Prefix byte for two-child internal node hashes: {@code hash(0x02 || leftHash || rightHash)}.
     * Used when an internal node has both children present.
     */
    public static final byte[] TWO_CHILDREN_NODE_PREFIX = new byte[] {0x02};

    /**
     * Hash a leaf node with the appropriate prefix.
     *
     * <p>Computes: {@code hash(0x00 || leafData)}
     *
     * @param digest the MessageDigest instance to use for hashing (should be SHA-384)
     * @param leafData the serialized data of the leaf (typically protobuf-encoded)
     * @return the 48-byte SHA-384 hash of the prefixed leaf data
     */
    public static byte[] hashLeaf(@NonNull final MessageDigest digest, @NonNull final byte[] leafData) {
        digest.update(LEAF_PREFIX);
        return digest.digest(leafData);
    }

    /**
     * Hash an internal node by combining the hashes of its children with the appropriate prefix.
     *
     * <p>Depending on which children are present:
     * <ul>
     *   <li>Both children: {@code hash(0x02 || firstChild || secondChild)}</li>
     *   <li>Only firstChild: {@code hash(0x01 || firstChild)}</li>
     *   <li>Only secondChild: {@code hash(0x01 || secondChild)}</li>
     * </ul>
     *
     * @param digest the MessageDigest instance to use for hashing (should be SHA-384)
     * @param firstChild the hash of the first (left) child, can never be null
     * @param secondChild the hash of the second (right) child, or null if missing
     * @return the 48-byte SHA-384 hash of the prefixed internal node
     */
    public static byte[] hashInternalNode(
            @NonNull final MessageDigest digest, @NonNull final byte[] firstChild, @Nullable final byte[] secondChild) {
        Objects.requireNonNull(firstChild, "firstChild cannot be null");
        if (secondChild != null) {
            // Two-child internal node: hash(0x02 || firstChild || secondChild)
            digest.update(TWO_CHILDREN_NODE_PREFIX);
            digest.update(firstChild);
            digest.update(secondChild);
        } else {
            // Single-child internal node (first child only): hash(0x01 || firstChild)
            digest.update(SINGLE_CHILD_PREFIX);
            digest.update(firstChild);
        }
        return digest.digest();
    }
}
