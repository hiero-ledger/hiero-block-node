// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.model.hashing;

import java.security.MessageDigest;

public class HashingUtils {
    /** Prefix byte for hash contents for leaf nodes. */
    public static final byte[] LEAF_PREFIX = new byte[] {0};
    /** Prefix byte for hash contents for internal nodes. */
    public static final byte[] INTERNAL_NODE_PREFIX = new byte[] {2};

    /**
     * Hash a leaf node with the appropriate prefix.
     *
     * @param digest the MessageDigest instance to use for hashing
     * @param leafData the data of the leaf
     * @return the hash of the leaf node
     */
    public static byte[] hashLeaf(final MessageDigest digest, final byte[] leafData) {
        digest.update(LEAF_PREFIX);
        return digest.digest(leafData);
    }

    /**
     * Hash an internal node by combining the hashes of its two children with the appropriate prefix.
     *
     * @param digest the MessageDigest instance to use for hashing
     * @param firstChild the hash of the first child, can be null if the branch is missing
     * @param secondChild the hash of the second child, can be null if the branch is missing
     * @return the hash of the internal node
     */
    public static byte[] hashInternalNode(
            final MessageDigest digest, final byte[] firstChild, final byte[] secondChild) {
        digest.update(INTERNAL_NODE_PREFIX);
        if (secondChild != null) digest.update(firstChild);
        if (secondChild != null) digest.update(secondChild);
        return digest.digest();
    }
}
