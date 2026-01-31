// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.hiero.block.tools.states.utils.CryptoUtils;
import org.junit.jupiter.api.Test;

/** Tests for {@link FCMNode}, {@link FCMInternalNode}, and {@link FCMLeaf}. */
class FCMapMerkleTest {

    // ==================== FCMNode.EMPTY_HASH ====================

    @Test
    void emptyHashIs48Zeros() {
        assertEquals(48, FCMNode.EMPTY_HASH.length);
        for (byte b : FCMNode.EMPTY_HASH) {
            assertEquals(0, b);
        }
    }

    // ==================== FCMInternalNode ====================

    @Test
    void internalNodeWithNullChildrenUsesEmptyHash() {
        FCMInternalNode<MapKey, MapValue> node = new FCMInternalNode<>(null, null);
        byte[] hash = node.getHash();
        assertNotNull(hash);
        assertEquals(48, hash.length);

        // Hash should be SHA-384(EMPTY_HASH + EMPTY_HASH)
        var digest = CryptoUtils.getMessageDigest();
        digest.update(FCMNode.EMPTY_HASH);
        digest.update(FCMNode.EMPTY_HASH);
        byte[] expected = digest.digest();
        assertArrayEquals(expected, hash);
    }

    @Test
    void internalNodeHashIsCached() {
        FCMInternalNode<MapKey, MapValue> node = new FCMInternalNode<>(null, null);
        byte[] hash1 = node.getHash();
        byte[] hash2 = node.getHash();
        // Should return the same array reference (cached)
        assertEquals(hash1, hash2);
    }

    // ==================== FCMLeaf ====================

    @Test
    void leafNodeHasNoChildren() {
        FCMLeaf<MapKey, MapValue> leaf = new FCMLeaf<>(new MapKey(0, 0, 1), null);
        assertNull(leaf.getLeftChild());
        assertNull(leaf.getRightChild());
    }

    @Test
    void leafNodeKeyAndValue() {
        MapKey key = new MapKey(0, 0, 42);
        FCMLeaf<MapKey, MapValue> leaf = new FCMLeaf<>(key, null);
        assertEquals(key, leaf.key());
        assertNull(leaf.value());
    }
}
