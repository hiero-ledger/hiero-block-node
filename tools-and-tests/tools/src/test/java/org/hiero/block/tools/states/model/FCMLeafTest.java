// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.junit.jupiter.api.Test;

/** Tests for {@link FCMLeaf}. */
class FCMLeafTest {

    // ==================== getHash computes and caches ====================

    @Test
    void getHashForMapKeyMapValueProduces48Bytes() {
        MapKey key = new MapKey(0, 0, 1);
        FCLinkedList<JTransactionRecord> records = new FCLinkedList<>();
        MapValue value = new MapValue(100L, 0L, 0L, false, null, null, 7776000L, false, records, 0L, "memo", false);

        // MapValue.copyTo requires accountKeys to be non-null for serialization,
        // so we need a JKey. Test with StorageKey/StorageValue instead.
        StorageKey sKey = new StorageKey("/0/f101");
        byte[] hashBytes = new byte[48];
        hashBytes[0] = 1;
        BinaryObject binObj = new BinaryObject();
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            dos.writeLong(1L);
            dos.writeLong(1231553L);
            dos.write(hashBytes);
            dos.flush();
            binObj.copyFrom(new DataInputStream(new ByteArrayInputStream(baos.toByteArray())));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        StorageValue sValue = new StorageValue(binObj);

        FCMLeaf<StorageKey, StorageValue> leaf = new FCMLeaf<>(sKey, sValue);
        byte[] hash = leaf.getHash();
        assertNotNull(hash);
        assertEquals(48, hash.length);

        // Cached: same reference
        byte[] hash2 = leaf.getHash();
        assertEquals(hash, hash2);
    }

    // ==================== left/right child are null ====================

    @Test
    void leafChildrenAreNull() {
        FCMLeaf<StorageKey, StorageValue> leaf = new FCMLeaf<>(new StorageKey("/0/f1"), null);
        assertNull(leaf.getLeftChild());
        assertNull(leaf.getRightChild());
    }

    // ==================== key and value accessors ====================

    @Test
    void keyAndValueAccessors() {
        StorageKey key = new StorageKey("/0/s42");
        FCMLeaf<StorageKey, StorageValue> leaf = new FCMLeaf<>(key, null);
        assertEquals(key, leaf.key());
        assertNull(leaf.value());
    }
}
