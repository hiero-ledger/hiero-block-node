// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.hiero.block.tools.states.utils.CryptoUtils;
import org.hiero.block.tools.states.utils.HashingOutputStream;

/**
 * A leaf node in the FCMap Merkle tree. Its hash is SHA-384(key.copyTo() + value.copyTo()).
 */
public final class FCMLeaf<K, V> implements FCMNode<K, V> {
    private final K key;
    private final V value;
    private byte[] hash;

    public FCMLeaf(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public K key() {
        return key;
    }

    public V value() {
        return value;
    }

    @Override
    public FCMNode<K, V> getLeftChild() {
        return null;
    }

    @Override
    public FCMNode<K, V> getRightChild() {
        return null;
    }

    @Override
    public byte[] getHash() {
        if (hash == null) {
            hash = computeHash();
        }
        return hash;
    }

    private byte[] computeHash() {
        try {
            var digest = CryptoUtils.getMessageDigest();
            try (HashingOutputStream hashOut = new HashingOutputStream(digest);
                    BufferedOutputStream bufOut = new BufferedOutputStream(hashOut);
                    DataOutputStream dos = new DataOutputStream(bufOut)) {
                copyTo(dos, key);
                copyTo(dos, value);
                dos.flush();
                return digest.digest();
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to compute leaf hash", e);
        }
    }

    /** Dispatches copyTo based on runtime type. */
    private static void copyTo(DataOutputStream dos, Object obj) throws IOException {
        if (obj instanceof MapKey mk) {
            mk.copyTo(dos);
        } else if (obj instanceof MapValue mv) {
            mv.copyTo(dos);
        } else if (obj instanceof StorageKey sk) {
            sk.copyTo(dos);
        } else if (obj instanceof StorageValue sv) {
            sv.copyTo(dos);
        } else {
            throw new UnsupportedOperationException(
                    "Cannot serialize: " + obj.getClass().getName());
        }
    }
}
