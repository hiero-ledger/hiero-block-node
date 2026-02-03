// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import org.hiero.block.tools.states.utils.CryptoUtils;

/**
 * An internal node in the FCMap Merkle tree. Its hash is SHA-384(leftChildHash + rightChildHash).
 * When a child is absent, EMPTY_HASH (48 zero bytes) is used.
 */
public final class FCMInternalNode<K, V> implements FCMNode<K, V> {
    private final FCMNode<K, V> leftChild;
    private final FCMNode<K, V> rightChild;
    private byte[] hash;

    public FCMInternalNode(FCMNode<K, V> leftChild, FCMNode<K, V> rightChild) {
        this.leftChild = leftChild;
        this.rightChild = rightChild;
    }

    @Override
    public FCMNode<K, V> getLeftChild() {
        return leftChild;
    }

    @Override
    public FCMNode<K, V> getRightChild() {
        return rightChild;
    }

    @Override
    public byte[] getHash() {
        if (hash == null) {
            hash = computeHash();
        }
        return hash;
    }

    private byte[] computeHash() {
        byte[] leftHash = leftChild != null ? leftChild.getHash() : EMPTY_HASH;
        byte[] rightHash = rightChild != null ? rightChild.getHash() : EMPTY_HASH;
        var digest = CryptoUtils.getMessageDigest();
        digest.update(leftHash);
        digest.update(rightHash);
        return digest.digest();
    }
}
