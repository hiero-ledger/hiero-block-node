// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

/** A node in the FCMap Merkle tree. */
@SuppressWarnings("unused")
public interface FCMNode<K, V> {
    /** 48-byte all-zeros hash used when a child is absent. */
    byte[] EMPTY_HASH = new byte[48];

    FCMNode<K, V> getLeftChild();

    FCMNode<K, V> getRightChild();

    byte[] getHash();
}
