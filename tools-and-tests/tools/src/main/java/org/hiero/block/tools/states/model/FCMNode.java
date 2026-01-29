package org.hiero.block.tools.states.model;

public interface FCMNode<K,V> {

    FCMNode<K, V> getLeftChild();

    FCMNode<K, V> getRightChild();

    byte[] getHash();
}
