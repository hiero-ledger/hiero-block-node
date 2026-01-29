package org.hiero.block.tools.states.model;

public record FCMInternalNode<K,V>(FCMNode<K,V> leftChild, FCMNode<K,V> rightChild) implements  FCMNode<K,V> {
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
        return new byte[0];
    }
}
