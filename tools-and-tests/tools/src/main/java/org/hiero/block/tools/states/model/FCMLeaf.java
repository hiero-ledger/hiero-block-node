package org.hiero.block.tools.states.model;

import org.hiero.block.tools.states.utils.FCDataInputStream;
import java.io.IOException;

public record  FCMLeaf<K,V>(K key, V value) implements  FCMNode<K,V> {

    public static <K,V> FCMLeaf<K, V> copyFrom(final FCDataInputStream inputStream,
            ParseFunction<K> keyDeserializer,
            ParseFunction<V> valueDeserializer) throws IOException {
        final K key = keyDeserializer.copyFrom(inputStream);
        final V value = valueDeserializer.copyFrom(inputStream);
        return new FCMLeaf<>(key, value);
    }

    @Override
    public FCMNode<K,V> getLeftChild() {
        return null;
    }

    @Override
    public FCMNode<K,V> getRightChild() {
        return null;
    }

    @Override
    public byte[] getHash() {
        return new byte[0];
    }
}
