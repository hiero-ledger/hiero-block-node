package org.hiero.block.tools.states;

public interface DeserializeFunction<T extends Record> {
    T deserialize(FCDataInputStream inStream) throws java.io.IOException;
}
