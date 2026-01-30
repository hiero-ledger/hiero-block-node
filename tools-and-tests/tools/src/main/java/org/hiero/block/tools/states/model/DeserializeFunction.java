package org.hiero.block.tools.states.model;

import org.hiero.block.tools.states.utils.FCDataInputStream;

public interface DeserializeFunction<T extends Record> {
    T deserialize(FCDataInputStream inStream) throws java.io.IOException;
}
