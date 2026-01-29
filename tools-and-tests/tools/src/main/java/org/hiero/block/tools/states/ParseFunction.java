package org.hiero.block.tools.states;

public interface ParseFunction<T>  {
    T copyFrom(FCDataInputStream inStream) throws java.io.IOException;
}
