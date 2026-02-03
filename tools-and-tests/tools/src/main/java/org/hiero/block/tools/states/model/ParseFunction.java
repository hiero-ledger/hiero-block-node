// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.DataInputStream;

/** A function that reads and constructs an object from a {@link DataInputStream}. */
public interface ParseFunction<T> {
    /**
     * Reads and constructs an object from the given stream.
     *
     * @param inStream the stream to read from
     * @return the constructed object
     * @throws java.io.IOException if an I/O error occurs
     */
    T copyFrom(DataInputStream inStream) throws java.io.IOException;
}
