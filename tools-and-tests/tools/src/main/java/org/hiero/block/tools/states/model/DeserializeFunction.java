// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.DataInputStream;

/** A function that deserializes a record type from a {@link DataInputStream}. */
public interface DeserializeFunction<T extends Record> {
    /**
     * Deserializes a record from the given stream.
     *
     * @param inStream the stream to read from
     * @return the deserialized record
     * @throws java.io.IOException if an I/O error occurs
     */
    T deserialize(DataInputStream inStream) throws java.io.IOException;
}
