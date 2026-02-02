// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * A serializable timestamp consisting of epoch seconds and nanosecond offset.
 *
 * @param seconds the epoch seconds
 * @param nano the nanosecond offset within the second
 */
public record JTimestamp(long seconds, int nano) {
    /** The legacy serialization version. */
    private static final long LEGACY_VERSION_1 = 1;
    /** The current serialization version. */
    private static final long CURRENT_VERSION = 2;

    /**
     * Deserializes a JTimestamp from the given stream.
     *
     * @param inStream the stream to read from
     * @return the deserialized JTimestamp
     * @throws IOException if an I/O error occurs
     * @throws IllegalStateException if the version or object type is invalid
     */
    public static JTimestamp copyFrom(final DataInputStream inStream) throws IOException {
        long version = inStream.readLong();
        if (version < LEGACY_VERSION_1 || version > CURRENT_VERSION) {
            throw new IllegalStateException("Illegal version was read from the stream");
        }

        long objectType = inStream.readLong();
        JObjectType type = JObjectType.valueOf(objectType);
        if (!JObjectType.JTimestamp.equals(type)) {
            throw new IllegalStateException("Illegal JObjectType was read from the stream");
        }

        long seconds = inStream.readLong();
        int nano = inStream.readInt();

        return new JTimestamp(seconds, nano);
    }
}
