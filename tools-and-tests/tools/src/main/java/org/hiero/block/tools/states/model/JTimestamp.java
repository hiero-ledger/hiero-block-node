// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.IOException;
import java.io.DataInputStream;

public record JTimestamp(long seconds, int nano) {
    private static final long LEGACY_VERSION_1 = 1;
    private static final long CURRENT_VERSION = 2;

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
