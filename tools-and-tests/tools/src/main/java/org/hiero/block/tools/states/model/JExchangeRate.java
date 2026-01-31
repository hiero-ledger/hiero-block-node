// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.DataInputStream;
import java.io.IOException;

/** A serializable exchange rate entry with HBAR and cent equivalents and an expiration time. */
public record JExchangeRate(int hbarEquiv, int centEquiv, long expirationTime) {
    private static final long LEGACY_VERSION_1 = 1;
    private static final long CURRENT_VERSION = 2;

    public static JExchangeRate copyFrom(final DataInputStream inStream) throws IOException {
        int hbarEquiv;
        int centEquiv;
        long expirationTime;
        long version = inStream.readLong();
        if (version < LEGACY_VERSION_1 || version > CURRENT_VERSION) {
            throw new IllegalStateException("Illegal version was read from the stream");
        }

        long objectType = inStream.readLong();
        JObjectType type = JObjectType.valueOf(objectType);
        if (!JObjectType.JExchangeRate.equals(type)) {
            throw new IllegalStateException("Illegal JObjectType was read from the stream");
        }

        hbarEquiv = inStream.readInt();
        centEquiv = inStream.readInt();
        expirationTime = inStream.readLong();

        return new JExchangeRate(hbarEquiv, centEquiv, expirationTime);
    }
}
