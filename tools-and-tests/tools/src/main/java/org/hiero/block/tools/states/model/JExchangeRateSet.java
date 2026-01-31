// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * A serializable pair of current and next exchange rates.
 *
 * @param currentRate the current exchange rate, or {@code null} if absent
 * @param nextRate the next exchange rate, or {@code null} if absent
 */
public record JExchangeRateSet(JExchangeRate currentRate, JExchangeRate nextRate) {
    /** The legacy serialization version identifier (version 1). */
    private static final long LEGACY_VERSION_1 = 1;
    /** The current serialization version identifier (version 2). */
    private static final long CURRENT_VERSION = 2;

    /**
     * Deserializes a JExchangeRateSet from the given input stream.
     *
     * @param inStream the input stream to read from
     * @return the deserialized JExchangeRateSet instance
     * @throws IOException if an I/O error occurs during deserialization
     */
    public static JExchangeRateSet copyFrom(final DataInputStream inStream) throws IOException {
        JExchangeRate currentRate = null;
        JExchangeRate nextRate = null;

        long version = inStream.readLong();
        if (version < LEGACY_VERSION_1 || version > CURRENT_VERSION) {
            throw new IllegalStateException("Illegal version was read from the stream");
        }

        long objectType = inStream.readLong();
        JObjectType type = JObjectType.valueOf(objectType);
        if (!JObjectType.JExchangeRateSet.equals(type)) {
            throw new IllegalStateException("Illegal JObjectType was read from the stream");
        }

        final boolean currentRatePresent = inStream.readBoolean();
        if (currentRatePresent) {
            currentRate = JExchangeRate.copyFrom(inStream);
        }

        final boolean nextRatePresent = inStream.readBoolean();
        if (nextRatePresent) {
            nextRate = JExchangeRate.copyFrom(inStream);
        }

        return new JExchangeRateSet(currentRate, nextRate);
    }
}
