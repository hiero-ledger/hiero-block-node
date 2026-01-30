package org.hiero.block.tools.states.model;

import org.hiero.block.tools.states.utils.FCDataInputStream;
import java.io.IOException;

public record JExchangeRateSet(JExchangeRate currentRate, JExchangeRate nextRate) {
    private static final long LEGACY_VERSION_1 = 1;
    private static final long CURRENT_VERSION = 2;

    public static JExchangeRateSet copyFrom(final FCDataInputStream inStream) throws IOException {
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
