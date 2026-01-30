// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.IOException;
import java.io.DataInputStream;

public record JAccountAmount(JAccountID accountID, long amount) {
    private static final long LEGACY_VERSION_1 = 1;
    private static final long CURRENT_VERSION = 2;

    public static JAccountAmount copyFrom(final DataInputStream inStream) throws IOException {
        final long version = inStream.readLong();
        if (version < LEGACY_VERSION_1 || version > CURRENT_VERSION) {
            throw new IllegalStateException("Illegal version was read from the stream");
        }

        final long objectType = inStream.readLong();
        final JObjectType type = JObjectType.valueOf(objectType);
        if (!JObjectType.JAccountAmount.equals(type)) {
            throw new IllegalStateException("Illegal JObjectType was read from the stream");
        }

        var accountID = JAccountID.copyFrom(inStream);
        var amount = inStream.readLong();
        return new JAccountAmount(accountID, amount);
    }
}
