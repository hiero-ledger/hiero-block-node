// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * A serializable account-amount pair used in transfer lists.
 *
 * @param accountID the account identifier
 * @param amount the HBAR amount in tinybars
 */
public record JAccountAmount(JAccountID accountID, long amount) {
    /** The legacy serialization version identifier (version 1). */
    private static final long LEGACY_VERSION_1 = 1;
    /** The current serialization version identifier (version 2). */
    private static final long CURRENT_VERSION = 2;

    /**
     * Deserializes a JAccountAmount from the given input stream.
     *
     * @param inStream the input stream to read from
     * @return the deserialized JAccountAmount instance
     * @throws IOException if an I/O error occurs during deserialization
     */
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
