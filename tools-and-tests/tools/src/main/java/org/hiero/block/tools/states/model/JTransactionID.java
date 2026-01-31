// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * A serializable transaction identifier consisting of a payer account and start timestamp.
 *
 * @param payerAccount the payer account ID, or {@code null} if absent
 * @param startTime the transaction start timestamp, or {@code null} if absent
 */
public record JTransactionID(JAccountID payerAccount, JTimestamp startTime) {
    /** The legacy serialization version. */
    private static final long LEGACY_VERSION_1 = 1;
    /** The current serialization version. */
    private static final long CURRENT_VERSION = 2;

    /**
     * Deserializes a JTransactionID from the given stream.
     *
     * @param inStream the stream to read from
     * @return the deserialized JTransactionID
     * @throws IOException if an I/O error occurs
     * @throws IllegalStateException if the version or object type is invalid
     */
    public static JTransactionID copyFrom(final DataInputStream inStream) throws IOException {
        JAccountID payerAccount = null;
        JTimestamp startTime = null;

        long version = inStream.readLong();
        if (version < LEGACY_VERSION_1 || version > CURRENT_VERSION) {
            throw new IllegalStateException("Illegal version was read from the stream");
        }

        long objectType = inStream.readLong();
        JObjectType type = JObjectType.valueOf(objectType);
        if (!JObjectType.JTransactionID.equals(type)) {
            throw new IllegalStateException("Illegal JObjectType was read from the stream");
        }

        if (inStream.readChar() == ApplicationConstants.P) {
            payerAccount = JAccountID.copyFrom(inStream);
        }

        final boolean startTimePresent = inStream.readBoolean();
        if (startTimePresent) {
            startTime = JTimestamp.copyFrom(inStream);
        }

        return new JTransactionID(payerAccount, startTime);
    }
}
