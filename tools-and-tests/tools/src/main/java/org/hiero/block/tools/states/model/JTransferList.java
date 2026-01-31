// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;

/**
 * A serializable list of account-amount pairs representing HBAR transfers.
 *
 * @param jAccountAmountsList the list of account-amount pairs
 */
public record JTransferList(List<JAccountAmount> jAccountAmountsList) {
    /** The legacy serialization version identifier (version 1). */
    private static final long LEGACY_VERSION_1 = 1;
    /** The current serialization version identifier (version 2). */
    private static final long CURRENT_VERSION = 2;

    /**
     * Deserializes a JTransferList from the given input stream.
     *
     * @param inStream the input stream to read from
     * @return the deserialized JTransferList instance
     * @throws IOException if an I/O error occurs during deserialization
     */
    public static JTransferList copyFrom(final DataInputStream inStream) throws IOException {
        final long version = inStream.readLong();
        if (version < LEGACY_VERSION_1 || version > CURRENT_VERSION) {
            throw new IllegalStateException("Illegal version was read from the stream");
        }

        final long objectType = inStream.readLong();
        final JObjectType type = JObjectType.valueOf(objectType);
        if (!JObjectType.JTransferList.equals(type)) {
            throw new IllegalStateException("Illegal JObjectType was read from the stream");
        }
        List<JAccountAmount> jAccountAmountsList = new java.util.ArrayList<>();
        int listSize = inStream.readInt();
        for (int i = 0; i < listSize; i++) {
            jAccountAmountsList.add(JAccountAmount.copyFrom(inStream));
        }

        return new JTransferList(jAccountAmountsList);
    }
}
