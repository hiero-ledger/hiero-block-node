// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.IOException;
import org.hiero.block.tools.states.utils.FCDataInputStream;

public record JTransactionReceipt(
        String status, JAccountID accountID, JAccountID fileID, JAccountID contractID, JExchangeRateSet exchangeRate) {
    private static final long LEGACY_VERSION_1 = 1;
    private static final long CURRENT_VERSION = 2;

    public static JTransactionReceipt copyFrom(final FCDataInputStream inStream) throws IOException {
        String status;
        JAccountID accountID = null;
        JAccountID fileID = null;
        JAccountID contractID = null;
        JExchangeRateSet exchangeRate = null;

        final long version = inStream.readLong();
        if (version < LEGACY_VERSION_1 || version > CURRENT_VERSION) {
            throw new IllegalStateException("Illegal version was read from the stream");
        }

        final long objectType = inStream.readLong();
        final JObjectType type = JObjectType.valueOf(objectType);
        if (!JObjectType.JTransactionReceipt.equals(type)) {
            throw new IllegalStateException("Illegal JObjectType was read from the stream");
        }

        final boolean accountIDPresent = inStream.readBoolean();
        if (accountIDPresent) {
            accountID = JAccountID.copyFrom(inStream);
        }

        final boolean fileIDPresent = inStream.readBoolean();
        if (fileIDPresent) {
            fileID = JAccountID.copyFrom(inStream);
        }

        final boolean contractIDPresent = inStream.readBoolean();
        if (contractIDPresent) {
            contractID = JAccountID.copyFrom(inStream);
        }

        byte[] sBytes = new byte[inStream.readInt()];
        if (sBytes.length > 0) {
            inStream.readFully(sBytes);
            status = new String(sBytes);
        } else {
            status = null;
        }

        final boolean exchangeRatePresent = inStream.readBoolean();
        if (exchangeRatePresent) {
            exchangeRate = JExchangeRateSet.copyFrom(inStream);
        }

        return new JTransactionReceipt(status, accountID, fileID, contractID, exchangeRate);
    }
}
