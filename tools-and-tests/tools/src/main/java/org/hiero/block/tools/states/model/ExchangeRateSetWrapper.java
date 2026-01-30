// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.IOException;
import java.io.DataInputStream;
import java.io.DataOutputStream;

public record ExchangeRateSetWrapper(
        int currentHbarEquiv,
        int currentCentEquiv,
        long currentExpirationTime,
        int nextHbarEquiv,
        int nextCentEquiv,
        long nextExpirationTime) {
    private static final long VERSION = 1L;
    private static final long OBJECT_ID = 10000121L;

    public static ExchangeRateSetWrapper copyFrom(DataInputStream DataInputStream) throws IOException {
        // version number
        long version = DataInputStream.readLong();
        if (version != VERSION) {
            throw new IOException("Read Invalid version while calling ExchangeRateSetWrapper.copyFrom() - expected: "
                    + VERSION + ", got: " + version);
        }
        long objectId = DataInputStream.readLong();
        if (objectId != OBJECT_ID) {
            throw new IOException("Read Invalid ObjectID while calling ExchangeRateSetWrapper.copyFrom()");
        }
        int currentHbarEquiv = DataInputStream.readInt();
        int currentCentEquiv = DataInputStream.readInt();
        long currentExpirationTime = DataInputStream.readLong();
        int nextHbarEquiv = DataInputStream.readInt();
        int nextCentEquiv = DataInputStream.readInt();
        long nextExpirationTime = DataInputStream.readLong();
        return new ExchangeRateSetWrapper(
                currentHbarEquiv,
                currentCentEquiv,
                currentExpirationTime,
                nextHbarEquiv,
                nextCentEquiv,
                nextExpirationTime);
    }

    public void copyTo(DataOutputStream DataOutputStream) throws IOException {
        DataOutputStream.writeLong(VERSION);
        DataOutputStream.writeLong(OBJECT_ID);
        DataOutputStream.writeInt(currentHbarEquiv);
        DataOutputStream.writeInt(currentCentEquiv);
        DataOutputStream.writeLong(currentExpirationTime);
        DataOutputStream.writeInt(nextHbarEquiv);
        DataOutputStream.writeInt(nextCentEquiv);
        DataOutputStream.writeLong(nextExpirationTime);
    }
}
