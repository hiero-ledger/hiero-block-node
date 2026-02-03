// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * A flat serializable wrapper for exchange rate data stored directly in the HGC application state.
 *
 * @param currentHbarEquiv the current HBAR equivalent value
 * @param currentCentEquiv the current cent equivalent value
 * @param currentExpirationTime the current rate expiration time in seconds since epoch
 * @param nextHbarEquiv the next HBAR equivalent value
 * @param nextCentEquiv the next cent equivalent value
 * @param nextExpirationTime the next rate expiration time in seconds since epoch
 */
public record ExchangeRateSetWrapper(
        int currentHbarEquiv,
        int currentCentEquiv,
        long currentExpirationTime,
        int nextHbarEquiv,
        int nextCentEquiv,
        long nextExpirationTime) {
    /** The serialization version identifier. */
    private static final long VERSION = 1L;
    /** The unique object identifier for this type. */
    private static final long OBJECT_ID = 10000121L;

    /**
     * Deserializes an ExchangeRateSetWrapper from the given input stream.
     *
     * @param DataInputStream the input stream to read from
     * @return the deserialized ExchangeRateSetWrapper instance
     * @throws IOException if an I/O error occurs or version/object ID mismatch is detected
     */
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

    /**
     * Serializes this ExchangeRateSetWrapper to the given output stream.
     *
     * @param DataOutputStream the output stream to write to
     * @throws IOException if an I/O error occurs during serialization
     */
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
