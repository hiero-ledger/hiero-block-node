// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * A serializable Hedera account identifier consisting of shard, realm, and account numbers.
 *
 * @param shardNum the shard number
 * @param realmNum the realm number
 * @param accountNum the account number
 */
@SuppressWarnings("unused")
public record JAccountID(long shardNum, long realmNum, long accountNum) {
    /** The current serialization version. */
    private static final long VERSION = 2;
    /** The serialization object type identifier. */
    private static final long OBJECT_ID = JObjectType.JAccountID.longValue();

    /**
     * Deserializes a JAccountID from the given stream.
     *
     * @param inStream the stream to read from
     * @return the deserialized JAccountID
     * @throws IOException if an I/O error occurs
     */
    public static JAccountID copyFrom(DataInputStream inStream) throws IOException {
        long version = inStream.readLong();
        long objectType = inStream.readLong();
        long shardNum = inStream.readLong();
        long realmNum = inStream.readLong();
        long accountNum = inStream.readLong();
        return new JAccountID(shardNum, realmNum, accountNum);
    }

    /**
     * Serializes this JAccountID (copyTo + copyToExtra combined, since copyToExtra is empty).
     *
     * @param out the stream to write to
     * @throws IOException if an I/O error occurs
     */
    public void copyTo(DataOutputStream out) throws IOException {
        out.writeLong(VERSION);
        out.writeLong(OBJECT_ID);
        out.writeLong(shardNum);
        out.writeLong(realmNum);
        out.writeLong(accountNum);
    }
}
