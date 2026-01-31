// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * A serializable key for the account FCMap, consisting of realm, shard, and account IDs.
 *
 * @param realmId the realm identifier
 * @param shardId the shard identifier
 * @param accountId the account identifier
 */
public record MapKey(long realmId, long shardId, long accountId) {
    /** The current serialization version. */
    private static final long CURRENT_VERSION = 1;
    /** The unique object identifier for serialization. */
    private static final long OBJECT_ID = 15486487;

    /**
     * Deserializes a MapKey from the given stream.
     *
     * @param inStream the stream to read from
     * @return the deserialized MapKey
     * @throws IOException if an I/O error occurs or if the version or object ID is invalid
     */
    public static MapKey copyFrom(DataInputStream inStream) throws IOException {
        long version = inStream.readLong();
        if (version != CURRENT_VERSION) {
            throw new IOException("Unsupported MapKey version: " + version);
        }
        long objectId = inStream.readLong();
        if (objectId != OBJECT_ID) {
            throw new IOException("Unexpected MapKey object ID: " + objectId);
        }
        long realmId = inStream.readLong();
        long shardId = inStream.readLong();
        long accountId = inStream.readLong();

        return new MapKey(realmId, shardId, accountId);
    }

    /**
     * Serializes this MapKey to the given stream.
     *
     * @param out the stream to write to
     * @throws IOException if an I/O error occurs
     */
    public void copyTo(DataOutputStream out) throws IOException {
        out.writeLong(CURRENT_VERSION);
        out.writeLong(OBJECT_ID);
        out.writeLong(realmId);
        out.writeLong(shardId);
        out.writeLong(accountId);
    }
}
