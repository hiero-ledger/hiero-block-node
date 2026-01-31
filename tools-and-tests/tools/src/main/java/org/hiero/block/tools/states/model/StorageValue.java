// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * A serializable value for the storage FCMap, wrapping an optional {@link BinaryObject}.
 *
 * @param data the binary object data, or {@code null} if absent
 */
public record StorageValue(BinaryObject data) {
    /** The legacy serialization version. */
    private static final long LEGACY_VERSION = 1;
    /** The current serialization version. */
    private static final long CURRENT_VERSION = 2;
    /** The unique object identifier for serialization. */
    private static final long OBJECT_ID = 15487003;

    /**
     * Deserializes a StorageValue from the given stream.
     *
     * @param inStream the stream to read from
     * @return the deserialized StorageValue
     * @throws IOException if an I/O error occurs or if the version or object ID is invalid
     */
    public static StorageValue copyFrom(DataInputStream inStream) throws IOException {
        long version = inStream.readLong(); // read version
        if (version < LEGACY_VERSION || version > CURRENT_VERSION) {
            throw new IOException("Unsupported StorageValue version: " + version);
        }
        long objectId = inStream.readLong(); // read object id
        if (objectId != OBJECT_ID) {
            throw new IOException("Unexpected StorageValue object ID: " + objectId);
        }

        BinaryObject data = null;
        if (version == LEGACY_VERSION) {
            int length = inStream.readInt();
            if (length > 0) {
                byte[] newData = new byte[length];
                inStream.readFully(newData);
                data = new BinaryObject(newData);
            }
        } else {
            final boolean hasData = inStream.readBoolean();
            if (hasData) {
                data = new BinaryObject();
                data.copyFrom(inStream);
            }
        }

        return new StorageValue(data);
    }

    /**
     * Serializes this StorageValue to the given stream.
     *
     * @param out the stream to write to
     * @throws IOException if an I/O error occurs
     */
    public void copyTo(DataOutputStream out) throws IOException {
        out.writeLong(CURRENT_VERSION);
        out.writeLong(OBJECT_ID);
        if (data != null) {
            out.writeBoolean(true);
            data.copyTo(out);
        } else {
            out.writeBoolean(false);
        }
    }
}
