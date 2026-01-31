// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HexFormat;

/** A serializable binary data object with a SHA-384 hash, used for blob storage references. */
public final class BinaryObject {
    /** The serialization version. */
    private static final long VERSION = 1L;
    /** The serialization object identifier. */
    private static final long OBJECT_ID = 1231553L;
    /** The unique identifier for this object. */
    private final Long id = 0L;
    /** The SHA-384 hash of the data. */
    private Hash hash = null;
    /** The raw binary data. */
    private byte[] data;

    /** Creates an empty BinaryObject. */
    public BinaryObject() {}

    /**
     * Creates a BinaryObject with the given data.
     *
     * @param data the binary data
     */
    public BinaryObject(byte[] data) {
        this.data = data;
    }

    /**
     * Returns the unique identifier for this object.
     *
     * @return the object ID
     */
    public Long id() {
        return id;
    }

    /**
     * Returns the SHA-384 hash of the data.
     *
     * @return the hash, or {@code null} if not yet computed
     */
    public Hash hash() {
        return hash;
    }

    /**
     * Returns the raw binary data.
     *
     * @return the data bytes, or {@code null} if not set
     */
    public byte[] data() {
        return data;
    }

    /**
     * Deserializes this BinaryObject from the given stream.
     *
     * @param inStream the stream to read from
     * @throws IOException if an I/O error occurs or if the version or object ID is invalid
     */
    public synchronized void copyFrom(final DataInputStream inStream) throws IOException {
        long version = inStream.readLong();
        if (version != VERSION) {
            throw new IOException("Unsupported BinaryObject version: " + version);
        }
        long objectId = inStream.readLong();
        if (objectId != OBJECT_ID) {
            throw new IOException("Unexpected BinaryObject object ID: " + objectId);
        }

        final byte[] hashValue = new byte[48];
        inStream.readFully(hashValue);

        hash = new Hash(hashValue, 0);

        // BinaryObjectStore.getInstance().registerForRecovery(this);
    }

    /** Serializes this BinaryObject (copyTo + copyToExtra, since copyToExtra is empty). */
    public void copyTo(DataOutputStream out) throws IOException {
        out.writeLong(VERSION);
        out.writeLong(OBJECT_ID);
        out.write(hash.hash());
    }

    @Override
    public String toString() {
        return "BinaryObject{" + "id="
                + id + ", data="
                + (data == null ? "null" : HexFormat.of().formatHex(data)) + ", hash="
                + hash + '}';
    }
}
