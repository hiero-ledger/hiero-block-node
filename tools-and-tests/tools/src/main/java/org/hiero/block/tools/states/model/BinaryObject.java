// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HexFormat;

public final class BinaryObject {
    private static final long VERSION = 1L;
    private static final long OBJECT_ID = 1231553L;
    private final Long id = 0L;
    private Hash hash = null;
    private byte[] data;

    public BinaryObject() {}

    public BinaryObject(byte[] data) {
        this.data = data;
    }

    public Long id() {
        return id;
    }

    public Hash hash() {
        return hash;
    }

    public byte[] data() {
        return data;
    }

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
