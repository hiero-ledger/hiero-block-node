// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * A serializable file metadata record containing deletion status, WACL key, and expiration time.
 *
 * @param deleted whether the file has been deleted
 * @param wacl the write-access control list key
 * @param expirationTimeInSec the file expiration time in seconds since epoch
 */
public record JFileInfo(boolean deleted, JKey wacl, long expirationTimeInSec) {
    /** The binary pack serialization version identifier. */
    private static final long BPACK_VERSION = 1;

    /**
     * Deserializes a JFileInfo from the given byte array.
     *
     * <p>Wraps any IOException in a RuntimeException.
     *
     * @param bytes the serialized byte array
     * @return the deserialized JFileInfo instance
     */
    @SuppressWarnings("unused")
    public static JFileInfo deserialize(byte[] bytes) {
        try (DataInputStream stream = new DataInputStream(new ByteArrayInputStream(bytes))) {
            long version = stream.readLong();
            long objectType = stream.readLong();
            long length = stream.readLong();

            if (objectType != JObjectType.JFileInfo.longValue()) {
                throw new IllegalStateException(
                        "Illegal JObjectType was read from the stream! read objectType long value = " + objectType);
            } else if (version != BPACK_VERSION) {
                throw new IllegalStateException("Illegal version was read from the stream! read version = " + version);
            }
            // from unpack()
            boolean deleted = stream.readBoolean();
            long expirationTime = stream.readLong();
            byte[] key = stream.readAllBytes();
            final JKey wacl = JKey.copyFrom(new DataInputStream(new ByteArrayInputStream(key)));
            return new JFileInfo(deleted, wacl, expirationTime);
        } catch (IOException e) {
            throw new UncheckedIOException("Error in deserialization of JFileInfo", e);
        }
    }
}
