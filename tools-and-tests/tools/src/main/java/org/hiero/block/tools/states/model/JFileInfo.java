package org.hiero.block.tools.states.model;

import org.hiero.block.tools.states.FCDataInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

public record JFileInfo(boolean deleted, JKey wacl, long expirationTimeInSec) {
    private static final long BPACK_VERSION = 1;
    public static JFileInfo deserialize(byte[] bytes) {
        try (DataInputStream stream = new DataInputStream(new ByteArrayInputStream(bytes))) {
            long version = stream.readLong();
            long objectType = stream.readLong();
            long length = stream.readLong();

            if (objectType != JObjectType.JFileInfo.longValue()) {
                throw new IllegalStateException(
                        "Illegal JObjectType was read from the stream! read objectType long value = "
                                + objectType);
            } else if (version != BPACK_VERSION) {
                throw new IllegalStateException(
                        "Illegal version was read from the stream! read version = " + version);
            }
            // from unpack()
            boolean deleted = stream.readBoolean();
            long expirationTime = stream.readLong();
            byte[] key = stream.readAllBytes();
            final JKey wacl = JKey.copyFrom(new FCDataInputStream(new ByteArrayInputStream(key)));
            return new JFileInfo(deleted, wacl, expirationTime);
        } catch (IOException e) {
            throw new RuntimeException("Error in deserialization of JFileInfo!", e);
        }
    }
}
