// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.function.Function;

/** A serializable linked list used in the Hedera state with marker-delimited binary format. */
public class FCLinkedList<T> extends ArrayList<T> {
    /** marker value indicating the beginning of a serialized list */
    private static final int BEGIN_LIST_MARKER = 275624369;
    /** marker value indicating the end of a serialized list */
    private static final int END_LIST_MARKER = 275654143;
    /** marker value indicating the beginning of a list element */
    private static final int BEGIN_ELEMENT_MARKER = 282113441;
    /** marker value indicating the end of a list element */
    private static final int END_ELEMENT_MARKER = 282124951;
    /** hash algorithm used for computing the list hash */
    private static final String HASH_ALGORITHM = "SHA-384";
    /** serialization version number */
    static final long VERSION = 1L;
    /** unique object identifier for serialization */
    static final long OBJECT_ID = 695029169L;

    /** the recovered hash from deserialization */
    private byte[] hash;

    /**
     * Deserializes an FCLinkedList from the given stream.
     *
     * @param dis the stream to read from
     * @param elementDeserializer function to deserialize each element
     * @param <T> the element type
     * @return the deserialized list
     * @throws IOException if an I/O error occurs or markers are invalid
     */
    public static <T> FCLinkedList<T> copyFrom(DataInputStream dis, Function<DataInputStream, T> elementDeserializer)
            throws IOException {
        readValidLong(dis, "VERSION", VERSION);
        readValidLong(dis, "OBJECT_ID", OBJECT_ID);

        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance(HASH_ALGORITHM);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        //        byte[] hash = new byte[digest.getDigestLength()];
        byte[] recoveredHash = new byte[digest.getDigestLength()];
        dis.readFully(recoveredHash);

        // copyFromExtra
        FCLinkedList<T> list = new FCLinkedList<>();
        readValidInt(dis, "BEGIN_LIST_MARKER", BEGIN_LIST_MARKER);
        readValidLong(dis, "VERSION", VERSION);
        readValidLong(dis, "OBJECT_ID", OBJECT_ID);

        final int listSize = dis.readInt();

        for (int i = 0; i < listSize; i++) {
            readValidInt(dis, "BEGIN_ELEMENT_MARKER", BEGIN_ELEMENT_MARKER);
            final boolean elementPresent = !dis.readBoolean();

            if (!elementPresent) {
                list.add(null);
                continue;
            }

            final T element = elementDeserializer.apply(dis);
            list.add(element);

            readValidInt(dis, "END_ELEMENT_MARKER", END_ELEMENT_MARKER);
        }

        readValidInt(dis, "END_LIST_MARKER", END_LIST_MARKER);

        list.hash = recoveredHash;
        return list;
    }

    /** Writes the FCLinkedList hash header (version + objectId + 48-byte hash). */
    public void copyTo(DataOutputStream out) throws IOException {
        out.writeLong(VERSION);
        out.writeLong(OBJECT_ID);
        if (hash != null) {
            out.write(hash);
        } else {
            out.write(new byte[48]);
        }
    }

    private static void readValidInt(final DataInputStream dis, final String markerName, final int expectedValue)
            throws IOException {
        final int value = dis.readInt();
        if (value != expectedValue) {
            throw new IOException(String.format(
                    "Invalid value %d read from the stream, expected %d (%s) instead.",
                    value, expectedValue, markerName));
        }
    }

    private static void readValidLong(final DataInputStream dis, final String markerName, final long expectedValue)
            throws IOException {
        final long value = dis.readLong();
        if (value != expectedValue) {
            throw new IOException(String.format(
                    "Invalid value %d read from the stream, expected %d (%s) instead.",
                    value, expectedValue, markerName));
        }
    }
}
