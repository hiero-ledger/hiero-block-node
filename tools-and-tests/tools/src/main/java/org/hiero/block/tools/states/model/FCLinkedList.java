// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.DataInputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.function.Function;

public class FCLinkedList<T> extends ArrayList<T> {
    private static final int BEGIN_LIST_MARKER = 275624369;
    private static final int END_LIST_MARKER = 275654143;
    private static final int BEGIN_ELEMENT_MARKER = 282113441;
    private static final int END_ELEMENT_MARKER = 282124951;
    private static final String HASH_ALGORITHM = "SHA-384";
    static final long VERSION = 1L;
    static final long OBJECT_ID = 695029169L;

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

        //        if (recoveredHash != null && recoveredHash.length > 0) {
        //            if (!Arrays.equals(hash, recoveredHash)) {
        //                throw new ListDigestException(String.format(
        //                        "FCLinkedList: Invalid list signature detected during deserialization (Actual: %s,
        // Expected: " +
        //                                "%s for list of size %d)",
        //                        hex(hash), hex(recoveredHash), listSize));
        //            }
        //        }
        // TODO deal with hashes
        return list;
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
