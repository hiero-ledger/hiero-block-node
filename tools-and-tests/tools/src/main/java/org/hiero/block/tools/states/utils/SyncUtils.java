// SPDX-License-Identifier: Apache-2.0
/*
 * (c) 2016-2018 Swirlds, Inc.
 *
 * This software is the confidential and proprietary information of
 * Swirlds, Inc. ("Confidential Information"). You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Swirlds.
 *
 * SWIRLDS MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE SUITABILITY OF
 * THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
 * TO THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE, OR NON-INFRINGEMENT. SWIRLDS SHALL NOT BE LIABLE FOR
 * ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING, MODIFYING OR
 * DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
 */
package org.hiero.block.tools.states.utils;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Static IO utilities that are useful during syncs.
 */
public class SyncUtils {

    /**
     * This exception is thrown during a sync when bad data is received, such a bad checksum.
     */
    static class BadIOException extends IOException {
        private static final long serialVersionUID = 1L;

        BadIOException(String msg) {
            super(msg);
        }
    }

    /** write an Instant to a data stream and increment byteCount[0] by the number of bytes */
    static void writeInstant(DataOutput dos, Instant time, int[] byteCount) throws IOException {
        dos.writeLong(time.getEpochSecond());
        dos.writeLong(time.getNano());
        byteCount[0] += 2 * Long.BYTES;
    }

    /** read an Instant from a data stream and increment byteCount[0] by the number of bytes */
    public static Instant readInstant(DataInput dis, int[] byteCount) throws IOException {
        Instant time = Instant.ofEpochSecond( //
                dis.readLong(), // from getEpochSecond()
                dis.readLong()); // from getNano()
        byteCount[0] += 2 * Long.BYTES;
        return time;
    }

    /** write a long[] to a data stream and increment byteCount[0] by the number of bytes */
    static void writeLongArray(DataOutput dos, long[] data, int[] byteCount) throws IOException {
        dos.writeInt(data.length);
        for (int i = 0; i < data.length; i++) {
            dos.writeLong(data[i]);
        }
        byteCount[0] += Integer.BYTES + data.length * Long.BYTES;
    }

    /** read a long[] from a data stream and increment byteCount[0] by the number of bytes */
    static long[] readLongArray(DataInput dis, int[] byteCount, int maxArrayLength) throws IOException {
        int len = dis.readInt();
        if (len < 0) {
            throw new BadIOException("SyncServer.readByteArray2d tried to create array of length " + len);
        }
        checkArrayLength(len, maxArrayLength);
        long[] data = new long[len];
        for (int i = 0; i < len; i++) {
            data[i] = dis.readLong();
        }
        byteCount[0] += Integer.BYTES + len * Long.BYTES;
        return data;
    }

    /**
     * read a long[] from a data stream, and convert it to an AtomicLongArray, and increment byteCount[0] by
     * the number of bytes
     */
    static AtomicLongArray readAtomicLongArray(DataInput dis, int[] byteCount, int maxArrayLength) throws IOException {
        int len = dis.readInt();
        if (len < 0) {
            throw new BadIOException("SyncServer.readByteArray2d tried to create array of length " + len);
        }
        checkArrayLength(len, maxArrayLength);
        AtomicLongArray data = new AtomicLongArray(len);
        for (int i = 0; i < len; i++) {
            data.set(i, dis.readLong());
        }
        byteCount[0] += Integer.BYTES + len * Long.BYTES;
        return data;
    }

    /** write a byte[] to a data stream and increment byteCount[0] by the number of bytes */
    static void writeByteArray(DataOutputStream dos, byte[] data, int[] byteCount) throws IOException {
        dos.writeInt(data.length);
        // write a simple checksum to detect if at wrong place in the stream
        dos.writeInt(101 - data.length);
        dos.write(data);
        byteCount[0] += 2 * Integer.BYTES + data.length * Byte.BYTES;
    }

    static void writeNullableByteArray(DataOutputStream dos, byte[] data, int[] byteCount) throws IOException {
        if (data == null) {
            dos.writeInt(-1);
            byteCount[0] += Byte.BYTES;
            return;
        } else {
            writeByteArray(dos, data, byteCount);
        }
    }

    /**
     * write a byte[] of len random bytes to a data stream and increment byteCount[0] by the number of bytes
     */
    static void writeByteArray(DataOutputStream dos, int len, int[] byteCount) throws IOException {
        byte[] data = new byte[len];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) Math.random();
        }
        writeByteArray(dos, data, byteCount);
    }

    /** read a byte[] from a data stream and increment byteCount[0] by the number of bytes */
    static byte[] readByteArray(DataInputStream dis, int[] byteCount, int maxArrayLength) throws IOException {
        int len = dis.readInt();
        checkArrayLength(len, maxArrayLength);
        return readByteArrayOfLength(dis, byteCount, len);
    }

    public static byte[] readByteArray(DataInputStream dis, int[] byteCount) throws IOException {
        return readByteArray(dis, byteCount, Integer.MAX_VALUE);
    }

    static byte[] readNullableByteArray(DataInputStream dis, int[] byteCount, int maxArrayLength) throws IOException {
        int len = dis.readInt();
        checkArrayLength(len, maxArrayLength);
        if (len < 0) {
            return null;
        } else {
            return readByteArrayOfLength(dis, byteCount, len);
        }
    }

    static byte[] readNullableByteArray(DataInputStream dis, int[] byteCount) throws IOException {
        return readNullableByteArray(dis, byteCount, Integer.MAX_VALUE);
    }

    private static void checkArrayLength(int len, int maxArrayLength) {
        if (len > maxArrayLength) {
            throw new RuntimeException(
                    String.format("Array length (%d) is larger than maxArrayLength (%d)", len, maxArrayLength));
        }
    }

    private static byte[] readByteArrayOfLength(DataInputStream dis, int[] byteCount, int len) throws IOException {
        int checksum = dis.readInt();
        if (len < 0 || checksum != (101 - len)) { // must be at wrong place in the stream
            throw new BadIOException(
                    "SyncServer.readByteArray tried to create array of length " + len + " with wrong checksum.");
        }
        byte[] data = new byte[len];
        dis.readFully(data);
        byteCount[0] += 2 * Integer.BYTES + len * Byte.BYTES;
        return data;
    }

    /* write a boolean[] to a data stream and increment byteCount[0] by the number of bytes */
    static void writeBooleanArray(DataOutputStream dos, boolean[] data, int[] byteCount) throws IOException {
        byte[] bytes = new byte[data.length];
        for (int i = 0; i < data.length; i++) {
            bytes[i] = (data[i] ? (byte) 1 : (byte) 0);
        }
        writeByteArray(dos, bytes, byteCount);
    }

    /** read a boolean[] to a data stream and increment byteCount[0] by the number of bytes */
    static boolean[] readBooleanArray(DataInputStream dis, int[] byteCount) throws IOException {
        byte[] bytes = readByteArray(dis, byteCount);
        boolean[] data = new boolean[bytes.length];
        for (int i = 0; i < data.length; i++) {
            data[i] = (bytes[i] != 0);
        }
        return data;
    }

    /** write a byte[][] to a data stream and increment byteCount[0] by the number of bytes */
    static void writeByteArray2d(DataOutputStream dos, byte[][] data, int[] byteCount) throws IOException {
        dos.writeInt(data.length);
        // write a trivial kind of checksum to detect if at wrong place in the stream
        dos.writeInt(997 - data.length);

        for (int i = 0; i < data.length; i++) {
            writeByteArray(dos, data[i], byteCount);
        }
        byteCount[0] += 2 * Integer.BYTES;
    }

    /** read a byte[][] from a data stream and increment byteCount[0] by the number of bytes */
    static byte[][] readByteArray2d(DataInputStream dis, int[] byteCount) throws IOException {
        int len = dis.readInt();
        int checksum = dis.readInt();
        if (len < 0 || checksum != (997 - len)) { // must be at wrong place in the stream
            throw new BadIOException(
                    "SyncServer.readByteArray2d tried to create array of length " + len + " with wrong checksum.");
        }
        byte[][] data = new byte[len][];
        for (int i = 0; i < len; i++) {
            data[i] = readByteArray(dis, byteCount);
        }
        byteCount[0] += 2 * Integer.BYTES;
        return data;
    }
}
