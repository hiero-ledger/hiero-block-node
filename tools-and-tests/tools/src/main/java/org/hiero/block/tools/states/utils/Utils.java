// SPDX-License-Identifier: Apache-2.0
/*
 * (c) 2016-2019 Swirlds, Inc.
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
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.text.Normalizer.Form;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.hiero.block.tools.states.model.DeserializeFunction;
import org.hiero.block.tools.states.model.Deserializer;

/**
 * A collection of static utility methods for serialization and deserialization of primitive types,
 * byte arrays, strings, and instants to and from {@link DataOutputStream}/{@link DataInputStream}.
 *
 * <p>These methods follow the Swirlds serialization conventions, including NFD-normalized UTF-8
 * string encoding, checksummed byte array framing, and big-endian long encoding.
 */
public class Utils {

    /**
     * Write an Instant to the given stream
     *
     * @param stream
     * 		the stream to write to
     * @param instant
     * 		the Instant to write
     * @throws IOException
     * 		thrown if there are any problems during the operation
     */
    public static void writeInstant(DataOutputStream stream, Instant instant) throws IOException {
        stream.writeLong(instant.getEpochSecond());
        stream.writeLong(instant.getNano());
    }

    /**
     * Reads a String encoded in the Swirlds default charset (UTF8) from an input stream
     *
     * @param in
     * 		the stream to read from
     * @return the String read
     * @throws IOException
     * 		thrown if there are any problems during the operation
     */
    public static String readNormalisedString(DataInputStream in) throws IOException {
        final byte[] data = readByteArray(in);
        return new String(data, StandardCharsets.UTF_8);
    }

    /**
     * read an Instant from the given stream
     *
     * @param stream
     * 		the stream to read from
     * @return the Instant that was read
     * @throws IOException
     * 		thrown if there are any problems during the operation
     */
    public static Instant readInstant(DataInputStream stream) throws IOException {
        return Instant.ofEpochSecond( //
                stream.readLong(), // from getEpochSecond()
                stream.readLong()); // from getNano()
    }

    /**
     * read a FastCopyable array from the given stream
     *
     * @param <T>
     * 		the FastCopyable class that the array will consist of
     * @param stream
     * 		the stream to read from
     * @param deserializeFunction
     * 		The class that of objects that is contained in the array, the class must implement
     * 		FastCopyable and must have a default constructor
     * @return the list that was read
     * @throws IOException
     * 		thrown if there are any problems during the operation
     */
    public static <T extends Record> List<T> readFastCopyableArray(
            DataInputStream stream, DeserializeFunction<T> deserializeFunction) throws IOException {
        int len = stream.readInt();
        List<T> data = new ArrayList<>(len);
        for (int i = 0; i < len; i++) {
            // some of the array elements could be null, so the first byte that indicates whether it is
            // null or not
            byte isNull = stream.readByte();
            if (isNull != 0) {
                data.add(deserializeFunction.deserialize(stream));
            } else {
                data.add(null);
            }
        }
        return data;
    }

    /**
     * Reads a list from the stream deserializing the objects with the supplied method
     *
     * @param stream
     * 		the stream to read from
     * @param listSupplier
     * 		a method that supplies the list to add to
     * @param deserializer
     * 		a method used to deserialize the objects
     * @param <T>
     * 		the type of object contained in the list
     * @return a list that was read from the stream, can be null if that was written
     * @throws IOException
     * 		thrown if there are any problems during the operation
     */
    public static <T> List<T> readList(
            DataInputStream stream, Supplier<List<T>> listSupplier, Deserializer<T> deserializer) throws IOException {
        int listSize = stream.readInt();
        if (listSize < 0) {
            return null;
        }
        List<T> list = listSupplier.get();
        for (int i = 0; i < listSize; i++) {
            list.add(deserializer.deserialize(stream));
        }
        return list;
    }

    /**
     * Convert the given long to bytes, big endian, and put them into the array, starting at index start
     *
     * @param bytes
     * 		the array to hold the Long.BYTES bytes of result
     * @param n
     * 		the long to convert to bytes
     * @param start
     * 		the bytes are written to Long.BYTES elements of the array, starting with this index
     */
    public static void toBytes(long n, byte[] bytes, int start) {
        for (int i = start + Long.BYTES - 1; i >= start; i--) {
            bytes[i] = (byte) n;
            n >>>= 8;
        }
    }

    /**
     * convert part of the given byte array to a long, starting with index start
     *
     * @param b
     * 		the byte array to convert
     * @param start
     * 		the index of the first byte (most significant byte) of the 8 bytes to convert
     * @return the long
     */
    public static long toLong(byte[] b, int start) {
        long result = 0;
        for (int i = start; i < start + Long.BYTES; i++) {
            result <<= 8;
            result |= b[i] & 0xFF;
        }
        return result;
    }

    /**
     * Is the part more than 2/3 of the whole?
     *
     * @param part
     * 		a long with {@code part &lt; Long.MAX_VALUE / 2 }
     * @param whole
     * 		a long with @{code 0 &lt;= whole &lt; Long.MAX_VALUE / 3 }
     * @return true if part is more than two thirds of the whole
     */
    public static boolean isSupermajority(long part, long whole) {
        /*
        For nonnegative integers x and y,
        the following three inequalities are
        mathematically equivalent (for
        infinite precision real computations):

        x > y*2/3

        x > floor(y*2/3)

        x > floor(y/3)*2 + floor((y mod 3)*2/3)

        Therefore, given that Java long division
        rounds toward zero, it is equivalent to do
        the following:

        x > y / 3 * 2 + (y % 3) * 2 / 3;

        That avoids overflow for x and y
        if they are positive long variable.
        */

        return part > whole / 3 * 2 + (whole % 3) * 2 / 3;
    }

    /**
     * write a byte array to the given stream
     *
     * @param stream
     * 		the stream to write to
     * @param data
     * 		the array to write
     * @throws IOException
     * 		thrown if there are any problems during the operation
     */
    public static void writeByteArray(DataOutputStream stream, byte[] data) throws IOException {
        int len = (data == null ? 0 : data.length);
        stream.writeInt(len);
        for (int i = 0; i < len; i++) {
            stream.writeByte(data[i]);
        }
    }

    /**
     * read a byte array from the given stream
     *
     * @param stream
     * 		the stream to read from
     * @return the array that was read
     * @throws IOException
     * 		thrown if there are any problems during the operation
     */
    public static byte[] readByteArray(DataInputStream stream) throws IOException {
        int len = stream.readInt();
        byte[] data = new byte[len];
        for (int i = 0; i < len; i++) {
            data[i] = stream.readByte();
        }
        return data;
    }

    /**
     * Writes a string to the given stream after normalizing it to NFD form and encoding it as
     * UTF-8. The resulting byte array is written using {@link #writeByteArray(DataOutputStream, byte[])}.
     *
     * @param out the stream to write to
     * @param s the string to normalize and write; if {@code null}, an empty byte array is written
     * @throws IOException if an I/O error occurs
     */
    public static void writeNormalisedString(DataOutputStream out, String s) throws IOException {
        byte[] data = getNormalisedStringBytes(s);
        writeByteArray(out, data);
    }

    /**
     * Normalizes the string in accordance with the Swirlds default normalization method (NFD) and returns
     * the bytes of that normalized String encoded in the Swirlds default charset (UTF8). This is important
     * for having a consistent method of converting Strings to bytes that will guarantee that two identical
     * strings will have an identical byte representation
     *
     * @param s
     * 		the String to be converted to bytes
     * @return a byte representation of the String
     */
    public static byte[] getNormalisedStringBytes(String s) {
        if (s == null) {
            return new byte[0];
        }
        return Normalizer.normalize(s, Form.NFD).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Reverse of {@link #getNormalisedStringBytes(String)}
     *
     * @param bytes
     * 		the bytes to convert
     * @return a String created from the input bytes
     */
    public static String getNormalisedStringFromBytes(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Reads an {@link Instant} from the given {@link DataInput} stream (epoch seconds followed by
     * nanoseconds, each as a {@code long}) and increments the byte counter by the number of bytes
     * consumed (16 bytes total).
     *
     * @param dis the data input stream to read from
     * @param byteCount an array of length at least 1 whose first element is incremented by the
     *        number of bytes read (2 &times; {@link Long#BYTES})
     * @return the {@link Instant} that was read
     * @throws IOException if an I/O error occurs
     */
    public static Instant readInstant(DataInput dis, int[] byteCount) throws IOException {
        Instant time = Instant.ofEpochSecond( //
                dis.readLong(), // from getEpochSecond()
                dis.readLong()); // from getNano()
        byteCount[0] += 2 * Long.BYTES;
        return time;
    }

    /**
     * Reads a checksummed byte array from the given stream, verifying that the declared length does
     * not exceed the specified maximum, and increments the byte counter accordingly.
     *
     * <p>The wire format is: {@code int length}, {@code int checksum} (must equal
     * {@code 101 - length}), followed by {@code length} bytes of data.
     *
     * @param dis the data input stream to read from
     * @param byteCount an array of length at least 1 whose first element is incremented by the
     *        number of bytes read
     * @param maxArrayLength the maximum allowed array length; a {@link RuntimeException} is thrown
     *        if the declared length exceeds this value
     * @return the byte array read from the stream
     * @throws IOException if an I/O error occurs or the checksum does not match
     * @throws RuntimeException if the declared length exceeds {@code maxArrayLength}
     */
    static byte[] readByteArray(DataInputStream dis, int[] byteCount, int maxArrayLength) throws IOException {
        int len = dis.readInt();
        checkArrayLength(len, maxArrayLength);
        return readByteArrayOfLength(dis, byteCount, len);
    }

    /**
     * Read a byte[] from a data stream and increment byteCount[0] by the number of bytes
     *
     * @param dis data input stream to read from
     * @param byteCount an array of length at least 1, whose first element is incremented by the number of bytes read
     * @return the byte array read
     * @throws IOException if an I/O error occurs
     */
    public static byte[] readByteArray(DataInputStream dis, int[] byteCount) throws IOException {
        return readByteArray(dis, byteCount, Integer.MAX_VALUE);
    }

    /**
     * Validates that the given array length does not exceed the maximum allowed length.
     *
     * @param len the declared array length
     * @param maxArrayLength the maximum permitted length
     * @throws RuntimeException if {@code len} exceeds {@code maxArrayLength}
     */
    private static void checkArrayLength(int len, int maxArrayLength) {
        if (len > maxArrayLength) {
            throw new IllegalArgumentException(
                    String.format("Array length (%d) is larger than maxArrayLength (%d)", len, maxArrayLength));
        }
    }

    /**
     * Reads a byte array of the given length from the stream, first verifying a checksum integer
     * that must equal {@code 101 - len}. Increments the byte counter by
     * {@code 2 * Integer.BYTES + len} bytes.
     *
     * @param dis the data input stream to read from
     * @param byteCount an array of length at least 1 whose first element is incremented by the
     *        number of bytes consumed
     * @param len the number of data bytes to read
     * @return the byte array read from the stream
     * @throws IOException if an I/O error occurs, the length is negative, or the checksum is invalid
     */
    private static byte[] readByteArrayOfLength(DataInputStream dis, int[] byteCount, int len) throws IOException {
        int checksum = dis.readInt();
        if (len < 0 || checksum != (101 - len)) { // must be at wrong place in the stream
            throw new IOException(
                    "SyncServer.readByteArray tried to create array of length " + len + " with wrong checksum.");
        }
        byte[] data = new byte[len];
        dis.readFully(data);
        byteCount[0] += 2 * Integer.BYTES + len * Byte.BYTES;
        return data;
    }

    /**
     * Finds the leftmost (most significant) set bit in the given value. For example,
     * {@code findLeftMostBit(10)} returns {@code 8} (binary {@code 1000}), because 10 in binary
     * is {@code 1010} and the highest set bit has value 8.
     *
     * @param value the value to examine; returns 0 if the value is 0
     * @return a {@code long} with only the leftmost set bit of {@code value}, or 0 if
     *         {@code value} is 0
     */
    public static long findLeftMostBit(final long value) {
        if (value == 0) {
            return 0;
        }

        long leftMostBit = 1L << 62;
        while ((value & leftMostBit) == 0) {
            leftMostBit >>= 1;
        }

        return leftMostBit;
    }
}
