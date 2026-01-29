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

package org.hiero.block.tools.states;


import java.awt.Color;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.text.Normalizer.Form;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

/**
 * This is a collection of static utility methods, such as for comparing and deep cloning of arrays.
 */
public class Utilities extends DataStreamUtils {
	static Charset defaultCharset = StandardCharsets.UTF_8;
	public static void writeNormalisedString(DataOutputStream out, String s)
			throws IOException {
		byte data[] = getNormalisedStringBytes(s);
		writeByteArray(out, data);
	}

	public static byte[] getNormalisedStringBytes(String s) {
		if (s == null) {
			return new byte[0];
		}
		return Normalizer.normalize(s, Form.NFD).getBytes(defaultCharset);
	}

	public static void writeInstant(FCDataOutputStream stream, Instant instant)
			throws IOException {
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
	public static String readNormalisedString(DataInputStream in)
			throws IOException {
		byte data[] = readByteArray(in);
		return new String(data, defaultCharset);
	}

	/**
	 * Convert a string to a boolean.
	 *
	 * A false is defined to be any string that, after trimming leading/trailing whitespace and conversion
	 * to lowercase, is equal to null, or the empty string, or "off" or "0", or starts with "f" or "n". All
	 * other strings are true.
	 *
	 * @param par
	 * 		the string to convert (or null)
	 * @return the boolean value
	 */
	static boolean parseBoolean(String par) {
		if (par == null) {
			return false;
		}
		String p = par.trim().toLowerCase();
		if (p.equals("")) {
			return false;
		}
		String f = p.substring(0, 1);
		return !(p.equals("0") || f.equals("f") || f.equals("n")
				|| p.equals("off"));
	}

	/**
	 * Do a deep clone of a 2D array. Here, "deep" means that after doing x=deepClone(y), x won't be
	 * affected by changes to any part of y, such as assigning to y or to y[0] or to y[0][0].
	 *
	 * @param original
	 * 		the original array
	 * @return the deep clone
	 */
	public static long[][] deepClone(long[][] original) {
		if (original == null) {
			return null;
		}
		long[][] result = original.clone();
		for (int i = 0; i < original.length; i++) {
			if (original[i] != null) {
				result[i] = original[i].clone();
			}
		}
		return result;
	}

	/**
	 * Do a deep clone of a 2D array. Here, "deep" means that after doing x=deepClone(y), x won't be
	 * affected by changes to any part of y, such as assigning to y or to y[0] or to y[0][0].
	 *
	 * @param original
	 * 		the original array
	 * @return the deep clone
	 */
	public static byte[][] deepClone(byte[][] original) {
		if (original == null) {
			return null;
		}
		byte[][] result = original.clone();
		for (int i = 0; i < original.length; i++) {
			if (original[i] != null) {
				result[i] = original[i].clone();
			}
		}
		return result;
	}

	/**
	 * Do a deep clone of any serialiazable object that can reach only other serializable objects through
	 * following references.
	 *
	 * @param original
	 * 		the object to clone
	 * @return the clone
	 */
	public static Object deepCloneBySerializing(Object original) {
		ObjectOutputStream dos = null;
		ObjectInputStream dis = null;
		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			dos = new ObjectOutputStream(bos);
			// serialize and pass the object
			dos.writeObject(original);
			dos.flush();
			ByteArrayInputStream bin = new ByteArrayInputStream(
					bos.toByteArray());
			dis = new ObjectInputStream(bin);
			return dis.readObject();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				dos.close();
			} catch (Exception e) {
			}
			try {
				dis.close();
			} catch (Exception e) {
			}
		}
		return null;
	}

	/**
	 * Compare arrays lexicographically, with element 0 having the most influence.
	 *
	 * @param sig1
	 * 		first array
	 * @param sig2
	 * 		second array
	 * @return 1 if first is bigger, -1 if second, 0 otherwise
	 */
	static int arrayCompare(byte[] sig1, byte[] sig2) {
		if (sig1 == null && sig2 != null) {
			return -1;
		}
		if (sig1 != null && sig2 == null) {
			return 1;
		}
		for (int i = 0; i < Math.min(sig1.length, sig2.length); i++) {
			if (sig1[i] < sig2[i]) {
				return -1;
			}
			if (sig1[i] > sig2[i]) {
				return 1;
			}
		}
		if (sig1.length < sig2.length) {
			return -1;
		}
		if (sig1.length > sig2.length) {
			return 1;
		}
		return 0;
	}

	/**
	 * Compare arrays lexicographically, with element 0 having the most influence, as if each array was
	 * XORed with whitening before the comparison. The XOR doesn't actually happen, and the arrays are left
	 * unchanged.
	 *
	 * @param sig1
	 * 		first array
	 * @param sig2
	 * 		second array
	 * @param whitening
	 * 		the array virtually XORed with the other two
	 * @return 1 if first is bigger, -1 if second, 0 otherwise
	 */
	static int arrayCompare(byte[] sig1, byte[] sig2, byte[] whitening) {
		if (sig1 == null && sig2 != null) {
			return -1;
		}
		if (sig1 != null && sig2 == null) {
			return 1;
		}
		int len = Math.max(sig1.length, sig2.length);
		if (whitening.length < len) {
			whitening = Arrays.copyOf(whitening, len);
		}
		for (int i = 0; i < Math.min(sig1.length, sig2.length); i++) {
			if ((sig1[i] ^ whitening[i]) < (sig2[i] ^ whitening[i])) {
				return -1;
			}
			if ((sig1[i] ^ whitening[i]) > (sig2[i] ^ whitening[i])) {
				return 1;
			}
		}
		if (sig1.length < sig2.length) {
			return -1;
		}
		if (sig1.length > sig2.length) {
			return 1;
		}
		return 0;
	}

	/////////////////////////////////////////////////////////////
	// read from DataInputStream and
	// write to DataOutputStream


	/**
	 * read an Instant from the given stream
	 *
	 * @param stream
	 * 		the stream to read from
	 * @return the Instant that was read
	 * @throws IOException
	 * 		thrown if there are any problems during the operation
	 */
	public static Instant readInstant(FCDataInputStream stream)
			throws IOException {
		Instant time = Instant.ofEpochSecond(//
				stream.readLong(), // from getEpochSecond()
				stream.readLong()); // from getNano()
		return time;
	}

	/**
	 * write an int array to the given stream
	 *
	 * @param stream
	 * 		the stream to write to
	 * @param data
	 * 		the array to write
	 * @throws IOException
	 * 		thrown if there are any problems during the operation
	 */
	public static void writeIntArray(DataOutputStream stream, int[] data)
			throws IOException {
		int len = (data == null ? 0 : data.length);
		stream.writeInt(len);
		for (int i = 0; i < len; i++) {
			stream.writeInt(data[i]);
		}
	}

	/**
	 * read an int array from the given stream
	 *
	 * @param stream
	 * 		the stream to read from
	 * @return the array that was read
	 * @throws IOException
	 * 		thrown if there are any problems during the operation
	 */
	public static int[] readIntArray(DataInputStream stream)
			throws IOException {
		int len = stream.readInt();
		int[] data = new int[len];
		for (int i = 0; i < len; i++) {
			data[i] = stream.readInt();
		}
		return data;
	}

	/**
	 * write a long array to the given stream
	 *
	 * @param stream
	 * 		the stream to write to
	 * @param data
	 * 		the array to write
	 * @throws IOException
	 * 		thrown if there are any problems during the operation
	 */
	public static void writeLongArray(DataOutputStream stream, long[] data)
			throws IOException {
		int len = (data == null ? 0 : data.length);
		stream.writeInt(len);
		for (int i = 0; i < len; i++) {
			stream.writeLong(data[i]);
		}
	}

	/**
	 * read a long array from the given stream
	 *
	 * @param stream
	 * 		the stream to read from
	 * @return the array that was read
	 * @throws IOException
	 * 		thrown if there are any problems during the operation
	 */
	public static long[] readLongArray(DataInputStream stream)
			throws IOException {
		int len = stream.readInt();
		long[] data = new long[len];
		for (int i = 0; i < len; i++) {
			data[i] = stream.readLong();
		}
		return data;
	}


	/**
	 * read a 2D long array from the given stream
	 *
	 * @param stream
	 * 		the stream to read from
	 * @return the array that was read
	 * @throws IOException
	 * 		thrown if there are any problems during the operation
	 */
	public static long[][] readLongArray2D(FCDataInputStream stream)
			throws IOException {
		int len = stream.readInt();
		long[][] data = new long[len][];
		for (int i = 0; i < len; i++) {
			data[i] = readLongArray(stream);
		}
		return data;
	}

	/**
	 * read a String array from the given stream
	 *
	 * @param stream
	 * 		the stream to read from
	 * @return the array that was read
	 * @throws IOException
	 * 		thrown if there are any problems during the operation
	 */
	public static String[] readStringArray(FCDataInputStream stream)
			throws IOException {
		int len = stream.readInt();
		String[] data = new String[len];
		for (int i = 0; i < len; i++) {
			boolean notNull = stream.readBoolean();
			if (notNull) {
				data[i] = stream.readUTF();
			}
		}
		return data;
	}


	/**
	 * read a Color from the given stream
	 *
	 * @param stream
	 * 		the stream to read from
	 * @return the array that was read
	 * @throws IOException
	 * 		thrown if there are any problems during the operation
	 */
	public static Color readColor(FCDataInputStream stream) throws IOException {
		float r = stream.readFloat();
		float g = stream.readFloat();
		float b = stream.readFloat();
		float a = stream.readFloat();
		return new Color(r, g, b, a);
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
	 * @return the array that was read
	 * @throws IOException
	 * 		thrown if there are any problems during the operation
	 */
	@SuppressWarnings("unchecked")
	public static <T extends Record> List<T> readFastCopyableArray(
			FCDataInputStream stream,
			DeserializeFunction<T> deserializeFunction)
			throws IOException {
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
	 * read a Color array from the given stream
	 *
	 * @param stream
	 * 		the stream to read from
	 * @return the array that was read
	 * @throws IOException
	 * 		thrown if there are any problems during the operation
	 */
	public static Color[] readColorArray(FCDataInputStream stream)
			throws IOException {
		int len = stream.readInt();
		Color[] data = new Color[len];
		for (int i = 0; i < len; i++) {
			data[i] = readColor(stream);
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
	public static <T> List<T> readList(FCDataInputStream stream, Supplier<List<T>> listSupplier,
			Deserializer<T> deserializer) throws IOException {
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
	 * Convert the given long to bytes, big endian.
	 *
	 * @param n
	 * 		the long to convert
	 * @return a big-endian representation of n as an array of Long.BYTES bytes
	 */
	public static byte[] toBytes(long n) {
		byte[] bytes = new byte[Long.BYTES];
		toBytes(n, bytes, 0);
		return bytes;
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
	 * convert the given byte array to a long
	 *
	 * @param b
	 * 		the byte array to convert (at least 8 bytes)
	 * @return the long that was represented by the array
	 */
	public static long toLong(byte[] b) {
		return toLong(b, 0);
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
	 * Concatenate a long followed by multiple byte arrays (some of which could be null).
	 *
	 * @param n
	 * 		the long to concatenate
	 * @param arrays
	 * 		the byte arrays
	 * @return the resulting array
	 */
	static byte[] concat(long n, byte[]... arrays) {
		return concat(toBytes(n), concat(arrays));
	}

	/**
	 * Concatenate a single byte followed by multiple byte arrays (some of which could be null).
	 *
	 * @param b
	 * 		the byte to concatenate first
	 * @param arrays
	 * 		the byte arrays
	 * @return the resulting array
	 */
	static byte[] concat(byte b, byte[]... arrays) {
		return concat(new byte[] { b }, concat(arrays));
	}

	/**
	 * Concatenate a single byte followed by a long followed by multiple byte arrays (some of which could be
	 * null).
	 *
	 * @param b
	 * 		the byte to concatenate first
	 * @param n
	 * 		the long to concatenate second
	 * @param arrays
	 * 		the byte arrays
	 * @return the resulting array of bytes
	 */
	static byte[] concat(byte b, long n, byte[]... arrays) {
		return concat(b, concat(n, arrays));
	}

	/**
	 * Concatenate multiple byte arrays (some of which could be null).
	 *
	 * @param arrays
	 * 		the byte arrays
	 * @return the resulting array
	 */
	static byte[] concat(byte[]... arrays) {
		int len = 0;
		int pos = 0;
		for (int i = 0; i < arrays.length; i++) {
			len += arrays[i] == null ? 0 : arrays[i].length;
		}
		byte[] result = new byte[len];
		for (int i = 0; i < arrays.length; i++) {
			if (arrays[i] != null && arrays[i].length > 0) {
				System.arraycopy(arrays[i], 0, result, pos, arrays[i].length);
				pos += arrays[i].length;
			}
		}
		return result;
	}

	/**
	 * Convert an int to a byte array, little endian.
	 *
	 * @param i
	 * 		the int to convert
	 * @return the byte array
	 */
	static byte[] intToBytes(int i) {
		return new byte[] { (byte) i, (byte) (i >> 8), (byte) (i >> 16),
				(byte) (i) };
	}



	/**
	 * Insert line breaks into the given string so that each line is at most len characters long (not
	 * including trailing whitespace and the line break iteslf). Line breaks are only inserted after a
	 * whitespace character and before a non-whitespace character, resulting in some lines possibly being
	 * shorter. If a line has no whitespace, then it will insert between two non-whitespace characters to
	 * make it exactly len characters long.
	 *
	 * @param len
	 * 		the desired length
	 * @param str
	 * 		the input string, where a newline is always just \n (never \n\r or \r or \r\n)
	 */
	static String wrap(int len, String str) {
		StringBuilder ans = new StringBuilder();
		String[] lines = str.split("\n"); // break into lines
		for (String line : lines) { // we'll add \n to end of every line, then strip off the last
			if (line.length() == 0) {
				ans.append('\n');
			}
			char[] c = line.toCharArray();
			int i = 0;
			while (i < c.length) {  // repeatedly add a string starting at i, followed by a \n
				int j = i + len;
				if (j >= c.length) { // grab the rest of the characters
					j = c.length;
				} else if (Character.isWhitespace(c[j])) { // grab more than len characters
					while (j < c.length && Character.isWhitespace(c[j])) {
						j++;
					}
				} else {// grab len or fewer characters
					while (j >= i && !Character.isWhitespace(c[j])) {
						j--;
					}
					if (j < i) {// there is no whitespace before len
						j = i + len;
					} else {// the last whitespace before len is at j
						j++;
					}
				}
				ans.append(c, i, j - i);// append c[i...j-1]
				ans.append('\n');
				i = j; // continue, starting at j
			}
		}
		return ans.substring(0, ans.length() - 1);// remove the last '\n' that was added
	}

	//TODO: check whether it would be better to modify compare to be lexicographic comparison, which
	// means {1,2,3} < {4,5}.  Maybe just replace this compare with Java.Util.Arrays#compare. Why be different?


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
}

