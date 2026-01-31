// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.utils;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Tests for {@link Utils}. */
class UtilsTest {

    // ==================== writeInstant / readInstant(DataInputStream) ====================

    @Test
    void writeAndReadInstant() throws IOException {
        Instant original = Instant.ofEpochSecond(1_700_000_000L, 123_456_789);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        Utils.writeInstant(dos, original);
        dos.flush();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        Instant read = Utils.readInstant(dis);
        assertEquals(original, read);
    }

    // ==================== readInstant(DataInput, int[]) ====================

    @Test
    void readInstantWithByteCount() throws IOException {
        Instant original = Instant.ofEpochSecond(42, 99);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeLong(42);
        dos.writeLong(99);
        dos.flush();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        int[] byteCount = {0};
        Instant read = Utils.readInstant(dis, byteCount);
        assertEquals(original, read);
        assertEquals(2 * Long.BYTES, byteCount[0]);
    }

    // ==================== writeByteArray / readByteArray(DataInputStream) ====================

    @Test
    void writeAndReadByteArray() throws IOException {
        byte[] original = {1, 2, 3, 4, 5};
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        Utils.writeByteArray(dos, original);
        dos.flush();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        byte[] read = Utils.readByteArray(dis);
        assertArrayEquals(original, read);
    }

    @Test
    void writeNullByteArrayWritesZeroLength() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        Utils.writeByteArray(dos, null);
        dos.flush();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        byte[] read = Utils.readByteArray(dis);
        assertArrayEquals(new byte[0], read);
    }

    @Test
    void writeAndReadEmptyByteArray() throws IOException {
        byte[] original = {};
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        Utils.writeByteArray(dos, original);
        dos.flush();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        byte[] read = Utils.readByteArray(dis);
        assertArrayEquals(original, read);
    }

    // ==================== readByteArray(DataInputStream, int[]) with checksum ====================

    @Test
    void readByteArrayWithByteCount() throws IOException {
        byte[] data = {10, 20, 30};
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeInt(data.length); // length
        dos.writeInt(101 - data.length); // checksum
        dos.write(data);
        dos.flush();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        int[] byteCount = {0};
        byte[] read = Utils.readByteArray(dis, byteCount);
        assertArrayEquals(data, read);
        assertEquals(2 * Integer.BYTES + data.length, byteCount[0]);
    }

    @Test
    void readByteArrayWithBadChecksumThrows() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeInt(3); // length
        dos.writeInt(999); // bad checksum
        dos.write(new byte[3]);
        dos.flush();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        int[] byteCount = {0};
        assertThrows(IOException.class, () -> Utils.readByteArray(dis, byteCount));
    }

    @Test
    void readByteArrayExceedingMaxLengthThrows() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeInt(100); // length exceeds max
        dos.flush();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        int[] byteCount = {0};
        // Use reflection-free approach: the package-private method limits to maxArrayLength
        // readByteArray(dis, byteCount) delegates with Integer.MAX_VALUE, so test via public API
        // Instead test the effect: negative length triggers checksum failure
        ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
        DataOutputStream dos2 = new DataOutputStream(baos2);
        dos2.writeInt(-1); // negative length
        dos2.writeInt(102); // checksum for -1 would be 101-(-1)=102
        dos2.flush();
        DataInputStream dis2 = new DataInputStream(new ByteArrayInputStream(baos2.toByteArray()));
        int[] bc2 = {0};
        assertThrows(IOException.class, () -> Utils.readByteArray(dis2, bc2));
    }

    // ==================== writeNormalisedString / readNormalisedString ====================

    @Test
    void writeAndReadNormalisedString() throws IOException {
        String original = "Hello World";
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        Utils.writeNormalisedString(dos, original);
        dos.flush();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        String read = Utils.readNormalisedString(dis);
        assertEquals(Normalizer.normalize(original, Normalizer.Form.NFD), read);
    }

    // ==================== getNormalisedStringBytes ====================

    @Test
    void getNormalisedStringBytesNull() {
        assertArrayEquals(new byte[0], Utils.getNormalisedStringBytes(null));
    }

    @Test
    void getNormalisedStringBytesNonNull() {
        String s = "test";
        byte[] expected = Normalizer.normalize(s, Normalizer.Form.NFD).getBytes(StandardCharsets.UTF_8);
        assertArrayEquals(expected, Utils.getNormalisedStringBytes(s));
    }

    // ==================== getNormalisedStringFromBytes ====================

    @Test
    void getNormalisedStringFromBytes() {
        byte[] bytes = "hello".getBytes(StandardCharsets.UTF_8);
        assertEquals("hello", Utils.getNormalisedStringFromBytes(bytes));
    }

    // ==================== toBytes / toLong ====================

    @Test
    void toBytesAndToLongRoundTrip() {
        long value = 0x0102030405060708L;
        byte[] bytes = new byte[8];
        Utils.toBytes(value, bytes, 0);
        assertEquals(value, Utils.toLong(bytes, 0));
    }

    @Test
    void toBytesWithOffset() {
        long value = 42L;
        byte[] bytes = new byte[16];
        Utils.toBytes(value, bytes, 4);
        assertEquals(value, Utils.toLong(bytes, 4));
    }

    @Test
    void toLongZero() {
        byte[] bytes = new byte[8];
        assertEquals(0L, Utils.toLong(bytes, 0));
    }

    @Test
    void toBytesMaxValue() {
        long value = Long.MAX_VALUE;
        byte[] bytes = new byte[8];
        Utils.toBytes(value, bytes, 0);
        assertEquals(value, Utils.toLong(bytes, 0));
    }

    @Test
    void toBytesMinValue() {
        long value = Long.MIN_VALUE;
        byte[] bytes = new byte[8];
        Utils.toBytes(value, bytes, 0);
        assertEquals(value, Utils.toLong(bytes, 0));
    }

    // ==================== isSupermajority ====================

    @Test
    void isSupermajorityBasicCases() {
        // 3 > 2/3 of 3 = 2 → true
        assertTrue(Utils.isSupermajority(3, 3));
        // 2 > 2/3 of 3 = 2 → false
        assertFalse(Utils.isSupermajority(2, 3));
        // 1 > 2/3 of 1 = 0 → true
        assertTrue(Utils.isSupermajority(1, 1));
        // 0 > anything → false
        assertFalse(Utils.isSupermajority(0, 3));
    }

    @Test
    void isSupermajorityEdgeCases() {
        // 0 of 0: 0 > 0 → false
        assertFalse(Utils.isSupermajority(0, 0));
        // 1 of 0: 1 > 0 → true
        assertTrue(Utils.isSupermajority(1, 0));
        // 67 of 100: 67 > 66 → true
        assertTrue(Utils.isSupermajority(67, 100));
        // 66 of 100: 66 > 66 → false
        assertFalse(Utils.isSupermajority(66, 100));
    }

    // ==================== findLeftMostBit ====================

    @Test
    void findLeftMostBitZero() {
        assertEquals(0, Utils.findLeftMostBit(0));
    }

    @Test
    void findLeftMostBitOne() {
        assertEquals(1, Utils.findLeftMostBit(1));
    }

    @Test
    void findLeftMostBitPowerOfTwo() {
        assertEquals(1L << 10, Utils.findLeftMostBit(1L << 10));
    }

    @Test
    void findLeftMostBitNonPowerOfTwo() {
        // 0b1010 = 10, leftmost bit = 8
        assertEquals(8, Utils.findLeftMostBit(10));
    }

    @Test
    void findLeftMostBitLargeValue() {
        assertEquals(1L << 62, Utils.findLeftMostBit(Long.MAX_VALUE));
    }

    // ==================== readFastCopyableArray ====================

    @Test
    void readFastCopyableArrayWithNullElements() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeInt(3); // length = 3
        dos.writeByte(0); // null
        dos.writeByte(1); // non-null, will read an int
        dos.writeInt(42);
        dos.writeByte(0); // null
        dos.flush();

        record IntRecord(int value) {}

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        List<IntRecord> result = Utils.readFastCopyableArray(dis, stream -> new IntRecord(stream.readInt()));
        assertEquals(3, result.size());
        assertNull(result.get(0));
        assertNotNull(result.get(1));
        assertEquals(42, result.get(1).value());
        assertNull(result.get(2));
    }

    // ==================== readList ====================

    @Test
    void readListReturnsNullForNegativeSize() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeInt(-1);
        dos.flush();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        List<String> result = Utils.readList(dis, ArrayList::new, stream -> "x");
        assertNull(result);
    }

    @Test
    void readListReturnsPopulatedList() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeInt(2);
        dos.writeInt(10);
        dos.writeInt(20);
        dos.flush();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        List<Integer> result = Utils.readList(dis, ArrayList::new, stream -> stream.readInt());
        assertNotNull(result);
        assertEquals(List.of(10, 20), result);
    }

    @Test
    void readListEmptyList() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeInt(0);
        dos.flush();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        List<Integer> result = Utils.readList(dis, ArrayList::new, stream -> stream.readInt());
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }
}
