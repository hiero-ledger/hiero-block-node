// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.utils;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.junit.jupiter.api.Test;

/** Tests for {@link HashingOutputStream}. */
class HashingOutputStreamTest {

    private MessageDigest newSha384() {
        try {
            return MessageDigest.getInstance("SHA-384");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void writeSingleByte() throws IOException, NoSuchAlgorithmException {
        MessageDigest md = newSha384();
        HashingOutputStream hos = new HashingOutputStream(md);
        hos.write(0x42);

        MessageDigest expected = MessageDigest.getInstance("SHA-384");
        expected.update((byte) 0x42);
        assertArrayEquals(expected.digest(), md.digest());
    }

    @Test
    void writeByteArrayWithOffsetAndLength() throws IOException {
        MessageDigest md = newSha384();
        HashingOutputStream hos = new HashingOutputStream(md);
        byte[] data = {0x01, 0x02, 0x03, 0x04, 0x05};
        hos.write(data, 1, 3);

        MessageDigest expected = newSha384();
        expected.update(data, 1, 3);
        assertArrayEquals(expected.digest(), md.digest());
    }

    @Test
    void writeZeroLengthDoesNothing() throws IOException {
        MessageDigest md = newSha384();
        HashingOutputStream hos = new HashingOutputStream(md);
        byte[] data = {0x01, 0x02};
        hos.write(data, 0, 0);

        // digest of empty input
        MessageDigest expected = newSha384();
        assertArrayEquals(expected.digest(), md.digest());
    }

    @Test
    void writeNullArrayThrowsNpe() {
        MessageDigest md = newSha384();
        HashingOutputStream hos = new HashingOutputStream(md);
        assertThrows(NullPointerException.class, () -> hos.write(null, 0, 1));
    }

    @Test
    void writeNegativeOffsetThrowsIndexOutOfBounds() {
        MessageDigest md = newSha384();
        HashingOutputStream hos = new HashingOutputStream(md);
        byte[] data = {0x01};
        assertThrows(IndexOutOfBoundsException.class, () -> hos.write(data, -1, 1));
    }

    @Test
    void writeOffsetBeyondLengthThrowsIndexOutOfBounds() {
        MessageDigest md = newSha384();
        HashingOutputStream hos = new HashingOutputStream(md);
        byte[] data = {0x01};
        assertThrows(IndexOutOfBoundsException.class, () -> hos.write(data, 2, 1));
    }

    @Test
    void writeNegativeLenThrowsIndexOutOfBounds() {
        MessageDigest md = newSha384();
        HashingOutputStream hos = new HashingOutputStream(md);
        byte[] data = {0x01};
        assertThrows(IndexOutOfBoundsException.class, () -> hos.write(data, 0, -1));
    }

    @Test
    void writeOffPlusLenOverflowThrowsIndexOutOfBounds() {
        MessageDigest md = newSha384();
        HashingOutputStream hos = new HashingOutputStream(md);
        byte[] data = {0x01, 0x02};
        assertThrows(IndexOutOfBoundsException.class, () -> hos.write(data, 1, 2));
    }

    @Test
    void writeMultipleCallsAccumulatesHash() throws IOException {
        MessageDigest md = newSha384();
        HashingOutputStream hos = new HashingOutputStream(md);
        hos.write(0xAA);
        byte[] chunk = {0x10, 0x20, 0x30};
        hos.write(chunk, 0, chunk.length);

        MessageDigest expected = newSha384();
        expected.update((byte) 0xAA);
        expected.update(chunk);
        assertArrayEquals(expected.digest(), md.digest());
    }
}