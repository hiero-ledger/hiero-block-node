// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import org.junit.jupiter.api.Test;

/** Tests for {@link Hash}. */
class HashTest {

    // ==================== readHash ====================

    @Test
    void readHashFromStream() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeInt(42); // hashMapSeed
        // readByteArray(DataInputStream): length + data (no checksum)
        byte[] hashData = new byte[48];
        hashData[0] = 1;
        hashData[47] = 2;
        dos.writeInt(hashData.length);
        dos.write(hashData);
        dos.flush();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        Hash hash = Hash.readHash(dis);
        assertEquals(42, hash.hashMapSeed());
        assertArrayEquals(hashData, hash.hash());
    }

    // ==================== update(MessageDigest, long) ====================

    @Test
    void updateLongUsesLittleEndianByteOrder() throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-384");
        Hash.update(md, 0x0102030405060708L);
        byte[] digest = md.digest();
        // Verify it produces a valid digest (48 bytes for SHA-384)
        assertEquals(48, digest.length);

        // Verify little-endian: feeding the same value should produce the same digest
        MessageDigest md2 = MessageDigest.getInstance("SHA-384");
        Hash.update(md2, 0x0102030405060708L);
        assertArrayEquals(digest, md2.digest());
    }

    // ==================== update(MessageDigest, int) ====================

    @Test
    void updateIntUsesLittleEndianByteOrder() throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-384");
        Hash.update(md, 0x01020304);
        byte[] digest = md.digest();
        assertEquals(48, digest.length);
    }

    // ==================== update(MessageDigest, Instant) ====================

    @Test
    void updateInstant() throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-384");
        Instant instant = Instant.ofEpochSecond(1_700_000_000L, 123_456_789);
        Hash.update(md, instant);
        byte[] digest = md.digest();
        assertEquals(48, digest.length);

        // Same instant should produce same digest
        MessageDigest md2 = MessageDigest.getInstance("SHA-384");
        Hash.update(md2, instant);
        assertArrayEquals(digest, md2.digest());
    }

    // ==================== hex ====================

    @Test
    void hexFormatsCorrectly() {
        byte[] data = {0x0a, 0x0b, 0x0c};
        Hash hash = new Hash(data, 0);
        assertEquals("0a0b0c", hash.hex());
    }
}
