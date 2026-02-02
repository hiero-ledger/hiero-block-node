// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import org.hiero.block.tools.states.utils.CryptoUtils;
import org.junit.jupiter.api.Test;

/** Tests for {@link Address}. */
class AddressTest {

    private static PublicKey generatePublicKey(String algorithm) {
        try {
            KeyPairGenerator gen = KeyPairGenerator.getInstance(algorithm);
            if ("RSA".equals(algorithm)) {
                gen.initialize(2048);
            } else {
                gen.initialize(256);
            }
            KeyPair pair = gen.generateKeyPair();
            return pair.getPublic();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    // ==================== formatIpv4 ====================

    @Test
    void formatIpv4ValidAddress() {
        byte[] addr = new byte[] {(byte) 192, (byte) 168, 1, 100};
        assertEquals("192.168.1.100", Address.formatIpv4(addr));
    }

    @Test
    void formatIpv4NullReturnsEmpty() {
        assertEquals("EMPTY", Address.formatIpv4(null));
    }

    @Test
    void formatIpv4WrongLengthReturnsEmpty() {
        assertEquals("EMPTY", Address.formatIpv4(new byte[] {1, 2}));
    }

    @Test
    void formatIpv4AllZeros() {
        assertEquals("0.0.0.0", Address.formatIpv4(new byte[] {0, 0, 0, 0}));
    }

    // ==================== writeAddress / readAddress roundtrip ====================

    @Test
    void writeAndReadAddressRoundTrip() throws IOException {
        PublicKey sigKey = generatePublicKey("RSA");
        PublicKey encKey = generatePublicKey("EC");
        PublicKey agrKey = generatePublicKey("EC");

        Address original = new Address(
                42L,
                "node0",
                "self0",
                1000L,
                false,
                new byte[] {10, 0, 0, 1},
                50211,
                new byte[] {10, 0, 0, 2},
                50212,
                null,
                0,
                null,
                0,
                sigKey,
                encKey,
                agrKey,
                "test memo");

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        original.writeAddress(dos);
        dos.flush();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        Address read = Address.readAddress(dis);

        assertEquals(original.id(), read.id());
        assertEquals(original.nickname(), read.nickname());
        assertEquals(original.selfName(), read.selfName());
        assertEquals(original.stake(), read.stake());
        assertEquals(original.portInternalIpv4(), read.portInternalIpv4());
        assertEquals(original.memo(), read.memo());
    }

    // ==================== updateHash ====================

    @Test
    void updateHashDoesNotThrow() {
        PublicKey sigKey = generatePublicKey("RSA");
        PublicKey encKey = generatePublicKey("EC");
        PublicKey agrKey = generatePublicKey("EC");

        Address addr = new Address(
                1L,
                "nick",
                "self",
                100L,
                false,
                new byte[] {1, 2, 3, 4},
                80,
                new byte[] {5, 6, 7, 8},
                443,
                null,
                0,
                null,
                0,
                sigKey,
                encKey,
                agrKey,
                "memo");
        MessageDigest md = CryptoUtils.getMessageDigest();
        addr.updateHash(md);
        byte[] hash = md.digest();
        assertNotNull(hash);
        assertEquals(48, hash.length);
    }

    // ==================== toString ====================

    @Test
    void toStringContainsFields() {
        Address addr = new Address(
                5L,
                "nick",
                "self",
                50L,
                true,
                new byte[] {10, 0, 0, 1},
                8080,
                null,
                0,
                null,
                0,
                null,
                0,
                null,
                null,
                null,
                "memo");
        String str = addr.toString();
        assertNotNull(str);
        assertTrue(str.contains("nick"));
        assertTrue(str.contains("10.0.0.1"));
    }
}
