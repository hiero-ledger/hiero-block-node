// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Tests for {@link JKey} and its subtypes. */
class JKeyTest {

    // ==================== JEd25519Key ====================

    @Test
    void ed25519KeySerializeRoundTrip() throws IOException {
        byte[] keyBytes = new byte[32];
        keyBytes[0] = 1;
        keyBytes[31] = 2;
        JKey.JEd25519Key original = new JKey.JEd25519Key(keyBytes);
        byte[] serialized = original.serialize();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(serialized));
        JKey read = JKey.copyFrom(dis);
        assertInstanceOf(JKey.JEd25519Key.class, read);
        assertArrayEquals(keyBytes, read.getEd25519());
    }

    @Test
    void ed25519KeyHasContractIDFalse() {
        assertFalse(new JKey.JEd25519Key(new byte[32]).hasContractID());
    }

    // ==================== JECDSA_384Key ====================

    @Test
    void ecdsa384KeySerializeRoundTrip() throws IOException {
        byte[] keyBytes = new byte[48];
        keyBytes[0] = 3;
        JKey.JECDSA_384Key original = new JKey.JECDSA_384Key(keyBytes);
        byte[] serialized = original.serialize();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(serialized));
        JKey read = JKey.copyFrom(dis);
        assertInstanceOf(JKey.JECDSA_384Key.class, read);
        assertArrayEquals(keyBytes, read.getECDSA384());
    }

    // ==================== JRSA_3072Key ====================

    @Test
    void rsa3072KeySerializeRoundTrip() throws IOException {
        byte[] keyBytes = new byte[384];
        keyBytes[0] = 5;
        JKey.JRSA_3072Key original = new JKey.JRSA_3072Key(keyBytes);
        byte[] serialized = original.serialize();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(serialized));
        JKey read = JKey.copyFrom(dis);
        assertInstanceOf(JKey.JRSA_3072Key.class, read);
        assertArrayEquals(keyBytes, read.getRSA3072());
    }

    // ==================== JContractIDKey ====================

    @Test
    void contractIdKeySerializeRoundTrip() throws IOException {
        JKey.JContractIDKey original = new JKey.JContractIDKey(0, 0, 1001);
        byte[] serialized = original.serialize();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(serialized));
        JKey read = JKey.copyFrom(dis);
        assertInstanceOf(JKey.JContractIDKey.class, read);
        assertTrue(read.hasContractID());
        JKey.JContractIDKey contractKey = (JKey.JContractIDKey) read;
        assertEquals(0, contractKey.getShardNum());
        assertEquals(0, contractKey.getRealmNum());
        assertEquals(1001, contractKey.getContractNum());
    }

    // ==================== JKeyList ====================

    @Test
    void keyListSerializeRoundTrip() throws IOException {
        JKey.JEd25519Key key1 = new JKey.JEd25519Key(new byte[32]);
        JKey.JEd25519Key key2 = new JKey.JEd25519Key(new byte[32]);
        JKey.JKeyList original = new JKey.JKeyList(List.of(key1, key2));
        byte[] serialized = original.serialize();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(serialized));
        JKey read = JKey.copyFrom(dis);
        assertInstanceOf(JKey.JKeyList.class, read);
        assertEquals(2, ((JKey.JKeyList) read).getKeysList().size());
    }

    // ==================== JThresholdKey ====================

    @Test
    void thresholdKeySerializeRoundTrip() throws IOException {
        JKey.JKeyList keyList = new JKey.JKeyList(List.of(
                new JKey.JEd25519Key(new byte[32]),
                new JKey.JEd25519Key(new byte[32]),
                new JKey.JEd25519Key(new byte[32])));
        JKey.JThresholdKey original = new JKey.JThresholdKey(keyList, 2);
        byte[] serialized = original.serialize();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(serialized));
        JKey read = JKey.copyFrom(dis);
        assertInstanceOf(JKey.JThresholdKey.class, read);
        JKey.JThresholdKey thresholdKey = (JKey.JThresholdKey) read;
        assertEquals(2, thresholdKey.getThreshold());
        assertEquals(3, thresholdKey.getKeys().getKeysList().size());
    }

    // ==================== Nested key lists ====================

    @Test
    void nestedKeyListSerializeRoundTrip() throws IOException {
        JKey.JKeyList inner = new JKey.JKeyList(List.of(new JKey.JEd25519Key(new byte[32])));
        JKey.JKeyList outer = new JKey.JKeyList(List.of(inner, new JKey.JEd25519Key(new byte[32])));
        byte[] serialized = outer.serialize();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(serialized));
        JKey read = JKey.copyFrom(dis);
        assertInstanceOf(JKey.JKeyList.class, read);
        JKey.JKeyList readList = (JKey.JKeyList) read;
        assertEquals(2, readList.getKeysList().size());
        assertInstanceOf(JKey.JKeyList.class, readList.getKeysList().getFirst());
    }
}
