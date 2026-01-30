// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.utils;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.hedera.hapi.node.base.Key;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.PublicKey;
import java.util.List;
import org.hiero.block.tools.states.model.JKey;
import org.hiero.block.tools.states.model.JKey.JContractIDKey;
import org.hiero.block.tools.states.model.JKey.JECDSA_384Key;
import org.hiero.block.tools.states.model.JKey.JEd25519Key;
import org.hiero.block.tools.states.model.JKey.JKeyList;
import org.hiero.block.tools.states.model.JKey.JRSA_3072Key;
import org.hiero.block.tools.states.model.JKey.JThresholdKey;
import org.junit.jupiter.api.Test;

/** Tests for {@link CryptoUtils}. */
@SuppressWarnings("DataFlowIssue")
class CryptoUtilsTest {

    // ==================== getMessageDigest ====================

    @Test
    void getMessageDigestReturnsSha384() {
        MessageDigest md = CryptoUtils.getMessageDigest();
        assertNotNull(md);
        assertEquals("SHA-384", md.getAlgorithm());
    }

    // ==================== publicKeyToBytes / bytesToPublicKey ====================

    @Test
    void publicKeyRoundTrip() throws Exception {
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("EC");
        kpg.initialize(256);
        KeyPair kp = kpg.generateKeyPair();
        PublicKey original = kp.getPublic();

        byte[] bytes = CryptoUtils.publicKeyToBytes(original);
        PublicKey restored = CryptoUtils.bytesToPublicKey(bytes, CryptoUtils.AGR_TYPE);
        assertEquals(original, restored);
    }

    @Test
    void bytesToPublicKeyInvalidBytesThrows() {
        byte[] badBytes = {0x00, 0x01, 0x02};
        assertThrows(RuntimeException.class, () -> CryptoUtils.bytesToPublicKey(badBytes, CryptoUtils.AGR_TYPE));
    }

    @Test
    void bytesToPublicKeyBadAlgorithmThrows() throws Exception {
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("EC");
        kpg.initialize(256);
        byte[] bytes = CryptoUtils.publicKeyToBytes(kpg.generateKeyPair().getPublic());
        assertThrows(RuntimeException.class, () -> CryptoUtils.bytesToPublicKey(bytes, "NONEXISTENT"));
    }

    // ==================== convertKey ====================

    @Test
    void convertEd25519Key() {
        byte[] keyBytes = new byte[32];
        keyBytes[0] = 1;
        JEd25519Key jKey = new JEd25519Key(keyBytes);
        Key key = CryptoUtils.convertKey(jKey);
        assertNotNull(key.ed25519());
        assertArrayEquals(keyBytes, key.ed25519().toByteArray());
    }

    @Test
    void convertECDSA384Key() {
        byte[] keyBytes = new byte[48];
        keyBytes[0] = 2;
        JECDSA_384Key jKey = new JECDSA_384Key(keyBytes);
        Key key = CryptoUtils.convertKey(jKey);
        assertNotNull(key.ecdsa384());
        assertArrayEquals(keyBytes, key.ecdsa384().toByteArray());
    }

    @Test
    void convertRSA3072Key() {
        byte[] keyBytes = new byte[384];
        keyBytes[0] = 3;
        JRSA_3072Key jKey = new JRSA_3072Key(keyBytes);
        Key key = CryptoUtils.convertKey(jKey);
        assertNotNull(key.rsa3072());
        assertArrayEquals(keyBytes, key.rsa3072().toByteArray());
    }

    @Test
    void convertContractIDKey() {
        JContractIDKey jKey = new JContractIDKey(0, 0, 1234);
        Key key = CryptoUtils.convertKey(jKey);
        assertNotNull(key.contractID());
        assertEquals(0, key.contractID().shardNum());
        assertEquals(0, key.contractID().realmNum());
        assertEquals(1234L, key.contractID().contractNum());
    }

    @Test
    void convertKeyList() {
        JEd25519Key k1 = new JEd25519Key(new byte[32]);
        JEd25519Key k2 = new JEd25519Key(new byte[32]);
        JKeyList jKeyList = new JKeyList(List.of(k1, k2));
        Key key = CryptoUtils.convertKey(jKeyList);
        assertNotNull(key.keyList());
        assertEquals(2, key.keyList().keys().size());
    }

    @Test
    void convertThresholdKey() {
        JEd25519Key k1 = new JEd25519Key(new byte[32]);
        JEd25519Key k2 = new JEd25519Key(new byte[32]);
        JKeyList keyList = new JKeyList(List.of(k1, k2));
        JThresholdKey jKey = new JThresholdKey(keyList, 1);
        Key key = CryptoUtils.convertKey(jKey);
        assertNotNull(key.thresholdKey());
        assertEquals(1, key.thresholdKey().threshold());
        assertEquals(2, key.thresholdKey().keys().keys().size());
    }

    @Test
    void convertUnsupportedKeyTypeThrows() {
        // A bare JKey (not a recognized subclass) should throw
        JKey unsupported = new JKey();
        assertThrows(IllegalArgumentException.class, () -> CryptoUtils.convertKey(unsupported));
    }

    // ==================== convertKeyList (package-private) ====================

    @Test
    void convertKeyListDirect() {
        JEd25519Key k1 = new JEd25519Key(new byte[32]);
        JKeyList jKeyList = new JKeyList(List.of(k1));
        var keyList = CryptoUtils.convertKeyList(jKeyList);
        assertNotNull(keyList);
        assertEquals(1, keyList.keys().size());
    }

    @Test
    void convertNestedKeyList() {
        JEd25519Key inner = new JEd25519Key(new byte[32]);
        JKeyList innerList = new JKeyList(List.of(inner));
        JKeyList outerList = new JKeyList(List.of(innerList));
        Key key = CryptoUtils.convertKey(outerList);
        assertNotNull(key.keyList());
        assertEquals(1, key.keyList().keys().size());
        // The inner element should itself have a keyList
        assertNotNull(key.keyList().keys().getFirst().keyList());
    }
}
