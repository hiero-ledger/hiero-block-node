// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PublicKey;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Unit tests for `RsaKeyDecoder.buildKeyMap`.
class RsaKeyDecoderTest {

    @Test
    @DisplayName("null or empty address book returns empty map")
    void nullAndEmptyBook_returnEmptyMap() {
        assertTrue(RsaKeyDecoder.buildKeyMap(null).isEmpty(), "null book must return empty map");
        assertTrue(
                RsaKeyDecoder.buildKeyMap(NodeAddressBook.DEFAULT).isEmpty(),
                "DEFAULT (empty) book must return empty map");
        assertTrue(
                RsaKeyDecoder.buildKeyMap(NodeAddressBook.newBuilder()
                                .nodeAddress(List.of())
                                .build())
                        .isEmpty(),
                "Explicitly empty nodeAddress list must return empty map");
    }

    @Test
    @DisplayName("valid hex-DER RSA key is decoded and added to the map")
    void validKey_decodedAndMapped() throws Exception {
        final KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(2048);
        final KeyPair kp = kpg.generateKeyPair();
        final String hexKey = HexFormat.of().formatHex(kp.getPublic().getEncoded());

        final NodeAddressBook book = NodeAddressBook.newBuilder()
                .nodeAddress(List.of(
                        NodeAddress.newBuilder().nodeId(7L).rsaPubKey(hexKey).build()))
                .build();

        final Map<Long, PublicKey> result = RsaKeyDecoder.buildKeyMap(book);

        assertEquals(1, result.size());
        assertNotNull(result.get(7L), "Node 7's PublicKey must be present");
    }

    @Test
    @DisplayName("blank rsaPubKey is skipped; malformed hex-DER is skipped; valid keys are retained")
    void blankAndMalformedKeys_skipped_validKeysRetained() throws Exception {
        final KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(2048);
        final KeyPair kp = kpg.generateKeyPair();
        final String validHex = HexFormat.of().formatHex(kp.getPublic().getEncoded());

        final NodeAddressBook book = NodeAddressBook.newBuilder()
                .nodeAddress(List.of(
                        NodeAddress.newBuilder().nodeId(0L).rsaPubKey("").build(), // blank — skipped
                        NodeAddress.newBuilder()
                                .nodeId(1L)
                                .rsaPubKey("deadbeef")
                                .build(), // malformed DER — skipped
                        NodeAddress.newBuilder().nodeId(2L).rsaPubKey(validHex).build() // valid
                        ))
                .build();

        final Map<Long, PublicKey> result = RsaKeyDecoder.buildKeyMap(book);

        assertEquals(1, result.size(), "Only the valid key must appear in the map");
        assertNotNull(result.get(2L), "Node 2's valid key must be decoded");
    }
}
