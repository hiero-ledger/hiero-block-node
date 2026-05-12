// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification;

import static java.lang.System.Logger.Level.WARNING;

import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.Collections;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Map;

/**
 * Utility for decoding RSA public keys from a `NodeAddressBook` into a
 * `node_id → PublicKey` map used by the RSA WRB verification path.
 *
 * Keys are expected to be hex-encoded DER X.509 `SubjectPublicKeyInfo`,
 * **without** a `0x` prefix.
 *
 * This logic mirrors `SigFileUtils.decodePublicKey` in `tools-and-tests/tools`
 * but is inlined here because that module cannot be imported from
 * `block-node/verification`.
 *
 * TODO: reconcile with `SigFileUtils.decodePublicKey` and `AddressBookRegistry`
 * which store `rsaPubKey` as hex-encoded *certificate* DER (cert wrapping the
 * SPKI) instead of the bare SPKI bytes accepted here. Once accepting both forms,
 * the WRB fixture generator can drop its SPKI-extraction step and write the cert
 * DER verbatim.
 */
public final class RsaKeyDecoder {

    private static final System.Logger LOGGER = System.getLogger(RsaKeyDecoder.class.getName());

    private RsaKeyDecoder() {}

    /**
     * Builds an immutable `node_id → PublicKey` map from the given address book.
     *
     * Entries where `rsaPubKey()` is blank or whose key bytes cannot be decoded
     * as an RSA X.509 public key are silently skipped (with a WARN log). This
     * matches the fail-soft behaviour required by the verification path: one bad
     * key must not prevent the other nodes from being counted.
     *
     * @param book the address book loaded by `RsaRosterBootstrapPlugin`
     * @return an unmodifiable map from node ID to `PublicKey`
     */
    public static Map<Long, PublicKey> buildKeyMap(final NodeAddressBook book) {
        if (book == null || book.nodeAddress().isEmpty()) {
            return Map.of();
        }
        final Map<Long, PublicKey> map = new HashMap<>();
        final HexFormat hex = HexFormat.of();
        // Obtain KeyFactory once — provider lookup is not cheap and RSA must always be available.
        final KeyFactory kf;
        try {
            kf = KeyFactory.getInstance("RSA");
        } catch (NoSuchAlgorithmException e) {
            // RSA must be available in every JVM — this is a JVM misconfiguration
            throw new IllegalStateException("RSA KeyFactory not available", e);
        }
        for (final NodeAddress addr : book.nodeAddress()) {
            if (addr.rsaPubKey().isBlank()) {
                continue;
            }
            try {
                final byte[] keyBytes = hex.parseHex(addr.rsaPubKey());
                final PublicKey key = kf.generatePublic(new X509EncodedKeySpec(keyBytes));
                map.put(addr.nodeId(), key);
            } catch (InvalidKeySpecException | IllegalArgumentException e) {
                LOGGER.log(WARNING, "Malformed RSA_PubKey for node {0} — skipped: {1}", addr.nodeId(), e.getMessage());
            }
        }
        return Collections.unmodifiableMap(map);
    }
}
