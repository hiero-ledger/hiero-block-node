// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.records;

import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.Signature;
import java.security.spec.X509EncodedKeySpec;
import java.util.HexFormat;

/**
 * Utility class for handling signature files and RSA signature verification.
 */
public class SigFileUtils {
    /**
     * Extract the node account number from a signature file path.
     * Expected filename format: node_0.0.X.rcd_sig
     *
     * @param path the signature file path
     * @return the parsed node account number, or null if it cannot be determined
     */
    public static int extractNodeAccountNumFromSignaturePath(Path path) {
        final String fileName = path.getFileName().toString();
        final String prefix = "node_0.0.";
        final int idx = fileName.indexOf(prefix);
        if (idx < 0) throw new RuntimeException("Invalid signature file name: " + fileName);
        int start = idx + prefix.length();
        int end = fileName.indexOf('.', start);
        if (end < 0) end = fileName.length();
        final String accountNumStr = fileName.substring(start, end);
        return Integer.parseInt(accountNumStr);
    }

    /**
     * Decode an RSA public key from a hex-encoded X.509 SubjectPublicKeyInfo DER string.
     * If the bytes represent a DER-encoded X.509 certificate, extract the public key from the certificate.
     *
     * @param rsaPubKeyHexDer hex-encoded DER of an RSA public key or X.509 certificate
     * @return the decoded RSA PublicKey
     * @throws Exception if the key cannot be decoded
     */
    public static PublicKey decodePublicKey(String rsaPubKeyHexDer) throws Exception {
        final byte[] keyBytes = HexFormat.of().parseHex(rsaPubKeyHexDer);
        try {
            final X509EncodedKeySpec spec = new X509EncodedKeySpec(keyBytes);
            return KeyFactory.getInstance("RSA").generatePublic(spec);
        } catch (Exception e) {
            // Try parsing as X.509 certificate
            try {
                final java.security.cert.CertificateFactory cf =
                        java.security.cert.CertificateFactory.getInstance("X.509");
                final java.security.cert.X509Certificate cert = (java.security.cert.X509Certificate)
                        cf.generateCertificate(new java.io.ByteArrayInputStream(keyBytes));
                return cert.getPublicKey();
            } catch (Exception ignored) {
                throw e;
            }
        }
    }

    /**
     * Verify an RSA signature using SHA384withRSA over the given data.
     *
     * @param pubKey the RSA public key
     * @param data the data that was signed
     * @param signatureBytes the signature bytes
     * @return true if the signature verifies; false otherwise
     * @throws Exception if the verification operation fails unexpectedly
     */
    static boolean verifyWithInput(PublicKey pubKey, byte[] data, byte[] signatureBytes) throws Exception {
        final Signature sig = Signature.getInstance("SHA384withRSA");
        sig.initVerify(pubKey);
        sig.update(data);
        return sig.verify(signatureBytes);
    }

    /**
     * Verify a signature using an RSA public key over record file hash computed depending on record file format
     * version.
     *
     * @param rsaPubKeyHexDer RSA public key in hex-encoded DER (or certificate DER) as stored in the address book
     * @param data48ByteHash the 48-byte SHA-384 file hash parsed from the signature file
     * @param signatureBytes the signature bytes to verify
     * @return true if the signature verifies under either input; false otherwise
     */
    public static boolean verifyRsaSha384(String rsaPubKeyHexDer, byte[] data48ByteHash, byte[] signatureBytes) {
        try {
            return verifyWithInput(decodePublicKey(rsaPubKeyHexDer), data48ByteHash, signatureBytes);
        } catch (Exception e) {
            return false;
        }
    }
}
