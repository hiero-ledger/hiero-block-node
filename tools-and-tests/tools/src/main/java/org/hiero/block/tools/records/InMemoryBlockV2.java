// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.records;

import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.SemanticVersion;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.Signature;
import java.security.spec.X509EncodedKeySpec;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import org.hiero.block.tools.commands.days.model.AddressBookRegistry;

@SuppressWarnings("DuplicatedCode")
/**
 * In-memory representation and validator for version 2 Hedera record stream files.
 * <p>
 * Validation recomputes the v2 file hash per the legacy formula hash(header || hash(contents)) and verifies
 * signature files against the computed file hash using RSA public keys from the provided NodeAddressBook.
 * It also checks that the provided startRunningHash (when supplied) matches the previous-file hash in the header.
 */
public class InMemoryBlockV2 extends InMemoryBlock {
    /* The length of the header in a v2 record file */
    private static final int V2_HEADER_LENGTH = Integer.BYTES + Integer.BYTES + 1 + 48;

    /**
     * Creates a v2 in-memory block wrapper.
     *
     * @param recordFileTime the consensus time of the block (derived from the filename)
     * @param primaryRecordFile the primary record file for this block
     * @param otherRecordFiles additional record files (if any)
     * @param signatureFiles the set of signature files for the record file
     * @param primarySidecarFiles primary sidecar files (not used by v2)
     * @param otherSidecarFiles additional sidecar files (not used by v2)
     */
    protected InMemoryBlockV2(
            Instant recordFileTime,
            InMemoryFile primaryRecordFile,
            List<InMemoryFile> otherRecordFiles,
            List<InMemoryFile> signatureFiles,
            List<InMemoryFile> primarySidecarFiles,
            List<InMemoryFile> otherSidecarFiles) {
        super(
                recordFileTime,
                primaryRecordFile,
                otherRecordFiles,
                signatureFiles,
                primarySidecarFiles,
                otherSidecarFiles);
    }

    /**
     * Validate a v2 record stream file.
     * <p>
     * Performs the following checks:
     * - Confirms the on-disk version equals 2 and parses HAPI major version.
     * - Reads and compares the previous-file hash in the header against the provided startRunningHash, when supplied.
     * - Recomputes the v2 file hash as SHA-384(header || SHA-384(contents)) and returns it as the end-running hash.
     * - If an address book is provided, validates each signature file by:
     *   - Extracting the node account number from the signature filename (node_0.0.X.rcd_sig)
     *   - Looking up the node RSA public key via AddressBookRegistry.publicKeyForNode()
     *   - Verifying the signature over the 48-byte file hash (and falling back to entire file bytes if needed).
     *
     * @param startRunningHash the expected previous file hash (start running hash) for this block; may be null
     * @param addressBook the address book containing node RSA public keys; may be null to skip signature verification
     * @return a ValidationResult indicating validity, any warnings, computed end-running hash, and the HAPI version
     */
    @Override
    public ValidationResult validate(byte[] startRunningHash, NodeAddressBook addressBook) {
        final byte[] recordFileBytes = primaryRecordFile().data();
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(recordFileBytes))) {
            boolean isValid = true;
            final StringBuilder warningMessages = new StringBuilder();
            // Read and verify the record file version
            final int fileVersion = in.readInt();
            if (fileVersion != 2) {
                throw new IllegalStateException("Invalid v2 record file version: " + fileVersion);
            }
            final int hapiMajorVersion = in.readInt();
            final SemanticVersion hapiVersion = new SemanticVersion(hapiMajorVersion, 0, 0, null, null);
            final byte previousFileHashMarker = in.readByte();
            if (previousFileHashMarker != 1) {
                throw new IllegalStateException("Invalid previous file hash marker in v2 record file");
            }
            final byte[] previousHash = new byte[48];
            in.readFully(previousHash);
            // check the start running hash is the same as the previous hash
            if (startRunningHash != null
                    && startRunningHash.length > 0
                    && !Arrays.equals(startRunningHash, previousHash)) {
                isValid = false;
                warningMessages.append("Start running hash does not match previous hash in v2 record file\n");
            }
            // The hash for v2 files is the hash(header, hash(content)) this is different to other versions
            // the block hash is not available in the file so we have to calculate it
            MessageDigest digest = MessageDigest.getInstance("SHA-384");
            digest.update(recordFileBytes, V2_HEADER_LENGTH, recordFileBytes.length - V2_HEADER_LENGTH);
            final byte[] contentHash = digest.digest();
            digest.update(recordFileBytes, 0, V2_HEADER_LENGTH);
            digest.update(contentHash);
            final byte[] blockHash = digest.digest();

            // Validate all signature files if an address book is provided
            if (addressBook != null && !signatureFiles().isEmpty()) {
                for (InMemoryFile sigFile : signatureFiles()) {
                    try (DataInputStream sin = new DataInputStream(new ByteArrayInputStream(sigFile.data()))) {
                        final int firstByte = sin.read();
                        if (firstByte != 4) {
                            warningMessages
                                    .append("Unexpected signature file first byte (expected 4) in ")
                                    .append(sigFile.path())
                                    .append("\n");
                            isValid = false;
                            continue;
                        }
                        final byte[] fileHashFromSig = new byte[48];
                        sin.readFully(fileHashFromSig);
                        if (!Arrays.equals(fileHashFromSig, blockHash)) {
                            warningMessages
                                    .append("Signature file hash does not match computed block hash for ")
                                    .append(sigFile.path())
                                    .append("\n");
                            isValid = false;
                        }
                        if (sin.read() != 3) {
                            warningMessages
                                    .append("Invalid signature marker in ")
                                    .append(sigFile.path())
                                    .append("\n");
                            isValid = false;
                            continue;
                        }
                        final int sigLen = sin.readInt();
                        final byte[] signatureBytes = new byte[sigLen];
                        sin.readFully(signatureBytes);

                        // Extract node ID from filename and fetch RSA public key from address book
                        final Long accountNum = extractNodeAccountNumFromSignaturePath(sigFile.path());
                        if (accountNum == null) {
                            warningMessages
                                    .append("Unable to extract node account number from signature filename: ")
                                    .append(sigFile.path())
                                    .append("\n");
                            isValid = false;
                            continue;
                        }
                        // Look up RSA public key via AddressBookRegistry helper
                        String rsaPubKey;
                        try {
                            rsaPubKey = AddressBookRegistry.publicKeyForNode(addressBook, 0, 0, accountNum);
                        } catch (Exception e) {
                            warningMessages
                                    .append("No RSA public key found for 0.0.")
                                    .append(accountNum)
                                    .append(" in provided address book; file ")
                                    .append(sigFile.path())
                                    .append("\n");
                            isValid = false;
                            continue;
                        }
                        if (rsaPubKey == null || rsaPubKey.isEmpty()) {
                            warningMessages
                                    .append("Empty RSA public key for 0.0.")
                                    .append(accountNum)
                                    .append("; file ")
                                    .append(sigFile.path())
                                    .append("\n");
                            isValid = false;
                            continue;
                        }

                        final boolean verified =
                                verifyRsaSha384(rsaPubKey, fileHashFromSig, recordFileBytes, signatureBytes);
                        if (!verified) {
                            warningMessages
                                    .append("RSA signature verification failed for node account 0.0.")
                                    .append(accountNum)
                                    .append(" (file ")
                                    .append(sigFile.path())
                                    .append(")\n");
                            isValid = false;
                        }
                    }
                }
            }

            return new ValidationResult(
                    isValid, warningMessages.toString(), blockHash, hapiVersion, Collections.emptyList());
        } catch (IOException | NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Extract the node account number from a signature file path.
     * Expected filename format: node_0.0.X.rcd_sig
     *
     * @param path the signature file path
     * @return the parsed node account number, or null if it cannot be determined
     */
    private static Long extractNodeAccountNumFromSignaturePath(Path path) {
        // Expected filename format: node_0.0.X.rcd_sig (possibly with directories)
        final String fileName = path.getFileName().toString();
        final String prefix = "node_0.0.";
        final int idx = fileName.indexOf(prefix);
        if (idx < 0) return null;
        int start = idx + prefix.length();
        int end = fileName.indexOf('.', start);
        if (end < 0) end = fileName.length();
        final String accountNumStr = fileName.substring(start, end);
        try {
            return Long.parseLong(accountNumStr);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Verify a v2 signature using an RSA public key over either the 48-byte file hash or the entire file bytes.
     * Tries verifying over the 48-byte hash first (legacy behavior), and falls back to entire file bytes if needed.
     *
     * @param rsaPubKeyHexDer RSA public key in hex-encoded DER (or certificate DER) as stored in the address book
     * @param data48ByteHash the 48-byte SHA-384 file hash parsed from the signature file
     * @param entireFileBytes the entire record file bytes
     * @param signatureBytes the signature bytes to verify
     * @return true if the signature verifies under either input; false otherwise
     */
    private static boolean verifyRsaSha384(
            String rsaPubKeyHexDer, byte[] data48ByteHash, byte[] entireFileBytes, byte[] signatureBytes) {
        try {
            final PublicKey pubKey = decodePublicKey(rsaPubKeyHexDer);
            // Try verifying signature over the 48-byte hash (expected per legacy v2 format)
            if (verifyWithInput(pubKey, data48ByteHash, signatureBytes)) {
                return true;
            }
            // Fallback: try verifying over the entire file bytes (in case signature was produced over entire file)
            return verifyWithInput(pubKey, entireFileBytes, signatureBytes);
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Decode an RSA public key from a hex-encoded X.509 SubjectPublicKeyInfo DER string.
     * If the bytes represent a DER-encoded X.509 certificate, extract the public key from the certificate.
     *
     * @param rsaPubKeyHexDer hex-encoded DER of an RSA public key or X.509 certificate
     * @return the decoded RSA PublicKey
     * @throws Exception if the key cannot be decoded
     */
    private static PublicKey decodePublicKey(String rsaPubKeyHexDer) throws Exception {
        final byte[] keyBytes = HexFormat.of().parseHex(rsaPubKeyHexDer);
        try {
            final X509EncodedKeySpec spec = new X509EncodedKeySpec(keyBytes);
            return KeyFactory.getInstance("RSA").generatePublic(spec);
        } catch (Exception e) {
            // Try parsing as X.509 certificate if it's a cert DER
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
    private static boolean verifyWithInput(PublicKey pubKey, byte[] data, byte[] signatureBytes) throws Exception {
        final Signature sig = Signature.getInstance("SHA384withRSA");
        sig.initVerify(pubKey);
        sig.update(data);
        return sig.verify(signatureBytes);
    }
}
