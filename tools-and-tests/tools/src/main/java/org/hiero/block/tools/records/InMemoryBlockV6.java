// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.records;

import static org.hiero.block.tools.utils.Sha384.sha384Digest;

import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.hapi.node.base.Transaction;
import com.hedera.hapi.node.transaction.TransactionBody;
import com.hedera.hapi.streams.RecordStreamFile;
import com.hedera.hapi.streams.RecordStreamItem;
import com.hedera.hapi.streams.SignatureFile;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.stream.ReadableStreamingData;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.PublicKey;
import java.security.Signature;
import java.security.spec.X509EncodedKeySpec;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;
import org.hiero.block.tools.commands.days.model.AddressBookRegistry;

/**
 * In-memory representation and validator for version 6 Hedera record stream files.
 * <p>
 * Parses the protobuf-encoded RecordStreamFile after the version int, validates the provided start running hash
 * (when given), returns the end running hash, verifies that provided sidecar files match the metadata hashes
 * listed in the file, and performs signature validation using RSA public keys from the provided address book.
 */
@SuppressWarnings("DataFlowIssue")
public class InMemoryBlockV6 extends InMemoryBlock {

    /**
     * Creates a v6 in-memory block wrapper.
     *
     * @param recordFileTime the consensus time of the block
     * @param primaryRecordFile the primary record file for this block
     * @param otherRecordFiles additional record files (if any)
     * @param signatureFiles the set of signature files for the record file
     * @param primarySidecarFiles primary sidecar files produced for this block
     * @param otherSidecarFiles additional sidecar files produced for this block
     */
    public InMemoryBlockV6(
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
     * Validate a v6 record stream file.
     * <p>
     * Parses the RecordStreamFile protobuf, compares the provided startRunningHash (if present) with the
     * Start Object Running Hash in the file, collects the End Object Running Hash to return, validates that
     * the provided sidecar files match the hashes listed in the SidecarMetadata list, and performs signature
     * verification if an address book is provided.
     *
     * @param startRunningHash the expected start object running hash; may be null to skip comparison
     * @param addressBook the address book containing node RSA public keys; may be null to skip signature verification
     * @return validation result including the end running hash and HAPI semantic version
     */
    @Override
    public ValidationResult validate(byte[] startRunningHash, NodeAddressBook addressBook) {
        final byte[] recordFileBytes = primaryRecordFile().data();
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(recordFileBytes))) {
            boolean isValid = true;
            final StringBuilder warnings = new StringBuilder();

            final int fileVersion = in.readInt();
            if (fileVersion != 6) {
                throw new IllegalStateException("Invalid v6 record file version: " + fileVersion);
            }

            // Parse protobuf portion
            final RecordStreamFile rsf = RecordStreamFile.PROTOBUF.parse(new ReadableStreamingData(in));
            final SemanticVersion hapiVersion = rsf.hapiProtoVersion();

            // Compute the entire file hash for signature validation
            final MessageDigest sha384 = sha384Digest();
            final byte[] entireFileHash = sha384.digest(recordFileBytes);

            // Compare start running hash
            final byte[] startHashInFile = rsf.startObjectRunningHash().hash().toByteArray();
            if (startRunningHash != null
                    && startRunningHash.length > 0
                    && !java.util.Arrays.equals(startRunningHash, startHashInFile)) {
                warnings.append("Start running hash does not match provided start hash (v6).\n");
                isValid = false;
            }

            // End running hash from file
            final byte[] endRunningHash = rsf.endObjectRunningHash().hash().toByteArray();

            // Validate sidecar hashes: compute SHA-384 of provided sidecar files and compare sets
            final List<InMemoryFile> allSidecars = new ArrayList<>();
            allSidecars.addAll(primarySidecarFiles());
            allSidecars.addAll(otherSidecarFiles());

            final Set<String> providedSidecarHashes = new HashSet<>();
            sha384.reset();
            for (InMemoryFile sc : allSidecars) {
                sha384.reset();
                final byte[] hash = sha384.digest(sc.data());
                providedSidecarHashes.add(HexFormat.of().formatHex(hash));
            }

            final Set<String> expectedSidecarHashes = new HashSet<>();
            rsf.sidecars()
                    .forEach(meta -> expectedSidecarHashes.add(
                            HexFormat.of().formatHex(meta.hash().hash().toByteArray())));

            if (!expectedSidecarHashes.equals(providedSidecarHashes)) {
                warnings.append("Sidecar hashes do not match metadata (v6). Expected ")
                        .append(expectedSidecarHashes.size())
                        .append(", provided ")
                        .append(providedSidecarHashes.size())
                        .append('\n');
                isValid = false;
            }

            // Validate signatures
            isValid = isValid && validateSignatures(addressBook, warnings, entireFileHash);

            // get all transactions in the record file
            final List<Transaction> transactions = rsf.recordStreamItems().stream()
                    .filter(RecordStreamItem::hasTransaction)
                    .map(RecordStreamItem::transaction)
                    .toList();

            // feed the transactions to the address book registry to extract any address book transactions
            final List<TransactionBody> addressBookTransactions =
                    AddressBookRegistry.filterToJustAddressBookTransactions(transactions);
            return new ValidationResult(
                    isValid, warnings.toString(), endRunningHash, hapiVersion, addressBookTransactions);
        } catch (IOException | ParseException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Validate all signature files against the computed entire file hash using RSA public keys from the address book.
     * Make sure we have 1/3 of all nodes in address book have signed with valid signatures.
     *
     * @param addressBook     the address book containing node RSA public keys; may be null to skip signature
     *                        verification
     * @param warningMessages a StringBuilder to append any warning messages to
     * @param entireFileHash  the computed 48-byte SHA-384 hash of the entire record file
     * @return the updated validity state after checking all signatures
     * @throws IOException if an I/O error occurs reading a signature file
     */
    private boolean validateSignatures(
            NodeAddressBook addressBook, StringBuilder warningMessages, byte[] entireFileHash) throws IOException {
        if (addressBook != null && !signatureFiles().isEmpty()) {
            int validSignatureCount = 0;
            for (InMemoryFile sigFile : signatureFiles()) {
                try (DataInputStream sin = new DataInputStream(new ByteArrayInputStream(sigFile.data()))) {
                    final int firstByte = sin.read();
                    if (firstByte != 6) {
                        warningMessages
                                .append("Unexpected signature file first byte (expected 6) in ")
                                .append(sigFile.path())
                                .append("\n");
                        continue;
                    }
                    // Parse protobuf portion
                    final SignatureFile signatureFile = SignatureFile.PROTOBUF.parse(new ReadableStreamingData(sin));
                    if (signatureFile.fileSignature() == null) {
                        warningMessages
                                .append("Invalid signature file, missing file signature in ")
                                .append(sigFile.path())
                                .append("\n");
                        continue;
                    }
                    final byte[] fileHashFromSig =
                            signatureFile.fileSignature().hashObject().hash().toByteArray();
                    if (!Arrays.equals(fileHashFromSig, entireFileHash)) {
                        warningMessages
                                .append("Signature file hash does not match computed entire file hash for ")
                                .append(sigFile.path())
                                .append("\n");
                        continue;
                    }
                    final byte[] signatureBytes =
                            signatureFile.fileSignature().signature().toByteArray();

                    // Extract node account num
                    final Long accountNum = extractNodeAccountNumFromSignaturePath(sigFile.path());
                    if (accountNum == null) {
                        warningMessages
                                .append("Unable to extract node account number from signature filename: ")
                                .append(sigFile.path())
                                .append("\n");
                        continue;
                    }
                    // Get public key
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
                        continue;
                    }
                    if (rsaPubKey == null || rsaPubKey.isEmpty()) {
                        warningMessages
                                .append("Empty RSA public key for 0.0.")
                                .append(accountNum)
                                .append("; file ")
                                .append(sigFile.path())
                                .append("\n");
                        continue;
                    }

                    final boolean verified = verifyRsaSha384(rsaPubKey, fileHashFromSig, signatureBytes);
                    if (!verified) {
                        warningMessages
                                .append("RSA signature verification failed for node account 0.0.")
                                .append(accountNum)
                                .append(" (file ")
                                .append(sigFile.path())
                                .append(")\n");
                        continue;
                    }
                    validSignatureCount++;
                } catch (Exception e) {
                    warningMessages
                            .append("Error processing signature file ")
                            .append(sigFile.path())
                            .append(": ")
                            .append(e.getMessage())
                            .append("\n");
                }
            }
            final int totalNodeCount = addressBook.nodeAddress().size();
            final int requiredSignatures = (totalNodeCount / 3) + 1;
            if (validSignatureCount < requiredSignatures) {
                warningMessages
                        .append("Insufficient valid signatures: ")
                        .append(validSignatureCount)
                        .append(" of ")
                        .append(totalNodeCount)
                        .append(" nodes; required ")
                        .append(requiredSignatures)
                        .append("\n");
                return false;
            }
            return true;
        }
        return false;
    }

    /**
     * Extract the node account number from a signature file path.
     * Expected filename format: node_0.0.X.rcd_sig
     *
     * @param path the signature file path
     * @return the parsed node account number, or null if it cannot be determined
     */
    private static Long extractNodeAccountNumFromSignaturePath(Path path) {
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
     * Verify a v6 signature using an RSA public key over the 48-byte file hash.
     *
     * @param rsaPubKeyHexDer RSA public key in hex-encoded DER (or certificate DER) as stored in the address book
     * @param data48ByteHash the 48-byte SHA-384 file hash parsed from the signature file
     * @param signatureBytes the signature bytes to verify
     * @return true if the signature verifies; false otherwise
     */
    private static boolean verifyRsaSha384(String rsaPubKeyHexDer, byte[] data48ByteHash, byte[] signatureBytes) {
        try {
            final PublicKey pubKey = decodePublicKey(rsaPubKeyHexDer);
            return verifyWithInput(pubKey, data48ByteHash, signatureBytes);
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
    private static boolean verifyWithInput(PublicKey pubKey, byte[] data, byte[] signatureBytes) throws Exception {
        final Signature sig = Signature.getInstance("SHA384withRSA");
        sig.initVerify(pubKey);
        sig.update(data);
        return sig.verify(signatureBytes);
    }
}
