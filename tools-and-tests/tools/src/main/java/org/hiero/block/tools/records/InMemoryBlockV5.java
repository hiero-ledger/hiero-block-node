// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.records;

import static org.hiero.block.tools.records.SerializationV5Utils.HASH_OBJECT_SIZE_BYTES;
import static org.hiero.block.tools.records.SerializationV5Utils.readV5HashObject;
import static org.hiero.block.tools.utils.Sha384.SHA_384_HASH_SIZE;
import static org.hiero.block.tools.utils.Sha384.hashSha384;

import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.hapi.node.base.Transaction;
import com.hedera.hapi.node.transaction.TransactionBody;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.BufferedData;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.Signature;
import java.security.spec.X509EncodedKeySpec;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import org.hiero.block.tools.commands.days.model.AddressBookRegistry;

/**
 * In-memory representation and validator for version 5 Hedera record stream files.
 * <p>
 * This implementation parses the v5 header, start object running hash, and end object running hash
 * as specified in RecordFileFormat.md. Validation ensures the provided start running hash (when
 * supplied) matches the hash in the file and returns the end-running hash contained in the file.
 * Signature file validation is performed using RSA public keys from the provided address book.
 */
@SuppressWarnings("DuplicatedCode")
public class InMemoryBlockV5 extends InMemoryBlock {
    private static final long RECORD_STREAM_OBJECT_CLASS_ID = Long.parseUnsignedLong("e370929ba5429d8b", 16);
    public static final int RECORD_STREAM_OBJECT_CLASS_VERSION = 1;

    /**
     * Creates a v5 in-memory block wrapper.
     *
     * @param recordFileTime the consensus time of the block
     * @param primaryRecordFile the primary record file for this block
     * @param otherRecordFiles additional record files (if any)
     * @param signatureFiles the set of signature files for the record file
     * @param primarySidecarFiles primary sidecar files (not used by v5)
     * @param otherSidecarFiles additional sidecar files (not used by v5)
     */
    public InMemoryBlockV5(
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
     * Validate a v5 record stream file.
     * <p>
     * Parses the v5 header, HAPI semantic version, object stream version, and the Hash Objects containing the
     * start and end running hashes. If a startRunningHash is provided, it is compared to the hash read from the file.
     * The returned ValidationResult contains the end running hash and parsed HAPI version. Signature verification is
     * performed if an address book is provided.
     *
     * @param startRunningHash expected start running hash to compare against the Start Object Running Hash; may be null
     * @param addressBook the address book containing node RSA public keys; may be null to skip signature verification
     * @return validation result including computed validity and end running hash read from file
     */
    @Override
    public ValidationResult validate(final byte[] startRunningHash, final NodeAddressBook addressBook) {
        // Parse the v5 record file per RecordFileFormat.md and verify hashes
        final byte[] recordFileBytes = primaryRecordFile().data();
        try {
            final BufferedData in = BufferedData.wrap(recordFileBytes);
            boolean isValid = true;
            final StringBuilder warnings = new StringBuilder();
            // compute the entire file hash
            final byte[] entireFileHash = hashSha384(recordFileBytes);
            // Version already read by factory, but the file begins with version int (5)
            final int fileVersion = in.readInt();
            if (fileVersion != 5) {
                throw new IllegalStateException("Invalid v5 record file version: " + fileVersion);
            }
            // read HAPI semantic version
            final int hapiMajor = in.readInt();
            final int hapiMinor = in.readInt();
            final int hapiPatch = in.readInt();
            final SemanticVersion hapiVersion = new SemanticVersion(hapiMajor, hapiMinor, hapiPatch, null, null);
            // read object stream version
            final int objectStreamVersion = in.readInt();
            if (objectStreamVersion != RECORD_STREAM_OBJECT_CLASS_VERSION) {
                warnings.append("Unexpected object stream version (v5): ")
                        .append(objectStreamVersion)
                        .append('\n');
                isValid = false; // treat as invalid format
            }
            // read start running hash is a Hash Object in v5 format; parse to extract SHA-384 bytes
            final byte[] startHashInFile = readV5HashObject(in);
            if (startRunningHash != null
                    && (startRunningHash.length != SHA_384_HASH_SIZE
                            || startHashInFile.length != SHA_384_HASH_SIZE
                            || !java.util.Arrays.equals(startRunningHash, startHashInFile))) {
                warnings.append("Start running hash does not match provided start hash (v5).\n");
                isValid = false;
            }
            // read all transactions in the file
            final List<Transaction> transactions = new java.util.ArrayList<>();
            while (in.remaining() > HASH_OBJECT_SIZE_BYTES) {
                // read a RecordStreamObject
                final long classId = in.readLong();
                if (classId != RECORD_STREAM_OBJECT_CLASS_ID) {
                    warnings.append("Unexpected class ID in record file: ")
                            .append(Long.toHexString(classId))
                            .append(" expected ")
                            .append(Long.toHexString(RECORD_STREAM_OBJECT_CLASS_ID))
                            .append("\n");
                    isValid = false;
                    break; // cannot continue parsing
                }
                final int classVersion = in.readInt();
                if (classVersion != RECORD_STREAM_OBJECT_CLASS_VERSION) { // expecting transaction object
                    warnings.append("Unexpected class version in record file: ")
                            .append(classVersion)
                            .append(" expected ")
                            .append(RECORD_STREAM_OBJECT_CLASS_VERSION)
                            .append("\n");
                    isValid = false;
                    break; // cannot continue parsing
                }
                final int transactionRecordLength = in.readInt();
                if (transactionRecordLength <= 0 || transactionRecordLength > in.remaining() - HASH_OBJECT_SIZE_BYTES) {
                    warnings.append("Invalid transaction record length in record file: ")
                            .append(transactionRecordLength)
                            .append('\n');
                    isValid = false;
                    break; // cannot continue parsing
                }
                // skip over the transaction record bytes; not needed for this validation
                in.skip(transactionRecordLength);
                // read the transaction bytes length and the transaction bytes
                final int transactionLength = in.readInt();
                if (transactionLength <= 0 || transactionLength > in.remaining() - HASH_OBJECT_SIZE_BYTES) {
                    warnings.append("Invalid transaction length in record file: ")
                            .append(transactionLength)
                            .append('\n');
                    isValid = false;
                    break; // cannot continue parsing
                }
                final Transaction transaction = Transaction.PROTOBUF.parse(in.readBytes(transactionLength));
                transactions.add(transaction);
            }
            if (in.remaining() != HASH_OBJECT_SIZE_BYTES) {
                warnings.append("Expected ")
                        .append(HASH_OBJECT_SIZE_BYTES)
                        .append(" bytes remaining for end running hash, but found ")
                        .append(in.remaining())
                        .append('\n');
                return new ValidationResult(false, warnings.toString(), null, hapiVersion, Collections.emptyList());
            }
            // read the end running hash
            final byte[] endRunningHash = readV5HashObject(in);
            // Validate signatures
            isValid = isValid && validateSignatures(addressBook, warnings, entireFileHash);
            // feed the transactions to the address book registry to extract any address book transactions
            final List<TransactionBody> addressBookTransactions =
                    AddressBookRegistry.filterToJustAddressBookTransactions(transactions);
            // return the validation result
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
                    if (firstByte != 5) {
                        warningMessages
                                .append("Unexpected signature file first byte (expected 5) in ")
                                .append(sigFile.path())
                                .append("\n");
                        continue;
                    }
                    final int objStreamVer = sin.readInt();
                    if (objStreamVer != RECORD_STREAM_OBJECT_CLASS_VERSION) {
                        warningMessages
                                .append("Unexpected object stream version (expected 1) in ")
                                .append(sigFile.path())
                                .append("\n");
                        continue;
                    }
                    final byte[] fileHashFromSig = readV5HashObject(sin);
                    if (!Arrays.equals(fileHashFromSig, entireFileHash)) {
                        warningMessages
                                .append("Signature file hash does not match computed entire file hash for ")
                                .append(sigFile.path())
                                .append("\n");
                        continue;
                    }
                    // read signature object
                    final long sigClassId = sin.readLong();
                    if (sigClassId != 0x13dc4b399b245c69L) {
                        warningMessages
                                .append("Invalid signature object class ID in ")
                                .append(sigFile.path())
                                .append("\n");
                        continue;
                    }
                    final int sigClassVer = sin.readInt();
                    if (sigClassVer != RECORD_STREAM_OBJECT_CLASS_VERSION) {
                        warningMessages
                                .append("Invalid signature object class version in ")
                                .append(sigFile.path())
                                .append("\n");
                        continue;
                    }
                    final int sigType = sin.readInt();
                    if (sigType != RECORD_STREAM_OBJECT_CLASS_VERSION) {
                        warningMessages
                                .append("Invalid signature type in ")
                                .append(sigFile.path())
                                .append("\n");
                        continue;
                    }
                    final int sigLen = sin.readInt();
                    final int checksum = sin.readInt();
                    if (checksum != 101 - sigLen) {
                        warningMessages
                                .append("Invalid checksum in ")
                                .append(sigFile.path())
                                .append("\n");
                        continue;
                    }
                    final byte[] signatureBytes = new byte[sigLen];
                    sin.readFully(signatureBytes);

                    // extract node account num
                    final Long accountNum = extractNodeAccountNumFromSignaturePath(sigFile.path());
                    if (accountNum == null) {
                        warningMessages
                                .append("Unable to extract node account number from signature filename: ")
                                .append(sigFile.path())
                                .append("\n");
                        continue;
                    }
                    // get public key
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
            final int requiredSignatures = (totalNodeCount / 3) + RECORD_STREAM_OBJECT_CLASS_VERSION;
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
     * Verify a v5 signature using an RSA public key over the 48-byte file hash.
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
