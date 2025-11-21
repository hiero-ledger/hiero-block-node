// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.records.model.parsed;

import static org.hiero.block.tools.days.model.AddressBookRegistry.nodeIdForNode;
import static org.hiero.block.tools.records.SigFileUtils.extractNodeAccountNumFromSignaturePath;
import static org.hiero.block.tools.records.SigFileUtils.verifyRsaSha384;
import static org.hiero.block.tools.records.model.parsed.SerializationV5Utils.readV5HashObject;
import static org.hiero.block.tools.utils.Sha384.SHA_384_HASH_SIZE;

import com.hedera.hapi.block.stream.experimental.RecordFileSignature;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.hedera.pbj.runtime.io.stream.ReadableStreamingData;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Objects;
import org.hiero.block.tools.days.model.AddressBookRegistry;
import org.hiero.block.tools.records.model.unparsed.InMemoryFile;

/**
 * Universal model for a parsed signature file that supports multiple signature file
 * formats used by record file versions 2, 5 and 6.
 */
@SuppressWarnings("unused")
public class ParsedSignatureFile {
    /** The expected object version used when reading record stream signature objects. */
    public static final int RECORD_STREAM_OBJECT_CLASS_VERSION = 1;
    /** The class id used for signature objects in v5 record stream signature files. */
    public static final long SIGNATURE_CLASS_ID = 0x13dc4b399b245c69L;
    /** The SHA-384 file hash extracted from the signature file. */
    private final byte[] fileHashFromSig;
    /** The raw RSA signature bytes extracted from the signature file. */
    private final byte[] signatureBytes;
    /** The numeric account number (for example, 3 for 0.0.3) of the node that produced the signature.*/
    private final int accountNum;
    /** The name of the signature file that was parsed if known */
    private final String signatureFileName;

    /**
     * Parse a signature file into a {@link ParsedSignatureFile} instance.
     *
     * @param sigFile the in-memory signature file to parse
     * @throws RuntimeException if there is no RSA public key for the node, the public key is empty, or the
     *                          signature file cannot be parsed
     */
    public ParsedSignatureFile(InMemoryFile sigFile) {
        signatureFileName = sigFile.path().getFileName().toString();
        // Extract node ID from the file name
        accountNum = extractNodeAccountNumFromSignaturePath(sigFile.path());
        try (DataInputStream sin = new DataInputStream(new ByteArrayInputStream(sigFile.data()))) {
            final int firstByte = sin.read();
            switch (firstByte) {
                case 4: { // version 2 record file signature file
                    // Read 48-byte file hash
                    fileHashFromSig = new byte[SHA_384_HASH_SIZE];
                    sin.readFully(fileHashFromSig);
                    // Read signature marker
                    byte marker = sin.readByte();
                    if (marker != 3) {
                        throw new IOException("Invalid signature marker byte in " + sigFile.path());
                    }
                    // Read signature length and signature bytes
                    final int sigLen = sin.readInt();
                    signatureBytes = new byte[sigLen];
                    sin.readFully(signatureBytes);
                    break;
                }
                case 5: { // version 5 record file signature file
                    final int objStreamVer = sin.readInt();
                    if (objStreamVer != RECORD_STREAM_OBJECT_CLASS_VERSION) {
                        throw new RuntimeException(
                                "Unexpected object stream version (expected 1) in " + sigFile.path());
                    }
                    fileHashFromSig = readV5HashObject(sin);
                    // read signature object
                    final long sigClassId = sin.readLong();
                    if (sigClassId != SIGNATURE_CLASS_ID) {
                        throw new RuntimeException("Invalid signature object class ID in " + sigFile.path());
                    }
                    final int sigClassVer = sin.readInt();
                    if (sigClassVer != RECORD_STREAM_OBJECT_CLASS_VERSION) {
                        throw new RuntimeException("Invalid signature object class version in " + sigFile.path());
                    }
                    final int sigType = sin.readInt();
                    if (sigType != RECORD_STREAM_OBJECT_CLASS_VERSION) {
                        throw new IOException("Invalid signature type in " + sigFile.path());
                    }
                    final int sigLen = sin.readInt();
                    final int checksum = sin.readInt();
                    if (checksum != 101 - sigLen) {
                        throw new IOException("Invalid checksum in " + sigFile.path());
                    }
                    signatureBytes = new byte[sigLen];
                    sin.readFully(signatureBytes);
                    break;
                }
                case 6: { // version 6 record file signature file
                    // Parse protobuf portion
                    final com.hedera.hapi.streams.SignatureFile signatureFile =
                            com.hedera.hapi.streams.SignatureFile.PROTOBUF.parse(new ReadableStreamingData(sin));
                    if (signatureFile.fileSignature() == null) {
                        throw new IOException("Invalid signature file, missing file signature in " + sigFile.path());
                    }
                    fileHashFromSig = signatureFile
                            .fileSignature()
                            .hashObjectOrThrow()
                            .hash()
                            .toByteArray();
                    signatureBytes = signatureFile.fileSignature().signature().toByteArray();
                    break;
                }
                default: {
                    throw new IOException(
                            "Unrecognized signature file format version " + firstByte + " in file " + sigFile.path());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "Error processing signature file " + sigFile.path() + " because: " + e.getMessage(), e);
        }
    }

    public String getRsaPubKey(NodeAddressBook addressBook) {
        // Look up RSA public key via AddressBookRegistry helper
        String rsaPubKey;
        try {
            rsaPubKey = AddressBookRegistry.publicKeyForNode(addressBook, 0, 0, accountNum);
        } catch (Exception e) {
            throw new RuntimeException(
                    "No RSA public key found for 0.0." + accountNum + " in provided address book; file "
                            + signatureFileName,
                    e);
        }
        if (rsaPubKey == null || rsaPubKey.isEmpty()) {
            throw new RuntimeException("Empty RSA public key for 0.0." + accountNum + "; file " + signatureFileName);
        }
        return rsaPubKey;
    }

    public long getNodeId(NodeAddressBook addressBook) {
        // Get node ID from AddressBookRegistry helper
        return nodeIdForNode(addressBook, 0, 0, accountNum);
    }

    /**
     * Validate this parsed signature file against a provided file hash.
     *
     * @param hash the file hash to validate against (expected SHA-384)
     * @param addressBook the address book used to resolve the public key for the signature
     * @return true if the provided hash matches the embedded hash and the RSA signature verifies; false otherwise
     */
    public boolean isValid(byte[] hash, NodeAddressBook addressBook) {
        return isValid(hash, getRsaPubKey(addressBook));
    }

    /**
     * Validate this parsed signature file against a provided file hash.
     *
     * @param hash the file hash to validate against (expected SHA-384)
     * @param rsaPubKey the RSA public key used to verify the signature
     * @return true if the provided hash matches the embedded hash and the RSA signature verifies; false otherwise
     */
    public boolean isValid(byte[] hash, String rsaPubKey) {
        if (!Arrays.equals(hash, fileHashFromSig)) {
            return false;
        }
        return verifyRsaSha384(rsaPubKey, hash, signatureBytes);
    }

    /**
     * Convert this parsed signature into the wire model {@link RecordFileSignature} used by the block stream.
     *
     * @param addressBook the address book used to resolve the node ID for the signature
     * @return a RecordFileSignature containing the raw signature bytes and the resolved node id
     */
    public RecordFileSignature toRecordFileSignature(NodeAddressBook addressBook) {
        return new RecordFileSignature(Bytes.wrap(signatureBytes), getNodeId(addressBook));
    }

    /** @return the SHA-384 file hash extracted from the signature file */
    public byte[] fileHashFromSig() {
        return fileHashFromSig;
    }

    /** @return the raw RSA signature bytes extracted from the signature file */
    public byte[] signatureBytes() {
        return signatureBytes;
    }

    /** @return the numeric account number that produced the signature (e.g. 3 for account 0.0.3) */
    public int accountNum() {
        return accountNum;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "SignatureFile{" + "fileHashFromSig="
                + HexFormat.of().formatHex(fileHashFromSig) + ", signatureBytes="
                + HexFormat.of().formatHex(signatureBytes) + ", accountNum="
                + accountNum
                + '}';
    }

    /**
     * Two ParsedSignatureFile instances are equal when all identifying fields and the
     * embedded byte arrays are equal.
     *
     * @param o the other object to compare
     * @return true when equal, false otherwise
     */
    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ParsedSignatureFile that = (ParsedSignatureFile) o;
        return accountNum == that.accountNum
                && Objects.deepEquals(fileHashFromSig, that.fileHashFromSig)
                && Objects.deepEquals(signatureBytes, that.signatureBytes);
    }

    /**
     * Compute a hash code consistent with {@link #equals(Object)}.
     *
     * @return an int hash code
     */
    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(fileHashFromSig), Arrays.hashCode(signatureBytes), accountNum);
    }
}
