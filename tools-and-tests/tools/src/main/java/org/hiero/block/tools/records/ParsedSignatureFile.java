package org.hiero.block.tools.records;

import static org.hiero.block.tools.commands.days.model.AddressBookRegistry.nodeIdForNode;
import static org.hiero.block.tools.records.SerializationV5Utils.readV5HashObject;
import static org.hiero.block.tools.records.SigFileUtils.extractNodeAccountNumFromSignaturePath;
import static org.hiero.block.tools.records.SigFileUtils.verifyRsaSha384;
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
import org.hiero.block.tools.commands.days.model.AddressBookRegistry;

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
    /** The numeric account number (for example 3 for 0.0.3) of the node that produced the signature.*/
    private final int accountNum;
    /** The node ID (as defined by the address book) corresponding to {@link #accountNum}. */
    private final long nodeId;
    /**  The RSA public key (PEM or string form as provided in the address book) used to verify the signature. */
    private final String rsaPubKey;

    /**
     * Parse a signature file into a {@link ParsedSignatureFile} instance.
     *
     * @param addressBook the address book used to resolve node information and public keys
     * @param sigFile the in-memory signature file to parse
     * @throws RuntimeException if there is no RSA public key for the node, the public key is empty, or the
     *                          signature file cannot be parsed
     */
    public ParsedSignatureFile(NodeAddressBook addressBook, InMemoryFile sigFile) {
        // Extract node ID from filename and fetch RSA public key from address book
        accountNum = extractNodeAccountNumFromSignaturePath(sigFile.path());
        // Get node ID from AddressBookRegistry helper
        nodeId = nodeIdForNode(addressBook, 0, 0, accountNum);

        // Look up RSA public key via AddressBookRegistry helper
        try {
            rsaPubKey = AddressBookRegistry.publicKeyForNode(addressBook, 0, 0, accountNum);
        } catch (Exception e) {
            throw new RuntimeException("No RSA public key found for 0.0."+accountNum+" in provided address book; file "+
                sigFile.path(),e);
        }
        if (rsaPubKey == null || rsaPubKey.isEmpty()) {
            throw new RuntimeException("Empty RSA public key for 0.0."+accountNum+"; file "+sigFile.path());
        }
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
                        throw new IOException("Invalid signature marker byte in "+sigFile.path());
                    }
                    // Read signature length and signature bytes
                    final int sigLen = sin.readInt();
                    signatureBytes = new byte[sigLen];
                    sin.readFully(signatureBytes);
                    break;
                }
                case 5: {// version 5 record file signature file
                    final int objStreamVer = sin.readInt();
                    if (objStreamVer != RECORD_STREAM_OBJECT_CLASS_VERSION) {
                        throw new RuntimeException("Unexpected object stream version (expected 1) in "+sigFile.path());
                    }
                    fileHashFromSig = readV5HashObject(sin);
                    // read signature object
                    final long sigClassId = sin.readLong();
                    if (sigClassId != SIGNATURE_CLASS_ID) {
                        throw new RuntimeException("Invalid signature object class ID in "+sigFile.path());
                    }
                    final int sigClassVer = sin.readInt();
                    if (sigClassVer != RECORD_STREAM_OBJECT_CLASS_VERSION) {
                        throw new RuntimeException("Invalid signature object class version in "+sigFile.path());
                    }
                    final int sigType = sin.readInt();
                    if (sigType != RECORD_STREAM_OBJECT_CLASS_VERSION) {
                        throw new IOException("Invalid signature type in "+sigFile.path());
                    }
                    final int sigLen = sin.readInt();
                    final int checksum = sin.readInt();
                    if (checksum != 101 - sigLen) {
                        throw new IOException("Invalid checksum in "+sigFile.path());
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
                        throw new IOException("Invalid signature file, missing file signature in "+sigFile.path());
                    }
                    fileHashFromSig = signatureFile.fileSignature().hashObjectOrThrow().hash().toByteArray();
                    signatureBytes = signatureFile.fileSignature().signature().toByteArray();
                    break;
                }
                default: {
                    throw new IOException("Unrecognized signature file format version "+firstByte+
                        " in file "+sigFile.path());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Error processing signature file "+sigFile.path()+
                " because: "+e.getMessage(),e);
        }
    }

    /**
     * Validate this parsed signature file against a provided file hash.
     *
     * @param hash the file hash to validate against (expected SHA-384)
     * @return true if the provided hash matches the embedded hash and the RSA signature verifies; false otherwise
     */
    public boolean isValid(byte[] hash) {
        if (!Arrays.equals(hash, fileHashFromSig)) {
            return false;
        }
        return verifyRsaSha384(rsaPubKey, hash, signatureBytes);
    }

    /**
     * Convert this parsed signature into the wire model {@link RecordFileSignature} used by the block stream.
     *
     * @return a RecordFileSignature containing the raw signature bytes and the resolved node id
     */
    public RecordFileSignature toRecordFileSignature() {
        return new RecordFileSignature(Bytes.wrap(signatureBytes),nodeId);
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

    /** @return the resolved node id corresponding to {@link #accountNum} */
    public long nodeId() {
        return nodeId;
    }

    /** @return the RSA public key string used to verify signatures for this node */
    public String rsaPubKey() {
        return rsaPubKey;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "SignatureFile{" +
            "fileHashFromSig=" + HexFormat.of().formatHex(fileHashFromSig) +
            ", signatureBytes=" + HexFormat.of().formatHex(signatureBytes) +
            ", accountNum=" + accountNum +
            ", nodeId=" + nodeId +
            ", rsaPubKey='" + rsaPubKey + '\'' +
            '}';
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
        return accountNum == that.accountNum && nodeId == that.nodeId && Objects.deepEquals(fileHashFromSig,
            that.fileHashFromSig) && Objects.deepEquals(signatureBytes, that.signatureBytes)
            && Objects.equals(rsaPubKey, that.rsaPubKey);
    }

    /**
     * Compute a hash code consistent with {@link #equals(Object)}.
     *
     * @return an int hash code
     */
    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(fileHashFromSig), Arrays.hashCode(signatureBytes), accountNum, nodeId,
            rsaPubKey);
    }
}
