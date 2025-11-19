// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.records;

import static org.hiero.block.tools.records.SerializationV5Utils.HASH_OBJECT_SIZE_BYTES;
import static org.hiero.block.tools.records.SerializationV5Utils.readV5HashObject;
import static org.hiero.block.tools.utils.Sha384.SHA_384_HASH_SIZE;
import static org.hiero.block.tools.utils.Sha384.hashSha384;

import com.hedera.hapi.block.stream.experimental.Block;
import com.hedera.hapi.block.stream.experimental.BlockFooter;
import com.hedera.hapi.block.stream.experimental.BlockItem;
import com.hedera.hapi.block.stream.experimental.BlockItem.ItemOneOfType;
import com.hedera.hapi.block.stream.experimental.BlockProof;
import com.hedera.hapi.block.stream.experimental.BlockProof.ProofOneOfType;
import com.hedera.hapi.block.stream.experimental.RecordFileSignature;
import com.hedera.hapi.block.stream.experimental.SignedRecordFileProof;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.hapi.node.base.Transaction;
import com.hedera.hapi.node.transaction.TransactionBody;
import com.hedera.hapi.node.transaction.TransactionRecord;
import com.hedera.hapi.streams.HashAlgorithm;
import com.hedera.hapi.streams.HashObject;
import com.hedera.hapi.streams.RecordStreamFile;
import com.hedera.hapi.streams.RecordStreamItem;
import com.hedera.pbj.runtime.OneOf;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.BufferedData;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import org.hiero.block.tools.days.model.AddressBookRegistry;

/**
 * In-memory representation and validator for version 5 Hedera record stream files.
 * <p>
 * This implementation parses the v5 header, start object running hash, and end object running hash
 * as specified in RecordFileFormat.md. Validation ensures the provided start running hash (when
 * supplied) matches the hash in the file and returns the end-running hash contained in the file.
 * Signature file validation is performed using RSA public keys from the provided address book.
 */
@SuppressWarnings("DuplicatedCode")
public class RecordFileBlockV5 extends RecordFileBlock {
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
    public RecordFileBlockV5(
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
     * Convert this record file block into a block stream wrapped block.
     *
     * @param blockNumber the number of the block, starting 0 for first block. This has to be specified as it can not
     *                    be computed from record stream data.
     * @param addressBook the NodeAddressBook to use for signature verification
     * @param previousBlockHash the hash of the previous block, the hash of block stream block N-1
     * @param rootHashOfBlockHashesMerkleTree the root hash of the block hashes merkle tree including all blocks up to N-1
     * @return the Block read from the InMemoryBlock
     * @throws IOException if an I/O error occurs
     */
    @Override
    public Block toWrappedBlock(
            final long blockNumber,
            final byte[] previousBlockHash,
            final byte[] rootHashOfBlockHashesMerkleTree,
            final NodeAddressBook addressBook)
            throws IOException {
        try {
            final byte[] recordFileBytes = primaryRecordFile.data();
            final BufferedData in = BufferedData.wrap(recordFileBytes);
            // compute the entire file hash used for signature verification
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
                throw new IllegalStateException("Unexpected object stream version (v5): " + objectStreamVersion);
            }
            // read start running hash is a Hash Object in v5 format; parse to extract SHA-384 bytes
            final byte[] startHashInFile = readV5HashObject(in);
            // read all record stream objects and build RecordStreamItem list
            final List<RecordStreamItem> recordStreamItems = new java.util.ArrayList<>();
            while (in.remaining() > HASH_OBJECT_SIZE_BYTES) {
                // read a RecordStreamObject
                final long classId = in.readLong();
                if (classId != RECORD_STREAM_OBJECT_CLASS_ID) {
                    throw new IOException("Unexpected class ID in record file: " + Long.toHexString(classId));
                }
                final int classVersion = in.readInt();
                if (classVersion != RECORD_STREAM_OBJECT_CLASS_VERSION) {
                    throw new IOException("Unexpected class version in record file: " + classVersion);
                }
                final int transactionRecordLength = in.readInt();
                if (transactionRecordLength <= 0 || transactionRecordLength > in.remaining() - HASH_OBJECT_SIZE_BYTES) {
                    throw new IOException(
                            "Invalid transaction record length in record file: " + transactionRecordLength);
                }
                final TransactionRecord txnRecord =
                        TransactionRecord.PROTOBUF.parse(in.readBytes(transactionRecordLength));
                final int transactionLength = in.readInt();
                if (transactionLength <= 0 || transactionLength > in.remaining() - HASH_OBJECT_SIZE_BYTES) {
                    throw new IOException("Invalid transaction length in record file: " + transactionLength);
                }
                final Transaction transaction = Transaction.PROTOBUF.parse(in.readBytes(transactionLength));
                recordStreamItems.add(new RecordStreamItem(transaction, txnRecord));
            }
            if (in.remaining() != HASH_OBJECT_SIZE_BYTES) {
                throw new IOException("Expected " + HASH_OBJECT_SIZE_BYTES
                        + " bytes remaining for end running hash, but found " + in.remaining());
            }
            // read the end running hash
            final byte[] endRunningHash = readV5HashObject(in);
            // build the RecordStreamFile model used by block stream
            final RecordStreamFile recordStreamFile = new RecordStreamFile(
                    hapiVersion,
                    new HashObject(HashAlgorithm.SHA_384, SHA_384_HASH_SIZE, Bytes.wrap(startHashInFile)),
                    recordStreamItems,
                    new HashObject(HashAlgorithm.SHA_384, SHA_384_HASH_SIZE, Bytes.wrap(endRunningHash)),
                    blockNumber,
                    Collections.emptyList() // v5 files do not have sidecars
                    );
            // convert signatures into block proof
            final List<RecordFileSignature> signatures = signatureFiles.stream()
                    .parallel()
                    .map(sf -> new ParsedSignatureFile(addressBook, sf))
                    .filter(psf -> psf.isValid(entireFileHash))
                    .map(ParsedSignatureFile::toRecordFileSignature)
                    .toList();
            final BlockProof blockProof = new BlockProof(
                    new OneOf<>(ProofOneOfType.SIGNED_RECORD_FILE_PROOF, new SignedRecordFileProof(5, signatures)));
            // create footer
            final BlockFooter blockFooter =
                    new BlockFooter(Bytes.wrap(previousBlockHash), Bytes.wrap(rootHashOfBlockHashesMerkleTree), null);
            // create and return the Block
            return new Block(List.of(
                    new BlockItem(new OneOf<>(ItemOneOfType.RECORD_FILE, recordStreamFile)),
                    new BlockItem(new OneOf<>(ItemOneOfType.BLOCK_FOOTER, blockFooter)),
                    new BlockItem(new OneOf<>(ItemOneOfType.BLOCK_PROOF, blockProof))));
        } catch (ParseException e) {
            throw new IOException(e);
        }
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
            final StringBuffer warnings = new StringBuffer();
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
}
