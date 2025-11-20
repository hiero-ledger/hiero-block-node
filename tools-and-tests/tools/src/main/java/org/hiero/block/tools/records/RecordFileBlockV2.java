// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.records;

import static org.hiero.block.tools.utils.Sha384.SHA_384_HASH_SIZE;

import com.hedera.hapi.block.stream.experimental.Block;
import com.hedera.hapi.block.stream.experimental.BlockFooter;
import com.hedera.hapi.block.stream.experimental.BlockItem;
import com.hedera.hapi.block.stream.experimental.BlockItem.ItemOneOfType;
import com.hedera.hapi.block.stream.experimental.BlockProof;
import com.hedera.hapi.block.stream.experimental.BlockProof.ProofOneOfType;
import com.hedera.hapi.block.stream.experimental.RecordFileSignature;
import com.hedera.hapi.block.stream.experimental.SignedRecordFileProof;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.BlockHashAlgorithm;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.hapi.node.base.Timestamp;
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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import org.hiero.block.tools.days.model.AddressBookRegistry;

/**
 * In-memory representation and validator for version 2 Hedera record stream files.
 * <p>
 * Validation recomputes the v2 file hash per the legacy formula hash(header || hash(contents)) and verifies
 * signature files against the computed file hash using RSA public keys from the provided NodeAddressBook.
 * It also checks that the provided startRunningHash (when supplied) matches the previous-file hash in the header.
 */
@SuppressWarnings({"DuplicatedCode", "StringConcatenationInsideStringBufferAppend"})
public class RecordFileBlockV2 extends RecordFileBlock {
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
    protected RecordFileBlockV2(
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
     * Convert this record file block into a block-stream wrapped block.
     *
     * @param blockNumber the number of the block, starting 0 for the first block. This has to be specified as it cannot
     *                    be computed from record stream data.
     * @param blockTime the consensus time of the block
     * @param addressBook the NodeAddressBook to use for signature verification
     * @param previousBlockHash the hash of the previous block, the hash of block stream block N-1
     * @param rootHashOfBlockHashesMerkleTree the root hash of the block hashes merkle tree including all blocks up to N-1
     * @return the Block read from the InMemoryBlock
     * @throws IOException if an I/O error occurs
     */
    @Override
    public Block toWrappedBlock(
            final long blockNumber,
            final Instant blockTime,
            final byte[] previousBlockHash,
            final byte[] rootHashOfBlockHashesMerkleTree,
            final NodeAddressBook addressBook)
            throws IOException {
        try {
            final byte[] recordFileBytes = primaryRecordFile.data();
            // Parse the primary record file
            final BufferedData in = BufferedData.wrap(recordFileBytes);
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
            // create a block header
            final BlockHeader blockHeader = new BlockHeader(
                    hapiVersion,
                    hapiVersion, // TODO is this right? could be unset, not if that is better
                    blockNumber,
                    new Timestamp(
                            blockTime.getEpochSecond(), blockTime.getNano()), // TODO is the the right time to use?
                    BlockHashAlgorithm.SHA2_384);
            // read staring hash which is also the previous block ending hash
            final byte[] startingHash = new byte[SHA_384_HASH_SIZE];
            in.readBytes(startingHash);

            // The hash for v2 files is the hash (header, hash(content)) this is different to other versions
            // the block hash is not available in the file, so we have to calculate it
            MessageDigest digest = MessageDigest.getInstance("SHA-384");
            digest.update(recordFileBytes, V2_HEADER_LENGTH, recordFileBytes.length - V2_HEADER_LENGTH);
            final byte[] contentHash = digest.digest();
            digest.update(recordFileBytes, 0, V2_HEADER_LENGTH);
            digest.update(contentHash);
            final byte[] blockHash = digest.digest();

            // read all the transactions and transaction records
            final List<RecordStreamItem> recordStreamItems = new ArrayList<>();
            while (in.hasRemaining()) {
                final byte recordMarker = in.readByte();
                if (recordMarker != 2) {
                    throw new IOException(
                            "Unexpected record marker " + recordMarker + " (expected 2) in v2 record file");
                }
                final int txnLength = in.readInt();
                if (txnLength <= 0 || txnLength > in.remaining()) {
                    throw new IOException("Invalid transaction length in v2 record file");
                }
                // Parse the transaction from an explicit Bytes slice so the protobuf parser only reads
                // the transaction bytes and does not consume later record fields.
                Transaction txn = Transaction.PROTOBUF.parse(in.slice(in.position(), txnLength));
                in.skip(txnLength);
                if (in.remaining() < 4) {
                    throw new IOException("Insufficient bytes for transaction record length in v2 record file");
                }
                // Parse transaction record
                final int txnRecordLength = in.readInt();
                if (txnRecordLength <= 0 || txnRecordLength > in.remaining()) {
                    throw new IOException("Invalid transaction record length in v2 record file");
                }
                TransactionRecord txnRecord =
                        TransactionRecord.PROTOBUF.parse(in.slice(in.position(), txnRecordLength));
                in.skip(txnRecordLength);
                recordStreamItems.add(new RecordStreamItem(txn, txnRecord));
            }
            // build a V5 RecordStreamFile
            final RecordStreamFile recordStreamFile = new RecordStreamFile(
                    hapiVersion,
                    new HashObject(HashAlgorithm.SHA_384, SHA_384_HASH_SIZE, Bytes.wrap(startingHash)),
                    recordStreamItems,
                    null, // V2 record files do not have a streaming hash so there is no end hash
                    blockNumber,
                    Collections.emptyList() // V2 record files do not have sidecars
                    );
            // convert signatures into block proof
            final List<RecordFileSignature> signatures = signatureFiles.stream()
                    .parallel()
                    .map(sf -> new ParsedSignatureFile(addressBook, sf))
                    .filter(psf -> psf.isValid(blockHash))
                    .map(ParsedSignatureFile::toRecordFileSignature)
                    .toList();
            BlockProof blockProof = new BlockProof(
                    new OneOf<>(ProofOneOfType.SIGNED_RECORD_FILE_PROOF, new SignedRecordFileProof(2, signatures)));
            // create footer
            final BlockFooter blockFooter =
                    new BlockFooter(Bytes.wrap(previousBlockHash), Bytes.wrap(rootHashOfBlockHashesMerkleTree), null);
            // create and return the Block
            return new Block(List.of(
                    new BlockItem(new OneOf<>(ItemOneOfType.BLOCK_HEADER, blockHeader)),
                    new BlockItem(new OneOf<>(ItemOneOfType.RECORD_FILE, recordStreamFile)),
                    new BlockItem(new OneOf<>(ItemOneOfType.BLOCK_FOOTER, blockFooter)),
                    new BlockItem(new OneOf<>(ItemOneOfType.BLOCK_PROOF, blockProof))));
        } catch (ParseException | NoSuchAlgorithmException e) {
            throw new IOException(e);
        }
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
        try {
            final BufferedData in = BufferedData.wrap(recordFileBytes);
            boolean isValid = true;
            final StringBuffer warningMessages = new StringBuffer();
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
            in.readBytes(previousHash);
            // check the start running hash is the same as the previous hash
            if (startRunningHash != null
                    && startRunningHash.length > 0
                    && !Arrays.equals(startRunningHash, previousHash)) {
                isValid = false;
                warningMessages.append(
                        "Start running hash does not match previous hash in v2 record file\n" + "  Expected: "
                                + HexFormat.of().formatHex(startRunningHash) + "\n" + "  Found:    "
                                + HexFormat.of().formatHex(previousHash) + "\n");
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
            isValid = isValid && validateSignatures(addressBook, warningMessages, blockHash);

            // read all the transactions
            final List<Transaction> transactions = new ArrayList<>();
            while (in.hasRemaining()) {
                final byte recordMarker = in.readByte();
                if (recordMarker != 2) {
                    warningMessages.append(
                            "Unexpected record marker " + recordMarker + " (expected 2) in v2 record file\n");
                    isValid = false;
                    break;
                }
                final int txnLength = in.readInt();
                if (txnLength <= 0 || txnLength > in.remaining()) {
                    warningMessages.append("Invalid transaction length in v2 record file\n");
                    isValid = false;
                    break;
                }
                // Parse the transaction from an explicit Bytes slice so the protobuf parser only reads
                // the transaction bytes and does not consume subsequent record fields.
                Transaction txn = Transaction.PROTOBUF.parse(in.readBytes(txnLength));
                transactions.add(txn);
                if (in.remaining() < 4) {
                    warningMessages.append("Insufficient bytes for transaction record length in v2 record file\n");
                    isValid = false;
                    break;
                }
                final int txnRecordLength = in.readInt();
                if (txnRecordLength <= 0 || txnRecordLength > in.remaining()) {
                    warningMessages.append("Invalid transaction record length in v2 record file\n");
                    isValid = false;
                    break;
                }
                in.skip(txnRecordLength);
            }
            // feed the transactions to the address book registry to extract any address book transactions
            final List<TransactionBody> addressBookTransactions =
                    AddressBookRegistry.filterToJustAddressBookTransactions(transactions);
            // return the validation result
            return new ValidationResult(
                    isValid, warningMessages.toString(), blockHash, hapiVersion, addressBookTransactions);
        } catch (IOException | NoSuchAlgorithmException | ParseException e) {
            throw new RuntimeException(e);
        }
    }
}
