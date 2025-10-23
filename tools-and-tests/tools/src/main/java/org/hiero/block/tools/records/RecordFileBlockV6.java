// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.records;

import static org.hiero.block.tools.utils.Sha384.sha384Digest;

import com.hedera.hapi.block.stream.experimental.Block;
import com.hedera.hapi.block.stream.experimental.BlockFooter;
import com.hedera.hapi.block.stream.experimental.BlockItem;
import com.hedera.hapi.block.stream.experimental.BlockItem.ItemOneOfType;
import com.hedera.hapi.block.stream.experimental.BlockProof;
import com.hedera.hapi.block.stream.experimental.BlockProof.ProofOneOfType;
import com.hedera.hapi.block.stream.experimental.SignedRecordFileProof;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.hapi.node.base.Transaction;
import com.hedera.hapi.node.transaction.TransactionBody;
import com.hedera.hapi.streams.RecordStreamFile;
import com.hedera.hapi.streams.RecordStreamItem;
import com.hedera.pbj.runtime.OneOf;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.stream.ReadableStreamingData;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
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
public class RecordFileBlockV6 extends RecordFileBlock {

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
    public RecordFileBlockV6(
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
    public Block toWrappedBlock(final long blockNumber,
        final byte[] previousBlockHash, final byte[] rootHashOfBlockHashesMerkleTree,
        final NodeAddressBook addressBook) throws IOException {
        final byte[] bytes = primaryRecordFile.data();
        try (final DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            // read and verify header
            final int fileVersion = in.readInt();
            if (fileVersion != 6) {
                throw new IOException("Invalid v6 record file version: " + fileVersion);
            }
            // parse protobuf RecordStreamFile
            RecordStreamFile recordStreamFile = RecordStreamFile.PROTOBUF.parse(new ReadableStreamingData(in));
            // compute entire file SHA-384 for signature verification
            final MessageDigest sha384 = sha384Digest();
            final byte[] entireFileHash = sha384.digest(bytes);
            // check or set block number
            if (recordStreamFile.blockNumber() > 0) {
                if(recordStreamFile.blockNumber() != blockNumber) {
                    throw new IOException(
                        "Provided block number " + blockNumber + " does not match record file block number " +
                            recordStreamFile.blockNumber());
                }
            } else {
                // older v5 record stream files do not have block number
                recordStreamFile = recordStreamFile.copyBuilder().blockNumber(blockNumber).build();
            }
            // convert signatures into block proof
            final List<com.hedera.hapi.block.stream.experimental.RecordFileSignature> signatures = signatureFiles().stream()
                    .parallel()
                    .map(sf -> new ParsedSignatureFile(addressBook, sf))
                    .filter(psf -> psf.isValid(entireFileHash))
                    .map(ParsedSignatureFile::toRecordFileSignature)
                    .toList();
            final BlockProof blockProof = new BlockProof(new OneOf<>(
                    ProofOneOfType.SIGNED_RECORD_FILE_PROOF,
                    new SignedRecordFileProof(6, signatures)));
            // create footer
            final BlockFooter blockFooter = new BlockFooter(
                    com.hedera.pbj.runtime.io.buffer.Bytes.wrap(previousBlockHash),
                    com.hedera.pbj.runtime.io.buffer.Bytes.wrap(rootHashOfBlockHashesMerkleTree),
                    null
            );
            // create and return the Block
            return new Block(List.of(
                    new BlockItem(new OneOf<>(ItemOneOfType.RECORD_FILE, recordStreamFile)),
                    new BlockItem(new OneOf<>(ItemOneOfType.BLOCK_FOOTER, blockFooter)),
                    new BlockItem(new OneOf<>(ItemOneOfType.BLOCK_PROOF, blockProof))
            ));
        } catch (ParseException e) {
            throw new IOException(e);
        }
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
            final StringBuffer warnings = new StringBuffer();

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
}
