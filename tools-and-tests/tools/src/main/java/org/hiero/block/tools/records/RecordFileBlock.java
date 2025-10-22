// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.records;

import com.hedera.hapi.block.stream.experimental.Block;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.hapi.node.transaction.TransactionBody;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * In-memory representation of a set of record files for a block. Typically, a 2 or 5 seconds period of consensus time.
 * The record file set includes the primary most common record file, signature files, and sidecar files.
 * <p>InMemoryBlocks can be read and written as a set of files in a directory with common timestamp, or they can be
 * read from compressed tar.zstd day files.</p>
 */
public abstract class RecordFileBlock {

    /**
     * Validation result for a block.
     *
     * @param isValid true if the block is valid, false otherwise
     * @param warningMessages Warning messages, if any warnings or errors were encountered
     * @param endRunningHash The end-running hash of the block
     * @param hapiVersion HAPI proto version
     * @param addressBookTransactions Transactions that affect the address book
     */
    public record ValidationResult(
            boolean isValid,
            String warningMessages,
            byte[] endRunningHash,
            SemanticVersion hapiVersion,
            List<TransactionBody> addressBookTransactions) {}

    /** the consensus time of the block */
    protected final Instant recordFileTime;
    /** the primary record file for the block */
    protected final InMemoryFile primaryRecordFile;
    /** the optional other record files for the block */
    protected final List<InMemoryFile> otherRecordFiles;
    /** the signature files for the block */
    protected final List<InMemoryFile> signatureFiles;
    /** the sidecar files for the block */
    protected final List<InMemoryFile> primarySidecarFiles;
    /** the other sidecar files for the block */
    protected final List<InMemoryFile> otherSidecarFiles;

    /**
     *  Create a new InMemoryBlock instance by passing in all files associated with the block. They are then divided into
     *  record files, sidecar files, and signature files.
     *
     * @param recordFileTime the consensus time of the block
     * @param primaryRecordFile the primary record file for the block
     * @param otherRecordFiles the other record files for the block
     * @param signatureFiles the signature files for the block
     * @param primarySidecarFiles the primary sidecar files for the block
     * @param otherSidecarFiles the other sidecar files for the block
     * @return the new InMemoryBlock instance
     */
    public static RecordFileBlock newInMemoryBlock(
            Instant recordFileTime,
            InMemoryFile primaryRecordFile,
            List<InMemoryFile> otherRecordFiles,
            List<InMemoryFile> signatureFiles,
            List<InMemoryFile> primarySidecarFiles,
            List<InMemoryFile> otherSidecarFiles) {
        // get the record file data
        final byte[] recordFileBytes = primaryRecordFile.data();
        // read first for bytes as a Java integer in the same format as written by DataOutputStream
        // Read 32-bit big-endian version from first 4 bytes (DataInputStream.readInt() semantics)
        final int recordFormatVersion = ((recordFileBytes[0] & 0xFF) << 24)
                | ((recordFileBytes[1] & 0xFF) << 16)
                | ((recordFileBytes[2] & 0xFF) << 8)
                | (recordFileBytes[3] & 0xFF);
        return switch (recordFormatVersion) {
            case 2 ->
                new RecordFileBlockV2(
                        recordFileTime,
                        primaryRecordFile,
                        otherRecordFiles,
                        signatureFiles,
                        primarySidecarFiles,
                        otherSidecarFiles);
            case 5 ->
                new RecordFileBlockV5(
                        recordFileTime,
                        primaryRecordFile,
                        otherRecordFiles,
                        signatureFiles,
                        primarySidecarFiles,
                        otherSidecarFiles);
            case 6 ->
                new RecordFileBlockV6(
                        recordFileTime,
                        primaryRecordFile,
                        otherRecordFiles,
                        signatureFiles,
                        primarySidecarFiles,
                        otherSidecarFiles);
            default ->
                throw new IllegalArgumentException("Unsupported record file format version: " + recordFormatVersion);
        };
    }

    /**
     *  Create a new InMemoryBlock instance.
     *
     * @param recordFileTime the consensus time of the block
     * @param primaryRecordFile the primary record file for the block
     * @param otherRecordFiles the other record files for the block
     * @param signatureFiles the signature files for the block
     * @param primarySidecarFiles the primary sidecar files for the block
     * @param otherSidecarFiles the other sidecar files for the block
     */
    protected RecordFileBlock(
            Instant recordFileTime,
            InMemoryFile primaryRecordFile,
            List<InMemoryFile> otherRecordFiles,
            List<InMemoryFile> signatureFiles,
            List<InMemoryFile> primarySidecarFiles,
            List<InMemoryFile> otherSidecarFiles) {
        if (recordFileTime == null) {
            throw new IllegalArgumentException("recordFileTime cannot be null");
        }
        if (signatureFiles == null) {
            throw new IllegalArgumentException("signatureFiles cannot be null");
        }
        if (primarySidecarFiles == null) {
            throw new IllegalArgumentException("primarySidecarFiles cannot be null");
        }
        if (otherSidecarFiles == null) {
            throw new IllegalArgumentException("otherSidecarFiles cannot be null");
        }
        if (otherRecordFiles == null) {
            throw new IllegalArgumentException("otherRecordFiles cannot be null");
        }
        this.recordFileTime = recordFileTime;
        this.primaryRecordFile = primaryRecordFile;
        this.otherRecordFiles = otherRecordFiles;
        this.signatureFiles = signatureFiles;
        this.primarySidecarFiles = primarySidecarFiles;
        this.otherSidecarFiles = otherSidecarFiles;
    }

    // === Abstract Methods ===========================================================================================

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
    public abstract Block toWrappedBlock(final long blockNumber,
        final byte[] previousBlockHash, final byte[] rootHashOfBlockHashesMerkleTree,
        final NodeAddressBook addressBook) throws IOException;
    /**
     * Validate the record file. This recomputes the running hash. Checks the provided starting running hash with the
     * one read from the file. It also computes the end-running hash, checks it against the one in the file if the file
     * has one. Then returns the end-running hash in the ValidationResult. If the file is v6 and has sidecar files, then
     * their hashes are also validated.
     * <p>
     * Signature files are also validated using the RSA public keys for each node in the address book. If
     *
     * @param startRunningHash the running hash from the previous block, null if we do not want to validate it
     * @param addressBook the address book to use to validate signatures with
     * @return the validation result
     */
    public abstract ValidationResult validate(byte[] startRunningHash, NodeAddressBook addressBook);

    // === Public Methods =============================================================================================


    /**
     * Get the total size in bytes of all files in the block.
     *
     * @return the total size in bytes
     */
    public long getTotalSizeBytes() {
        long total = 0L;
        if (primaryRecordFile != null) {
            total += primaryRecordFile.data().length;
        }
        for (InMemoryFile f : otherRecordFiles) {
            total += f.data().length;
        }
        for (InMemoryFile f : signatureFiles) {
            total += f.data().length;
        }
        for (InMemoryFile f : primarySidecarFiles) {
            total += f.data().length;
        }
        for (InMemoryFile f : otherSidecarFiles) {
            total += f.data().length;
        }
        return total;
    }

    @Override
    public @NonNull String toString() {
        return String.format(
                "-- RecordFileSet @ %-32s :: primary=%b, signatures=%2d%s%s",
                recordFileTime,
                primaryRecordFile != null,
                signatureFiles.size(),
                primarySidecarFiles.isEmpty() ? "" : ", primary sidecars=" + primarySidecarFiles.size(),
                otherRecordFiles.isEmpty() ? "" : ", other record files=" + otherRecordFiles.size());
    }

    public Instant recordFileTime() {
        return recordFileTime;
    }

    public InMemoryFile primaryRecordFile() {
        return primaryRecordFile;
    }

    public List<InMemoryFile> otherRecordFiles() {
        return otherRecordFiles;
    }

    public List<InMemoryFile> signatureFiles() {
        return signatureFiles;
    }

    public List<InMemoryFile> primarySidecarFiles() {
        return primarySidecarFiles;
    }

    public List<InMemoryFile> otherSidecarFiles() {
        return otherSidecarFiles;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        var that = (RecordFileBlock) obj;
        return Objects.equals(this.recordFileTime, that.recordFileTime)
                && Objects.equals(this.primaryRecordFile, that.primaryRecordFile)
                && Objects.equals(this.otherRecordFiles, that.otherRecordFiles)
                && Objects.equals(this.signatureFiles, that.signatureFiles)
                && Objects.equals(this.primarySidecarFiles, that.primarySidecarFiles)
                && Objects.equals(this.otherSidecarFiles, that.otherSidecarFiles);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                recordFileTime,
                primaryRecordFile,
                otherRecordFiles,
                signatureFiles,
                primarySidecarFiles,
                otherSidecarFiles);
    }

    // === Protected Methods ===========================================================================================

    /**
     * Validate all signature files against the computed entire file hash using RSA public keys from the address book.
     * Make sure we have 1/3 of all nodes in address book have signed with valid signatures.
     *
     * @param addressBook     the address book containing node RSA public keys; may be null to skip signature
     *                        verification
     * @param warningMessages a StringBuilder to append any warning messages to
     * @param recordFileSignedHash  the computed 48-byte SHA-384 hash that is signed for this version record file
     * @return the updated validity state after checking all signatures
     * @throws IOException if an I/O error occurs reading a signature file
     */
    protected boolean validateSignatures(
        NodeAddressBook addressBook, StringBuffer warningMessages, byte[] recordFileSignedHash)
        throws IOException {
        if (addressBook != null && !signatureFiles().isEmpty()) {
            try {
                final long validSignatureCount = signatureFiles().stream().parallel()
                    .map(sigFile -> new ParsedSignatureFile(addressBook, sigFile))
                    .filter(sf -> sf.isValid(recordFileSignedHash))
                    .count();
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
            } catch (Exception e) {
                warningMessages.append("Error validating signatures: "+e.getMessage()+"\n");
                return false;
            }
        }
        return false;
    }
}
