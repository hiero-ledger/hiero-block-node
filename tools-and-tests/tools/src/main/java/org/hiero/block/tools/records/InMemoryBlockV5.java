// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.records;

import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.hapi.node.base.Transaction;
import com.hedera.hapi.node.transaction.TransactionBody;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.stream.ReadableStreamingData;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import org.hiero.block.tools.commands.days.model.AddressBookRegistry;

/**
 * In-memory representation and validator for version 5 Hedera record stream files.
 * <p>
 * This implementation parses the v5 header, start object running hash, and end object running hash
 * as specified in RecordFileFormat.md. Validation ensures the provided start running hash (when
 * supplied) matches the hash in the file and returns the end-running hash contained in the file.
 * Signature file validation is not performed here (v5 signatures are validated elsewhere if needed).
 */
@SuppressWarnings("DuplicatedCode")
public class InMemoryBlockV5 extends InMemoryBlock {
    private static final long RECORD_STREAM_OBJECT_CLASS_ID = Long.parseLong("e370929ba5429d8b", 16);
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
     * not performed by this method.
     *
     * @param startRunningHash expected start running hash to compare against the Start Object Running Hash; may be null
     * @param addressBook ignored for v5; may be null
     * @return validation result including computed validity and end running hash read from file
     */
    @Override
    public ValidationResult validate(byte[] startRunningHash, NodeAddressBook addressBook) {
        // Parse the v5 record file per RecordFileFormat.md and verify hashes
        final byte[] recordFileBytes = primaryRecordFile().data();
        try (ReadableStreamingData in = new ReadableStreamingData(new ByteArrayInputStream(recordFileBytes))) {
            boolean isValid = true;
            final StringBuilder warnings = new StringBuilder();

            // Version already read by factory, but the file begins with version int (5)
            final int fileVersion = in.readInt();
            if (fileVersion != 5) {
                throw new IllegalStateException("Invalid v5 record file version: " + fileVersion);
            }

            final int hapiMajor = in.readInt();
            final int hapiMinor = in.readInt();
            final int hapiPatch = in.readInt();
            final SemanticVersion hapiVersion = new SemanticVersion(hapiMajor, hapiMinor, hapiPatch, null, null);

            final int objectStreamVersion = in.readInt();
            if (objectStreamVersion != 1) {
                warnings.append("Unexpected object stream version (v5): ")
                        .append(objectStreamVersion)
                        .append('\n');
                isValid = false; // treat as invalid format
            }

            // Start running hash is a Hash Object in v5 format; parse to extract SHA-384 bytes
            final byte[] startHashInFile =
                    org.hiero.block.tools.commands.record2blocks.model.ParsedSignatureFile.readHashObject(in);
            if (startRunningHash != null
                    && startRunningHash.length > 0
                    && !java.util.Arrays.equals(startRunningHash, startHashInFile)) {
                warnings.append("Start running hash does not match provided start hash (v5).\n");
                isValid = false;
            }

            // read all transactions in the file
            final List<Transaction> transactions = new java.util.ArrayList<>();
            while (in.position() < in.limit() - 48) { // 48 bytes for end running hash
                // read a RecordStreamObject
                final long classId = in.readLong();
                if (classId != RECORD_STREAM_OBJECT_CLASS_ID) {
                    warnings.append("Unexpected class ID in record file: ").append(Long.toHexString(classId))
                        .append(" expected f422da83a251741e\n");
                    isValid = false;
                    break; // cannot continue parsing
                }
                final int classVersion = in.readInt();
                if (classVersion != 1) { // expecting transaction object
                    warnings.append("Unexpected class version in record file: ").append(classVersion)
                        .append(" expected 1\n");
                    isValid = false;
                    break; // cannot continue parsing
                }
                final int transactionRecordLength = in.readInt();
                if (transactionRecordLength <= 0 || transactionRecordLength > in.limit() - in.position() - 48) {
                    warnings.append("Invalid transaction record length in record file: ").append(transactionRecordLength).append('\n');
                    isValid = false;
                    break; // cannot continue parsing
                }
                // skip over the transaction record bytes; not needed for this validation
                in.skip(transactionRecordLength);
                // read the transaction bytes length and the transaction bytes
                final int transactionLength = in.readInt();
                if (transactionLength <= 0 || transactionLength > in.limit() - in.position() - 48) {
                    warnings.append("Invalid transaction length in record file: ").append(transactionLength).append('\n');
                    isValid = false;
                    break; // cannot continue parsing
                }
                final Transaction transaction = Transaction.PROTOBUF.parse(in.readBytes(transactionLength));
                transactions.add(transaction);
            }
            // read the end running hash object
            final byte[] endRunningHash =
                    org.hiero.block.tools.commands.record2blocks.model.ParsedSignatureFile.readHashObject(in);
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
