// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.records;

import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.SemanticVersion;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.List;

/**
 * In-memory representation and validator for version 5 Hedera record stream files.
 * <p>
 * This implementation parses the v5 header, start object running hash, and end object running hash
 * as specified in RecordFileFormat.md. Validation ensures the provided start running hash (when
 * supplied) matches the hash in the file and returns the end-running hash contained in the file.
 * Signature file validation is not performed here (v5 signatures are validated elsewhere if needed).
 */
public class InMemoryBlockV5 extends InMemoryBlock {
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
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(recordFileBytes))) {
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

            // Skip forward to the last hash object (End Object Running Hash)
            final int bytesRemaining = in.available();
            if (bytesRemaining
                    < org.hiero.block.tools.commands.record2blocks.model.ParsedSignatureFile.HASH_OBJECT_SIZE_BYTES) {
                throw new IllegalStateException("v5 record file too short to contain end running hash");
            }
            in.skipBytes(bytesRemaining
                    - org.hiero.block.tools.commands.record2blocks.model.ParsedSignatureFile.HASH_OBJECT_SIZE_BYTES);
            final byte[] endRunningHash =
                    org.hiero.block.tools.commands.record2blocks.model.ParsedSignatureFile.readHashObject(in);

            return new ValidationResult(
                    isValid, warnings.toString(), endRunningHash, hapiVersion, java.util.Collections.emptyList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
