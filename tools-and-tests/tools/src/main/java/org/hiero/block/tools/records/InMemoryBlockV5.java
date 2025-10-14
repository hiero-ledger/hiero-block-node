package org.hiero.block.tools.records;

import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.SemanticVersion;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.List;

public class InMemoryBlockV5 extends InMemoryBlock {
    public InMemoryBlockV5(Instant recordFileTime, InMemoryFile primaryRecordFile,
        List<InMemoryFile> otherRecordFiles, List<InMemoryFile> signatureFiles,
        List<InMemoryFile> primarySidecarFiles, List<InMemoryFile> otherSidecarFiles) {
        super(recordFileTime, primaryRecordFile, otherRecordFiles, signatureFiles, primarySidecarFiles,
            otherSidecarFiles);
    }

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
                warnings.append("Unexpected object stream version (v5): ").append(objectStreamVersion).append('\n');
                isValid = false; // treat as invalid format
            }

            // Start running hash is a Hash Object in v5 format; parse to extract SHA-384 bytes
            final byte[] startHashInFile = org.hiero.block.tools.commands.record2blocks.model.ParsedSignatureFile.readHashObject(in);
            if (startRunningHash != null && startRunningHash.length > 0 && !java.util.Arrays.equals(startRunningHash, startHashInFile)) {
                warnings.append("Start running hash does not match provided start hash (v5).\n");
                isValid = false;
            }

            // Skip forward to the last hash object (End Object Running Hash)
            final int bytesRemaining = in.available();
            if (bytesRemaining < org.hiero.block.tools.commands.record2blocks.model.ParsedSignatureFile.HASH_OBJECT_SIZE_BYTES) {
                throw new IllegalStateException("v5 record file too short to contain end running hash");
            }
            in.skipBytes(bytesRemaining - org.hiero.block.tools.commands.record2blocks.model.ParsedSignatureFile.HASH_OBJECT_SIZE_BYTES);
            final byte[] endRunningHash = org.hiero.block.tools.commands.record2blocks.model.ParsedSignatureFile.readHashObject(in);

            return new ValidationResult(isValid, warnings.toString(), endRunningHash, hapiVersion, java.util.Collections.emptyList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
