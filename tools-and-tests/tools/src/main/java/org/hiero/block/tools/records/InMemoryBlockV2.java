package org.hiero.block.tools.records;

import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.SemanticVersion;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@SuppressWarnings("DuplicatedCode")
public class InMemoryBlockV2 extends InMemoryBlock {
    /* The length of the header in a v2 record file */
    private static final int V2_HEADER_LENGTH = Integer.BYTES + Integer.BYTES + 1 + 48;

    protected InMemoryBlockV2(Instant recordFileTime, InMemoryFile primaryRecordFile,
        List<InMemoryFile> otherRecordFiles, List<InMemoryFile> signatureFiles,
        List<InMemoryFile> primarySidecarFiles, List<InMemoryFile> otherSidecarFiles) {
        super(recordFileTime, primaryRecordFile, otherRecordFiles, signatureFiles, primarySidecarFiles,
            otherSidecarFiles);
    }


    @Override
    public ValidationResult validate(byte[] startRunningHash, NodeAddressBook addressBook) {
        final byte[] recordFileBytes = primaryRecordFile().data();
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(recordFileBytes))) {
            boolean isValid = true;
            final StringBuilder warningMessages = new StringBuilder();
            final int hapiMajorVersion = in.readInt();
            final SemanticVersion hapiVersion = new SemanticVersion(hapiMajorVersion, 0, 0, null, null);
            final byte previousFileHashMarker = in.readByte();
            if (previousFileHashMarker != 1) {
                throw new IllegalStateException("Invalid previous file hash marker in v2 record file");
            }
            final byte[] previousHash = new byte[48];
            in.readFully(previousHash);
            // check the start running hash is the same as the previous hash
            if (!Arrays.equals(startRunningHash, previousHash)) {
                isValid = false;
                warningMessages.append("Start running hash does not match previous hash in v2 record file\n");
            }
            // The hash for v2 files is the hash(header, hash(content)) this is different to other versions
            // the block hash is not available in the file so we have to calculate it
            MessageDigest digest = MessageDigest.getInstance("SHA-384");
            digest.update(recordFileBytes, V2_HEADER_LENGTH, recordFileBytes.length - V2_HEADER_LENGTH);
            final byte[] contentHash = digest.digest();
            digest.update(recordFileBytes, 0, V2_HEADER_LENGTH);
            digest.update(contentHash);
            return new ValidationResult(isValid, warningMessages.toString(), contentHash, hapiVersion, Collections.emptyList());
        } catch (IOException | NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
