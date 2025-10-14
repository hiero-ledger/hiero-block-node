package org.hiero.block.tools.records;

import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.hapi.streams.RecordStreamFile;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.stream.ReadableStreamingData;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;

public class InMemoryBlockV6 extends InMemoryBlock {

    public InMemoryBlockV6(Instant recordFileTime, InMemoryFile primaryRecordFile,
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
            final StringBuilder warnings = new StringBuilder();

            final int fileVersion = in.readInt();
            if (fileVersion != 6) {
                throw new IllegalStateException("Invalid v6 record file version: " + fileVersion);
            }

            // Parse protobuf portion
            final RecordStreamFile rsf = RecordStreamFile.PROTOBUF.parse(new ReadableStreamingData(in));
            final SemanticVersion hapiVersion = rsf.hapiProtoVersion();

            // Compare start running hash
            final byte[] startHashInFile = rsf.startObjectRunningHash().hash().toByteArray();
            if (startRunningHash != null && startRunningHash.length > 0 && !java.util.Arrays.equals(startRunningHash, startHashInFile)) {
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
            final MessageDigest sha384 = MessageDigest.getInstance("SHA-384");
            for (InMemoryFile sc : allSidecars) {
                sha384.reset();
                final byte[] hash = sha384.digest(sc.data());
                providedSidecarHashes.add(HexFormat.of().formatHex(hash));
            }

            final Set<String> expectedSidecarHashes = new HashSet<>();
            if (rsf.sidecars() != null) {
                rsf.sidecars().forEach(meta -> expectedSidecarHashes.add(
                    HexFormat.of().formatHex(meta.hash().hash().toByteArray())));
            }

            if (!expectedSidecarHashes.equals(providedSidecarHashes)) {
                warnings.append("Sidecar hashes do not match metadata (v6). Expected ")
                        .append(expectedSidecarHashes.size()).append(", provided ")
                        .append(providedSidecarHashes.size()).append('\n');
                isValid = false;
            }

            return new ValidationResult(isValid, warnings.toString(), endRunningHash, hapiVersion, java.util.Collections.emptyList());
        } catch (IOException | NoSuchAlgorithmException | ParseException e) {
            throw new RuntimeException(e);
        }
    }
}
