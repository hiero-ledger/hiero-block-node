// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.records;

import static org.hiero.block.tools.commands.record2blocks.model.ParsedSignatureFile.HASH_OBJECT_SIZE_BYTES;
import static org.hiero.block.tools.commands.record2blocks.model.ParsedSignatureFile.readHashObject;

import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.hapi.streams.RecordStreamFile;
import com.hedera.pbj.runtime.io.stream.ReadableStreamingData;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.security.MessageDigest;

/**
 * Class for computing the SHA384 hash for the record file.
 */
public class RecordFileHasher {
    /* The length of the header in a v2 record file */
    private static final int V2_HEADER_LENGTH = Integer.BYTES + Integer.BYTES + 1 + 48;

    /**
     * Computes the SHA384 hash for the record file, the way in which the hash is calculated depends on the record file
     * format version.
     *
     * @param recordFile the record file bytes to hash
     * @return the record file version info
     */
    public static byte[] hash(byte[] recordFile) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(recordFile))) {
            final int recordFormatVersion = in.readInt();
            // This is a minimal parser for all record file formats only extracting the necessary information
            return switch (recordFormatVersion) {
                case 2 -> {
                    final int hapiMajorVersion = in.readInt();
                    final SemanticVersion hapiProtoVersion = new SemanticVersion(hapiMajorVersion, 0, 0, null, null);
                    final byte previousFileHashMarker = in.readByte();
                    if (previousFileHashMarker != 1) {
                        throw new IllegalStateException("Invalid previous file hash marker in v2 record file");
                    }
                    final byte[] previousHash = new byte[48];
                    in.readFully(previousHash);
                    // The hash for v2 files is the hash(header, hash(content)) this is different to other versions
                    // the block hash is not available in the file so we have to calculate it
                    MessageDigest digest = MessageDigest.getInstance("SHA-384");
                    digest.update(recordFile, V2_HEADER_LENGTH, recordFile.length - V2_HEADER_LENGTH);
                    final byte[] contentHash = digest.digest();
                    digest.update(recordFile, 0, V2_HEADER_LENGTH);
                    digest.update(contentHash);
                    yield new byte[0];
                }
                case 5 -> {
                    final int hapiMajorVersion = in.readInt();
                    final int hapiMinorVersion = in.readInt();
                    final int hapiPatchVersion = in.readInt();
                    final SemanticVersion hapiProtoVersion =
                            new SemanticVersion(hapiMajorVersion, hapiMinorVersion, hapiPatchVersion, null, null);
                    final int objectStreamVersion = in.readInt();
                    byte[] startObjectRunningHash = new byte[48];
                    in.readFully(startObjectRunningHash);

                    // skip to last hash object. This trick allows us to not have to understand the format for record
                    // file items and their contents which is much more complicated. For v5 and v6 the block hash is the
                    // end running hash which is written as a special item at the end of the file.
                    in.skipBytes(in.available() - HASH_OBJECT_SIZE_BYTES);
                    final byte[] endHashObject = readHashObject(in);
                    yield new byte[0];
                }
                case 6 -> {
                    // V6 is nice and easy as it is all protobuf encoded after the first version integer
                    final RecordStreamFile recordStreamFile =
                            RecordStreamFile.PROTOBUF.parse(new ReadableStreamingData(in));
                    // For v6 the block hash is the end running hash which is accessed via endObjectRunningHash()
                    if (recordStreamFile.endObjectRunningHash() == null) {
                        throw new IllegalStateException("No end object running hash in record file");
                    }
                    yield new byte[0];
                }
                default ->
                    throw new UnsupportedOperationException(
                            "Unsupported record format version: " + recordFormatVersion);
            };
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
