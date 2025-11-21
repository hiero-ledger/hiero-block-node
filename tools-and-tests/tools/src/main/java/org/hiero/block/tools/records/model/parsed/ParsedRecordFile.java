// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.records.model.parsed;

import static org.hiero.block.tools.records.RecordFileDates.extractRecordFileTime;
import static org.hiero.block.tools.records.RecordFileDates.instantToRecordFileName;
import static org.hiero.block.tools.records.model.parsed.SerializationV5Utils.HASH_OBJECT_SIZE_BYTES;
import static org.hiero.block.tools.records.model.parsed.SerializationV5Utils.readV5HashObject;
import static org.hiero.block.tools.utils.Sha384.SHA_384_HASH_SIZE;
import static org.hiero.block.tools.utils.Sha384.hashSha384;

import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.hapi.node.base.Transaction;
import com.hedera.hapi.node.transaction.TransactionRecord;
import com.hedera.hapi.streams.HashAlgorithm;
import com.hedera.hapi.streams.HashObject;
import com.hedera.hapi.streams.RecordStreamFile;
import com.hedera.hapi.streams.RecordStreamItem;
import com.hedera.pbj.runtime.io.buffer.BufferedData;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.zip.GZIPOutputStream;
import org.hiero.block.tools.records.model.unparsed.InMemoryFile;
import org.hiero.block.tools.utils.PrettyPrint;

/**
 * Universal record file record object that handles the parsed form of all versions of record files.
 * <p>
 * The old record file formats are documented in the
 * <a href="https://github.com/search?q=repo%3Ahashgraph%2Fhedera-mirror-node%20%22implements%20RecordFileReader%22&type=code">
 * Mirror Node code.</a> and in the legacy documentation on
 * <a href="http://web.archive.org/web/20221006192449/https://docs.hedera.com/guides/docs/record-and-event-stream-file-formats">
 * Way Back Machine</a>
 * </p>
 *
 * @param blockTime the block time, this is from the record file name
 * @param recordFormatVersion the record file format version
 * @param hapiProtoVersion the HAPI protocol version
 * @param previousBlockHash the hash of the previous block
 * @param blockHash the block hash
 * @param recordFileContents the record file contents, the complete file as byte[]
 * @param recordStreamFile the record stream file, read directly from V6, converted from V2 and V5
 * @param numOfSidecarFiles the number of sidecar files
 */
@SuppressWarnings({"DuplicatedCode", "DataFlowIssue"})
public record ParsedRecordFile(
        Instant blockTime,
        int recordFormatVersion,
        SemanticVersion hapiProtoVersion,
        byte[] previousBlockHash,
        byte[] blockHash,
        byte[] recordFileContents,
        RecordStreamFile recordStreamFile,
        int numOfSidecarFiles) {
    /** The length of the header in a v2 record file */
    private static final int V2_HEADER_LENGTH = Integer.BYTES + Integer.BYTES + 1 + 48;
    /** The class ID for the record stream object in V5 */
    public static final long V5_RECORD_STREAM_OBJECT_CLASS_ID = Long.parseUnsignedLong("e370929ba5429d8b", 16);
    /** The class version for the record stream object in V5 */
    public static final int V5_RECORD_STREAM_OBJECT_CLASS_VERSION = 1;

    public void write(Path directory, boolean gzipped) throws IOException {
        String fileName = instantToRecordFileName(blockTime) + (gzipped ? ".gz" : "");
        Path recordFilePath = directory.resolve(fileName);
        try (WritableStreamingData out = new WritableStreamingData(
                gzipped
                        ? new GZIPOutputStream(Files.newOutputStream(
                                recordFilePath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE))
                        : Files.newOutputStream(
                                recordFilePath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE))) {
            // write the version number
            out.writeInt(recordFormatVersion);
            switch (recordFormatVersion) {
                case 2 -> {}
                case 5 -> {}
                case 6 -> {}
                default ->
                    throw new UnsupportedOperationException(
                            "Unsupported record format version: " + recordFormatVersion);
            }
        }
    }

    /**
     * Parses the record file to extract the HAPI protocol version and the block hash.
     *
     * @param recordFileData the record file bytes and name to parse
     * @return the record file version info
     */
    @SuppressWarnings("unused")
    public static ParsedRecordFile parse(InMemoryFile recordFileData) {
        try {
            final Instant blockTime =
                    extractRecordFileTime(recordFileData.path().getFileName().toString());
            final byte[] recordFileBytes = recordFileData.data();
            final BufferedData in = BufferedData.wrap(recordFileBytes);
            final int recordFormatVersion = in.readInt();
            // This is a minimal parser for all record file formats only extracting the necessary information
            return switch (recordFormatVersion) {
                case 2 -> {
                    final int hapiMajorVersion = in.readInt();
                    final SemanticVersion hapiVersion = new SemanticVersion(hapiMajorVersion, 0, 0, null, null);
                    final byte previousFileHashMarker = in.readByte();
                    if (previousFileHashMarker != 1) {
                        throw new IllegalStateException("Invalid previous file hash marker in v2 record file");
                    }
                    // read staring hash which is also the previous block ending hash
                    final byte[] previousHash = new byte[SHA_384_HASH_SIZE];
                    in.readBytes(previousHash);
                    // Compute the block hash, for v2 files is the `hash(header, hash(content))` this is different to
                    // other versions the block hash is not available in the file, so we have to calculate it
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
                    // build a RecordStreamFile
                    final RecordStreamFile recordStreamFile = new RecordStreamFile(
                            hapiVersion,
                            new HashObject(HashAlgorithm.SHA_384, SHA_384_HASH_SIZE, Bytes.wrap(previousHash)),
                            recordStreamItems,
                            null, // V2 record files do not have a streaming hash, so there is no end hash
                            -1, // V2 record files do not have block numbers
                            Collections.emptyList() // V2 record files do not have sidecars
                            );
                    yield new ParsedRecordFile(
                            blockTime,
                            recordFormatVersion,
                            hapiVersion,
                            previousHash,
                            blockHash,
                            recordFileBytes,
                            recordStreamFile,
                            0);
                }
                case 5 -> {
                    // compute the entire file hash used for signature verification
                    final byte[] blockHash = hashSha384(recordFileBytes);
                    // read object stream version
                    final int hapiMajorVersion = in.readInt();
                    final int hapiMinorVersion = in.readInt();
                    final int hapiPatchVersion = in.readInt();
                    final SemanticVersion hapiVersion =
                            new SemanticVersion(hapiMajorVersion, hapiMinorVersion, hapiPatchVersion, null, null);
                    // read object stream version
                    final int objectStreamVersion = in.readInt();
                    if (objectStreamVersion != V5_RECORD_STREAM_OBJECT_CLASS_VERSION) {
                        throw new IllegalStateException(
                                "Unexpected object stream version (v5): " + objectStreamVersion);
                    }
                    // Start Object Running Hash is a Hash Object; parse to extract SHA-384 bytes
                    final byte[] startObjectRunningHash = readV5HashObject(in);
                    // read all record stream objects and build the RecordStreamItem list
                    final List<RecordStreamItem> recordStreamItems = new java.util.ArrayList<>();
                    while (in.remaining() > HASH_OBJECT_SIZE_BYTES) {
                        // read a RecordStreamObject
                        final long classId = in.readLong();
                        if (classId != V5_RECORD_STREAM_OBJECT_CLASS_ID) {
                            throw new IOException("Unexpected class ID in record file: " + Long.toHexString(classId));
                        }
                        final int classVersion = in.readInt();
                        if (classVersion != V5_RECORD_STREAM_OBJECT_CLASS_VERSION) {
                            throw new IOException("Unexpected class version in record file: " + classVersion);
                        }
                        final int transactionRecordLength = in.readInt();
                        if (transactionRecordLength <= 0
                                || transactionRecordLength > in.remaining() - HASH_OBJECT_SIZE_BYTES) {
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
                    // read the end-running hash
                    final byte[] endObjectRunningHash = readV5HashObject(in);
                    // build the RecordStreamFile model used by block stream
                    final RecordStreamFile recordStreamFile = new RecordStreamFile(
                            hapiVersion,
                            new HashObject(
                                    HashAlgorithm.SHA_384, SHA_384_HASH_SIZE, Bytes.wrap(startObjectRunningHash)),
                            recordStreamItems,
                            new HashObject(HashAlgorithm.SHA_384, SHA_384_HASH_SIZE, Bytes.wrap(endObjectRunningHash)),
                            -1, // V5 does not support block numbers
                            Collections.emptyList() // v5 files do not have sidecars
                            );
                    yield new ParsedRecordFile(
                            blockTime,
                            recordFormatVersion,
                            hapiVersion,
                            startObjectRunningHash,
                            endObjectRunningHash, // TODO check is running hash the block hash?
                            recordFileBytes,
                            recordStreamFile,
                            0); // V5 doesn't support sidecars
                }
                case 6 -> {
                    // V6 is nice and easy as it is all protobuf encoded after the first version integer
                    final RecordStreamFile recordStreamFile = RecordStreamFile.PROTOBUF.parse(in);
                    // For v6 the block hash is the end-running-hash accessed via endObjectRunningHash()
                    if (recordStreamFile.endObjectRunningHash() == null) {
                        throw new IllegalStateException("No end object running hash in record file");
                    }
                    yield new ParsedRecordFile(
                            blockTime,
                            recordFormatVersion,
                            recordStreamFile.hapiProtoVersion(),
                            recordStreamFile.startObjectRunningHash().hash().toByteArray(),
                            recordStreamFile.endObjectRunningHash().hash().toByteArray(),
                            recordFileBytes,
                            recordStreamFile,
                            recordStreamFile.sidecars().size());
                }
                default ->
                    throw new UnsupportedOperationException(
                            "Unsupported record format version: " + recordFormatVersion);
            };
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Pretty prints the record file info.
     *
     * @return the pretty string
     */
    public String prettyToString() {
        return "UniversalRecordFile{\n" + "       recordFormatVersion    = "
                + recordFormatVersion + "\n" + "       hapiProtoVersion       = "
                + hapiProtoVersion + "\n" + "       previousBlockHash      = "
                + HexFormat.of().formatHex(previousBlockHash).substring(0, 8) + "\n"
                + "       blockHash              = "
                + HexFormat.of().formatHex(blockHash).substring(0, 8) + "\n" + "       numOfTransactions = "
                + recordStreamFile.recordStreamItems().stream()
                        .filter(RecordStreamItem::hasTransaction)
                        .count() + "\n" + "       numOfSidecarFiles              = "
                + numOfSidecarFiles + "\n" + "       recordFileContentsSize = "
                + PrettyPrint.prettyPrintFileSize(recordFileContents.length) + "\n" + '}';
    }
}
