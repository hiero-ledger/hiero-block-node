// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.records.model.parsed;

import static org.hiero.block.tools.records.RecordFileDates.extractRecordFileTime;
import static org.hiero.block.tools.records.RecordFileDates.instantToRecordFileName;
import static org.hiero.block.tools.records.model.parsed.SerializationV5Utils.HASH_OBJECT_SIZE_BYTES;
import static org.hiero.block.tools.records.model.parsed.SerializationV5Utils.readV5HashObject;
import static org.hiero.block.tools.records.model.parsed.SerializationV5Utils.writeV5HashObject;
import static org.hiero.block.tools.utils.Sha384.SHA_384_HASH_SIZE;
import static org.hiero.block.tools.utils.Sha384.hashSha384;
import static org.hiero.block.tools.utils.Sha384.sha384Digest;

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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteOrder;
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
 * @param signedHash the signed hash of the record file, this is what signature files sign
 * @param recordFileContents the record file contents, the complete file as byte[]
 * @param recordStreamFile the record stream file, read directly from V6, converted from V2 and V5
 */
@SuppressWarnings({"DuplicatedCode", "DataFlowIssue"})
public record ParsedRecordFile(
        Instant blockTime,
        int recordFormatVersion,
        SemanticVersion hapiProtoVersion,
        byte[] previousBlockHash,
        byte[] blockHash,
        byte[] signedHash,
        byte[] recordFileContents,
        RecordStreamFile recordStreamFile) {

    /* The record marker byte in a v2 record file indicating the previous file hash */
    public static final byte V2_PREVIOUS_FILE_HASH_MAKER = 1;
    /* The record marker byte in a v2 record file */
    public static final byte V2_RECORD_MAKER = 2;
    /* The length of the header in a v2 record file */
    public static final int V2_HEADER_LENGTH = Integer.BYTES + Integer.BYTES + V2_PREVIOUS_FILE_HASH_MAKER + 48;
    /** The class ID for the record stream object in V5 */
    public static final long V5_RECORD_STREAM_OBJECT_CLASS_ID = Long.parseUnsignedLong("e370929ba5429d8b", 16);
    /** The class version for the record stream object in V5 */
    public static final int V5_RECORD_STREAM_OBJECT_CLASS_VERSION = V2_PREVIOUS_FILE_HASH_MAKER;

    /**
     * Recomputes the block hash for the record file based on data in file, not on any stored hashes
     *
     * @return the recomputed block hash
     */
    public byte[] recomputeBlockHash() {
        switch (recordFormatVersion) {
            case 2 -> {
                return computeV2BlockHash(recordFileContents);
            }
            case 5, 6 -> {
                return computeV5V6BlockHash(recordStreamFile);
            }
            default ->
                throw new UnsupportedOperationException(
                        "Unsupported record format version: " + recordFormatVersion);
        }
    }

    /**
     * Writes the record file to the specified directory in the appropriate format version.
     *
     * @param directory the directory to write the record file to
     * @param gzipped whether to gzip the record file
     * @throws IOException if an I/O error occurs during writing
     */
    public void write(Path directory, boolean gzipped) throws IOException {
        String fileName = instantToRecordFileName(blockTime) + (gzipped ? ".gz" : "");
        Path recordFilePath = directory.resolve(fileName);
        try (OutputStream out = gzipped
                ? new GZIPOutputStream(
                        Files.newOutputStream(recordFilePath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE))
                : Files.newOutputStream(recordFilePath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            out.write(recordFileContents);
        }
    }

    /**
     * Serializes the record file to the specified writable streaming data in the appropriate format version.
     *
     * @param out the writable streaming data to write to
     * @throws IOException if an I/O error occurs during serialization
     */
    public void write(WritableStreamingData out) throws IOException {
        out.writeBytes(recordFileContents);
    }

    /**
     * Serializes the record file to the specified writable streaming data in the appropriate format version.
     *
     * @param out the writable streaming data to write to
     * @throws IOException if an I/O error occurs during serialization
     */
    private static void reconstructRecordFile(
            WritableStreamingData out,
            int recordFormatVersion,
            SemanticVersion hapiProtoVersion,
            byte[] previousBlockHash,
            RecordStreamFile recordStreamFile)
            throws IOException {
        // write the version number
        out.writeInt(recordFormatVersion);
        switch (recordFormatVersion) {
            case 2 -> {
                // V2 is a bit more complex, need to write the header and then all transactions and records
                // write HAPI major version
                out.writeInt(hapiProtoVersion.major());
                // write previous file hash marker
                out.writeByte(V2_PREVIOUS_FILE_HASH_MAKER);
                // write previous file hash
                out.writeBytes(previousBlockHash);
                // write all record stream items
                for (RecordStreamItem item : recordStreamFile.recordStreamItems()) {
                    // write record marker
                    out.writeByte(V2_RECORD_MAKER);
                    // write transaction
                    out.writeInt(Transaction.PROTOBUF.measureRecord(item.transaction()));
                    Transaction.PROTOBUF.write(item.transaction(), out);
                    // write transaction record
                    out.writeInt(TransactionRecord.PROTOBUF.measureRecord(item.record()));
                    TransactionRecord.PROTOBUF.write(item.record(), out);
                }
            }
            case 5 -> {
                // V5 is a bit more complex, need to write the header and then the object stream
                // write HAPI semantic version
                out.writeInt(hapiProtoVersion.major());
                out.writeInt(hapiProtoVersion.minor());
                out.writeInt(hapiProtoVersion.patch());
                // write object stream version
                out.writeInt(V5_RECORD_STREAM_OBJECT_CLASS_VERSION);
                // write start object running hash
                writeV5HashObject(out, recordStreamFile.startObjectRunningHash());
                // write all record stream items
                for (RecordStreamItem item : recordStreamFile.recordStreamItems()) {
                    // write class ID
                    out.writeLong(V5_RECORD_STREAM_OBJECT_CLASS_ID);
                    // write class version
                    out.writeInt(V5_RECORD_STREAM_OBJECT_CLASS_VERSION);
                    // write transaction record
                    out.writeInt(TransactionRecord.PROTOBUF.measureRecord(item.record()));
                    TransactionRecord.PROTOBUF.write(item.record(), out);
                    // write transaction
                    out.writeInt(Transaction.PROTOBUF.measureRecord(item.transaction()));
                    Transaction.PROTOBUF.write(item.transaction(), out);
                }
                // write end object running hash
                writeV5HashObject(out, recordStreamFile.endObjectRunningHash());
            }
            case 6 ->
                // V6 is easy, just write the protobuf RecordStreamFile
                RecordStreamFile.PROTOBUF.write(recordStreamFile, out);
            default ->
                throw new UnsupportedOperationException("Unsupported record format version: " + recordFormatVersion);
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
                    if (previousFileHashMarker != V2_PREVIOUS_FILE_HASH_MAKER) {
                        throw new IllegalStateException("Invalid previous file hash marker in v2 record file");
                    }
                    // read staring hash which is also the previous block ending hash
                    final byte[] previousHash = new byte[SHA_384_HASH_SIZE];
                    in.readBytes(previousHash);
                    // Compute the block hash, for v2 files is the `hash(header, hash(content))` this is different to
                    // other versions the block hash is not available in the file, so we have to calculate it
                    final byte[] blockHash = computeV2BlockHash(recordFileBytes);
                    // read all the transactions and transaction records
                    final List<RecordStreamItem> recordStreamItems = new ArrayList<>();
                    while (in.hasRemaining()) {
                        final byte recordMarker = in.readByte();
                        if (recordMarker != V2_RECORD_MAKER) {
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
                            -V2_PREVIOUS_FILE_HASH_MAKER, // V2 record files do not have block numbers
                            Collections.emptyList() // V2 record files do not have sidecars
                            );
                    yield new ParsedRecordFile(
                            blockTime,
                            recordFormatVersion,
                            hapiVersion,
                            previousHash,
                            blockHash, // TODO is the block hash on mirror node really the signed hash?
                            blockHash,
                            recordFileBytes,
                            recordStreamFile);
                }
                case 5 -> {
                    // compute the entire file hash used for signature verification
                    final byte[] fileHash = hashSha384(recordFileBytes);
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
                            -V2_PREVIOUS_FILE_HASH_MAKER, // V5 does not support block numbers
                            Collections.emptyList() // v5 files do not have sidecars
                            );
                    yield new ParsedRecordFile(
                            blockTime,
                            recordFormatVersion,
                            hapiVersion,
                            startObjectRunningHash,
                            endObjectRunningHash, // TODO check is running hash the block hash?
                            fileHash,
                            recordFileBytes,
                            recordStreamFile);
                }
                case 6 -> {
                    // compute the entire file hash used for signature verification
                    final byte[] fileHash = hashSha384(recordFileBytes);
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
                            fileHash,
                            recordFileBytes,
                            recordStreamFile);
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
     * Parse a ParsedRecordFile from data available in the block stream. This reconstructs the record file.
     *
     * @param blockTime the block time
     * @param recordFormatVersion the record format version
     * @param hapiProtoVersion the HAPI protocol version
     * @param previousBlockHash the previous block hash
     * @param recordStreamFile the record stream file
     * @return the parsed record file
     */
    public static ParsedRecordFile parse(
            Instant blockTime,
            int recordFormatVersion,
            SemanticVersion hapiProtoVersion,
            byte[] previousBlockHash,
            RecordStreamFile recordStreamFile) {
        // reconstruct the record file bytes
        try (ByteArrayOutputStream bout = new ByteArrayOutputStream();
                WritableStreamingData out = new WritableStreamingData(bout)) {
            reconstructRecordFile(out, recordFormatVersion, hapiProtoVersion, previousBlockHash, recordStreamFile);
            final byte[] recordFileData = bout.toByteArray();
            // compute the signed hash
            final byte[] signedHash =
                    recordFormatVersion == 2 ? computeV2BlockHash(recordFileData) : hashSha384(recordFileData);
            // construct the ParsedRecordFile
            return new ParsedRecordFile(
                    blockTime,
                    recordFormatVersion,
                    hapiProtoVersion,
                    previousBlockHash,
                    recordFormatVersion == 2 ? signedHash :
                        recordStreamFile.endObjectRunningHash() != null
                                ? recordStreamFile.endObjectRunningHash().hash().toByteArray()
                                : new byte[0],
                    signedHash,
                    recordFileData,
                    recordStreamFile);
        } catch (IOException e) {
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
                + recordStreamFile.sidecars().size() + "\n" + "       recordFileContentsSize = "
                + PrettyPrint.prettyPrintFileSize(recordFileContents.length) + "\n" + '}';
    }

    /**
     * Computes the v2 block hash as per v2 record file format.
     *
     * @param recordFileBytes the record file bytes
     * @return the computed block hash
     */
    private static byte[] computeV2BlockHash(byte[] recordFileBytes) {
        // TODO this doesn't seem to match docs tools-and-tests/tools/docs/record-file-format.md
        MessageDigest digest = sha384Digest();
        digest.update(recordFileBytes, V2_HEADER_LENGTH, recordFileBytes.length - V2_HEADER_LENGTH);
        final byte[] contentHash = digest.digest();
        digest.update(recordFileBytes, 0, V2_HEADER_LENGTH);
        digest.update(contentHash);
        return digest.digest();
    }

    /**
     * Computes the V5/V6 block hash (end running hash) by iterating through all record stream items
     * and computing the running hash as per the V5/V6 record file format specification.
     *
     * @param recordStreamFile the record stream file containing items to hash
     * @return the computed block hash (end running hash)
     */
    private static byte[] computeV5V6BlockHash(RecordStreamFile recordStreamFile) {
        // Hash Class ID and Version for combining hashes (from SerializationV5Utils)
        final long HASH_CLASS_ID = 0xf422da83a251741eL;
        final int HASH_CLASS_VERSION = 1;

        // Start with the initial running hash from the file
        byte[] runningHash = recordStreamFile.startObjectRunningHash().hash().toByteArray();

        // Process each record stream item
        for (RecordStreamItem item : recordStreamFile.recordStreamItems()) {
            try {
                // Step 1: Hash the RecordStreamItem
                byte[] itemHash = hashRecordStreamItem(item);

                // Step 2: Combine item hash with running hash to compute new running hash
                runningHash = combineHashesForRunningHash(HASH_CLASS_ID, HASH_CLASS_VERSION, runningHash, itemHash);
            } catch (IOException e) {
                throw new RuntimeException("Error computing hash for record stream item", e);
            }
        }

        return runningHash;
    }

    /**
     * Hashes a single RecordStreamItem according to V5/V6 format specification.
     * Format: ClassID (8 bytes) + ClassVersion (4 bytes) +
     *         RecordLength (4 bytes) + Record + TxnLength (4 bytes) + Transaction
     *
     * @param item the record stream item to hash
     * @return the SHA-384 hash of the item
     * @throws IOException if serialization fails
     */
    private static byte[] hashRecordStreamItem(RecordStreamItem item) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             WritableStreamingData out = new WritableStreamingData(baos)) {
            // Write RecordStreamObject Class ID
            out.writeLong(V5_RECORD_STREAM_OBJECT_CLASS_ID);

            // Write RecordStreamObject Version
            out.writeInt(V5_RECORD_STREAM_OBJECT_CLASS_VERSION);

            // Write TransactionRecord length and data
            out.writeInt(TransactionRecord.PROTOBUF.measureRecord(item.record()));
            TransactionRecord.PROTOBUF.write(item.record(), out);

            // Write Transaction length and data
            out.writeInt(Transaction.PROTOBUF.measureRecord(item.transaction()));
            Transaction.PROTOBUF.write(item.transaction(), out);

            // Hash all the bytes
            return hashSha384(baos.toByteArray());
        }
    }

    /**
     * Combines two hashes to produce a new running hash according to V5/V6 format.
     * Format: HashClassID (8 bytes LE) + HashVersion (4 bytes LE) + PrevHash (48 bytes) +
     *         HashClassID (8 bytes LE) + HashVersion (4 bytes LE) + ItemHash (48 bytes)
     *
     * @param hashClassId the hash class ID
     * @param hashClassVersion the hash class version
     * @param previousRunningHash the previous running hash (48 bytes)
     * @param itemHash the item hash to combine (48 bytes)
     * @return the new running hash (48 bytes)
     * @throws IOException if serialization fails
     */
    private static byte[] combineHashesForRunningHash(
            long hashClassId, int hashClassVersion, byte[] previousRunningHash, byte[] itemHash) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             WritableStreamingData out = new WritableStreamingData(baos)) {
            // Write Hash Class ID (little endian)
            out.writeLong(hashClassId, ByteOrder.LITTLE_ENDIAN);

            // Write Hash Version (little endian)
            out.writeInt(hashClassVersion, ByteOrder.LITTLE_ENDIAN);

            // Write previous running hash (48 bytes)
            out.writeBytes(previousRunningHash);

            // Write Hash Class ID again (little endian)
            out.writeLong(hashClassId, ByteOrder.LITTLE_ENDIAN);

            // Write Hash Version again (little endian)
            out.writeInt(hashClassVersion, ByteOrder.LITTLE_ENDIAN);

            // Write item hash (48 bytes)
            out.writeBytes(itemHash);

            // Hash all the bytes to produce new running hash
            return hashSha384(baos.toByteArray());
        }
    }

}
