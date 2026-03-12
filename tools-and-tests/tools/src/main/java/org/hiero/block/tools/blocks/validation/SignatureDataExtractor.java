// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

import static com.hedera.pbj.runtime.ProtoConstants.TAG_WIRE_TYPE_MASK;
import static org.hiero.block.tools.records.model.parsed.ParsedRecordFile.V2_HEADER_LENGTH;
import static org.hiero.block.tools.records.model.parsed.ParsedRecordFile.V2_PREVIOUS_FILE_HASH_MAKER;
import static org.hiero.block.tools.records.model.parsed.ParsedRecordFile.V2_RECORD_MAKER;
import static org.hiero.block.tools.records.model.parsed.ParsedRecordFile.V5_RECORD_STREAM_OBJECT_CLASS_ID;
import static org.hiero.block.tools.records.model.parsed.ParsedRecordFile.V5_RECORD_STREAM_OBJECT_CLASS_VERSION;
import static org.hiero.block.tools.records.model.parsed.SerializationV5Utils.writeV5HashObject;
import static org.hiero.block.tools.utils.Sha384.hashSha384;
import static org.hiero.block.tools.utils.Sha384.sha384Digest;

import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.ProtoConstants;
import com.hedera.pbj.runtime.ProtoParserTools;
import com.hedera.pbj.runtime.io.ReadableSequentialData;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.hedera.pbj.runtime.io.stream.EOFException;
import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

/**
 * Lightweight protobuf extractor that navigates RecordFileItem bytes to extract
 * only the raw byte spans needed for signature hash computation, avoiding full
 * parsing of Transaction and TransactionRecord objects.
 *
 * <p>This follows the same wire-format navigation pattern as {@link TransferListExtractor},
 * extracting raw byte slices from the protobuf stream instead of fully parsing
 * expensive nested messages.
 */
final class SignatureDataExtractor {

    private SignatureDataExtractor() {} // utility class

    // ---- Protobuf wire tags (field_number << 3 | wire_type) ----

    // RecordFileItem fields
    private static final int RFI_CREATION_TIME = 10; // field 1, LEN
    private static final int RFI_RECORD_FILE_CONTENTS = 18; // field 2, LEN

    // Timestamp fields
    private static final int TS_SECONDS = 8; // field 1, VARINT
    private static final int TS_NANOS = 16; // field 2, VARINT

    // RecordStreamFile fields
    private static final int RSF_START_RUNNING_HASH = 18; // field 2, LEN
    private static final int RSF_RECORD_STREAM_ITEMS = 26; // field 3, LEN
    private static final int RSF_END_RUNNING_HASH = 34; // field 4, LEN

    // HashObject fields
    private static final int HO_HASH = 26; // field 3, LEN

    // RecordStreamItem fields
    private static final int RSI_TRANSACTION = 10; // field 1, LEN
    private static final int RSI_RECORD = 18; // field 2, LEN

    // ---- Data records ----

    /** Extracted data needed for signature hash computation. */
    record SignatureData(
            long creationTimeSeconds,
            int creationTimeNanos,
            byte[] rawRecordStreamFileBytes,
            byte[] startRunningHash,
            byte[] endRunningHash,
            List<RawRecordStreamItem> items) {}

    /** Raw transaction and record bytes from a single RecordStreamItem. */
    record RawRecordStreamItem(byte[] transactionBytes, byte[] recordBytes) {}

    /**
     * Extracts signature data from raw RecordFileItem protobuf bytes.
     *
     * <p>For V6, only the raw RecordStreamFile bytes are captured. For V5/V2,
     * the start/end running hashes and per-item raw transaction/record bytes
     * are also extracted.
     *
     * @param recordFileItemBytes the raw bytes of a RecordFileItem
     * @param recordFormatVersion the record file format version (2, 5, or 6)
     * @return the extracted signature data
     * @throws ParseException if the protobuf structure is malformed
     */
    static SignatureData extract(final Bytes recordFileItemBytes, final int recordFormatVersion) throws ParseException {
        long seconds = 0;
        int nanos = 0;
        byte[] rawRecordStreamFileBytes = null;
        byte[] startRunningHash = null;
        byte[] endRunningHash = null;
        final List<RawRecordStreamItem> items = new ArrayList<>();

        final ReadableSequentialData input = recordFileItemBytes.toReadableSequentialData();
        try {
            while (input.hasRemaining()) {
                final int tag = readTag(input);
                if (tag == -1) break;
                switch (tag) {
                    case RFI_CREATION_TIME -> {
                        final int len = input.readVarInt(false);
                        final ReadableSequentialData sub = input.view(len);
                        while (sub.hasRemaining()) {
                            final int tTag = readTag(sub);
                            if (tTag == -1) break;
                            switch (tTag) {
                                case TS_SECONDS -> seconds = sub.readVarLong(false);
                                case TS_NANOS -> nanos = sub.readVarInt(false);
                                default -> skipTaggedField(sub, tTag);
                            }
                        }
                    }
                    case RFI_RECORD_FILE_CONTENTS -> {
                        final int len = input.readVarInt(false);
                        rawRecordStreamFileBytes = new byte[len];
                        input.readBytes(rawRecordStreamFileBytes);
                        if (recordFormatVersion != 6) {
                            // Navigate into RecordStreamFile for V2/V5 to extract hashes and items
                            final ReadableSequentialData rsfInput =
                                    Bytes.wrap(rawRecordStreamFileBytes).toReadableSequentialData();
                            while (rsfInput.hasRemaining()) {
                                final int rsfTag = readTag(rsfInput);
                                if (rsfTag == -1) break;
                                switch (rsfTag) {
                                    case RSF_START_RUNNING_HASH -> {
                                        final int hLen = rsfInput.readVarInt(false);
                                        final ReadableSequentialData hSub = rsfInput.view(hLen);
                                        startRunningHash = extractHashFromHashObject(hSub);
                                    }
                                    case RSF_RECORD_STREAM_ITEMS -> {
                                        final int iLen = rsfInput.readVarInt(false);
                                        final ReadableSequentialData iSub = rsfInput.view(iLen);
                                        items.add(extractRawRecordStreamItem(iSub));
                                    }
                                    case RSF_END_RUNNING_HASH -> {
                                        final int hLen = rsfInput.readVarInt(false);
                                        final ReadableSequentialData hSub = rsfInput.view(hLen);
                                        endRunningHash = extractHashFromHashObject(hSub);
                                    }
                                    default -> skipTaggedField(rsfInput, rsfTag);
                                }
                            }
                        }
                    }
                    default -> skipTaggedField(input, tag);
                }
            }
        } catch (ParseException e) {
            throw e;
        } catch (Exception e) {
            throw new ParseException(e);
        }

        return new SignatureData(seconds, nanos, rawRecordStreamFileBytes, startRunningHash, endRunningHash, items);
    }

    /**
     * Computes the signed hash from extracted signature data, matching the
     * reconstruction logic in {@link org.hiero.block.tools.records.model.parsed.ParsedRecordFile}.
     *
     * @param version the record file format version (2, 5, or 6)
     * @param hapi the HAPI protocol version (from BlockHeader)
     * @param data the extracted signature data
     * @return the signed hash bytes
     * @throws IOException if an I/O error occurs during hash computation
     */
    static byte[] computeSignedHash(final int version, final SemanticVersion hapi, final SignatureData data)
            throws IOException {
        return switch (version) {
            case 6 -> {
                // V6: SHA-384(int(6) + rawRecordStreamFileBytes)
                final MessageDigest digest = sha384Digest();
                digest.update(new byte[] {0, 0, 0, 6});
                digest.update(data.rawRecordStreamFileBytes());
                yield digest.digest();
            }
            case 5 -> {
                // V5: reconstruct binary format then SHA-384
                try (ByteArrayOutputStream bout = new ByteArrayOutputStream();
                        WritableStreamingData out = new WritableStreamingData(bout)) {
                    out.writeInt(5);
                    out.writeInt(hapi.major());
                    out.writeInt(hapi.minor());
                    out.writeInt(hapi.patch());
                    out.writeInt(V5_RECORD_STREAM_OBJECT_CLASS_VERSION);
                    writeV5HashObject(out, data.startRunningHash());
                    for (final RawRecordStreamItem item : data.items()) {
                        out.writeLong(V5_RECORD_STREAM_OBJECT_CLASS_ID);
                        out.writeInt(V5_RECORD_STREAM_OBJECT_CLASS_VERSION);
                        // V5 order: record first, then transaction
                        out.writeInt(item.recordBytes().length);
                        out.writeBytes(item.recordBytes());
                        out.writeInt(item.transactionBytes().length);
                        out.writeBytes(item.transactionBytes());
                    }
                    writeV5HashObject(out, data.endRunningHash());
                    yield hashSha384(bout.toByteArray());
                }
            }
            case 2 -> {
                // V2: reconstruct binary format then double-hash
                try (ByteArrayOutputStream bout = new ByteArrayOutputStream();
                        WritableStreamingData out = new WritableStreamingData(bout)) {
                    out.writeInt(2);
                    out.writeInt(hapi.major());
                    out.writeByte(V2_PREVIOUS_FILE_HASH_MAKER);
                    out.writeBytes(data.startRunningHash()); // previousBlockHash
                    for (final RawRecordStreamItem item : data.items()) {
                        out.writeByte(V2_RECORD_MAKER);
                        // V2 order: transaction first, then record
                        out.writeInt(item.transactionBytes().length);
                        out.writeBytes(item.transactionBytes());
                        out.writeInt(item.recordBytes().length);
                        out.writeBytes(item.recordBytes());
                    }
                    yield computeV2BlockHash(bout.toByteArray());
                }
            }
            default -> throw new UnsupportedOperationException("Unsupported record format version: " + version);
        };
    }

    // ---- Helper methods ----

    /** Extracts the hash bytes from a HashObject protobuf message. */
    private static byte[] extractHashFromHashObject(final ReadableSequentialData input) throws Exception {
        while (input.hasRemaining()) {
            final int tag = readTag(input);
            if (tag == -1) break;
            if (tag == HO_HASH) {
                final int len = input.readVarInt(false);
                final byte[] hash = new byte[len];
                input.readBytes(hash);
                return hash;
            } else {
                skipTaggedField(input, tag);
            }
        }
        return new byte[0];
    }

    /** Extracts raw transaction and record bytes from a RecordStreamItem. */
    private static RawRecordStreamItem extractRawRecordStreamItem(final ReadableSequentialData input) throws Exception {
        byte[] transactionBytes = new byte[0];
        byte[] recordBytes = new byte[0];
        while (input.hasRemaining()) {
            final int tag = readTag(input);
            if (tag == -1) break;
            switch (tag) {
                case RSI_TRANSACTION -> {
                    final int len = input.readVarInt(false);
                    transactionBytes = new byte[len];
                    input.readBytes(transactionBytes);
                }
                case RSI_RECORD -> {
                    final int len = input.readVarInt(false);
                    recordBytes = new byte[len];
                    input.readBytes(recordBytes);
                }
                default -> skipTaggedField(input, tag);
            }
        }
        return new RawRecordStreamItem(transactionBytes, recordBytes);
    }

    /** Computes the V2 block hash (double-hash: hash(header, hash(content))). */
    private static byte[] computeV2BlockHash(final byte[] recordFileBytes) {
        final MessageDigest digest = sha384Digest();
        digest.update(recordFileBytes, V2_HEADER_LENGTH, recordFileBytes.length - V2_HEADER_LENGTH);
        final byte[] contentHash = digest.digest();
        digest.update(recordFileBytes, 0, V2_HEADER_LENGTH);
        digest.update(contentHash);
        return digest.digest();
    }

    // ---- Protobuf wire format utilities ----

    /** Reads a protobuf tag, returning -1 on EOF. */
    private static int readTag(final ReadableSequentialData input) {
        if (!input.hasRemaining()) return -1;
        try {
            return input.readVarInt(false);
        } catch (EOFException e) {
            return -1;
        }
    }

    /** Skips a field value based on the wire type encoded in the tag. */
    private static void skipTaggedField(final ReadableSequentialData input, final int tag) throws Exception {
        final int wireType = tag & TAG_WIRE_TYPE_MASK;
        ProtoParserTools.skipField(input, ProtoConstants.get(wireType));
    }
}
