// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.verifier;

import static com.hedera.pbj.runtime.ProtoConstants.TAG_WIRE_TYPE_MASK;

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
import org.hiero.block.common.hasher.HashingUtilities;

/// Computes the record-file signed payload for a Wrapped Record Block (WRB) proof, for every
/// record file format version that ever existed on mainnet: 2, 5 and 6.
///
/// The consensus nodes signed each record file's hash with their RSA keys, but the bytes that
/// were hashed depend on the record file format version the file was originally produced in.
/// A wrapped record block always carries the file content normalized to the version 6 protobuf
/// shape (`proto.RecordStreamFile`, see `block/stream/record_file_item.proto`),
/// so for versions 2 and 5 the original legacy binary file must first be reconstructed,
/// losslessly, from that protobuf before hashing. The signed payload per version is:
///
/// - **Version 6**: `SHA-384(int32be(6) || recordFileContents)` over the verbatim
///     protobuf bytes (delegates to [HashingUtilities#computeV6SignedPayload(Bytes)]).
/// - **Version 5**: reconstruct the legacy v5 binary file `int32(5) || hapiMajor ||
///   hapiMinor || hapiPatch || int32(objectStreamVersion=1) || startRunningHash as
///   v5 HashObject || per item [classId || classVersion || recordLen+recordBytes ||
///   transactionLen+transactionBytes] || endRunningHash as v5HashObject` and take a
///   single `SHA-384` of the whole reconstruction.
/// - **Version 2**: reconstruct the legacy v2 binary file `int32(2) ||
///   int32(hapiMinor) || byte(0x01) || 48-byte previous file hash(the start running
///   hash) || per item [byte(0x02) || transactionLen+transactionBytes ||
///   recordLen+recordBytes]` and take the double hash `SHA-384(first 57 header bytes ||
///   SHA-384(bytes after the 57-byte header))`.
///
/// The reconstruction copies raw byte spans navigated directly out of the protobuf wire format.
/// It never re-serializes parsed `Transaction` or `TransactionRecord` messages,
/// because a re-serialization could produce subtly different bytes and a diverging hash.
///
/// For version 2, the wrap CLI stores the original single-int HAPI version of the legacy file
/// in `BlockHeader.hapiProtoVersion.minor`, which is why the v2 header int is
/// `hapiProtoVersion.minor()`.
///
/// Sources of truth: the reconstruction pseudocode in
/// `protobuf-sources/src/main/proto-overrides/block/stream/record_file_item.proto` and the
/// mainnet-validated implementation in the tools module
/// (`org.hiero.block.tools.blocks.validation.SignatureDataExtractor` and
/// `org.hiero.block.tools.records.model.parsed.ParsedRecordFile`).
public final class RecordFileSignedPayload {

    // ---- Legacy v2 binary format constants ----

    /// The v2 marker byte written before the previous file hash.
    private static final byte V2_PREVIOUS_FILE_HASH_MARKER = 1;
    /// The v2 marker byte written before each transaction and record pair.
    private static final byte V2_RECORD_MARKER = 2;
    /// The length of the v2 file header: version int, HAPI version int, previous file hash
    /// marker byte and the 48-byte previous file hash. The v2 signed hash is a double hash
    /// split exactly at this offset.
    private static final int V2_HEADER_LENGTH = Integer.BYTES + Integer.BYTES + 1 + HashingUtilities.HASH_SIZE;

    // ---- Legacy v5 binary format constants ----

    /// The serialization class id of a v5 record stream object (a transaction and record pair).
    private static final long V5_RECORD_STREAM_OBJECT_CLASS_ID = Long.parseUnsignedLong("e370929ba5429d8b", 16);
    /// The serialization class version of a v5 record stream object.
    private static final int V5_RECORD_STREAM_OBJECT_CLASS_VERSION = 1;
    /// The object stream version int written in the v5 file header.
    private static final int V5_OBJECT_STREAM_VERSION = 1;
    /// The serialization class id of a v5 hash object.
    private static final long V5_HASH_OBJECT_CLASS_ID = Long.parseUnsignedLong("f422da83a251741e", 16);
    /// The serialization class version of a v5 hash object.
    private static final int V5_HASH_OBJECT_CLASS_VERSION = 1;
    /// The digest type id for SHA-384 in a v5 hash object.
    private static final int V5_DIGEST_TYPE_SHA384 = 0x58ff811b;

    // ---- Protobuf wire tags (field_number << 3 | wire_type) ----

    /// `RecordStreamFile.start_object_running_hash` (field 2, LEN).
    private static final int RSF_START_RUNNING_HASH_TAG = 18;
    /// `RecordStreamFile.record_stream_items` (field 3, LEN).
    private static final int RSF_RECORD_STREAM_ITEMS_TAG = 26;
    /// `RecordStreamFile.end_object_running_hash` (field 4, LEN).
    private static final int RSF_END_RUNNING_HASH_TAG = 34;
    /// `HashObject.hash` (field 3, LEN).
    private static final int HASH_OBJECT_HASH_TAG = 26;
    /// `RecordStreamItem.transaction` (field 1, LEN).
    private static final int RSI_TRANSACTION_TAG = 10;
    /// `RecordStreamItem.record` (field 2, LEN).
    private static final int RSI_RECORD_TAG = 18;

    /// The raw components of the record stream file needed to reconstruct a legacy binary file.
    private record ExtractedRecordData(Bytes startRunningHash, Bytes endRunningHash, List<RawRecordStreamItem> items) {}

    /// Raw transaction and record byte spans of a single `RecordStreamItem`.
    private record RawRecordStreamItem(Bytes transactionBytes, Bytes recordBytes) {}

    private RecordFileSignedPayload() {
        throw new UnsupportedOperationException("Utility Class");
    }

    /// Returns `true` when the given record file format version is one this class can
    /// compute a signed payload for, i.e. one of the versions that ever existed on mainnet:
    /// 2, 5 and 6.
    ///
    /// @param version the record file format version declared by a proof
    /// @return `true` when the version is supported
    public static boolean isSupportedVersion(final int version) {
        return version == 2 || version == 5 || version == 6;
    }

    /// Computes the 48-byte signed payload for the given record file format version, i.e. the
    /// exact bytes the consensus nodes signed with `SHA384withRSA` for this record file.
    ///
    /// Returns `null` when the record file contents are structurally missing a component
    /// that is mandatory for the requested version: the start running hash (versions 2 and 5)
    /// or the end running hash (version 5), including hashes that are not exactly 48 bytes.
    /// An empty record stream item list is legal and reconstructs to a header-and-hashes-only
    /// file.
    ///
    /// @param version the record file format version declared by the proof, must be 2, 5 or 6
    /// @param hapiProtoVersion the HAPI protocol version from the block header, must not be null
    /// @param recordFileContents the verbatim `RecordFileItem.record_file_contents` bytes,
    ///     must not be null
    /// @return the 48-byte signed payload, or `null` when mandatory components are missing
    /// @throws ParseException if the record file contents are malformed protobuf wire data
    ///     or if the version is not 2, 5 or 6; callers are expected to
    ///     gate the version before calling
    public static byte[] computeSignedPayload(
            final int version, final SemanticVersion hapiProtoVersion, final Bytes recordFileContents)
            throws ParseException {
        try {
            return switch (version) {
                case 6 -> HashingUtilities.computeV6SignedPayload(recordFileContents);
                case 5 -> computeV5SignedPayload(hapiProtoVersion, extract(recordFileContents));
                case 2 -> computeV2SignedPayload(hapiProtoVersion, extract(recordFileContents));
                default -> throw new ParseException("Unsupported record file format version: " + version);
            };
        } catch (IOException | RuntimeException e) {
            throw new ParseException(e);
        }
    }

    /// Computes the v5 signed payload: a single SHA-384 over the reconstructed legacy v5 binary
    /// file.
    ///
    /// @param hapiProtoVersion the HAPI protocol version from the block header
    /// @param data the raw components extracted from the record stream file protobuf
    /// @return the 48-byte signed payload, or `null` when a running hash is missing
    /// @throws IOException if the wire data is malformed
    private static byte[] computeV5SignedPayload(final SemanticVersion hapiProtoVersion, final ExtractedRecordData data)
            throws IOException {
        final byte[] result;
        if (!isValidHash(data.startRunningHash()) || !isValidHash(data.endRunningHash())) {
            result = null;
        } else {
            final ByteArrayOutputStream bout = new ByteArrayOutputStream();
            try (final WritableStreamingData out = new WritableStreamingData(bout)) {
                out.writeInt(5);
                out.writeInt(hapiProtoVersion.major());
                out.writeInt(hapiProtoVersion.minor());
                out.writeInt(hapiProtoVersion.patch());
                out.writeInt(V5_OBJECT_STREAM_VERSION);
                writeV5HashObject(out, data.startRunningHash());
                for (final RawRecordStreamItem item : data.items()) {
                    out.writeLong(V5_RECORD_STREAM_OBJECT_CLASS_ID);
                    out.writeInt(V5_RECORD_STREAM_OBJECT_CLASS_VERSION);
                    // v5 order: record first, then transaction
                    out.writeInt((int) item.recordBytes().length());
                    item.recordBytes().writeTo(out);
                    out.writeInt((int) item.transactionBytes().length());
                    item.transactionBytes().writeTo(out);
                }
                writeV5HashObject(out, data.endRunningHash());
            }
            result = HashingUtilities.noThrowSha384HashOf(bout.toByteArray());
        }
        return result;
    }

    /// Computes the v2 signed payload: the double hash of the reconstructed legacy v2 binary
    /// file, `SHA-384(header || SHA-384(content))` split at the 57-byte header boundary.
    ///
    /// @param hapiProtoVersion the HAPI protocol version from the block header; its minor
    ///     component carries the original single-int v2 HAPI version
    /// @param data the raw components extracted from the record stream file protobuf
    /// @return the 48-byte signed payload, or `null` when the start running hash is missing
    /// @throws IOException if the wire data is malformed
    private static byte[] computeV2SignedPayload(final SemanticVersion hapiProtoVersion, final ExtractedRecordData data)
            throws IOException {
        final byte[] result;
        if (!isValidHash(data.startRunningHash())) {
            result = null;
        } else {
            final ByteArrayOutputStream bout = new ByteArrayOutputStream();
            try (final WritableStreamingData out = new WritableStreamingData(bout)) {
                out.writeInt(2);
                out.writeInt(hapiProtoVersion.minor());
                out.writeByte(V2_PREVIOUS_FILE_HASH_MARKER);
                // In the v2 format the start running hash is the previous file hash
                data.startRunningHash().writeTo(out);
                for (final RawRecordStreamItem item : data.items()) {
                    out.writeByte(V2_RECORD_MARKER);
                    // v2 order: transaction first, then record
                    out.writeInt((int) item.transactionBytes().length());
                    item.transactionBytes().writeTo(out);
                    out.writeInt((int) item.recordBytes().length());
                    item.recordBytes().writeTo(out);
                }
            }
            result = computeV2DoubleHash(bout.toByteArray());
        }
        return result;
    }

    /// Computes the v2 double hash of a reconstructed v2 file:
    /// `SHA-384(header || SHA-384(content))` where the header is the first
    /// [#V2_HEADER_LENGTH] bytes and the content is everything after it.
    ///
    /// @param recordFileBytes the reconstructed v2 file bytes
    /// @return the 48-byte double hash
    private static byte[] computeV2DoubleHash(final byte[] recordFileBytes) {
        final MessageDigest digest = HashingUtilities.sha384DigestOrThrow();
        digest.update(recordFileBytes, V2_HEADER_LENGTH, recordFileBytes.length - V2_HEADER_LENGTH);
        final byte[] contentHash = digest.digest();
        digest.update(recordFileBytes, 0, V2_HEADER_LENGTH);
        digest.update(contentHash);
        return digest.digest();
    }

    /// Writes a hash as a legacy v5 hash object: class id, class version, digest type,
    /// hash length and the hash bytes.
    ///
    /// @param out the output to write to
    /// @param hash the 48-byte hash to write
    private static void writeV5HashObject(final WritableStreamingData out, final Bytes hash) {
        out.writeLong(V5_HASH_OBJECT_CLASS_ID);
        out.writeInt(V5_HASH_OBJECT_CLASS_VERSION);
        out.writeInt(V5_DIGEST_TYPE_SHA384);
        out.writeInt(HashingUtilities.HASH_SIZE);
        hash.writeTo(out);
    }

    /// Returns `true` when the given hash is present and exactly 48 bytes long.
    ///
    /// @param hash the hash to check, may be null
    /// @return `true` when the hash can be used in a legacy reconstruction
    private static boolean isValidHash(final Bytes hash) {
        return hash != null && hash.length() == HashingUtilities.HASH_SIZE;
    }

    /// Extracts the raw components needed for a legacy reconstruction by navigating the
    /// `RecordStreamFile` protobuf wire format directly: the start and end running hashes
    /// and the raw transaction and record byte spans of every record stream item. The message is
    /// deliberately not deserialized, so the copied spans stay byte-for-byte identical to what
    /// the consensus node serialized and hashed.
    ///
    /// @param recordStreamFileBytes the verbatim `record_file_contents` bytes
    /// @return the extracted components; a missing running hash is left `null`
    private static ExtractedRecordData extract(final Bytes recordStreamFileBytes) throws IOException {
        Bytes startRunningHash = null;
        Bytes endRunningHash = null;
        final List<RawRecordStreamItem> items = new ArrayList<>();
        final ReadableSequentialData input = recordStreamFileBytes.toReadableSequentialData();
        while (input.hasRemaining()) {
            final int tag = readTag(input);
            if (tag == -1) {
                break;
            } else {
                switch (tag) {
                    case RSF_START_RUNNING_HASH_TAG -> {
                        final int length = input.readVarInt(false);
                        startRunningHash = extractHashFromHashObject(input.view(length));
                    }
                    case RSF_RECORD_STREAM_ITEMS_TAG -> {
                        final int length = input.readVarInt(false);
                        items.add(extractRawRecordStreamItem(input.view(length)));
                    }
                    case RSF_END_RUNNING_HASH_TAG -> {
                        final int length = input.readVarInt(false);
                        endRunningHash = extractHashFromHashObject(input.view(length));
                    }
                    default -> skipTaggedField(input, tag);
                }
            }
        }
        return new ExtractedRecordData(startRunningHash, endRunningHash, items);
    }

    /// Extracts the hash bytes from a `HashObject` protobuf message.
    ///
    /// @param input the message bytes to navigate
    /// @return the hash bytes, or `null` when the hash field is absent
    /// @throws IOException if the wire data is malformed
    private static Bytes extractHashFromHashObject(final ReadableSequentialData input) throws IOException {
        Bytes result = null;
        while (result == null && input.hasRemaining()) {
            final int tag = readTag(input);
            if (tag == -1) {
                break;
            } else if (tag == HASH_OBJECT_HASH_TAG) {
                final int length = input.readVarInt(false);
                final byte[] hash = new byte[length];
                input.readBytes(hash);
                result = Bytes.wrap(hash);
            } else {
                skipTaggedField(input, tag);
            }
        }
        return result;
    }

    /// Extracts the raw transaction and record byte spans from a `RecordStreamItem`
    /// protobuf message.
    ///
    /// @param input the message bytes to navigate
    /// @return the raw spans; an absent field is represented as [Bytes#EMPTY]
    /// @throws IOException if the wire data is malformed
    private static RawRecordStreamItem extractRawRecordStreamItem(final ReadableSequentialData input)
            throws IOException {
        Bytes transactionBytes = Bytes.EMPTY;
        Bytes recordBytes = Bytes.EMPTY;
        while (input.hasRemaining()) {
            final int tag = readTag(input);
            if (tag == -1) {
                break;
            } else {
                switch (tag) {
                    case RSI_TRANSACTION_TAG -> {
                        final int length = input.readVarInt(false);
                        final byte[] bytes = new byte[length];
                        input.readBytes(bytes);
                        transactionBytes = Bytes.wrap(bytes);
                    }
                    case RSI_RECORD_TAG -> {
                        final int length = input.readVarInt(false);
                        final byte[] bytes = new byte[length];
                        input.readBytes(bytes);
                        recordBytes = Bytes.wrap(bytes);
                    }
                    default -> skipTaggedField(input, tag);
                }
            }
        }
        return new RawRecordStreamItem(transactionBytes, recordBytes);
    }

    /// Reads the next protobuf tag varint.
    ///
    /// @param input the input to read from
    /// @return the tag, or `-1` on end of input
    private static int readTag(final ReadableSequentialData input) {
        final int result;
        if (!input.hasRemaining()) {
            result = -1;
        } else {
            int tag;
            try {
                tag = input.readVarInt(false);
            } catch (final EOFException e) {
                tag = -1;
            }
            result = tag;
        }
        return result;
    }

    /// Skips a field value based on the wire type encoded in its tag.
    ///
    /// @param input the input to advance
    /// @param tag the tag whose field value must be skipped
    /// @throws IOException if the wire data is malformed
    private static void skipTaggedField(final ReadableSequentialData input, final int tag) throws IOException {
        final int wireType = tag & TAG_WIRE_TYPE_MASK;
        ProtoParserTools.skipField(input, ProtoConstants.get(wireType));
    }
}
