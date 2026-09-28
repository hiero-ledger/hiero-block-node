// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.verifier;

import static com.hedera.pbj.runtime.ProtoConstants.TAG_WIRE_TYPE_MASK;
import static java.lang.System.Logger.Level.WARNING;

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
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.block.verification.session.SessionFailureType;

/// Computes the record-file signed payload for a Wrapped Record Block (WRB) proof, for every
/// record file format version ever used by a production network's record stream: 2, 5 and 6.
/// Earlier format versions (1 and 3) existed but were never used on a current production
/// network, so they are not supported.
///
/// The consensus nodes signed each record file's hash with their RSA keys, but the bytes that
/// were hashed depend on the record file format version the file was originally produced in.
/// A wrapped record block always carries the file content normalized to the version 6 protobuf
/// shape (`proto.RecordStreamFile`, see `block/stream/record_file_item.proto`),
/// so for versions 2 and 5 the original legacy binary file must first be reconstructed,
/// losslessly, from that protobuf before hashing. The signed payload per version is:
///
/// - **Version 6**: `SHA-384(int32be(6) || recordFileContents)` over the verbatim
///     protobuf bytes; this formula must stay byte-for-byte identical to
///     `HashingUtilities.computeV6SignedPayload` in the common module, which the
///     block-signing test signer uses to produce V6 fixtures.
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
/// implementation in the tools module, validated against the real record stream archive
/// (`org.hiero.block.tools.blocks.validation.SignatureDataExtractor` and
/// `org.hiero.block.tools.records.model.parsed.ParsedRecordFile`).
public final class RecordFileSignedPayload {
    /// Logger for the payload computation.
    private static final System.Logger LOGGER = System.getLogger(RecordFileSignedPayload.class.getName());
    /// The standard name of the SHA2 384-bit hash algorithm.
    private static final String HASH_ALGORITHM = "SHA-384";
    /// The size of an SHA-384 hash, in bytes.
    private static final int SHA_384_HASH_SIZE = 48;

    // ---- Legacy v2 binary format constants ----

    /// The v2 marker byte written before the previous file hash.
    private static final byte V2_PREVIOUS_FILE_HASH_MARKER = 1;
    /// The v2 marker byte written before each transaction and record pair.
    private static final byte V2_RECORD_MARKER = 2;
    /// The length of the v2 file header: version int, HAPI version int, previous file hash
    /// marker byte and the 48-byte previous file hash. The v2 signed hash is a double hash
    /// split exactly at this offset.
    private static final int V2_HEADER_LENGTH = Integer.BYTES + Integer.BYTES + 1 + SHA_384_HASH_SIZE;

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

    /// The outcome of the signed payload computation: exactly one of the two components is
    /// non-null. Carries either the computed payload or the failure the block must be refused
    /// with.
    record SignedPayloadResult(byte[] payload, SessionFailureType failure) {
        SignedPayloadResult {
            if ((payload == null && failure == null) || (payload != null && failure != null)) {
                throw new IllegalArgumentException("SignedPayloadResult must have exactly one non-null component");
            }
        }
    }

    private RecordFileSignedPayload() {
        throw new UnsupportedOperationException("Utility Class");
    }

    /// Computes the signed payload for the proof's declared record file format version from
    /// the block's `RECORD_FILE` item, or the failure the block must be refused with:
    /// - no `RECORD_FILE` item in the block: `MISSING_VERIFICATION_DATA`
    /// - unsupported version (outside 2, 5 and 6): `MISSING_MANDATORY_FIELD`
    /// - `record_file_contents` absent or missing components mandatory for the version
    ///   (the running hashes of the legacy reconstructions): `MISSING_MANDATORY_FIELD`
    /// - malformed protobuf wire data: `UNABLE_TO_PARSE`
    ///
    /// @param block the whole block being verified, carrying the `RECORD_FILE` item
    /// @param version the record file format version declared by the proof
    /// @param hapiProtoVersion the HAPI protocol version from the block header
    /// @param blockNumber the number of the block being verified, used for logging
    /// @return the payload or the failure, exactly one of the two, never both
    static SignedPayloadResult computeSignedWRBPayload(
            final BlockUnparsed block,
            final int version,
            final SemanticVersion hapiProtoVersion,
            final long blockNumber) {
        final SignedPayloadResult result;
        final Bytes rawRecordFileItemBytes = findRecordFileItemBytes(block);
        if (rawRecordFileItemBytes == null) {
            LOGGER.log(WARNING, "WRB block {0} carries no RECORD_FILE item to verify the proof against", blockNumber);
            result = new SignedPayloadResult(null, SessionFailureType.MISSING_VERIFICATION_DATA);
        } else if (!isSupportedVersion(version)) {
            final String message =
                    "Unsupported SignedRecordFileProof version {0} in block {1} - only versions 2, 5 and 6 are supported";
            LOGGER.log(WARNING, message, version, blockNumber);
            result = new SignedPayloadResult(null, SessionFailureType.MISSING_MANDATORY_FIELD);
        } else {
            result = getSignedPayload(rawRecordFileItemBytes, version, hapiProtoVersion, blockNumber);
        }
        return result;
    }

    /// Extracts the `record_file_contents` from the raw `RecordFileItem` bytes and computes
    /// the signed payload for the given version, classifying every failure.
    ///
    /// @param rawRecordFileItemBytes raw serialized bytes of the `RecordFileItem` proto message
    /// @param version the record file format version declared by the proof, must be 2, 5 or 6
    /// @param hapiProtoVersion the HAPI protocol version from the block header
    /// @param blockNumber the number of the block being verified, used for logging
    /// @return the payload or the failure, exactly one of the two, never both
    private static SignedPayloadResult getSignedPayload(
            final Bytes rawRecordFileItemBytes,
            final int version,
            final SemanticVersion hapiProtoVersion,
            final long blockNumber) {
        SignedPayloadResult computed;
        try {
            final Bytes recordFileContents = extractRecordStreamFileBytes(rawRecordFileItemBytes);
            if (recordFileContents.length() == 0) {
                LOGGER.log(WARNING, "WRB block {0} carries no record_file_contents", blockNumber);
                computed = new SignedPayloadResult(null, SessionFailureType.MISSING_MANDATORY_FIELD);
            } else {
                final byte[] payload = computeSignedPayload(version, hapiProtoVersion, recordFileContents);
                if (payload == null) {
                    // the contents are missing components mandatory for this version
                    LOGGER.log(
                            WARNING,
                            "WRB block {0} record_file_contents miss components mandatory for version {1}",
                            blockNumber,
                            version);
                    computed = new SignedPayloadResult(null, SessionFailureType.MISSING_MANDATORY_FIELD);
                } else {
                    computed = new SignedPayloadResult(payload, null);
                }
            }
        } catch (final ParseException e) {
            LOGGER.log(
                    WARNING,
                    "WRB block %d record_file_contents are malformed - rejecting block".formatted(blockNumber),
                    e);
            computed = new SignedPayloadResult(null, SessionFailureType.UNABLE_TO_PARSE);
        }
        return computed;
    }

    /// Finds the raw serialized bytes of the block's `RecordFileItem` proto message.
    ///
    /// @param block the block to search
    /// @return the raw `RECORD_FILE` item bytes, or `null` when the block carries no such item
    private static Bytes findRecordFileItemBytes(final BlockUnparsed block) {
        Bytes result = null;
        for (final BlockItemUnparsed item : block.blockItems()) {
            if (item.hasRecordFile()) {
                result = item.recordFile();
                break;
            }
        }
        return result;
    }

    /// Extracts the raw `record_file_contents` bytes from a serialized `RecordFileItem`
    /// proto message by walking the protobuf wire format directly, without deserializing the message.
    ///
    /// `record_file_contents` is proto field 2 of `RecordFileItem`. These bytes are
    /// the normalized `RecordStreamFile` content exactly as the wrap CLI serialized it. They
    /// must be returned byte-for-byte identical to what the signed payload was computed over;
    /// full deserialization via `RecordFileItem.PROTOBUF.parse()` is deliberately avoided
    /// because re-serializing a parsed object can produce subtly different bytes (e.g. omitting
    /// default-value fields, different varint encoding choices), which would cause the
    /// recomputed payload to diverge from the one the consensus nodes signed.
    ///
    /// **Protobuf wire format:** every field on the wire is encoded as a tag varint followed
    /// by its value. The tag packs two things:
    /// - `fieldNumber = tag >>> 3`
    /// - `wireType = tag & 0x7`
    ///
    /// Wire type 2 (`LEN`) means the value is length-prefixed bytes, used for `bytes`,
    /// `string`, and embedded messages. It is encoded as:
    /// `[tag varint] [length varint] [raw bytes...]`.
    ///
    /// **Algorithm:**
    /// 1. Read the next field tag varint and decode its field number and wire type.
    /// 2. If `fieldNumber == 2` and `wireType == LEN`: read the length prefix varint,
    ///    read exactly that many bytes, and return them - these are the
    ///    `record_file_contents`.
    /// 3. Otherwise skip the field using the wire type to know how many bytes to consume:
    ///    - VARINT (wire 0): read and discard one varint
    ///    - I64 (wire 1): skip 8 bytes fixed
    ///    - LEN (wire 2): read the length prefix, skip that many bytes
    ///    - I32 (wire 5): skip 4 bytes fixed
    /// 4. Repeat until field 2 is found or input is exhausted.
    ///
    /// @param recordFileItemBytes raw serialized bytes of a `RecordFileItem` proto message
    /// @return verbatim bytes of the `record_file_contents` field (proto field 2), or
    ///         [Bytes#EMPTY] if field 2 is not present or an unknown wire type is encountered
    /// @throws ParseException if the wire data is malformed (e.g. truncated)
    private static Bytes extractRecordStreamFileBytes(final Bytes recordFileItemBytes) throws ParseException {
        try {
            final ReadableSequentialData input = recordFileItemBytes.toReadableSequentialData();
            while (input.hasRemaining()) {
                // Each field starts with a tag varint: high bits = field number, low 3 bits = wire type
                final int tag = input.readVarInt(false);
                final int wireType = tag & 0x7;
                final int fieldNumber = tag >>> 3;
                if (fieldNumber == 2 && wireType == 2) {
                    // Found record_file_contents (field 2, LEN wire type).
                    // Read the length-prefix varint then copy the raw payload bytes verbatim.
                    final int len = input.readVarInt(false);
                    final byte[] raw = new byte[len];
                    input.readBytes(raw);
                    return Bytes.wrap(raw);
                }
                // Not field 2 - skip this field using its wire type to advance the cursor correctly
                switch (wireType) {
                    case 0 -> input.readVarLong(false); // VARINT: read and discard the value
                    case 1 -> input.skip(8); // I64: fixed 64-bit, skip 8 bytes
                    case 2 -> { // LEN: read length prefix, skip content
                        final int l = input.readVarInt(false);
                        input.skip(l);
                    }
                    case 5 -> input.skip(4); // I32: fixed 32-bit, skip 4 bytes
                    default -> {
                        return Bytes.EMPTY; // Unknown wire type - bail out safely
                    }
                }
            }
        } catch (final RuntimeException e) {
            throw new ParseException(e);
        }
        return Bytes.EMPTY; // field 2 not present in the message
    }

    /// Returns `true` when the given record file format version is one this class can
    /// compute a signed payload for, i.e. one of the versions ever used by a production
    /// network's record stream: 2, 5 and 6.
    ///
    /// @param version the record file format version declared by a proof
    /// @return `true` when the version is supported
    private static boolean isSupportedVersion(final int version) {
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
    static byte[] computeSignedPayload(
            final int version, final SemanticVersion hapiProtoVersion, final Bytes recordFileContents)
            throws ParseException {
        try {
            return switch (version) {
                case 6 -> computeV6SignedPayload(recordFileContents);
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
            result = sha384Digest().digest(bout.toByteArray());
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

    /// Computes the v6 signed payload: `SHA-384(int32be(6) || recordFileContents)` over the
    /// verbatim protobuf bytes, where `int32be(6)` is the four bytes `0x00 0x00 0x00 0x06`.
    ///
    /// This formula must stay byte-for-byte identical to
    /// `HashingUtilities.computeV6SignedPayload` in the common module, which the block-signing
    /// test signer uses to produce V6 fixtures; the alignment is pinned by a test.
    ///
    /// @param recordFileContents the verbatim `record_file_contents` bytes
    /// @return the 48-byte signed payload
    private static byte[] computeV6SignedPayload(final Bytes recordFileContents) {
        final MessageDigest digest = sha384Digest();
        digest.update(new byte[] {0, 0, 0, 6});
        recordFileContents.writeTo(digest);
        return digest.digest();
    }

    /// Returns a [MessageDigest] for the SHA-384 algorithm.
    ///
    /// @return a fresh SHA-384 digest
    private static MessageDigest sha384Digest() {
        try {
            return MessageDigest.getInstance(HASH_ALGORITHM);
        } catch (final NoSuchAlgorithmException fatal) {
            // Cannot occur: SHA-384 is a mandatory JCA algorithm
            throw new IllegalStateException(fatal);
        }
    }

    /// Computes the v2 double hash of a reconstructed v2 file:
    /// `SHA-384(header || SHA-384(content))` where the header is the first
    /// [#V2_HEADER_LENGTH] bytes and the content is everything after it.
    ///
    /// @param recordFileBytes the reconstructed v2 file bytes
    /// @return the 48-byte double hash
    private static byte[] computeV2DoubleHash(final byte[] recordFileBytes) {
        final MessageDigest digest = sha384Digest();
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
        out.writeInt(SHA_384_HASH_SIZE);
        hash.writeTo(out);
    }

    /// Returns `true` when the given hash is present and exactly 48 bytes long.
    ///
    /// @param hash the hash to check, may be null
    /// @return `true` when the hash can be used in a legacy reconstruction
    private static boolean isValidHash(final Bytes hash) {
        return hash != null && hash.length() == SHA_384_HASH_SIZE;
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
