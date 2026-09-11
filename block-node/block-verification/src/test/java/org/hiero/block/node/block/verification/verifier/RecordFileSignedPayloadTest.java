// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.verifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.hapi.node.base.Transaction;
import com.hedera.hapi.node.transaction.TransactionRecord;
import com.hedera.hapi.streams.HashAlgorithm;
import com.hedera.hapi.streams.HashObject;
import com.hedera.hapi.streams.RecordStreamFile;
import com.hedera.hapi.streams.RecordStreamItem;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.hiero.block.common.hasher.HashingUtilities;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/// Tests for [RecordFileSignedPayload]. Expected values are produced by a local straight-line
/// reference reconstruction of the legacy v2/v5 record file binary formats, written with
/// [DataOutputStream] independently of the production wire-navigation code, so the two
/// implementations must agree byte-for-byte.
@DisplayName("Record File Signed Payload Tests")
class RecordFileSignedPayloadTest {
    /// The size of an SHA-384 hash, in bytes.
    private static final int HASH_SIZE = 48;
    /// The v5 record stream object class id, from the legacy v5 serialization format.
    private static final long V5_RECORD_STREAM_OBJECT_CLASS_ID = Long.parseUnsignedLong("e370929ba5429d8b", 16);
    /// The v5 hash object class id, from the legacy v5 serialization format.
    private static final long V5_HASH_OBJECT_CLASS_ID = Long.parseUnsignedLong("f422da83a251741e", 16);
    /// The v5 digest type id for SHA-384.
    private static final int V5_DIGEST_TYPE_SHA384 = 0x58ff811b;
    /// The length of the v2 file header: two ints, the marker byte and the 48-byte hash.
    private static final int V2_HEADER_LENGTH = 4 + 4 + 1 + HASH_SIZE;

    // ---- Fixture helpers ----------------------------------------------------------------------

    /// Creates a fake 48-byte hash filled with the given byte value.
    private static byte[] fakeHash(final int fillByte) {
        final byte[] hash = new byte[HASH_SIZE];
        Arrays.fill(hash, (byte) fillByte);
        return hash;
    }

    /// Creates a minimal transaction with recognizable body bytes.
    private static Transaction fakeTransaction(final int id) {
        final byte[] body = new byte[] {(byte) id, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
        return Transaction.newBuilder().bodyBytes(Bytes.wrap(body)).build();
    }

    /// Creates a minimal transaction record with a recognizable memo.
    private static TransactionRecord fakeTransactionRecord(final int id) {
        return TransactionRecord.newBuilder().memo("record-" + id).build();
    }

    /// Builds a normalized `RecordStreamFile` with the given running hashes and items, the shape
    /// the wrap CLI stores in `RecordFileItem.record_file_contents` for every source version.
    private static RecordStreamFile recordStreamFile(
            final SemanticVersion hapi,
            final byte[] startHash,
            final byte[] endHash,
            final List<RecordStreamItem> items) {
        final HashObject startHashObject = startHash == null
                ? null
                : new HashObject(HashAlgorithm.SHA_384, startHash.length, Bytes.wrap(startHash));
        final HashObject endHashObject =
                endHash == null ? null : new HashObject(HashAlgorithm.SHA_384, endHash.length, Bytes.wrap(endHash));
        return new RecordStreamFile(hapi, startHashObject, items, endHashObject, -1, Collections.emptyList());
    }

    /// Serializes a `RecordStreamFile` to the verbatim `record_file_contents` bytes.
    private static Bytes contentsOf(final RecordStreamFile recordStreamFile) {
        return RecordStreamFile.PROTOBUF.toBytes(recordStreamFile);
    }

    /// Creates a list of items with the given count, each with distinct transaction and record.
    private static List<RecordStreamItem> fakeItems(final int count) {
        return java.util.stream.IntStream.range(0, count)
                .mapToObj(i -> new RecordStreamItem(fakeTransaction(i + 1), fakeTransactionRecord(i + 1)))
                .toList();
    }

    // ---- Independent reference reconstruction -------------------------------------------------

    /// Returns the SHA-384 hash of the given bytes, using [MessageDigest] directly.
    private static byte[] refSha384(final byte[] bytes) throws NoSuchAlgorithmException {
        return MessageDigest.getInstance("SHA-384").digest(bytes);
    }

    /// Writes a legacy v5 hash object with [DataOutputStream].
    private static void refWriteV5HashObject(final DataOutputStream out, final byte[] hash) throws IOException {
        out.writeLong(V5_HASH_OBJECT_CLASS_ID);
        out.writeInt(1);
        out.writeInt(V5_DIGEST_TYPE_SHA384);
        out.writeInt(HASH_SIZE);
        out.write(hash);
    }

    /// Reference v5 signed payload: reconstruct the legacy v5 binary file with
    /// [DataOutputStream] and take a single SHA-384 of the whole reconstruction.
    private static byte[] refV5Payload(
            final SemanticVersion hapi,
            final byte[] startHash,
            final byte[] endHash,
            final List<RecordStreamItem> items)
            throws IOException, NoSuchAlgorithmException {
        final ByteArrayOutputStream bout = new ByteArrayOutputStream();
        final DataOutputStream out = new DataOutputStream(bout);
        out.writeInt(5);
        out.writeInt(hapi.major());
        out.writeInt(hapi.minor());
        out.writeInt(hapi.patch());
        out.writeInt(1); // object stream version
        refWriteV5HashObject(out, startHash);
        for (final RecordStreamItem item : items) {
            out.writeLong(V5_RECORD_STREAM_OBJECT_CLASS_ID);
            out.writeInt(1);
            final byte[] recordBytes =
                    TransactionRecord.PROTOBUF.toBytes(item.record()).toByteArray();
            out.writeInt(recordBytes.length);
            out.write(recordBytes);
            final byte[] transactionBytes =
                    Transaction.PROTOBUF.toBytes(item.transaction()).toByteArray();
            out.writeInt(transactionBytes.length);
            out.write(transactionBytes);
        }
        refWriteV5HashObject(out, endHash);
        return refSha384(bout.toByteArray());
    }

    /// Reference v2 signed payload: reconstruct the legacy v2 binary file with
    /// [DataOutputStream] and take the double hash split at the 57-byte header boundary.
    private static byte[] refV2Payload(
            final SemanticVersion hapi, final byte[] previousHash, final List<RecordStreamItem> items)
            throws IOException, NoSuchAlgorithmException {
        final ByteArrayOutputStream bout = new ByteArrayOutputStream();
        final DataOutputStream out = new DataOutputStream(bout);
        out.writeInt(2);
        out.writeInt(hapi.minor());
        out.writeByte(1);
        out.write(previousHash);
        for (final RecordStreamItem item : items) {
            out.writeByte(2);
            final byte[] transactionBytes =
                    Transaction.PROTOBUF.toBytes(item.transaction()).toByteArray();
            out.writeInt(transactionBytes.length);
            out.write(transactionBytes);
            final byte[] recordBytes =
                    TransactionRecord.PROTOBUF.toBytes(item.record()).toByteArray();
            out.writeInt(recordBytes.length);
            out.write(recordBytes);
        }
        final byte[] fileBytes = bout.toByteArray();
        final byte[] contentHash = refSha384(Arrays.copyOfRange(fileBytes, V2_HEADER_LENGTH, fileBytes.length));
        final ByteArrayOutputStream finalInput = new ByteArrayOutputStream();
        finalInput.write(fileBytes, 0, V2_HEADER_LENGTH);
        finalInput.write(contentHash, 0, contentHash.length);
        return refSha384(finalInput.toByteArray());
    }

    // ---- Tests --------------------------------------------------------------------------------

    /// Tests for the v5 payload construction.
    @Nested
    @DisplayName("V5 Payload Tests")
    class V5PayloadTests {
        /// This test aims to assert that the v5 signed payload computed from the normalized
        /// protobuf contents of a multi-item record stream file is byte-for-byte identical to
        /// the payload produced by the independent reference reconstruction of the legacy v5
        /// binary format, proving the wire navigation and reconstruction order (record before
        /// transaction) are correct.
        @Test
        @DisplayName("computeSignedPayload() v5 multi-item matches reference reconstruction")
        void testV5MultiItemMatchesReference() throws Exception {
            final SemanticVersion hapi = new SemanticVersion(0, 22, 3, null, null);
            final byte[] startHash = fakeHash(0xCC);
            final byte[] endHash = fakeHash(0xDD);
            final List<RecordStreamItem> items = fakeItems(3);
            final byte[] actual = RecordFileSignedPayload.computeSignedPayload(
                    5, hapi, contentsOf(recordStreamFile(hapi, startHash, endHash, items)));
            assertThat(actual).isEqualTo(refV5Payload(hapi, startHash, endHash, items));
        }

        /// This test aims to assert that the v5 signed payload of a single-item record stream
        /// file matches the independent reference reconstruction, covering the smallest
        /// realistic file shape.
        @Test
        @DisplayName("computeSignedPayload() v5 single-item matches reference reconstruction")
        void testV5SingleItemMatchesReference() throws Exception {
            final SemanticVersion hapi = new SemanticVersion(0, 30, 0, null, null);
            final byte[] startHash = fakeHash(0x33);
            final byte[] endHash = fakeHash(0x44);
            final List<RecordStreamItem> items = fakeItems(1);
            final byte[] actual = RecordFileSignedPayload.computeSignedPayload(
                    5, hapi, contentsOf(recordStreamFile(hapi, startHash, endHash, items)));
            assertThat(actual).isEqualTo(refV5Payload(hapi, startHash, endHash, items));
        }

        /// This test aims to assert that a v5 record stream file with an empty item list is
        /// legal and reconstructs to a header-and-hashes-only file whose payload matches the
        /// independent reference reconstruction.
        @Test
        @DisplayName("computeSignedPayload() v5 empty item list matches reference reconstruction")
        void testV5EmptyItemsMatchesReference() throws Exception {
            final SemanticVersion hapi = new SemanticVersion(0, 25, 0, null, null);
            final byte[] startHash = fakeHash(0x55);
            final byte[] endHash = fakeHash(0x66);
            final byte[] actual = RecordFileSignedPayload.computeSignedPayload(
                    5, hapi, contentsOf(recordStreamFile(hapi, startHash, endHash, List.of())));
            assertThat(actual).isEqualTo(refV5Payload(hapi, startHash, endHash, List.of()));
        }

        /// This test aims to assert that the v5 signed payload is a 48-byte SHA-384 digest and
        /// differs from the v6 payload computed over the same normalized protobuf contents,
        /// proving the version parameter selects a genuinely different hash construction.
        @Test
        @DisplayName("computeSignedPayload() v5 payload is 48 bytes and differs from v6")
        void testV5PayloadDiffersFromV6() throws Exception {
            final SemanticVersion hapi = new SemanticVersion(0, 22, 0, null, null);
            final Bytes contents = contentsOf(recordStreamFile(hapi, fakeHash(0xAB), fakeHash(0xBA), fakeItems(2)));
            final byte[] v5Payload = RecordFileSignedPayload.computeSignedPayload(5, hapi, contents);
            final byte[] v6Payload = RecordFileSignedPayload.computeSignedPayload(6, hapi, contents);
            assertThat(v5Payload).hasSize(HASH_SIZE).isNotEqualTo(v6Payload);
        }
    }

    /// Tests for the v2 payload construction.
    @Nested
    @DisplayName("V2 Payload Tests")
    class V2PayloadTests {
        /// This test aims to assert that the v2 signed payload computed from the normalized
        /// protobuf contents is byte-for-byte identical to the payload produced by the
        /// independent reference reconstruction of the legacy v2 binary format, proving the
        /// wire navigation and reconstruction order (transaction before record) are correct.
        @Test
        @DisplayName("computeSignedPayload() v2 matches reference reconstruction")
        void testV2MatchesReference() throws Exception {
            final SemanticVersion hapi = new SemanticVersion(0, 3, 0, null, null);
            final byte[] previousHash = fakeHash(0xEE);
            final List<RecordStreamItem> items = fakeItems(2);
            // v2 files have no end running hash
            final byte[] actual = RecordFileSignedPayload.computeSignedPayload(
                    2, hapi, contentsOf(recordStreamFile(hapi, previousHash, null, items)));
            assertThat(actual).isEqualTo(refV2Payload(hapi, previousHash, items));
        }

        /// This test aims to assert that the v2 payload really is the double hash
        /// SHA-384(header || SHA-384(content)) split exactly at the 57-byte header boundary,
        /// by recomputing the two digest stages manually from the reference reconstruction.
        @Test
        @DisplayName("computeSignedPayload() v2 double hash splits at the 57-byte header")
        void testV2DoubleHashSplit() throws Exception {
            final SemanticVersion hapi = new SemanticVersion(0, 4, 0, null, null);
            final byte[] previousHash = fakeHash(0x77);
            final List<RecordStreamItem> items = fakeItems(1);
            // Rebuild the legacy file with the reference writer and hash the two stages manually
            final ByteArrayOutputStream bout = new ByteArrayOutputStream();
            final DataOutputStream out = new DataOutputStream(bout);
            out.writeInt(2);
            out.writeInt(hapi.minor());
            out.writeByte(1);
            out.write(previousHash);
            for (final RecordStreamItem item : items) {
                out.writeByte(2);
                final byte[] transactionBytes =
                        Transaction.PROTOBUF.toBytes(item.transaction()).toByteArray();
                out.writeInt(transactionBytes.length);
                out.write(transactionBytes);
                final byte[] recordBytes =
                        TransactionRecord.PROTOBUF.toBytes(item.record()).toByteArray();
                out.writeInt(recordBytes.length);
                out.write(recordBytes);
            }
            final byte[] fileBytes = bout.toByteArray();
            final MessageDigest digest = MessageDigest.getInstance("SHA-384");
            digest.update(fileBytes, V2_HEADER_LENGTH, fileBytes.length - V2_HEADER_LENGTH);
            final byte[] contentHash = digest.digest();
            digest.update(fileBytes, 0, V2_HEADER_LENGTH);
            digest.update(contentHash);
            final byte[] expected = digest.digest();
            final byte[] actual = RecordFileSignedPayload.computeSignedPayload(
                    2, hapi, contentsOf(recordStreamFile(hapi, previousHash, null, items)));
            assertThat(actual).isEqualTo(expected);
        }

        /// This test aims to assert that the v2 payload depends on the minor component of the
        /// HAPI protocol version, which carries the original single-int v2 version, so two
        /// different minors over identical contents produce different payloads.
        @Test
        @DisplayName("computeSignedPayload() v2 payload is sensitive to hapi minor")
        void testV2HapiMinorSensitivity() throws Exception {
            final byte[] previousHash = fakeHash(0x88);
            final List<RecordStreamItem> items = fakeItems(1);
            final SemanticVersion hapiMinor3 = new SemanticVersion(0, 3, 0, null, null);
            final SemanticVersion hapiMinor4 = new SemanticVersion(0, 4, 0, null, null);
            final Bytes contents = contentsOf(recordStreamFile(hapiMinor3, previousHash, null, items));
            final byte[] payloadMinor3 = RecordFileSignedPayload.computeSignedPayload(2, hapiMinor3, contents);
            final byte[] payloadMinor4 = RecordFileSignedPayload.computeSignedPayload(2, hapiMinor4, contents);
            assertThat(payloadMinor3).isNotEqualTo(payloadMinor4);
        }

        /// This test aims to assert that a v2 record stream file with an empty item list is
        /// legal and its payload matches the independent reference reconstruction of a
        /// header-only file.
        @Test
        @DisplayName("computeSignedPayload() v2 empty item list matches reference reconstruction")
        void testV2EmptyItemsMatchesReference() throws Exception {
            final SemanticVersion hapi = new SemanticVersion(0, 5, 0, null, null);
            final byte[] previousHash = fakeHash(0x99);
            final byte[] actual = RecordFileSignedPayload.computeSignedPayload(
                    2, hapi, contentsOf(recordStreamFile(hapi, previousHash, null, List.of())));
            assertThat(actual).isEqualTo(refV2Payload(hapi, previousHash, List.of()));
        }
    }

    /// Tests for the v6 payload delegation.
    @Nested
    @DisplayName("V6 Payload Tests")
    class V6PayloadTests {
        /// This test aims to assert that the version 6 branch returns exactly the payload of
        /// [HashingUtilities#computeV6SignedPayload], i.e. SHA-384(int32(6) || contents) over
        /// the verbatim bytes with no extraction involved, even for bytes that are not valid
        /// protobuf.
        @Test
        @DisplayName("computeSignedPayload() v6 delegates to HashingUtilities")
        void testV6DelegatesToHashingUtilities() throws Exception {
            final SemanticVersion hapi = new SemanticVersion(0, 72, 0, null, null);
            final Bytes arbitraryBytes = Bytes.wrap("not-a-record-stream-file".getBytes());
            final byte[] actual = RecordFileSignedPayload.computeSignedPayload(6, hapi, arbitraryBytes);
            assertThat(actual).isEqualTo(HashingUtilities.computeV6SignedPayload(arbitraryBytes));
        }
    }

    /// Tests for missing components, malformed input and unsupported versions.
    @Nested
    @DisplayName("Negative Tests")
    class NegativeTests {
        /// This test aims to assert that a v5 record stream file without a start running hash
        /// cannot be reconstructed and yields the null missing-components sentinel instead of
        /// a wrong payload.
        @Test
        @DisplayName("computeSignedPayload() v5 missing start running hash returns null")
        void testV5MissingStartHashReturnsNull() throws Exception {
            final SemanticVersion hapi = new SemanticVersion(0, 22, 0, null, null);
            final Bytes contents = contentsOf(recordStreamFile(hapi, null, fakeHash(0x11), fakeItems(1)));
            assertThat(RecordFileSignedPayload.computeSignedPayload(5, hapi, contents))
                    .isNull();
        }

        /// This test aims to assert that a v5 record stream file without an end running hash
        /// cannot be reconstructed and yields the null missing-components sentinel.
        @Test
        @DisplayName("computeSignedPayload() v5 missing end running hash returns null")
        void testV5MissingEndHashReturnsNull() throws Exception {
            final SemanticVersion hapi = new SemanticVersion(0, 22, 0, null, null);
            final Bytes contents = contentsOf(recordStreamFile(hapi, fakeHash(0x11), null, fakeItems(1)));
            assertThat(RecordFileSignedPayload.computeSignedPayload(5, hapi, contents))
                    .isNull();
        }

        /// This test aims to assert that a v2 record stream file without a start running hash
        /// (the previous file hash) cannot be reconstructed and yields the null sentinel, while
        /// a missing end running hash is tolerated because the v2 format has no end hash.
        @Test
        @DisplayName("computeSignedPayload() v2 requires only the start running hash")
        void testV2MissingStartHashReturnsNull() throws Exception {
            final SemanticVersion hapi = new SemanticVersion(0, 3, 0, null, null);
            final Bytes noStartHash = contentsOf(recordStreamFile(hapi, null, null, fakeItems(1)));
            assertThat(RecordFileSignedPayload.computeSignedPayload(2, hapi, noStartHash))
                    .isNull();
            final Bytes noEndHash = contentsOf(recordStreamFile(hapi, fakeHash(0x12), null, fakeItems(1)));
            assertThat(RecordFileSignedPayload.computeSignedPayload(2, hapi, noEndHash))
                    .isNotNull();
        }

        /// This test aims to assert that a running hash that is not exactly 48 bytes is
        /// treated as missing for both v2 and v5, because feeding a short hash into the
        /// reconstruction would silently shift the legacy binary layout.
        @Test
        @DisplayName("computeSignedPayload() wrong-length running hash returns null")
        void testWrongLengthHashReturnsNull() throws Exception {
            final SemanticVersion hapi = new SemanticVersion(0, 22, 0, null, null);
            final byte[] shortHash = new byte[32];
            Arrays.fill(shortHash, (byte) 0x21);
            final Bytes contents = contentsOf(recordStreamFile(hapi, shortHash, fakeHash(0x22), fakeItems(1)));
            assertThat(RecordFileSignedPayload.computeSignedPayload(5, hapi, contents))
                    .isNull();
            assertThat(RecordFileSignedPayload.computeSignedPayload(2, hapi, contents))
                    .isNull();
        }

        /// This test aims to assert that malformed protobuf wire data (a length-delimited
        /// field whose declared length exceeds the available bytes) surfaces as a
        /// [ParseException] for the versions that navigate the contents, instead of a wrong
        /// payload or an unrelated runtime exception.
        @ParameterizedTest
        @ValueSource(ints = {2, 5})
        @DisplayName("computeSignedPayload() malformed wire data throws ParseException")
        void testMalformedContentsThrowParseException(final int version) {
            final SemanticVersion hapi = new SemanticVersion(0, 22, 0, null, null);
            // Field 2 (LEN) declaring 127 bytes with none following
            final Bytes truncated = Bytes.wrap(new byte[] {0x12, 0x7F});
            assertThatExceptionOfType(ParseException.class)
                    .isThrownBy(() -> RecordFileSignedPayload.computeSignedPayload(version, hapi, truncated));
        }

        /// This test aims to assert that record file format versions that never existed on
        /// mainnet are rejected with an [IllegalArgumentException], because callers are
        /// expected to gate the version before requesting a payload.
        @ParameterizedTest
        @ValueSource(ints = {0, 1, 3, 4, 7})
        @DisplayName("computeSignedPayload() unsupported version throws IllegalArgumentException")
        void testUnsupportedVersionThrows(final int version) {
            final SemanticVersion hapi = new SemanticVersion(0, 22, 0, null, null);
            final Bytes contents = contentsOf(recordStreamFile(hapi, fakeHash(0x01), fakeHash(0x02), fakeItems(1)));
            assertThatExceptionOfType(IllegalArgumentException.class)
                    .isThrownBy(() -> RecordFileSignedPayload.computeSignedPayload(version, hapi, contents));
        }
    }
}
