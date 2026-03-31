// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.hedera.hapi.block.stream.RecordFileItem;
import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.hapi.node.base.Transaction;
import com.hedera.hapi.node.transaction.TransactionRecord;
import com.hedera.hapi.streams.HashAlgorithm;
import com.hedera.hapi.streams.HashObject;
import com.hedera.hapi.streams.RecordStreamFile;
import com.hedera.hapi.streams.RecordStreamItem;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
import java.io.ByteArrayOutputStream;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import org.hiero.block.tools.records.model.parsed.ParsedRecordFile;
import org.junit.jupiter.api.Test;

/**
 * Tests that {@link SignatureDataExtractor} produces signed hashes identical to the full
 * {@link ParsedRecordFile} parse-and-reconstruct path, for all supported record format versions.
 */
class SignatureDataExtractorTest {

    private static final int SHA_384_SIZE = 48;

    /** Creates a fake 48-byte hash filled with a given byte value. */
    private static byte[] fakeHash(final int fillByte) {
        final byte[] hash = new byte[SHA_384_SIZE];
        java.util.Arrays.fill(hash, (byte) fillByte);
        return hash;
    }

    /** Creates a minimal Transaction with some recognizable bytes. */
    private static Transaction fakeTransaction(final int id) {
        // Build a small bodyBytes payload so the transaction is non-trivial
        final byte[] body = new byte[] {(byte) id, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
        return new Transaction(null, null, null, Bytes.wrap(body), null);
    }

    /** Creates a minimal TransactionRecord with some recognizable bytes. */
    private static TransactionRecord fakeTransactionRecord(final int id) {
        // Use DEFAULT and override just enough to have non-empty serialized form
        // TransactionRecord has many fields; we construct a minimal one via the alias field
        return TransactionRecord.DEFAULT;
    }

    /** Builds a RecordFileItem and serializes it to Bytes. */
    private static Bytes buildRecordFileItem(
            final long seconds, final int nanos, final RecordStreamFile recordStreamFile) throws Exception {
        final Timestamp timestamp = new Timestamp(seconds, nanos);
        final RecordFileItem item =
                new RecordFileItem(timestamp, recordStreamFile, Collections.emptyList(), Collections.emptyList());
        final ByteArrayOutputStream bout = new ByteArrayOutputStream();
        final WritableStreamingData out = new WritableStreamingData(bout);
        RecordFileItem.PROTOBUF.write(item, out);
        out.flush();
        return Bytes.wrap(bout.toByteArray());
    }

    /** Computes the signed hash via the old full-parse path for comparison. */
    private static byte[] computeSignedHashViaFullParse(
            final long seconds,
            final int nanos,
            final int version,
            final SemanticVersion hapi,
            final byte[] startHash,
            final RecordStreamFile rsf) {
        final Instant blockTime = Instant.ofEpochSecond(seconds, nanos);
        final ParsedRecordFile parsed = ParsedRecordFile.parse(blockTime, version, hapi, startHash, rsf);
        return parsed.signedHash();
    }

    @Test
    void testV6SignedHashMatchesFullParse() throws Exception {
        final byte[] startHash = fakeHash(0xAA);
        final byte[] endHash = fakeHash(0xBB);
        final SemanticVersion hapi = new SemanticVersion(0, 47, 0, null, null);

        final List<RecordStreamItem> items = List.of(
                new RecordStreamItem(fakeTransaction(1), fakeTransactionRecord(1)),
                new RecordStreamItem(fakeTransaction(2), fakeTransactionRecord(2)));

        final RecordStreamFile rsf = new RecordStreamFile(
                hapi,
                new HashObject(HashAlgorithm.SHA_384, SHA_384_SIZE, Bytes.wrap(startHash)),
                items,
                new HashObject(HashAlgorithm.SHA_384, SHA_384_SIZE, Bytes.wrap(endHash)),
                -1,
                Collections.emptyList());

        final long seconds = 1695000000L;
        final int nanos = 123456789;
        final Bytes serialized = buildRecordFileItem(seconds, nanos, rsf);

        // Extract and compute via new path
        final SignatureDataExtractor.SignatureData sigData = SignatureDataExtractor.extract(serialized, 6);
        final byte[] newHash = SignatureDataExtractor.computeSignedHash(6, hapi, sigData);

        // Compute via old full-parse path
        final byte[] oldHash = computeSignedHashViaFullParse(seconds, nanos, 6, hapi, startHash, rsf);

        assertArrayEquals(oldHash, newHash, "V6 signed hash should match full parse path");
        assertEquals(seconds, sigData.creationTimeSeconds());
        assertEquals(nanos, sigData.creationTimeNanos());
    }

    @Test
    void testV5SignedHashMatchesFullParse() throws Exception {
        final byte[] startHash = fakeHash(0xCC);
        final byte[] endHash = fakeHash(0xDD);
        final SemanticVersion hapi = new SemanticVersion(0, 37, 2, null, null);

        final List<RecordStreamItem> items = List.of(
                new RecordStreamItem(fakeTransaction(10), fakeTransactionRecord(10)),
                new RecordStreamItem(fakeTransaction(20), fakeTransactionRecord(20)),
                new RecordStreamItem(fakeTransaction(30), fakeTransactionRecord(30)));

        final RecordStreamFile rsf = new RecordStreamFile(
                hapi,
                new HashObject(HashAlgorithm.SHA_384, SHA_384_SIZE, Bytes.wrap(startHash)),
                items,
                new HashObject(HashAlgorithm.SHA_384, SHA_384_SIZE, Bytes.wrap(endHash)),
                -1,
                Collections.emptyList());

        final long seconds = 1600000000L;
        final int nanos = 987654321;
        final Bytes serialized = buildRecordFileItem(seconds, nanos, rsf);

        // Extract and compute via new path
        final SignatureDataExtractor.SignatureData sigData = SignatureDataExtractor.extract(serialized, 5);
        final byte[] newHash = SignatureDataExtractor.computeSignedHash(5, hapi, sigData);

        // Compute via old full-parse path
        final byte[] oldHash = computeSignedHashViaFullParse(seconds, nanos, 5, hapi, startHash, rsf);

        assertArrayEquals(oldHash, newHash, "V5 signed hash should match full parse path");
        assertEquals(seconds, sigData.creationTimeSeconds());
        assertEquals(nanos, sigData.creationTimeNanos());
        assertArrayEquals(startHash, sigData.startRunningHash().toByteArray());
        assertArrayEquals(endHash, sigData.endRunningHash().toByteArray());
        assertEquals(3, sigData.items().size());
    }

    @Test
    void testV2SignedHashMatchesFullParse() throws Exception {
        final byte[] previousHash = fakeHash(0xEE);
        final SemanticVersion hapi = new SemanticVersion(0, 3, 0, null, null);

        final List<RecordStreamItem> items =
                List.of(new RecordStreamItem(fakeTransaction(42), fakeTransactionRecord(42)));

        // V2 has no end running hash
        final RecordStreamFile rsf = new RecordStreamFile(
                hapi,
                new HashObject(HashAlgorithm.SHA_384, SHA_384_SIZE, Bytes.wrap(previousHash)),
                items,
                null,
                -1,
                Collections.emptyList());

        final long seconds = 1568400000L;
        final int nanos = 0;
        final Bytes serialized = buildRecordFileItem(seconds, nanos, rsf);

        // Extract and compute via new path
        final SignatureDataExtractor.SignatureData sigData = SignatureDataExtractor.extract(serialized, 2);
        final byte[] newHash = SignatureDataExtractor.computeSignedHash(2, hapi, sigData);

        // Compute via old full-parse path
        final byte[] oldHash = computeSignedHashViaFullParse(seconds, nanos, 2, hapi, previousHash, rsf);

        assertArrayEquals(oldHash, newHash, "V2 signed hash should match full parse path");
        assertArrayEquals(previousHash, sigData.startRunningHash().toByteArray());
        assertEquals(1, sigData.items().size());
    }

    @Test
    void testV6SingleItemMatchesFullParse() throws Exception {
        final byte[] startHash = fakeHash(0x11);
        final byte[] endHash = fakeHash(0x22);
        final SemanticVersion hapi = new SemanticVersion(0, 49, 0, null, null);

        final List<RecordStreamItem> items =
                List.of(new RecordStreamItem(fakeTransaction(99), fakeTransactionRecord(99)));

        final RecordStreamFile rsf = new RecordStreamFile(
                hapi,
                new HashObject(HashAlgorithm.SHA_384, SHA_384_SIZE, Bytes.wrap(startHash)),
                items,
                new HashObject(HashAlgorithm.SHA_384, SHA_384_SIZE, Bytes.wrap(endHash)),
                -1,
                Collections.emptyList());

        final long seconds = 1700000000L;
        final int nanos = 500000000;
        final Bytes serialized = buildRecordFileItem(seconds, nanos, rsf);

        final SignatureDataExtractor.SignatureData sigData = SignatureDataExtractor.extract(serialized, 6);
        final byte[] newHash = SignatureDataExtractor.computeSignedHash(6, hapi, sigData);
        final byte[] oldHash = computeSignedHashViaFullParse(seconds, nanos, 6, hapi, startHash, rsf);

        assertArrayEquals(oldHash, newHash, "V6 single-item signed hash should match full parse path");
    }

    @Test
    void testV5SingleItemMatchesFullParse() throws Exception {
        final byte[] startHash = fakeHash(0x33);
        final byte[] endHash = fakeHash(0x44);
        final SemanticVersion hapi = new SemanticVersion(0, 30, 0, null, null);

        final List<RecordStreamItem> items =
                List.of(new RecordStreamItem(fakeTransaction(7), fakeTransactionRecord(7)));

        final RecordStreamFile rsf = new RecordStreamFile(
                hapi,
                new HashObject(HashAlgorithm.SHA_384, SHA_384_SIZE, Bytes.wrap(startHash)),
                items,
                new HashObject(HashAlgorithm.SHA_384, SHA_384_SIZE, Bytes.wrap(endHash)),
                -1,
                Collections.emptyList());

        final long seconds = 1590000000L;
        final int nanos = 100000000;
        final Bytes serialized = buildRecordFileItem(seconds, nanos, rsf);

        final SignatureDataExtractor.SignatureData sigData = SignatureDataExtractor.extract(serialized, 5);
        final byte[] newHash = SignatureDataExtractor.computeSignedHash(5, hapi, sigData);
        final byte[] oldHash = computeSignedHashViaFullParse(seconds, nanos, 5, hapi, startHash, rsf);

        assertArrayEquals(oldHash, newHash, "V5 single-item signed hash should match full parse path");
    }
}
