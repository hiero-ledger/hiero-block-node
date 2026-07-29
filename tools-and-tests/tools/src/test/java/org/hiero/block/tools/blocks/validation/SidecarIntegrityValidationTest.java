// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

import static org.hiero.block.node.base.ParseHelper.standardParse;
import static org.hiero.block.tools.utils.Sha384.hashSha384;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.RecordFileItem;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.hapi.node.transaction.TransactionRecord;
import com.hedera.hapi.streams.HashAlgorithm;
import com.hedera.hapi.streams.HashObject;
import com.hedera.hapi.streams.RecordStreamFile;
import com.hedera.hapi.streams.RecordStreamItem;
import com.hedera.hapi.streams.SidecarFile;
import com.hedera.hapi.streams.SidecarMetadata;
import com.hedera.hapi.streams.TransactionSidecarRecord;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.tools.blocks.validation.SidecarIntegrityValidation.TimestampMismatch;
import org.hiero.block.tools.records.model.parsed.ValidationException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Tests for {@link SidecarIntegrityValidation}. */
class SidecarIntegrityValidationTest {

    private final SidecarIntegrityValidation validation = new SidecarIntegrityValidation();

    private static BlockUnparsed toUnparsed(final Block block) {
        try {
            return standardParse(BlockUnparsed.PROTOBUF, Block.PROTOBUF.toBytes(block));
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static SidecarMetadata metadataFor(final SidecarFile sidecar) {
        final byte[] hash = hashSha384(SidecarFile.PROTOBUF.toBytes(sidecar).toByteArray());
        return SidecarMetadata.newBuilder()
                .hash(HashObject.newBuilder()
                        .algorithm(HashAlgorithm.SHA_384)
                        .length(hash.length)
                        .hash(Bytes.wrap(hash))
                        .build())
                .build();
    }

    private static SidecarFile sidecarWith(final int payloadByte) {
        // A minimal-but-distinct SidecarFile: distinct sidecar_records payloads produce
        // distinct serialized bytes and therefore distinct SHA-384 hashes.
        return SidecarFile.newBuilder()
                .sidecarRecords(List.of())
                // repeated bytes on the SidecarRecord type isn't available here; use a distinct
                // signature by varying the number of records via padding metadata. The specific
                // shape doesn't matter -- what matters is that different `payloadByte` values
                // produce different serialized bytes so the SHA-384 hashes differ.
                .build();
    }

    /**
     * Build a wrapped block containing a single RecordFile item whose recordFileContents
     * carries {@code sidecarMetadatas} and whose sidecarFileContents carries {@code sidecars}.
     */
    private static Block wrappedBlockWith(
            final List<SidecarFile> sidecars, final List<SidecarMetadata> sidecarMetadatas) {
        final RecordStreamFile recordStreamFile =
                RecordStreamFile.newBuilder().sidecars(sidecarMetadatas).build();
        final RecordFileItem recordFileItem = RecordFileItem.newBuilder()
                .recordFileContents(recordStreamFile)
                .sidecarFileContents(sidecars)
                .build();
        final BlockItem recordFileBlockItem =
                BlockItem.newBuilder().recordFile(recordFileItem).build();
        return new Block(List.of(recordFileBlockItem));
    }

    @Test
    @DisplayName("Empty sidecar list passes trivially")
    void noSidecars_passes() {
        final Block block = wrappedBlockWith(List.of(), List.of());
        assertDoesNotThrow(() -> validation.validate(toUnparsed(block), 42));
    }

    @Test
    @DisplayName("Block with no RecordFile item at all passes trivially")
    void noRecordFileItem_passes() {
        final Block block = new Block(List.of());
        assertDoesNotThrow(() -> validation.validate(toUnparsed(block), 42));
    }

    @Test
    @DisplayName("Sidecar whose SHA-384 matches an entry in signed metadata passes")
    void matchingSidecarHash_passes() {
        final SidecarFile sidecar = sidecarWith(1);
        final SidecarMetadata metadata = metadataFor(sidecar);
        final Block block = wrappedBlockWith(List.of(sidecar), List.of(metadata));
        assertDoesNotThrow(() -> validation.validate(toUnparsed(block), 42));
    }

    @Test
    @DisplayName("Sidecar with no matching hash in signed metadata fails with diagnostic")
    void unmatchedSidecarHash_fails() {
        final SidecarFile sidecar = sidecarWith(1);
        // Use a garbage 48-byte hash that cannot match anything real.
        final byte[] wrongHash = new byte[48];
        for (int i = 0; i < wrongHash.length; i++) {
            wrongHash[i] = (byte) 0xAA;
        }
        final SidecarMetadata wrongMetadata = SidecarMetadata.newBuilder()
                .hash(HashObject.newBuilder()
                        .algorithm(HashAlgorithm.SHA_384)
                        .length(wrongHash.length)
                        .hash(Bytes.wrap(wrongHash))
                        .build())
                .build();
        final Block block = wrappedBlockWith(List.of(sidecar), List.of(wrongMetadata));
        final ValidationException ex =
                assertThrows(ValidationException.class, () -> validation.validate(toUnparsed(block), 42));
        assertTrue(ex.getMessage().contains("Block 42"), "message must name the failing block number");
        assertTrue(ex.getMessage().contains("sidecar #0"), "message must name the failing sidecar index");
        assertTrue(
                ex.getMessage().contains("TAMPERED or EXTRA"),
                "message must classify the failure mode as TAMPERED/EXTRA");
        // The wrongMetadata hash points to nothing in the block, so it also reports as MISSING.
        assertTrue(ex.getMessage().contains("MISSING"), "message must also flag the orphan metadata entry as MISSING");
    }

    @Test
    @DisplayName("Sidecar with no recordFileContents to check against fails")
    void sidecarWithoutRecordFileContents_fails() {
        final SidecarFile sidecar = sidecarWith(1);
        final RecordFileItem recordFileItem = RecordFileItem.newBuilder()
                .sidecarFileContents(List.of(sidecar))
                .build();
        final BlockItem recordFileBlockItem =
                BlockItem.newBuilder().recordFile(recordFileItem).build();
        final Block block = new Block(List.of(recordFileBlockItem));
        final ValidationException ex =
                assertThrows(ValidationException.class, () -> validation.validate(toUnparsed(block), 42));
        assertTrue(ex.getMessage().contains("no recordFileContents to check them against"));
    }

    // ── Shared static helper (validateSidecars) — direct tests ─────────────

    @Test
    @DisplayName("Static helper: empty sidecar list passes without touching metadata")
    void staticHelper_empty_passes() {
        assertDoesNotThrow(() -> SidecarIntegrityValidation.validateSidecars(List.of(), List.of(), 42));
    }

    @Test
    @DisplayName("Static helper: single matching sidecar passes")
    void staticHelper_singleMatch_passes() {
        final SidecarFile s = SidecarFile.DEFAULT;
        assertDoesNotThrow(() -> SidecarIntegrityValidation.validateSidecars(List.of(s), List.of(metadataFor(s)), 42));
    }

    @Test
    @DisplayName("Static helper: multi-sidecar all-match passes")
    void staticHelper_multiMatch_passes() {
        final SidecarFile s0 = SidecarFile.DEFAULT;
        final SidecarFile s1 = SidecarFile.newBuilder().build();
        // s0 and s1 serialize identically here (both default); the check should still pass
        // because every sidecar has *some* matching hash in the metadata.
        assertDoesNotThrow(() -> SidecarIntegrityValidation.validateSidecars(
                List.of(s0, s1), List.of(metadataFor(s0), metadataFor(s1)), 42));
    }

    @Test
    @DisplayName("Static helper: unmatched extra metadata entry is flagged as MISSING sidecar")
    void staticHelper_extraMetadata_flaggedMissing() {
        final SidecarFile s = SidecarFile.DEFAULT;
        final byte[] unrelated = new byte[48];
        for (int i = 0; i < unrelated.length; i++) {
            unrelated[i] = (byte) 0xEE;
        }
        final SidecarMetadata unrelatedMeta = SidecarMetadata.newBuilder()
                .hash(HashObject.newBuilder()
                        .algorithm(HashAlgorithm.SHA_384)
                        .length(unrelated.length)
                        .hash(Bytes.wrap(unrelated))
                        .build())
                .build();
        // Signed list has two hashes: one for s (valid), one unrelated (missing sidecar).
        final ValidationException ex = assertThrows(
                ValidationException.class,
                () -> SidecarIntegrityValidation.validateSidecars(
                        List.of(s), List.of(unrelatedMeta, metadataFor(s)), 42));
        assertTrue(ex.getMessage().contains("MISSING"), "orphan metadata hash must be flagged as MISSING");
        assertTrue(ex.getMessage().contains("signed hash #0"), "message must name the missing metadata index");
    }

    @Test
    @DisplayName("Static helper: metadata entries with no hash field are skipped, not treated as matches")
    void staticHelper_metadataWithoutHash_skipped() {
        final SidecarFile s = SidecarFile.DEFAULT;
        final SidecarMetadata noHash = SidecarMetadata.newBuilder().build();
        // With only a hashless metadata entry present, the sidecar has no match and must fail.
        final ValidationException ex = assertThrows(
                ValidationException.class,
                () -> SidecarIntegrityValidation.validateSidecars(List.of(s), List.of(noHash), 42));
        assertTrue(ex.getMessage().contains("TAMPERED or EXTRA"), "sidecar with no matching hash is TAMPERED/EXTRA");
        assertTrue(ex.getMessage().contains("sidecar #0"), "message must name the failing sidecar index");
    }

    @Test
    @DisplayName("Static helper: MISSING failure mode surfaces when a sidecar is dropped from the block")
    void staticHelper_missingSidecar_flaggedMissing() {
        // Two hashes in the signed list, but only one sidecar in the block: the block is missing
        // the second sidecar. Assemble metadata as two DIFFERENT hashes so we can tell the
        // missing one from the present one.
        final SidecarFile present = SidecarFile.DEFAULT;
        final byte[] missingHash = new byte[48];
        for (int i = 0; i < missingHash.length; i++) {
            missingHash[i] = (byte) 0x99;
        }
        final SidecarMetadata missingMeta = SidecarMetadata.newBuilder()
                .hash(HashObject.newBuilder()
                        .algorithm(HashAlgorithm.SHA_384)
                        .length(missingHash.length)
                        .hash(Bytes.wrap(missingHash))
                        .build())
                .build();
        final ValidationException ex = assertThrows(
                ValidationException.class,
                () -> SidecarIntegrityValidation.validateSidecars(
                        List.of(present), List.of(metadataFor(present), missingMeta), 42));
        assertTrue(ex.getMessage().contains("MISSING"));
        assertTrue(ex.getMessage().contains("sidecars in block:    1"));
        assertTrue(ex.getMessage().contains("signed hash entries:  2"));
    }

    // ── Sidecar record consensus_timestamp cross-check (warn-only, issue #3319) ─────────────

    private static Timestamp ts(final long seconds, final int nanos) {
        return new Timestamp(seconds, nanos);
    }

    /** Build a RecordStreamFile whose recordStreamItems carry transactions with the given consensus timestamps. */
    private static RecordStreamFile recordStreamFileWithTxTimestamps(final Timestamp... txTimestamps) {
        final List<RecordStreamItem> items = new ArrayList<>(txTimestamps.length);
        for (final Timestamp t : txTimestamps) {
            items.add(RecordStreamItem.newBuilder()
                    .record(TransactionRecord.newBuilder().consensusTimestamp(t).build())
                    .build());
        }
        return RecordStreamFile.newBuilder().recordStreamItems(items).build();
    }

    /** Build a SidecarFile containing one TransactionSidecarRecord per supplied timestamp. */
    private static SidecarFile sidecarWithRecordTimestamps(final Timestamp... recordTimestamps) {
        final List<TransactionSidecarRecord> records = new ArrayList<>(recordTimestamps.length);
        for (final Timestamp t : recordTimestamps) {
            records.add(
                    TransactionSidecarRecord.newBuilder().consensusTimestamp(t).build());
        }
        return SidecarFile.newBuilder().sidecarRecords(records).build();
    }

    @Test
    @DisplayName("Timestamp check: all sidecar timestamps match tx timestamps -> no mismatches recorded")
    void timestampCheck_allMatch_noMismatches() {
        final Timestamp t1 = ts(100, 1);
        final Timestamp t2 = ts(100, 2);
        final SidecarFile sidecar = sidecarWithRecordTimestamps(t1, t2);
        final RecordStreamFile rsf = recordStreamFileWithTxTimestamps(t1, t2);
        final List<TimestampMismatch> collected = new ArrayList<>();

        SidecarIntegrityValidation.checkSidecarTimestamps(List.of(sidecar), rsf, 42, collected::add);

        assertEquals(0, collected.size(), "no warnings when every sidecar timestamp has a tx");
    }

    @Test
    @DisplayName("Timestamp check: one sidecar timestamp not in tx set -> single warning recorded")
    void timestampCheck_oneMismatch_singleWarning() {
        final Timestamp inRecord = ts(100, 1);
        final Timestamp orphan = ts(999, 999);
        final SidecarFile sidecar = sidecarWithRecordTimestamps(inRecord, orphan);
        final RecordStreamFile rsf = recordStreamFileWithTxTimestamps(inRecord);
        final List<TimestampMismatch> collected = new ArrayList<>();

        SidecarIntegrityValidation.checkSidecarTimestamps(List.of(sidecar), rsf, 42, collected::add);

        assertEquals(1, collected.size(), "one mismatch tuple recorded");
        final TimestampMismatch m = collected.get(0);
        assertEquals(42L, m.blockNumber());
        assertEquals(0, m.sidecarIndex());
        assertEquals(1, m.recordIndex(), "the orphan record is at position 1");
        assertEquals(999L, m.timestampSeconds());
        assertEquals(999, m.timestampNanos());
    }

    @Test
    @DisplayName("Timestamp check: sidecar with empty sidecar_records passes trivially")
    void timestampCheck_emptySidecarRecords_passes() {
        final SidecarFile empty = sidecarWithRecordTimestamps();
        final RecordStreamFile rsf = recordStreamFileWithTxTimestamps(ts(1, 0));
        final List<TimestampMismatch> collected = new ArrayList<>();

        SidecarIntegrityValidation.checkSidecarTimestamps(List.of(empty), rsf, 42, collected::add);

        assertEquals(0, collected.size());
    }

    @Test
    @DisplayName("Timestamp check: block with no sidecar files passes trivially")
    void timestampCheck_noSidecars_passes() {
        final RecordStreamFile rsf = recordStreamFileWithTxTimestamps(ts(1, 0));
        final List<TimestampMismatch> collected = new ArrayList<>();

        SidecarIntegrityValidation.checkSidecarTimestamps(List.of(), rsf, 42, collected::add);

        assertEquals(0, collected.size());
    }

    @Test
    @DisplayName("Timestamp check: empty recordStreamItems + non-empty sidecar -> warn for every record")
    void timestampCheck_emptyRecordStreamItems_warnsForEveryRecord() {
        final SidecarFile sidecar = sidecarWithRecordTimestamps(ts(1, 0), ts(2, 0), ts(3, 0));
        final RecordStreamFile rsf = RecordStreamFile.newBuilder().build(); // no items
        final List<TimestampMismatch> collected = new ArrayList<>();

        SidecarIntegrityValidation.checkSidecarTimestamps(List.of(sidecar), rsf, 42, collected::add);

        assertEquals(3, collected.size(), "every record has no matching tx -> every record warns");
    }

    @Test
    @DisplayName("Instance validate: timestamp mismatches accumulate across blocks (warn-only, no throw)")
    void instanceValidate_accumulatesTimestampMismatches() {
        final Timestamp inRecord = ts(100, 1);
        final Timestamp orphan = ts(999, 999);
        final SidecarFile sidecar = sidecarWithRecordTimestamps(orphan);
        final SidecarMetadata metadata = metadataFor(sidecar);
        final RecordStreamFile rsf = RecordStreamFile.newBuilder()
                .recordStreamItems(RecordStreamItem.newBuilder()
                        .record(TransactionRecord.newBuilder()
                                .consensusTimestamp(inRecord)
                                .build())
                        .build())
                .sidecars(List.of(metadata))
                .build();
        final RecordFileItem recordFileItem = RecordFileItem.newBuilder()
                .recordFileContents(rsf)
                .sidecarFileContents(List.of(sidecar))
                .build();
        final BlockItem recordFileBlockItem =
                BlockItem.newBuilder().recordFile(recordFileItem).build();
        final Block block = new Block(List.of(recordFileBlockItem));

        final SidecarIntegrityValidation v = new SidecarIntegrityValidation();

        // Hash check passes (sidecar hash matches metadata). Timestamp check fails but is warn-only.
        assertDoesNotThrow(() -> v.validate(toUnparsed(block), 42));

        assertEquals(1, v.recordedMismatches().size(), "the orphan timestamp is recorded in the accumulator");
        assertEquals(42L, v.recordedMismatches().get(0).blockNumber());
    }

    @Test
    @DisplayName("Composition: hash mismatch still throws even when timestamp mismatch is present")
    void composition_hashMismatchStillThrows_timestampAlsoRecorded() {
        final Timestamp orphan = ts(999, 999);
        final SidecarFile sidecar = sidecarWithRecordTimestamps(orphan);
        // Give the block a metadata entry whose hash does NOT match the sidecar.
        final byte[] bogus = new byte[48];
        for (int i = 0; i < bogus.length; i++) {
            bogus[i] = (byte) 0xBB;
        }
        final SidecarMetadata bogusMeta = SidecarMetadata.newBuilder()
                .hash(HashObject.newBuilder()
                        .algorithm(HashAlgorithm.SHA_384)
                        .length(bogus.length)
                        .hash(Bytes.wrap(bogus))
                        .build())
                .build();
        final RecordStreamFile rsf = RecordStreamFile.newBuilder()
                .recordStreamItems(RecordStreamItem.newBuilder()
                        .record(TransactionRecord.newBuilder()
                                .consensusTimestamp(ts(100, 1))
                                .build())
                        .build())
                .sidecars(List.of(bogusMeta))
                .build();
        final RecordFileItem recordFileItem = RecordFileItem.newBuilder()
                .recordFileContents(rsf)
                .sidecarFileContents(List.of(sidecar))
                .build();
        final BlockItem recordFileBlockItem =
                BlockItem.newBuilder().recordFile(recordFileItem).build();
        final Block block = new Block(List.of(recordFileBlockItem));

        final SidecarIntegrityValidation v = new SidecarIntegrityValidation();

        // Hash-mismatch still throws (existing behaviour preserved).
        final ValidationException ex = assertThrows(ValidationException.class, () -> v.validate(toUnparsed(block), 42));
        assertTrue(ex.getMessage().contains("TAMPERED or EXTRA"), "hash-mismatch classification preserved");

        // Timestamp WARN also fires before the throw, so the accumulator still has an entry.
        assertEquals(1, v.recordedMismatches().size(), "timestamp mismatch is recorded even when hash check throws");
    }

    @Test
    @DisplayName("save(): no file written when no timestamp mismatches were recorded")
    void save_emptyAccumulator_noFile(@TempDir final Path tempDir) throws IOException {
        final SidecarIntegrityValidation v = new SidecarIntegrityValidation();

        v.save(tempDir);

        final Path expected = tempDir.resolve(SidecarIntegrityValidation.MISMATCH_REPORT_FILE_NAME);
        assertFalse(Files.exists(expected), "empty accumulator must NOT create a report file");
    }

    @Test
    @DisplayName("save(): report file lists all tuples sorted by block, then sidecar, then record")
    void save_multipleMismatches_fileHasSortedTuples(@TempDir final Path tempDir) throws IOException {
        final SidecarIntegrityValidation v = new SidecarIntegrityValidation();
        // Insert out-of-order to prove the file writer sorts.
        v.record(new TimestampMismatch(99L, 0, 2, 500L, 5));
        v.record(new TimestampMismatch(42L, 1, 0, 100L, 1));
        v.record(new TimestampMismatch(42L, 0, 3, 200L, 2));
        v.record(new TimestampMismatch(42L, 0, 1, 150L, 3));

        v.save(tempDir);

        final Path outFile = tempDir.resolve(SidecarIntegrityValidation.MISMATCH_REPORT_FILE_NAME);
        assertTrue(Files.exists(outFile));
        final List<String> lines = Files.readAllLines(outFile);
        assertEquals(4, lines.size());
        // Expected sort: (42, 0, 1), (42, 0, 3), (42, 1, 0), (99, 0, 2)
        assertEquals("42\t0\t1\t150.000000003", lines.get(0));
        assertEquals("42\t0\t3\t200.000000002", lines.get(1));
        assertEquals("42\t1\t0\t100.000000001", lines.get(2));
        assertEquals("99\t0\t2\t500.000000005", lines.get(3));
    }

    @Test
    @DisplayName("finalize(): no-op with an empty accumulator (no throw)")
    void finalize_emptyAccumulator_noOp() {
        final SidecarIntegrityValidation v = new SidecarIntegrityValidation();
        assertDoesNotThrow(() -> v.finalize(0L, 0L));
    }
}
