// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

import static org.hiero.block.tools.blocks.validation.BlockExtractionUtils.extractRecordFileBytes;
import static org.hiero.block.tools.blocks.validation.ProtobufParsingConstants.MAX_MESSAGE_SIZE;
import static org.hiero.block.tools.utils.Sha384.sha384Digest;

import com.hedera.hapi.block.stream.RecordFileItem;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.hapi.streams.RecordStreamFile;
import com.hedera.hapi.streams.RecordStreamItem;
import com.hedera.hapi.streams.SidecarFile;
import com.hedera.hapi.streams.SidecarMetadata;
import com.hedera.hapi.streams.TransactionSidecarRecord;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.tools.records.model.parsed.ValidationException;

/**
 * Verifies that every {@code SidecarFile} embedded in a wrapped block matches a SHA-384 hash
 * listed in the record file's signed {@code sidecars[]} metadata.
 *
 * <p>The record file's RSA signature covers the sidecar hash list but not the sidecar bytes
 * themselves. Without this validation, a byte-vs-hash divergence at production time (or in
 * transit between the recordstream store and the wrapped block) would be silently baked into
 * the wrapped block and accepted downstream. See issue #3196 for background.
 *
 * <p>The check operates directly on the wrapped block: both the raw sidecar bytes
 * ({@code RecordFileItem.sidecarFileContents}) and the signed hash list
 * ({@code RecordFileItem.recordFileContents.sidecars[]}) are embedded at wrap time, so
 * historical blocks can be validated retroactively without re-wrapping.
 *
 * <p>Two severity levels are enforced:
 *
 * <ul>
 *   <li><b>Hash mismatch — hard fail.</b> Every sidecar's SHA-384 must appear in the signed
 *       hash list, and every signed hash must have a matching sidecar file. Failure throws a
 *       {@link ValidationException} classifying the discrepancy (TAMPERED_OR_EXTRA / MISSING).</li>
 *   <li><b>Sidecar {@link TransactionSidecarRecord#consensusTimestamp() consensus_timestamp}
 *       does not tie back to a transaction in the parent RecordStreamFile — warn only.</b>
 *       For each record in each sidecar we check that its {@code consensus_timestamp} appears
 *       as the {@code consensus_timestamp} of at least one {@code RecordStreamItem} in the
 *       record file. Mismatches log a WARN line but do not throw, since composition of the
 *       hash check + {@code SignatureValidation} already proves the sidecar bytes are what
 *       the CN produced — a timestamp miss only signals a CN-side "wrong sidecar attached to
 *       this block" bug that we track for triage rather than block on. Every warning is
 *       recorded in an in-memory accumulator so {@link #finalize(long, long)} can emit a
 *       consolidated affected-blocks summary and {@link #save(Path)} can write a
 *       machine-readable {@code sidecar-timestamp-mismatches.txt} file. See issue #3319.</li>
 * </ul>
 */
public final class SidecarIntegrityValidation implements BlockValidation {

    private static final int MAX_DEPTH = 512;

    /** Filename of the machine-readable mismatch report written by {@link #save(Path)}. */
    static final String MISMATCH_REPORT_FILE_NAME = "sidecar-timestamp-mismatches.txt";

    /**
     * One recorded timestamp-mismatch. Emitted as a WARN line at check time and appended to the
     * instance's accumulator for later reporting.
     *
     * @param blockNumber        block whose embedded sidecar contained the offending record
     * @param sidecarIndex       zero-based index into {@code RecordFileItem.sidecarFileContents}
     * @param recordIndex        zero-based index into {@code SidecarFile.sidecarRecords}
     * @param timestampSeconds   the record's {@code consensus_timestamp} seconds field
     * @param timestampNanos     the record's {@code consensus_timestamp} nanos field
     */
    public record TimestampMismatch(
            long blockNumber, int sidecarIndex, int recordIndex, long timestampSeconds, int timestampNanos) {}

    /** Thread-safe accumulator populated by {@link #validate} across parallel worker threads. */
    private final Queue<TimestampMismatch> timestampMismatches = new ConcurrentLinkedQueue<>();

    @Override
    public String name() {
        return "Sidecar Integrity";
    }

    @Override
    public String description() {
        return "Verifies each sidecar file's SHA-384 hash matches an entry in the record file's signed sidecar metadata";
    }

    @Override
    public boolean requiresGenesisStart() {
        return false;
    }

    @Override
    public void validate(final BlockUnparsed block, final long blockNumber) throws ValidationException {
        try {
            final Bytes recordFileBytes = extractRecordFileBytes(block);
            if (recordFileBytes == null || recordFileBytes.length() == 0) {
                // No RecordFile item in this block. Sidecars only ship alongside RecordFile items,
                // so nothing to check. Structural presence of RecordFile is the job of
                // RequiredItemsValidation, not this one.
                return;
            }

            final RecordFileItem recordFileItem = RecordFileItem.PROTOBUF.parse(
                    recordFileBytes.toReadableSequentialData(), false, false, MAX_DEPTH, MAX_MESSAGE_SIZE);

            final List<SidecarFile> sidecarFiles = recordFileItem.sidecarFileContents();
            if (sidecarFiles.isEmpty()) {
                return;
            }

            if (!recordFileItem.hasRecordFileContents()) {
                throw new ValidationException("Block " + blockNumber + " has " + sidecarFiles.size()
                        + " sidecar file(s) but no recordFileContents to check them against");
            }
            final RecordStreamFile recordStreamFile = recordFileItem.recordFileContentsOrThrow();
            validateSidecars(sidecarFiles, recordStreamFile.sidecars(), recordStreamFile, blockNumber, this::record);
        } catch (final ParseException e) {
            throw new ValidationException("Block " + blockNumber + " - sidecar integrity check failed: "
                    + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    /** Package-private accumulator hook used by {@link #validate} and tests. */
    void record(final TimestampMismatch mismatch) {
        timestampMismatches.offer(mismatch);
    }

    /**
     * Legacy no-record-stream-file overload retained for wrap-time callers that already pass
     * only the manifest list. Runs only the hash check; the timestamp cross-check is skipped
     * because we have no {@code RecordStreamFile} to draw transaction timestamps from.
     */
    public static void validateSidecars(
            final List<SidecarFile> sidecarFiles, final List<SidecarMetadata> sidecarMetadatas, final long blockNumber)
            throws ValidationException {
        validateSidecars(sidecarFiles, sidecarMetadatas, null, blockNumber, null);
    }

    /**
     * Verify every {@link SidecarFile} in {@code sidecarFiles} has a matching SHA-384 hash in
     * {@code sidecarMetadatas} (hard fail on discrepancy) AND, when {@code recordStreamFile}
     * is provided, that every {@link TransactionSidecarRecord}'s {@code consensus_timestamp}
     * appears as the {@code consensus_timestamp} of at least one {@link RecordStreamItem} in
     * the record file (WARN only, tracked via {@code mismatchSink}).
     *
     * <p>Shared entry point so the wrap-time paths ({@code ToWrappedBlocksCommand},
     * {@code LiveSequential}) can invoke the same combined check they'd get on read-back
     * through the validation suite / {@code validate-sidecars} command.
     *
     * <p>Passes silently on an empty {@code sidecarFiles} list. On any hash discrepancy,
     * aggregates every issue across all sidecars and metadata entries and throws a single
     * {@link ValidationException} whose message classifies each issue by failure mode
     * (TAMPERED_OR_EXTRA / MISSING). Timestamp mismatches always log a WARN line to
     * {@code stderr}; when {@code mismatchSink} is non-null, each is also passed to it so an
     * instance-scoped accumulator can collect them for end-of-run reporting.
     *
     * @param sidecarFiles       parsed sidecar files embedded in the WRB
     * @param sidecarMetadatas   the record file's signed {@code sidecars[]} manifest
     * @param recordStreamFile   the parent record file (nullable; timestamp check is skipped if null)
     * @param blockNumber        the block number, for diagnostics
     * @param mismatchSink       optional per-mismatch callback; when null, WARN logging still fires
     * @throws ValidationException if any sidecar hash does not match the signed manifest, or vice versa
     */
    public static void validateSidecars(
            final List<SidecarFile> sidecarFiles,
            final List<SidecarMetadata> sidecarMetadatas,
            final RecordStreamFile recordStreamFile,
            final long blockNumber,
            final Consumer<TimestampMismatch> mismatchSink)
            throws ValidationException {
        if (sidecarFiles.isEmpty()) {
            return;
        }

        // Compute each sidecar's SHA-384 up front so we can do the two-way cross-check without
        // rehashing.
        final MessageDigest digest = sha384Digest();
        final byte[][] sidecarHashes = new byte[sidecarFiles.size()][];
        for (int i = 0; i < sidecarFiles.size(); i++) {
            SidecarFile.PROTOBUF.toBytes(sidecarFiles.get(i)).writeTo(digest);
            sidecarHashes[i] = digest.digest();
        }

        // Extract the set of expected hashes from metadata, skipping entries with no hash field.
        final List<byte[]> metadataHashes = new ArrayList<>(sidecarMetadatas.size());
        for (final SidecarMetadata meta : sidecarMetadatas) {
            if (meta.hasHash()) {
                metadataHashes.add(meta.hashOrThrow().hash().toByteArray());
            }
        }

        // Pass 1: which sidecars have no match in the signed list?
        //   (bytes-vs-hash divergence — TAMPERED — or count mismatch — EXTRA)
        final List<String> discrepancies = new ArrayList<>();
        for (int i = 0; i < sidecarHashes.length; i++) {
            if (!containsHash(metadataHashes, sidecarHashes[i])) {
                discrepancies.add(String.format(
                        "sidecar #%d SHA-384 %s -> no matching hash in signed metadata (TAMPERED or EXTRA)",
                        i, hex(sidecarHashes[i])));
            }
        }

        // Pass 2: which signed hashes have no matching sidecar bytes?
        //   (dropped-sidecar-at-wrap — MISSING)
        for (int j = 0; j < metadataHashes.size(); j++) {
            final byte[] expected = metadataHashes.get(j);
            boolean found = false;
            for (final byte[] sidecarHash : sidecarHashes) {
                if (Arrays.equals(sidecarHash, expected)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                discrepancies.add(String.format(
                        "signed hash #%d SHA-384 %s -> no matching sidecar file in block (MISSING)", j, hex(expected)));
            }
        }

        // Timestamp check (warn only). Runs regardless of whether the hash check found
        // discrepancies — a hash-tampered sidecar's timestamps are still worth logging so the
        // investigator has a complete picture rather than only the first failure mode.
        if (recordStreamFile != null) {
            checkSidecarTimestamps(sidecarFiles, recordStreamFile, blockNumber, mismatchSink);
        }

        if (discrepancies.isEmpty()) {
            return;
        }

        final StringBuilder sb = new StringBuilder();
        sb.append("Block ").append(blockNumber).append(" sidecar integrity failed:");
        sb.append("\n  sidecars in block:    ").append(sidecarFiles.size());
        sb.append("\n  signed hash entries:  ").append(metadataHashes.size());
        sb.append("\n  discrepancies:");
        for (final String d : discrepancies) {
            sb.append("\n    - ").append(d);
        }
        throw new ValidationException(sb.toString());
    }

    /**
     * For each {@link TransactionSidecarRecord} in each {@link SidecarFile}, verify its
     * {@code consensus_timestamp} appears as the {@code consensus_timestamp} of at least one
     * {@link RecordStreamItem} in {@code recordStreamFile}. Mismatches emit a WARN line to
     * {@code stderr} and (when {@code mismatchSink} is non-null) are passed to the sink.
     * Never throws — see the class javadoc for the severity rationale.
     */
    static void checkSidecarTimestamps(
            final List<SidecarFile> sidecarFiles,
            final RecordStreamFile recordStreamFile,
            final long blockNumber,
            final Consumer<TimestampMismatch> mismatchSink) {
        // Build the set of transaction consensus_timestamps once per block.
        final Set<Timestamp> txTimestamps = new HashSet<>();
        for (final RecordStreamItem item : recordStreamFile.recordStreamItems()) {
            if (item.hasRecord() && item.recordOrThrow().hasConsensusTimestamp()) {
                txTimestamps.add(item.recordOrThrow().consensusTimestampOrThrow());
            }
        }

        for (int i = 0; i < sidecarFiles.size(); i++) {
            final List<TransactionSidecarRecord> records = sidecarFiles.get(i).sidecarRecords();
            for (int j = 0; j < records.size(); j++) {
                final TransactionSidecarRecord tsr = records.get(j);
                if (!tsr.hasConsensusTimestamp()) {
                    continue;
                }
                final Timestamp ts = tsr.consensusTimestampOrThrow();
                if (!txTimestamps.contains(ts)) {
                    System.err.println("[SidecarTimestamp] WARN block " + blockNumber + " sidecar #" + i + " record #"
                            + j + " consensus_timestamp " + ts.seconds() + "." + String.format("%09d", ts.nanos())
                            + " has no matching transaction in the parent RecordStreamFile");
                    if (mismatchSink != null) {
                        mismatchSink.accept(new TimestampMismatch(blockNumber, i, j, ts.seconds(), ts.nanos()));
                    }
                }
            }
        }
    }

    /**
     * If any timestamp mismatches have been recorded during this run, write a machine-readable
     * report to {@code <directory>/sidecar-timestamp-mismatches.txt}. Empty runs write no file.
     *
     * <p>Line format (tab-separated, sorted by block number then sidecar index then record
     * index):
     *
     * <pre>
     * &lt;blockNumber&gt;\t&lt;sidecarIndex&gt;\t&lt;recordIndex&gt;\t&lt;seconds&gt;.&lt;nanos&gt;
     * </pre>
     */
    @Override
    public void save(final Path directory) throws IOException {
        if (timestampMismatches.isEmpty()) {
            return;
        }
        Files.createDirectories(directory);
        final Path outFile = directory.resolve(MISMATCH_REPORT_FILE_NAME);
        final List<TimestampMismatch> sorted = new ArrayList<>(timestampMismatches);
        sorted.sort(Comparator.comparingLong(TimestampMismatch::blockNumber)
                .thenComparingInt(TimestampMismatch::sidecarIndex)
                .thenComparingInt(TimestampMismatch::recordIndex));
        try (BufferedWriter w = Files.newBufferedWriter(outFile)) {
            for (final TimestampMismatch m : sorted) {
                w.write(m.blockNumber() + "\t" + m.sidecarIndex() + "\t" + m.recordIndex() + "\t" + m.timestampSeconds()
                        + "." + String.format("%09d", m.timestampNanos()) + "\n");
            }
        }
    }

    /**
     * Emit a consolidated affected-blocks summary to {@code stderr} at end of run. No-op if no
     * timestamp mismatches were recorded. Does not throw — timestamp mismatches are warn-only.
     */
    @Override
    public void finalize(final long totalBlocksValidated, final long lastBlockNumber) {
        if (timestampMismatches.isEmpty()) {
            return;
        }
        final Set<Long> distinctBlocks = new TreeSet<>();
        for (final TimestampMismatch m : timestampMismatches) {
            distinctBlocks.add(m.blockNumber());
        }
        System.err.println("[SidecarTimestamp] WARN Timestamp-mismatch warning fired on " + distinctBlocks.size()
                + " block(s): " + distinctBlocks);
    }

    /** Package-private accessor used by tests to inspect the accumulator contents. */
    List<TimestampMismatch> recordedMismatches() {
        return new ArrayList<>(timestampMismatches);
    }

    private static boolean containsHash(final List<byte[]> hashes, final byte[] target) {
        for (final byte[] h : hashes) {
            if (Arrays.equals(h, target)) {
                return true;
            }
        }
        return false;
    }

    private static String hex(final byte[] bytes) {
        final StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (final byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
