// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.model;

import static org.hiero.block.tools.utils.TimeUtils.toTimestamp;

import com.hedera.hapi.node.tss.LedgerIdNodeContribution;
import com.hedera.hapi.node.tss.LedgerIdPublicationTransactionBody;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.hedera.pbj.runtime.io.stream.ReadableStreamingData;
import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.hiero.block.api.RosterEntry;
import org.hiero.block.api.TssData;
import org.hiero.block.api.TssRoster;
import org.hiero.block.internal.DatedTssPublication;
import org.hiero.block.internal.TssPublicationHistory;
import org.hiero.block.tools.blocks.HasherStateFiles;
import picocli.CommandLine.Help.Ansi;

/**
 * Registry of TSS (Threshold Signature Scheme) publication snapshots discovered from
 * {@code LedgerIdPublication} transactions in the block stream. Publications are ordered
 * by block timestamp (oldest first).
 *
 * <p>The registry starts empty. TSS data is populated as {@code LedgerIdPublication}
 * transactions are encountered in the block stream.
 */
public class TssEnablementRegistry {
    /** List of dated TSS publications, ordered by block timestamp, oldest first.
     * Uses CopyOnWriteArrayList for thread-safe reads during parallel processing
     * while the main thread may append new entries discovered from block data. */
    private final List<DatedTssPublication> publications = new CopyOnWriteArrayList<>();

    /**
     * Record a new TSS publication discovered from a {@code LedgerIdPublication} transaction.
     * The HAPI transaction body is converted to the Block Node's canonical {@link TssData} format.
     *
     * @param blockInstant the block timestamp
     * @param blockNumber the block number containing the publication
     * @param body the LedgerIdPublicationTransactionBody
     * @return a human-readable description of the publication
     */
    public String recordPublication(
            final Instant blockInstant, final long blockNumber, final LedgerIdPublicationTransactionBody body) {
        final TssData tssData = toTssData(body, blockNumber);
        publications.add(new DatedTssPublication(toTimestamp(blockInstant), blockNumber, tssData));

        final StringBuilder sb = new StringBuilder();
        sb.append("LedgerID=")
                .append(fingerprint(tssData.ledgerId()))
                .append(", WRAPS VK size=")
                .append(tssData.wrapsVerificationKey().length())
                .append(" bytes, roster=")
                .append(tssData.currentRosterOrThrow().rosterEntries().size())
                .append(" nodes\n");
        long totalWeight = 0;
        for (RosterEntry entry : tssData.currentRosterOrThrow().rosterEntries()) {
            totalWeight += entry.weight();
        }
        for (RosterEntry entry : tssData.currentRosterOrThrow().rosterEntries()) {
            final double pct = totalWeight > 0 ? (entry.weight() * 100.0) / totalWeight : 0;
            sb.append(String.format(
                    "    node %d: weight=%d (%.2f%%), schnorrPublicKey=%s\n",
                    entry.nodeId(), entry.weight(), pct, fingerprint(entry.schnorrPublicKey())));
        }
        return sb.toString();
    }

    /**
     * Get the most recent TSS publication, or {@code null} if none.
     *
     * @return the latest publication or null
     */
    public DatedTssPublication getLatestPublication() {
        return publications.isEmpty() ? null : publications.getLast();
    }

    /**
     * Get the latest {@link TssData} from the most recent publication.
     *
     * @return the latest TssData, or {@code null} if no data
     */
    public TssData getLatestTssData() {
        final DatedTssPublication latest = getLatestPublication();
        return latest == null ? null : latest.tssData();
    }

    /**
     * Save the registry to a JSON file.
     *
     * @param file the path to the JSON file
     */
    public void saveToJsonFile(final Path file) {
        try (var out = new WritableStreamingData(Files.newOutputStream(file))) {
            TssPublicationHistory history = new TssPublicationHistory(publications);
            TssPublicationHistory.JSON.write(history, out);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Reload the registry from a JSON file, replacing all current entries.
     *
     * @param jsonFile the path to the JSON file
     */
    public void reloadFromFile(final Path jsonFile) {
        try (var in = new ReadableStreamingData(Files.newInputStream(jsonFile))) {
            TssPublicationHistory history = TssPublicationHistory.JSON.parse(in);
            publications.clear();
            publications.addAll(history.publications());
        } catch (IOException | ParseException e) {
            throw new UncheckedIOException(
                    new IOException("Error reloading TSS Publication History JSON file " + jsonFile, e));
        }
    }

    /**
     * Write the latest {@link TssData} as raw protobuf binary for the block node to consume.
     *
     * @param file the output file path
     * @throws IOException if the file cannot be written
     */
    public void writeTssParametersBin(final Path file) throws Exception {
        final TssData tssData = getLatestTssData();
        if (tssData == null) {
            return;
        }
        HasherStateFiles.saveAtomically(file, tmpPath -> {
            final Bytes bytes = TssData.PROTOBUF.toBytes(tssData);
            Files.write(tmpPath, bytes.toByteArray());
        });
    }

    /**
     * Returns whether any TSS publication data has been recorded.
     *
     * @return true if at least one publication exists
     */
    public boolean hasTssData() {
        return !publications.isEmpty();
    }

    /**
     * Get the number of publications in the registry.
     *
     * @return the number of publications
     */
    public int getPublicationCount() {
        return publications.size();
    }

    /**
     * Get a pretty string representation of the registry.
     *
     * @return a pretty string representation
     */
    public String toPrettyString() {
        StringBuilder sb = new StringBuilder();
        for (DatedTssPublication pub : publications) {
            sb.append("@|yellow      Block Time:|@ ")
                    .append(Instant.ofEpochSecond(
                            pub.blockTimestampOrThrow().seconds(),
                            pub.blockTimestampOrThrow().nanos()))
                    .append("  @|yellow Block:|@ ")
                    .append(pub.blockNumber())
                    .append("  @|yellow Roster:|@ ")
                    .append(
                            pub.tssData() != null
                                    ? pub.tssData()
                                            .currentRosterOrThrow()
                                            .rosterEntries()
                                            .size()
                                    : 0)
                    .append(" nodes\n");
        }
        if (publications.isEmpty()) {
            sb.append("No TSS publications in registry.\n");
        }
        return Ansi.AUTO.string(sb.toString());
    }

    /**
     * Convert a HAPI {@link LedgerIdPublicationTransactionBody} to the Block Node's
     * canonical {@link TssData} format.
     *
     * @param body the LedgerIdPublication transaction body
     * @param blockNumber the block number containing the publication
     * @return the converted TssData
     * @throws IllegalArgumentException if the body is missing required fields
     */
    private static TssData toTssData(final LedgerIdPublicationTransactionBody body, final long blockNumber) {
        if (body.ledgerId() == null || body.ledgerId().length() == 0) {
            throw new IllegalArgumentException("LedgerIdPublication at block " + blockNumber + " is missing ledgerId");
        }
        if (body.historyProofVerificationKey() == null
                || body.historyProofVerificationKey().length() == 0) {
            throw new IllegalArgumentException(
                    "LedgerIdPublication at block " + blockNumber + " is missing historyProofVerificationKey");
        }
        if (body.nodeContributions() == null || body.nodeContributions().isEmpty()) {
            throw new IllegalArgumentException(
                    "LedgerIdPublication at block " + blockNumber + " has no nodeContributions");
        }
        final List<RosterEntry> rosterEntries = new ArrayList<>();
        for (final LedgerIdNodeContribution c : body.nodeContributions()) {
            rosterEntries.add(RosterEntry.newBuilder()
                    .nodeId(c.nodeId())
                    .weight(c.weight())
                    .schnorrPublicKey(c.historyProofKey())
                    .build());
        }
        return TssData.newBuilder()
                .ledgerId(body.ledgerId())
                .wrapsVerificationKey(body.historyProofVerificationKey())
                .currentRoster(TssRoster.newBuilder()
                        .rosterEntries(rosterEntries)
                        .validFromBlock(blockNumber)
                        .build())
                .validFromBlock(blockNumber)
                .build();
    }

    /**
     * Returns a short hex fingerprint (first 8 hex chars) of the given bytes.
     */
    private static String fingerprint(final Bytes bytes) {
        if (bytes == null || bytes.length() == 0) {
            return "(empty)";
        }
        final StringBuilder hex = new StringBuilder();
        final int len = (int) Math.min(4, bytes.length());
        for (int i = 0; i < len; i++) {
            hex.append(String.format("%02x", bytes.getByte(i)));
        }
        return hex.append("...").toString();
    }
}
