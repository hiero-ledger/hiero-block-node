// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.model;

import static org.hiero.block.tools.utils.TimeUtils.toTimestamp;

import com.hedera.hapi.node.base.Timestamp;
import com.hedera.hapi.node.transaction.NodeStake;
import com.hedera.hapi.node.transaction.NodeStakeUpdateTransactionBody;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.stream.ReadableStreamingData;
import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.hiero.block.internal.DatedNodeStake;
import org.hiero.block.internal.NodeStakeEntry;
import org.hiero.block.internal.NodeStakeHistory;
import picocli.CommandLine.Help.Ansi;

/**
 * Registry of node stake snapshots discovered from {@code NodeStakeUpdate} transactions
 * in the block stream. Snapshots are ordered by block timestamp (oldest first) and can
 * be looked up by block time to determine the stake map in effect for a given block.
 *
 * <p>The registry starts empty. Stake data is populated as {@code NodeStakeUpdate}
 * transactions are encountered. Before any stake data is available (pre-staking era),
 * callers should fall back to equal-weight consensus.
 */
public class NodeStakeRegistry {
    /** List of dated node stake snapshots, ordered by block timestamp, oldest first.
     * Uses CopyOnWriteArrayList for thread-safe reads during parallel signature validation
     * while the main thread may append new entries discovered from block data. */
    private final List<DatedNodeStake> stakeSnapshots = new CopyOnWriteArrayList<>();

    /**
     * Create a new empty NodeStakeRegistry.
     */
    public NodeStakeRegistry() {}

    /**
     * Create a new NodeStakeRegistry by loading from a JSON file.
     *
     * @param jsonFile the path to the JSON file
     */
    public NodeStakeRegistry(Path jsonFile) {
        try (var in = new ReadableStreamingData(Files.newInputStream(jsonFile))) {
            NodeStakeHistory history = NodeStakeHistory.JSON.parse(in);
            stakeSnapshots.addAll(history.stakeSnapshots());
        } catch (IOException | ParseException e) {
            throw new UncheckedIOException(
                    new IOException("Error loading Node Stake History JSON file " + jsonFile, e));
        }
    }

    /**
     * Save the node stake registry to a JSON file.
     *
     * @param file the path to the JSON file
     */
    public void saveToJsonFile(Path file) {
        try (var out = new WritableStreamingData(Files.newOutputStream(file))) {
            NodeStakeHistory history = new NodeStakeHistory(stakeSnapshots);
            NodeStakeHistory.JSON.write(history, out);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Reload the node stake registry from a JSON file, replacing all current entries.
     *
     * @param jsonFile the path to the JSON file
     */
    public void reloadFromFile(Path jsonFile) {
        try (var in = new ReadableStreamingData(Files.newInputStream(jsonFile))) {
            NodeStakeHistory history = NodeStakeHistory.JSON.parse(in);
            stakeSnapshots.clear();
            stakeSnapshots.addAll(history.stakeSnapshots());
        } catch (IOException | ParseException e) {
            throw new UncheckedIOException(
                    new IOException("Error reloading Node Stake History JSON file " + jsonFile, e));
        }
    }

    /**
     * Update the stake registry with data from a {@code NodeStakeUpdate} transaction.
     * A new snapshot is appended only if the stake data differs from the most recent snapshot.
     *
     * @param blockInstant the block timestamp (typically blockTime + 1ns so it applies to subsequent blocks)
     * @param body the NodeStakeUpdateTransactionBody containing stake entries
     * @return a description of changes, or null if no change
     */
    public String updateStakes(Instant blockInstant, NodeStakeUpdateTransactionBody body) {
        final List<NodeStakeEntry> entries = new ArrayList<>();
        for (NodeStake ns : body.nodeStake()) {
            entries.add(new NodeStakeEntry(ns.nodeId(), ns.stake()));
        }
        // Check if this is a duplicate of the last snapshot
        if (!stakeSnapshots.isEmpty()) {
            final DatedNodeStake last = stakeSnapshots.getLast();
            if (last.entries().equals(entries)) {
                return null;
            }
        }
        stakeSnapshots.add(new DatedNodeStake(toTimestamp(blockInstant), entries));
        final long totalStake =
                entries.stream().mapToLong(NodeStakeEntry::stake).sum();
        final StringBuilder sb = new StringBuilder();
        sb.append(entries.size())
                .append(" nodes, totalStake=")
                .append(totalStake)
                .append("\n");
        for (NodeStakeEntry entry : entries) {
            final double pct = totalStake > 0 ? (entry.stake() * 100.0) / totalStake : 0;
            sb.append(String.format("    node %d: stake=%d (%.2f%%)%n", entry.nodeId(), entry.stake(), pct));
        }
        return sb.toString();
    }

    /**
     * Get the stake map that was in effect at the given block time.
     *
     * @param blockTime the block time to look up
     * @return a map of nodeId to stake weight, or {@code null} if no stake data is available
     */
    public Map<Long, Long> getStakeMapForBlock(Instant blockTime) {
        if (stakeSnapshots.isEmpty()) {
            return null;
        }
        // Find the most recent snapshot with a timestamp less than or equal to blockTime
        DatedNodeStake selected = null;
        for (int i = 0; i < stakeSnapshots.size(); i++) {
            DatedNodeStake snapshot = stakeSnapshots.get(i);
            final Timestamp ts = snapshot.blockTimestampOrThrow();
            final Instant snapshotInstant = Instant.ofEpochSecond(ts.seconds(), ts.nanos());
            if (snapshotInstant.isAfter(blockTime)) {
                break;
            }
            selected = snapshot;
        }
        if (selected == null) {
            return null;
        }
        final Map<Long, Long> stakeMap = new LinkedHashMap<>();
        for (NodeStakeEntry entry : selected.entries()) {
            stakeMap.put(entry.nodeId(), entry.stake());
        }
        return Collections.unmodifiableMap(stakeMap);
    }

    /**
     * Returns whether any stake data has been recorded.
     *
     * @return true if at least one snapshot exists
     */
    public boolean hasStakeData() {
        return !stakeSnapshots.isEmpty();
    }

    /**
     * Get the number of stake snapshots in the registry.
     *
     * @return the number of snapshots
     */
    public int getSnapshotCount() {
        return stakeSnapshots.size();
    }

    /**
     * Get a pretty string representation of the stake registry.
     *
     * @return a pretty string representation
     */
    public String toPrettyString() {
        StringBuilder sb = new StringBuilder();
        for (DatedNodeStake snapshot : stakeSnapshots) {
            sb.append("@|yellow      Block Time:|@ ")
                    .append(Instant.ofEpochSecond(
                            snapshot.blockTimestampOrThrow().seconds(),
                            snapshot.blockTimestampOrThrow().nanos()))
                    .append("  @|yellow Node Count:|@ ")
                    .append(snapshot.entries().size())
                    .append("\n");
        }
        if (stakeSnapshots.isEmpty()) {
            sb.append("No stake snapshots in registry.\n");
        }
        return Ansi.AUTO.string(sb.toString());
    }
}
