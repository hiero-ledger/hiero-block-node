// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;
import org.hiero.block.tools.records.model.unparsed.InMemoryFile;
import org.hiero.block.tools.utils.PrettyPrint;

/**
 * Collects per-day signature statistics and writes them to a CSV file.
 *
 * <p>Extracted from {@code ValidateWithStats} to be reusable across all commands.
 * Extends the original CSV format with stake weight columns when
 * {@link SignatureBlockStats} data is available.
 *
 * <p>Supports two recording modes:
 * <ul>
 *   <li><b>Legacy</b>: {@link #recordBlock(List, int, int)} for pre-wrapped record file
 *       validation (e.g. {@code ValidateWithStats}). Stake columns are empty.
 *   <li><b>Rich</b>: {@link #accept(SignatureBlockStats)} for wrapped block validation
 *       with full stake weight data. Auto-detects day boundaries from block timestamps.
 * </ul>
 */
public class SignatureStatsCollector implements Consumer<SignatureBlockStats>, AutoCloseable {

    /** Maximum number of signature-count histogram columns in the CSV. */
    private static final int MAX_SIG_COUNT = 40;

    /** Number of stake weight columns appended after the histogram. */
    private static final int STAKE_COLUMN_COUNT = 17;

    private final Path csvOutputFile;
    private boolean csvHeaderWritten = false;
    private LocalDate currentDay = null;
    private DayStatistics currentDayStats = null;
    private final List<DayStatistics> dailyStatistics = new ArrayList<>();

    /** Per-day statistics for signature validation. */
    static class DayStatistics {
        final LocalDate date;
        int totalBlocks = 0;
        int totalSignatures = 0;
        int totalValidSignatures = 0;
        long totalNodeSlots = 0;
        int minSignaturesInBlock = Integer.MAX_VALUE;
        final Map<Integer, Integer> blocksBySignatureCount = new TreeMap<>();

        // Stake weight fields (populated when accept() provides SignatureBlockStats)
        String mode = null;
        long totalStake = 0;
        double minValidatedStakePct = Double.MAX_VALUE;
        double maxValidatedStakePct = -1;
        double sumValidatedStakePct = 0;
        long closestThresholdMargin = Long.MAX_VALUE;
        int minSigningNodes = Integer.MAX_VALUE;
        int maxSigningNodes = 0;
        long sumSigningNodes = 0;
        Map<Long, Long> latestNodeStakes = null;
        final Set<Long> allNodes = new HashSet<>();
        final Map<Long, Integer> nodeBlockSignCount = new HashMap<>();
        int blocksWithStakeData = 0;

        DayStatistics(LocalDate date) {
            this.date = date;
        }

        void recordBlock(int signatureCount, int validSignatureCount, int addressBookNodeCount) {
            totalBlocks++;
            totalSignatures += signatureCount;
            totalValidSignatures += validSignatureCount;
            totalNodeSlots += addressBookNodeCount;
            if (signatureCount < minSignaturesInBlock) {
                minSignaturesInBlock = signatureCount;
            }
            blocksBySignatureCount.merge(signatureCount, 1, Integer::sum);
        }

        void recordStakeStats(SignatureBlockStats stats) {
            blocksWithStakeData++;
            if (mode == null) {
                mode = stats.stakeWeighted() ? "stake-weighted" : "equal-weight";
            }
            totalStake = stats.totalStake();
            updateValidatedStakePct(stats);
            updateThresholdMargin(stats);
            updateSigningNodeCounts(stats);
            updateNodeParticipation(stats);
        }

        private void updateValidatedStakePct(SignatureBlockStats stats) {
            double stakePct = stats.totalStake() > 0 ? (100.0 * stats.validatedStake()) / stats.totalStake() : 0;
            minValidatedStakePct = Math.min(minValidatedStakePct, stakePct);
            maxValidatedStakePct = Math.max(maxValidatedStakePct, stakePct);
            sumValidatedStakePct += stakePct;
        }

        private void updateThresholdMargin(SignatureBlockStats stats) {
            long margin = stats.validatedStake() - stats.threshold();
            closestThresholdMargin = Math.min(closestThresholdMargin, margin);
        }

        private void updateSigningNodeCounts(SignatureBlockStats stats) {
            int signingNodes = stats.validatedNodes().size();
            minSigningNodes = Math.min(minSigningNodes, signingNodes);
            maxSigningNodes = Math.max(maxSigningNodes, signingNodes);
            sumSigningNodes += signingNodes;
            if (stats.nodeStakes() != null && !stats.nodeStakes().isEmpty()) {
                latestNodeStakes = stats.nodeStakes();
            }
        }

        private void updateNodeParticipation(SignatureBlockStats stats) {
            for (Map.Entry<Long, SignatureBlockStats.NodeResult> entry :
                    stats.perNodeResults().entrySet()) {
                allNodes.add(entry.getKey());
                if (entry.getValue() == SignatureBlockStats.NodeResult.VERIFIED) {
                    nodeBlockSignCount.merge(entry.getKey(), 1, Integer::sum);
                }
            }
        }

        int getAverageAddressBookNodeCount() {
            if (totalBlocks == 0) return 0;
            return (int) (totalNodeSlots / totalBlocks);
        }

        double getAveragePercentage() {
            if (totalBlocks == 0 || totalNodeSlots == 0) return 0.0;
            return (100.0 * totalSignatures) / totalNodeSlots;
        }

        double getValidPercentage() {
            if (totalBlocks == 0 || totalNodeSlots == 0) return 0.0;
            return (100.0 * totalValidSignatures) / totalNodeSlots;
        }

        double getMinPercentage() {
            int avgNodes = getAverageAddressBookNodeCount();
            if (totalBlocks == 0 || avgNodes == 0 || minSignaturesInBlock == Integer.MAX_VALUE) return 0.0;
            return (100.0 * minSignaturesInBlock) / avgNodes;
        }

        double getAveragePercentagePerBlock() {
            if (totalBlocks == 0) return 0.0;
            double avgSigsPerBlock = (double) totalSignatures / totalBlocks;
            int avgNodes = getAverageAddressBookNodeCount();
            if (avgNodes == 0) return 0.0;
            return (100.0 * avgSigsPerBlock) / avgNodes;
        }

        boolean hasStakeData() {
            return blocksWithStakeData > 0;
        }

        double getMeanValidatedStakePct() {
            return blocksWithStakeData > 0 ? sumValidatedStakePct / blocksWithStakeData : 0;
        }

        double getMeanSigningNodes() {
            return blocksWithStakeData > 0 ? (double) sumSigningNodes / blocksWithStakeData : 0;
        }

        int getMissingSigners() {
            int count = 0;
            for (long nodeId : allNodes) {
                if (!nodeBlockSignCount.containsKey(nodeId) || nodeBlockSignCount.get(nodeId) == 0) {
                    count++;
                }
            }
            return count;
        }

        int getReliableSigners() {
            if (totalBlocks == 0) return 0;
            int count = 0;
            for (int signCount : nodeBlockSignCount.values()) {
                if (signCount >= totalBlocks) {
                    count++;
                }
            }
            return count;
        }
    }

    /**
     * Creates a new collector that writes CSV to the given path.
     *
     * @param csvOutputFile path to the CSV output file, or null to skip CSV output
     */
    public SignatureStatsCollector(Path csvOutputFile) {
        this.csvOutputFile = csvOutputFile;
    }

    /**
     * Starts a new day, flushing the previous day's statistics to CSV.
     *
     * @param day the date of the new day
     * @param addressBookNodeCount the number of nodes in the address book
     */
    public void startDay(LocalDate day, int addressBookNodeCount) {
        if (currentDay != null && currentDayStats != null) {
            dailyStatistics.add(currentDayStats);
            writeDayToCsv(currentDayStats);
        }
        currentDay = day;
        currentDayStats = new DayStatistics(day);
    }

    /**
     * Records block statistics from pre-wrapped record file validation.
     * Stake weight columns will be empty in CSV output.
     *
     * @param signatureFiles the signature files for the block
     * @param validSignatureCount the number of valid signatures
     * @param addressBookNodeCount the number of nodes in the address book
     */
    public void recordBlock(List<InMemoryFile> signatureFiles, int validSignatureCount, int addressBookNodeCount) {
        int totalSignaturesInBlock = 0;
        for (InMemoryFile sigFile : signatureFiles) {
            String fileName = sigFile.path().getFileName().toString();
            if (fileName.startsWith("node_") && (fileName.endsWith(".rcd_sig") || fileName.endsWith(".rcs_sig"))) {
                totalSignaturesInBlock++;
            }
        }
        if (currentDayStats != null) {
            currentDayStats.recordBlock(totalSignaturesInBlock, validSignatureCount, addressBookNodeCount);
        }
    }

    /**
     * Accepts a {@link SignatureBlockStats} from wrapped block validation.
     * Auto-detects day boundaries from block timestamps and records both
     * basic and stake weight statistics.
     *
     * @param stats the per-block signature validation statistics
     */
    @Override
    public void accept(SignatureBlockStats stats) {
        LocalDate blockDay = stats.blockTime().atZone(ZoneOffset.UTC).toLocalDate();
        if (currentDay == null || !currentDay.equals(blockDay)) {
            startDay(blockDay, stats.totalNodes());
        }
        if (currentDayStats != null) {
            currentDayStats.recordBlock(
                    stats.totalSignatures(), stats.validatedNodes().size(), stats.totalNodes());
            currentDayStats.recordStakeStats(stats);
        }
    }

    /**
     * Finalizes the last day's statistics and writes them to CSV.
     * Call this after all blocks have been processed.
     */
    public void finalizeDayStats() {
        if (currentDay != null && currentDayStats != null) {
            dailyStatistics.add(currentDayStats);
            writeDayToCsv(currentDayStats);
        }
    }

    /**
     * Prints an overall summary of all collected statistics.
     */
    public void printFinalSummary() {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("VALIDATION COMPLETE - ALL STATISTICS WRITTEN TO CSV");
        System.out.println("=".repeat(80));

        if (dailyStatistics.isEmpty()) {
            System.out.println("No daily statistics collected.");
            return;
        }

        long totalBlocks =
                dailyStatistics.stream().mapToLong(ds -> ds.totalBlocks).sum();
        long totalSignatures =
                dailyStatistics.stream().mapToLong(ds -> ds.totalSignatures).sum();
        int totalDays = dailyStatistics.size();

        System.out.printf("Total Days:        %d%n", totalDays);
        System.out.printf("Total Blocks:      %,d%n", totalBlocks);
        System.out.printf("Total Signatures:  %,d%n", totalSignatures);

        if (totalBlocks > 0) {
            double avgSigsPerBlock = (double) totalSignatures / (double) totalBlocks;
            System.out.printf("Avg Sigs/Block:    %.2f%n", avgSigsPerBlock);
        }

        System.out.println("\nDetailed per-day statistics have been written to:");
        System.out.println("  " + csvOutputFile);
        System.out.println("\n" + "=".repeat(80));
    }

    /**
     * Prints a summary of a completed day's statistics.
     *
     * @param day the date to print statistics for
     */
    public void printDayCompletedSummary(LocalDate day) {
        final DayStatistics dayStats;
        if (currentDayStats != null && currentDayStats.date.equals(day)) {
            dayStats = currentDayStats;
        } else {
            dayStats = dailyStatistics.stream()
                    .filter(ds -> ds.date.equals(day))
                    .findFirst()
                    .orElse(null);
        }

        if (dayStats == null) {
            System.out.println("\n[WARNING] No statistics found for day: " + day);
            return;
        }

        PrettyPrint.clearProgress();
        System.out.println("\n" + "\u2550".repeat(80));
        System.out.println("DAY COMPLETED: " + day);
        System.out.println("\u2550".repeat(80));
        System.out.printf("  Date:                     %s%n", dayStats.date);
        System.out.printf("  Total Blocks:             %,d%n", dayStats.totalBlocks);
        System.out.printf("  Total Signatures:         %,d%n", dayStats.totalSignatures);
        System.out.printf("  Valid Signatures:         %,d%n", dayStats.totalValidSignatures);
        System.out.printf("  Address Book Nodes (avg): %d%n", dayStats.getAverageAddressBookNodeCount());
        System.out.printf("  Worst Block Coverage:     %.2f%%%n", dayStats.getMinPercentage());
        System.out.printf("  Avg Percentage Per Block: %.2f%%%n", dayStats.getAveragePercentagePerBlock());
        System.out.printf("  Average Percentage:       %.2f%%%n", dayStats.getAveragePercentage());
        System.out.printf("  Valid Percentage:         %.2f%%%n", dayStats.getValidPercentage());

        if (dayStats.hasStakeData()) {
            System.out.printf("  Mode:                     %s%n", dayStats.mode);
            System.out.printf("  Total Stake:              %,d%n", dayStats.totalStake);
            System.out.printf(
                    "  Validated Stake %%:        %.2f%% - %.2f%% (mean %.2f%%)%n",
                    dayStats.minValidatedStakePct, dayStats.maxValidatedStakePct, dayStats.getMeanValidatedStakePct());
            System.out.printf("  Closest Threshold Margin: %,d%n", dayStats.closestThresholdMargin);
            System.out.printf(
                    "  Signing Nodes:            %d - %d (mean %.1f)%n",
                    dayStats.minSigningNodes, dayStats.maxSigningNodes, dayStats.getMeanSigningNodes());
            System.out.printf("  Missing Signers:          %d%n", dayStats.getMissingSigners());
            System.out.printf("  Reliable Signers:         %d%n", dayStats.getReliableSigners());
        }

        if (!dayStats.blocksBySignatureCount.isEmpty()) {
            System.out.println("\n  Signature Distribution:");
            System.out.println("  " + "\u2500".repeat(60));
            System.out.printf("  %-20s %10s %10s%n", "Signatures/Block", "Block Count", "Percentage");
            System.out.println("  " + "\u2500".repeat(60));

            dayStats.blocksBySignatureCount.entrySet().stream()
                    .sorted((a, b) -> b.getValue().compareTo(a.getValue()))
                    .limit(10)
                    .forEach(entry -> {
                        int sigCount = entry.getKey();
                        int blockCount = entry.getValue();
                        double percentage = (100.0 * blockCount) / dayStats.totalBlocks;
                        System.out.printf("  %-20d %,10d %9.2f%%%n", sigCount, blockCount, percentage);
                    });

            System.out.println("  " + "\u2500".repeat(60));
        }
        System.out.println("\u2550".repeat(80) + "\n");
    }

    @Override
    public void close() {
        // No-op; CSV is flushed per day in writeDayToCsv
    }

    // ── CSV output ───────────────────────────────────────────────────────────

    private void writeCsvHeader() {
        if (csvHeaderWritten || csvOutputFile == null) return;
        try {
            if (Files.exists(csvOutputFile)) {
                long fileSize = Files.size(csvOutputFile);
                if (fileSize > 0) {
                    csvHeaderWritten = true;
                    System.out.println(
                            "[CSV] Resuming - appending to existing file (" + fileSize + " bytes): " + csvOutputFile);
                    return;
                }
                System.out.println("[CSV] Removing empty CSV file: " + csvOutputFile);
                Files.delete(csvOutputFile);
            }
            StringBuilder header = new StringBuilder();
            header.append(
                    "date,percentage,number_of_blocks,number_of_nodes,valid_signatures,worst_block_signature_coverage_percentage,avg_percentage_per_block");
            for (int i = 1; i <= MAX_SIG_COUNT; i++) {
                header.append(",blocks_with_").append(i).append("_sig");
            }
            // Stake weight columns
            header.append(",mode,total_stake");
            header.append(",min_validated_stake_pct,max_validated_stake_pct,mean_validated_stake_pct");
            header.append(",closest_threshold_margin");
            header.append(",min_signing_nodes,max_signing_nodes,mean_signing_nodes");
            header.append(",min_node_stake,max_node_stake,mean_node_stake,median_node_stake,std_dev_node_stake");
            header.append(",top3_concentration_pct,missing_signers,reliable_signers");
            header.append("\n");

            Files.writeString(csvOutputFile, header.toString(), StandardCharsets.UTF_8, CREATE_NEW, WRITE);
            csvHeaderWritten = true;
            System.out.println("[CSV] Created new CSV file: " + csvOutputFile);
        } catch (IOException e) {
            System.err.println("Failed to write CSV header to " + csvOutputFile + ": " + e.getMessage());
        }
    }

    private void writeDayToCsv(DayStatistics dayStats) {
        if (csvOutputFile == null) return;

        try {
            if (!csvHeaderWritten) {
                writeCsvHeader();
            }

            StringBuilder row = new StringBuilder();
            row.append(dayStats.date).append(",");
            row.append(String.format("%.4f", dayStats.getAveragePercentage())).append(",");
            row.append(dayStats.totalBlocks).append(",");
            row.append(dayStats.getAverageAddressBookNodeCount()).append(",");
            row.append(dayStats.totalValidSignatures).append(",");
            row.append(String.format("%.4f", dayStats.getMinPercentage())).append(",");
            row.append(String.format("%.4f", dayStats.getAveragePercentagePerBlock()));

            // Histogram columns
            for (int i = 1; i <= MAX_SIG_COUNT; i++) {
                int blockCount = dayStats.blocksBySignatureCount.getOrDefault(i, 0);
                row.append(",").append(blockCount);
            }

            // Stake weight columns
            if (dayStats.hasStakeData()) {
                row.append(",").append(dayStats.mode);
                row.append(",").append(dayStats.totalStake);
                row.append(",").append(String.format("%.4f", dayStats.minValidatedStakePct));
                row.append(",").append(String.format("%.4f", dayStats.maxValidatedStakePct));
                row.append(",").append(String.format("%.4f", dayStats.getMeanValidatedStakePct()));
                row.append(",").append(dayStats.closestThresholdMargin);
                row.append(",").append(dayStats.minSigningNodes);
                row.append(",").append(dayStats.maxSigningNodes);
                row.append(",").append(String.format("%.1f", dayStats.getMeanSigningNodes()));
                appendNodeStakeDistribution(row, dayStats);
                row.append(",").append(dayStats.getMissingSigners());
                row.append(",").append(dayStats.getReliableSigners());
            } else {
                // Empty stake columns
                for (int i = 0; i < STAKE_COLUMN_COUNT; i++) {
                    row.append(",");
                }
            }

            row.append("\n");

            Files.writeString(csvOutputFile, row.toString(), StandardCharsets.UTF_8, APPEND, CREATE);

            System.out.println("[CSV] Written statistics for " + dayStats.date + " to " + csvOutputFile);
        } catch (IOException e) {
            System.err.println("Failed to write day stats to CSV for " + dayStats.date + ": " + e.getMessage());
        }
    }

    /**
     * Appends node stake distribution columns (min, max, mean, median, stddev)
     * and top-3 concentration percentage to the CSV row.
     */
    private static void appendNodeStakeDistribution(StringBuilder row, DayStatistics dayStats) {
        if (dayStats.latestNodeStakes == null || dayStats.latestNodeStakes.isEmpty()) {
            // 6 empty columns: min/max/mean/median/stddev + top3
            row.append(",,,,,,");
            return;
        }

        List<Long> stakes = new ArrayList<>(dayStats.latestNodeStakes.values());
        Collections.sort(stakes);

        long minStake = stakes.getFirst();
        long maxStake = stakes.getLast();
        double meanStake = (double) dayStats.totalStake / stakes.size();

        // Median
        int mid = stakes.size() / 2;
        double medianStake = (stakes.size() % 2 == 0) ? (stakes.get(mid - 1) + stakes.get(mid)) / 2.0 : stakes.get(mid);

        // Standard deviation
        double sumSquareDiff = 0;
        for (long s : stakes) {
            double diff = s - meanStake;
            sumSquareDiff += diff * diff;
        }
        double stdDevStake = Math.sqrt(sumSquareDiff / stakes.size());

        row.append(",").append(minStake);
        row.append(",").append(maxStake);
        row.append(",").append(String.format("%.0f", meanStake));
        row.append(",").append(String.format("%.0f", medianStake));
        row.append(",").append(String.format("%.0f", stdDevStake));

        // Top 3 concentration (take last 3 from already-sorted ascending list)
        long top3Sum = 0;
        for (int i = Math.max(0, stakes.size() - 3); i < stakes.size(); i++) {
            top3Sum += stakes.get(i);
        }
        double top3Pct = dayStats.totalStake > 0 ? (100.0 * top3Sum) / dayStats.totalStake : 0;
        row.append(",").append(String.format("%.4f", top3Pct));
    }
}
