// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.subcommands;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.time.ZoneOffset.UTC;
import static org.hiero.block.tools.days.model.TarZstdDayUtils.parseDayFromFileName;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.hiero.block.tools.days.model.AddressBookRegistry;
import org.hiero.block.tools.days.model.TarZstdDayReaderUsingExec;
import org.hiero.block.tools.days.model.TarZstdDayUtils;
import org.hiero.block.tools.mirrornode.DayBlockInfo;
import org.hiero.block.tools.records.model.unparsed.InMemoryFile;
import org.hiero.block.tools.records.model.unparsed.UnparsedRecordBlock;
import org.hiero.block.tools.records.model.unparsed.UnparsedRecordBlock.ValidationResult;
import org.hiero.block.tools.utils.PrettyPrint;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

/***
 *
 * Each block has 30 signatures (n signatures)
 * How many signatures are valid?
 *
 * Histogram/Gauge
 *
 * How many blocks per day.
 *    -> How many nodes per day in the address book
 *      --> If n nodes how many blocks had n signatures?
 *      --> How many blocks had n -1 signatures etc
 *      --> One line per day.
 *
 *      --> Compute the average percentage of signatures.
 *      rolling average -> Where the average is number signatures per block over the number of nodes in the address book.
 *
 *       Then divide by the number of blocks at the time.
 *
 *
 *    Persist to a csv and print on screen per day.
 *
 *    date, percentage, number_of_blocks, number_of_nodes, blocks with 1 sig, blocks with 2, .... 32.
 *
 */

/**
 * Validate blockchain in record file blocks in day files, computing and checking running hash,
 * while collecting statistics about signature counts per node.
 */
@SuppressWarnings("CallToPrintStackTrace")
@Command(name = "validate-with-stats", description = "Validate blockchain with signature statistics per node")
public class ValidateWithStats implements Runnable {
    /** Zero hash for initial carry over */
    private static final byte[] ZERO_HASH = new byte[48];

    /** Gson instance for Status JSON serialization */
    private static final Gson GSON = new GsonBuilder().create();

    /**
     * Per-day statistics for signature validation
     */
    private static class DayStatistics {
        final LocalDate date;
        int totalBlocks = 0;
        int totalSignatures = 0;
        int totalValidSignatures = 0;
        long totalNodeSlots = 0; // Sum of address book node counts across all blocks
        int minSignaturesInBlock = Integer.MAX_VALUE;
        // Histogram: blocks grouped by total signature count (index = sig count, value = block count)
        final Map<Integer, Integer> blocksBySignatureCount = new TreeMap<>();

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
    }

    /**
     * Statistics tracker for signature counts per node
     */
    private static class SignatureStats {
        // Node ID -> list of signature counts per block
        private final Map<String, List<Integer>> nodeSignatureCounts = new TreeMap<>();
        // Node ID -> list of signature counts per day
        private final Map<String, List<DayStats>> nodeDayStats = new TreeMap<>();
        // Per-day statistics
        private final List<DayStatistics> dailyStatistics = new ArrayList<>();
        // Current day being tracked
        private LocalDate currentDay = null;
        // Current day signature counts per node
        private final Map<String, Integer> currentDaySignatures = new TreeMap<>();
        // Current day statistics
        private DayStatistics currentDayStats = null;
        // Rolling window for rolling average (last N blocks)
        private final int rollingWindowSize = 1000;
        private final Map<String, LinkedList<Integer>> rollingWindows = new TreeMap<>();
        // CSV output file
        private final Path csvOutputFile;
        private boolean csvHeaderWritten = false;

        private static class DayStats {
            final LocalDate date;
            final int count;

            DayStats(LocalDate date, int count) {
                this.date = date;
                this.count = count;
            }
        }

        SignatureStats(Path csvOutputFile) {
            this.csvOutputFile = csvOutputFile;
        }

        void startDay(LocalDate day, int addressBookNodeCount) {
            // Save previous day stats if any
            if (currentDay != null && currentDayStats != null) {
                for (Map.Entry<String, Integer> entry : currentDaySignatures.entrySet()) {
                    nodeDayStats
                            .computeIfAbsent(entry.getKey(), k -> new ArrayList<>())
                            .add(new DayStats(currentDay, entry.getValue()));
                }
                dailyStatistics.add(currentDayStats);
                writeDayToCsv(currentDayStats);
                currentDaySignatures.clear();
            }
            currentDay = day;
            currentDayStats = new DayStatistics(day);
        }

        void recordBlock(List<InMemoryFile> signatureFiles, int validSignatureCount, int addressBookNodeCount) {
            // Count signatures per node
            Map<String, Integer> blockSignatures = new TreeMap<>();
            int totalSignaturesInBlock = 0;

            for (InMemoryFile sigFile : signatureFiles) {
                String fileName = sigFile.path().getFileName().toString();
                // Extract node ID from filename like "node_0.0.3.rcd_sig" or "node_0.0.3.rcs_sig"
                if (fileName.startsWith("node_") && (fileName.endsWith(".rcd_sig") || fileName.endsWith(".rcs_sig"))) {
                    String nodeId = fileName.substring(5, fileName.lastIndexOf('.'));
                    blockSignatures.merge(nodeId, 1, Integer::sum);
                    totalSignaturesInBlock++;
                }
            }

            // Record total signatures for this block in daily statistics
            if (currentDayStats != null) {
                currentDayStats.recordBlock(totalSignaturesInBlock, validSignatureCount, addressBookNodeCount);
            }

            // Record per-block counts
            for (Map.Entry<String, Integer> entry : blockSignatures.entrySet()) {
                String nodeId = entry.getKey();
                int count = entry.getValue();

                nodeSignatureCounts
                        .computeIfAbsent(nodeId, k -> new ArrayList<>())
                        .add(count);
                currentDaySignatures.merge(nodeId, count, Integer::sum);

                // Update rolling window
                LinkedList<Integer> window = rollingWindows.computeIfAbsent(nodeId, k -> new LinkedList<>());
                window.addLast(count);
                if (window.size() > rollingWindowSize) {
                    window.removeFirst();
                }
            }
        }

        void finalizeDayStats() {
            if (currentDay != null && currentDayStats != null) {
                for (Map.Entry<String, Integer> entry : currentDaySignatures.entrySet()) {
                    nodeDayStats
                            .computeIfAbsent(entry.getKey(), k -> new ArrayList<>())
                            .add(new DayStats(currentDay, entry.getValue()));
                }
                dailyStatistics.add(currentDayStats);
                writeDayToCsv(currentDayStats);
            }
        }

        private void writeCsvHeader() {
            if (csvHeaderWritten || csvOutputFile == null) return;
            try {
                // If file exists, always append (resume case) - never overwrite existing data
                if (Files.exists(csvOutputFile)) {
                    long fileSize = Files.size(csvOutputFile);
                    if (fileSize > 0) {
                        csvHeaderWritten = true;
                        System.out.println("[CSV] Resuming - appending to existing file (" + fileSize + " bytes): "
                                + csvOutputFile);
                        return;
                    }
                    // File exists but is empty - delete it so we can create fresh
                    System.out.println("[CSV] Removing empty CSV file: " + csvOutputFile);
                    Files.delete(csvOutputFile);
                }
                // Create new file with header
                int maxSigCount = 40;
                StringBuilder header = new StringBuilder();
                header.append(
                        "date,percentage,number_of_blocks,number_of_nodes,valid_signatures,worst_block_signature_coverage_percentage,avg_percentage_per_block");
                for (int i = 1; i <= maxSigCount; i++) {
                    header.append(",blocks_with_").append(i).append("_sig");
                }
                header.append("\n");

                // Use CREATE_NEW to fail if file somehow exists (safety check)
                Files.writeString(
                        csvOutputFile,
                        header.toString(),
                        StandardCharsets.UTF_8,
                        java.nio.file.StandardOpenOption.CREATE_NEW,
                        java.nio.file.StandardOpenOption.WRITE);
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
                row.append(String.format("%.4f", dayStats.getAveragePercentage()))
                        .append(",");
                row.append(dayStats.totalBlocks).append(",");
                row.append(dayStats.getAverageAddressBookNodeCount()).append(",");
                row.append(dayStats.totalValidSignatures).append(",");
                row.append(String.format("%.4f", dayStats.getMinPercentage())).append(",");
                row.append(String.format("%.4f", dayStats.getAveragePercentagePerBlock()));

                // Write histogram data (blocks with 1 sig, blocks with 2 sigs, etc.)
                int maxSigCount = 40;
                for (int i = 1; i <= maxSigCount; i++) {
                    int blockCount = dayStats.blocksBySignatureCount.getOrDefault(i, 0);
                    row.append(",").append(blockCount);
                }
                row.append("\n");

                Files.writeString(
                        csvOutputFile,
                        row.toString(),
                        StandardCharsets.UTF_8,
                        java.nio.file.StandardOpenOption.APPEND,
                        java.nio.file.StandardOpenOption.CREATE);

                System.out.println("[CSV] Written statistics for " + dayStats.date + " to " + csvOutputFile);
            } catch (IOException e) {
                System.err.println("Failed to write day stats to CSV for " + dayStats.date + ": " + e.getMessage());
            }
        }

        void printStatistics() {
            System.out.println("\n" + "=".repeat(80));
            System.out.println("VALIDATION COMPLETE - ALL STATISTICS WRITTEN TO CSV");
            System.out.println("=".repeat(80));

            if (dailyStatistics.isEmpty()) {
                System.out.println("No daily statistics collected.");
                return;
            }

            // Print overall summary
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

        void printDayCompletedSummary(LocalDate day) {
            // Use currentDayStats if it matches, otherwise search in dailyStatistics
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
            System.out.println("\n" + "═".repeat(80));
            System.out.println("DAY COMPLETED: " + day);
            System.out.println("═".repeat(80));
            System.out.printf("  Date:                     %s%n", dayStats.date);
            System.out.printf("  Total Blocks:             %,d%n", dayStats.totalBlocks);
            System.out.printf("  Total Signatures:         %,d%n", dayStats.totalSignatures);
            System.out.printf("  Valid Signatures:         %,d%n", dayStats.totalValidSignatures);
            System.out.printf("  Address Book Nodes (avg): %d%n", dayStats.getAverageAddressBookNodeCount());
            System.out.printf("  Worst Block Coverage:     %.2f%%%n", dayStats.getMinPercentage());
            System.out.printf("  Avg Percentage Per Block: %.2f%%%n", dayStats.getAveragePercentagePerBlock());
            System.out.printf("  Average Percentage:       %.2f%%%n", dayStats.getAveragePercentage());
            System.out.printf("  Valid Percentage:         %.2f%%%n", dayStats.getValidPercentage());

            // Print histogram summary (show top signature counts)
            if (!dayStats.blocksBySignatureCount.isEmpty()) {
                System.out.println("\n  Signature Distribution:");
                System.out.println("  " + "─".repeat(60));
                System.out.printf("  %-20s %10s %10s%n", "Signatures/Block", "Block Count", "Percentage");
                System.out.println("  " + "─".repeat(60));

                // Show top 10 most common signature counts
                dayStats.blocksBySignatureCount.entrySet().stream()
                        .sorted((a, b) -> b.getValue().compareTo(a.getValue()))
                        .limit(10)
                        .forEach(entry -> {
                            int sigCount = entry.getKey();
                            int blockCount = entry.getValue();
                            double percentage = (100.0 * blockCount) / dayStats.totalBlocks;
                            System.out.printf("  %-20d %,10d %9.2f%%%n", sigCount, blockCount, percentage);
                        });

                System.out.println("  " + "─".repeat(60));
            }
            System.out.println("═".repeat(80) + "\n");
        }
    }

    /**
     * Simple wrapper for producer-consumer communication (day boundaries + blocks)
     */
    private record Item(Kind kind, LocalDate dayDate, Path dayFile, UnparsedRecordBlock block) {
        enum Kind {
            DAY_START,
            BLOCK,
            DAY_END,
            STREAM_END
        }

        static Item dayStart(LocalDate date, Path file) {
            return new Item(Kind.DAY_START, date, file, null);
        }

        static Item block(LocalDate date, Path file, UnparsedRecordBlock b) {
            return new Item(Kind.BLOCK, date, file, b);
        }

        static Item dayEnd(LocalDate date, Path file) {
            return new Item(Kind.DAY_END, date, file, null);
        }

        static Item streamEnd() {
            return new Item(Kind.STREAM_END, null, null, null);
        }
    }

    /**
     * Simple status object saved to compressedDaysDir/validateCmdStatus.json to allow resuming.
     */
    @SuppressWarnings("ClassCanBeRecord")
    private static final class Status {
        final String dayDate;
        final String recordFileTime;
        final String endRunningHashHex;

        Status(LocalDate dayDate, String recordFileTime, String endRunningHashHex) {
            this.dayDate = dayDate.toString();
            this.recordFileTime = recordFileTime;
            this.endRunningHashHex = endRunningHashHex;
        }

        LocalDate dayLocalDate() {
            return LocalDate.parse(dayDate);
        }

        Instant recordInstant() {
            return Instant.parse(recordFileTime);
        }

        byte[] hashBytes() {
            return HexFormat.of().parseHex(endRunningHashHex);
        }

        private static void writeStatusFile(Path statusFile, Status s) {
            if (statusFile == null || s == null) return;
            try {
                String json = GSON.toJson(s);
                Files.writeString(statusFile, json, StandardCharsets.UTF_8, CREATE, TRUNCATE_EXISTING);
                System.out.println("[DEBUG] Wrote status file: " + statusFile);
            } catch (IOException e) {
                System.err.println("Failed to write status file " + statusFile + ": " + e.getMessage());
                e.printStackTrace();
            }
        }

        private static Status readStatusFile(Path statusFile) {
            try {
                if (statusFile == null || !Files.exists(statusFile)) return null;
                String content = Files.readString(statusFile, StandardCharsets.UTF_8);
                return GSON.fromJson(content, Status.class);
            } catch (Exception e) {
                System.err.println("Failed to read/parse status file " + statusFile + ": " + e.getMessage());
                return null;
            }
        }
    }

    @Spec
    CommandSpec spec;

    @Option(
            names = {"-w", "--warnings-file"},
            description = "Write warnings to this file, rather than ignoring them")
    private File warningFile = null;

    @Option(
            names = {"-d", "--start-date"},
            description = "Start validation from this date (format: YYYY-MM-DD), ignoring any resume status file")
    private String startDate = null;

    @Option(
            names = {"--use-jni"},
            description = "Use zstd-jni library instead of subprocess for decompression")
    private boolean useJni = false;

    @Parameters(index = "0", description = "Directories of days to process")
    @SuppressWarnings("unused")
    private File compressedDaysDir;

    @Override
    public void run() {
        if (compressedDaysDir == null) {
            spec.commandLine().usage(spec.commandLine().getOut());
            return;
        }

        System.out.println("CompressedDaysDir path: " + compressedDaysDir);

        // Initialize CSV output file
        final Path csvOutputFile = compressedDaysDir.toPath().resolve("signature_statistics.csv");
        System.out.println("CSV output will be written to: " + csvOutputFile);

        // Initialize statistics tracker
        final SignatureStats stats = new SignatureStats(csvOutputFile);

        // create AddressBookRegistry
        final Path addressBookFile = compressedDaysDir.toPath().resolve("addressBookHistory.json");
        final AddressBookRegistry addressBookRegistry =
                Files.exists(addressBookFile) ? new AddressBookRegistry(addressBookFile) : new AddressBookRegistry();

        // load mirror node day info if available
        Map<LocalDate, DayBlockInfo> tempDayInfo;
        try {
            tempDayInfo = DayBlockInfo.loadDayBlockInfoMap();
        } catch (Exception e) {
            System.out.println("Failed to load day block info map so ignoring");
            tempDayInfo = null;
        }
        final Map<LocalDate, DayBlockInfo> dayInfo = tempDayInfo;

        // load resume status if available
        final Path statusFile = compressedDaysDir.toPath().resolve("validateCmdStatus.json");
        final Status resumeStatus = Status.readStatusFile(statusFile);
        final AtomicReference<Status> lastGood = new AtomicReference<>(resumeStatus);

        // load all the day paths
        final List<Path> dayPaths = TarZstdDayUtils.sortedDayPaths(new File[] {compressedDaysDir});
        if (dayPaths.isEmpty()) {
            System.out.println("No day files found in " + compressedDaysDir);
            return;
        }

        final AtomicReference<byte[]> carryOverHash = new AtomicReference<>();

        // Determine starting date
        LocalDate actualStartDate = null;
        if (startDate != null) {
            try {
                actualStartDate = LocalDate.parse(startDate);
                System.out.println("Starting from date: " + actualStartDate);
            } catch (Exception e) {
                System.err.println("Invalid start date format: " + startDate + ". Expected YYYY-MM-DD");
                System.exit(1);
            }
        } else if (resumeStatus != null) {
            actualStartDate = resumeStatus.dayLocalDate();
        }

        // Set up carry over hash
        if (resumeStatus != null && startDate == null) {
            byte[] hb = resumeStatus.hashBytes();
            if (hb.length > 0) carryOverHash.set(hb);
            System.out.printf(
                    "Resuming at %s with hash[%s]%n",
                    resumeStatus.recordFileTime, resumeStatus.endRunningHashHex.substring(0, 8));
        } else if (actualStartDate != null) {
            if (actualStartDate.equals(LocalDate.parse("2019-09-13"))) {
                carryOverHash.set(ZERO_HASH);
                System.out.println("Starting at genesis with hash[" + Bytes.wrap(carryOverHash.get()) + "]");
            } else if (dayInfo != null) {
                LocalDate priorDayDate = actualStartDate.minusDays(1);
                DayBlockInfo priorDayInfo = dayInfo.get(priorDayDate);
                if (priorDayInfo != null) {
                    byte[] priorHash = HexFormat.of().parseHex(priorDayInfo.lastBlockHash);
                    carryOverHash.set(priorHash);
                    System.out.printf(
                            "Starting at %s with mirror node last hash[%s]%n",
                            actualStartDate, Bytes.wrap(carryOverHash.get()));
                } else {
                    carryOverHash.set(null);
                    System.out.println(
                            "No prior day info for " + priorDayDate + ", cannot validate first block's previous hash");
                }
            }
        } else {
            if (dayPaths.getFirst().getFileName().toString().startsWith("2019-09-13")) {
                carryOverHash.set(ZERO_HASH);
                System.out.println("Starting at genesis with hash[" + Bytes.wrap(carryOverHash.get()) + "]");
            } else if (dayInfo != null) {
                LocalDate firstDayDate =
                        parseDayFromFileName(dayPaths.getFirst().getFileName().toString());
                LocalDate priorDayDate = firstDayDate.minusDays(1);
                DayBlockInfo priorDayInfo = dayInfo.get(priorDayDate);
                if (priorDayInfo != null) {
                    byte[] priorHash = HexFormat.of().parseHex(priorDayInfo.lastBlockHash);
                    carryOverHash.set(priorHash);
                    System.out.printf(
                            "Starting at %s with mirror node last hash[%s]%n",
                            firstDayDate, Bytes.wrap(carryOverHash.get()));
                } else {
                    carryOverHash.set(null);
                    System.out.println(
                            "No prior day info for " + priorDayDate + ", cannot validate first block's previous hash");
                }
            }
        }

        // Estimation/ETA support
        final long startNanos = System.nanoTime();
        final long INITIAL_ESTIMATE_PER_DAY = (24L * 60L * 60L) / 5L;
        final long dayCount = dayPaths.size();
        final AtomicLong totalProgress = new AtomicLong(dayCount * INITIAL_ESTIMATE_PER_DAY);
        final AtomicLong progress = new AtomicLong(0);
        final AtomicReference<Instant> lastSpeedCalcBlockTime = new AtomicReference<>();
        final AtomicLong lastSpeedCalcRealTimeNanos = new AtomicLong(0);

        final BlockingQueue<Item> queue = new LinkedBlockingQueue<>(1_000);

        // Shutdown hook
        Runtime.getRuntime()
                .addShutdownHook(new Thread(
                        () -> {
                            Status s = lastGood.get();
                            if (s != null) {
                                System.err.println("Shutdown: writing status to " + statusFile);
                                Status.writeStatusFile(statusFile, s);
                                System.err.println("Shutdown: address book to " + addressBookFile);
                                addressBookRegistry.saveAddressBookRegistryToJsonFile(addressBookFile);
                            }
                            // Print statistics on shutdown
                            stats.finalizeDayStats();
                            stats.printStatistics();
                        },
                        "validate-shutdown-hook"));

        // Determine start day index
        final int startDay;
        if (actualStartDate != null) {
            int foundIndex = -1;
            for (int i = 0; i < dayPaths.size(); i++) {
                LocalDate dayDate =
                        parseDayFromFileName(dayPaths.get(i).getFileName().toString());
                if (dayDate.equals(actualStartDate) || dayDate.isAfter(actualStartDate)) {
                    foundIndex = i;
                    break;
                }
            }
            if (foundIndex < 0) {
                System.err.println("Warning: Could not find day file for " + actualStartDate
                        + " or later, starting from beginning");
                startDay = 0;
            } else {
                startDay = foundIndex;
                LocalDate actualDay = parseDayFromFileName(
                        dayPaths.get(foundIndex).getFileName().toString());
                if (!actualDay.equals(actualStartDate)) {
                    System.out.println(
                            "Note: Exact date " + actualStartDate + " not found, starting from " + actualDay);
                }
            }
        } else {
            startDay = 0;
        }

        // Reader thread
        final Thread reader = new Thread(
                () -> {
                    try {
                        for (int day = startDay; day < dayPaths.size(); day++) {
                            final Path dayPath = dayPaths.get(day);
                            final LocalDate dayDate =
                                    parseDayFromFileName(dayPath.getFileName().toString());
                            queue.put(Item.dayStart(dayDate, dayPath));
                            try (var stream = TarZstdDayReaderUsingExec.streamTarZstd(dayPath, useJni)) {
                                stream.forEach(set -> {
                                    try {
                                        if (resumeStatus != null
                                                && startDate == null
                                                && dayDate.equals(resumeStatus.dayLocalDate())) {
                                            Instant ri = resumeStatus.recordInstant();
                                            if (!set.recordFileTime().isAfter(ri)) {
                                                return;
                                            }
                                        }
                                        queue.put(Item.block(dayDate, dayPath, set));
                                    } catch (InterruptedException ie) {
                                        Thread.currentThread().interrupt();
                                        throw new RuntimeException(
                                                "Interrupted while enqueueing block from " + dayPath, ie);
                                    }
                                });
                            } catch (Exception ex) {
                                PrettyPrint.clearProgress();
                                System.err.println("Failed processing day file: " + dayPath + ": " + ex.getMessage());
                                ex.printStackTrace();
                                Status s = lastGood.get();
                                if (s != null) Status.writeStatusFile(statusFile, s);
                                addressBookRegistry.saveAddressBookRegistryToJsonFile(addressBookFile);
                                System.exit(1);
                            }
                            queue.put(Item.dayEnd(dayDate, dayPath));
                        }
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        PrettyPrint.clearProgress();
                        System.err.println("Reader thread interrupted");
                        Status s = lastGood.get();
                        if (s != null) Status.writeStatusFile(statusFile, s);
                        addressBookRegistry.saveAddressBookRegistryToJsonFile(addressBookFile);
                        System.exit(1);
                    } finally {
                        try {
                            queue.put(Item.streamEnd());
                        } catch (InterruptedException ignored) {
                            Thread.currentThread().interrupt();
                        }
                    }
                },
                "validate-reader");
        reader.start();

        long progressAtStartOfDay = 0L;
        int currentDayCount = 0;
        long lastReportedMinute = Long.MIN_VALUE;

        try (FileWriter warningWriter = warningFile != null ? new FileWriter(warningFile, true) : null) {
            final AtomicLong blockInDayCounter = new AtomicLong(0L);

            while (true) {
                final Item item = queue.take();
                if (item.kind == Item.Kind.STREAM_END) break;

                switch (item.kind) {
                    case DAY_START -> {
                        progressAtStartOfDay = progress.get();
                        blockInDayCounter.set(0L);
                        int nodeCount = addressBookRegistry
                                .getCurrentAddressBook()
                                .nodeAddress()
                                .size();
                        stats.startDay(item.dayDate, nodeCount);
                    }
                    case BLOCK -> {
                        final UnparsedRecordBlock block = item.block;
                        progress.incrementAndGet();

                        // Get the address book for this specific block's timestamp
                        final NodeAddressBook blockAddressBook =
                                addressBookRegistry.getAddressBookForBlock(block.recordFileTime());
                        int nodeCount = blockAddressBook.nodeAddress().size();

                        try {
                            final byte[] previousBlockHash = carryOverHash.get();
                            final ValidationResult vr = block.validate(previousBlockHash, blockAddressBook);

                            // Record block statistics after validation to get valid signature count
                            stats.recordBlock(block.signatureFiles(), vr.validSignatureCount(), nodeCount);

                            if (warningWriter != null && !vr.warningMessages().isEmpty()) {
                                warningWriter.write(
                                        "Warnings for " + block.recordFileTime() + ":\n" + vr.warningMessages() + "\n");
                                warningWriter.flush();
                            }

                            if (!vr.isValid()) {
                                PrettyPrint.clearProgress();
                                System.err.println("Validation failed for " + block.recordFileTime() + ":\n"
                                        + vr.warningMessages());
                                System.out.flush();
                                Status s = lastGood.get();
                                if (s != null) Status.writeStatusFile(statusFile, s);
                                addressBookRegistry.saveAddressBookRegistryToJsonFile(addressBookFile);
                                System.exit(1);
                            }

                            if (blockInDayCounter.get() == 0L && dayInfo != null) {
                                LocalDate dayDate = parseDayFromFileName(
                                        item.dayFile.getFileName().toString());
                                DayBlockInfo thisDaysInfo = dayInfo.get(dayDate);
                                if (block.recordFileTime()
                                        .isBefore(dayDate.atStartOfDay(UTC)
                                                .plusSeconds(10)
                                                .toInstant())) {
                                    byte[] expectedHash = HexFormat.of().parseHex(thisDaysInfo.firstBlockHash);
                                    if (!Arrays.equals(vr.endRunningHash(), expectedHash)) {
                                        PrettyPrint.clearProgress();
                                        System.err.printf(
                                                "Validation failed for %s: first block of day has previous hash[%s] but "
                                                        + "expected[%s] from mirror node data%n",
                                                dayDate, Bytes.wrap(vr.endRunningHash()), Bytes.wrap(expectedHash));
                                        System.out.flush();
                                        Status s = lastGood.get();
                                        if (s != null) Status.writeStatusFile(statusFile, s);
                                        addressBookRegistry.saveAddressBookRegistryToJsonFile(addressBookFile);
                                        System.exit(1);
                                    }
                                }
                            }

                            String addressBookChanges = addressBookRegistry.updateAddressBook(
                                    block.recordFileTime(), vr.addressBookTransactions());
                            if (warningWriter != null && addressBookChanges != null) {
                                warningWriter.write(addressBookChanges + "\n");
                                warningWriter.flush();
                            }

                            carryOverHash.set(vr.endRunningHash());

                            String endHashHex = HexFormat.of().formatHex(vr.endRunningHash());
                            lastGood.set(new Status(
                                    item.dayDate, block.recordFileTime().toString(), endHashHex));

                            // Calculate processing speed
                            final long currentRealTimeNanos = System.nanoTime();
                            final long tenSecondsInNanos = 10_000_000_000L;
                            String speedString = "";

                            if (lastSpeedCalcBlockTime.get() == null) {
                                lastSpeedCalcBlockTime.set(block.recordFileTime());
                                lastSpeedCalcRealTimeNanos.set(currentRealTimeNanos);
                            }

                            long realTimeSinceLastCalc = currentRealTimeNanos - lastSpeedCalcRealTimeNanos.get();
                            if (realTimeSinceLastCalc >= tenSecondsInNanos) {
                                lastSpeedCalcBlockTime.set(block.recordFileTime());
                                lastSpeedCalcRealTimeNanos.set(currentRealTimeNanos);
                            }

                            if (realTimeSinceLastCalc >= 1_000_000_000L) {
                                long dataTimeElapsedMillis =
                                        block.recordFileTime().toEpochMilli()
                                                - lastSpeedCalcBlockTime.get().toEpochMilli();
                                long realTimeElapsedMillis = realTimeSinceLastCalc / 1_000_000L;
                                double speedMultiplier =
                                        (double) dataTimeElapsedMillis / (double) realTimeElapsedMillis;
                                speedString = String.format(" speed %.1fx", speedMultiplier);
                            }

                            final String progressString = String.format(
                                    "%s carry[%s] next[%s]%s",
                                    block.recordFileTime(),
                                    shortHash(previousBlockHash),
                                    shortHash(vr.endRunningHash()),
                                    speedString);

                            final long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000L;
                            final long processedSoFarAcrossAll = progress.get();
                            final long totalProgressFinal = totalProgress.get();
                            double percent = ((double) processedSoFarAcrossAll / (double) totalProgressFinal) * 100.0;
                            long remainingMillis = PrettyPrint.computeRemainingMilliseconds(
                                    processedSoFarAcrossAll, totalProgressFinal, elapsedMillis);

                            long blockMinute = block.recordFileTime().getEpochSecond() / 60L;
                            if (blockMinute != lastReportedMinute) {
                                PrettyPrint.printProgressWithEta(percent, progressString, remainingMillis);
                                lastReportedMinute = blockMinute;
                            }
                        } catch (Exception ex) {
                            PrettyPrint.clearProgress();
                            System.err.println(
                                    "Validation threw for " + block.recordFileTime() + ": " + ex.getMessage());
                            ex.printStackTrace();
                            Status s = lastGood.get();
                            if (s != null) Status.writeStatusFile(statusFile, s);
                            addressBookRegistry.saveAddressBookRegistryToJsonFile(addressBookFile);
                            System.exit(1);
                        }
                        blockInDayCounter.incrementAndGet();
                    }
                    case DAY_END -> {
                        currentDayCount++;
                        final long progressAtEndOfDay = progress.get();
                        final long blocksInDay = progressAtEndOfDay - progressAtStartOfDay;
                        final long remainingDays = dayCount - currentDayCount;
                        totalProgress.set(progressAtEndOfDay + (remainingDays * blocksInDay));

                        // Print day completed summary
                        stats.printDayCompletedSummary(item.dayDate);
                    }
                }
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            PrettyPrint.clearProgress();
            System.err.println("Validation interrupted");
            Status s = lastGood.get();
            if (s != null) Status.writeStatusFile(statusFile, s);
            addressBookRegistry.saveAddressBookRegistryToJsonFile(addressBookFile);
            System.exit(1);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        PrettyPrint.clearProgress();
        System.out.println(
                "Validation complete. Days processed: " + dayCount + " , Blocks processed: " + progress.get());

        // Finalize and print statistics
        stats.finalizeDayStats();
        stats.printStatistics();

        Status sFinal = lastGood.get();
        if (sFinal != null) Status.writeStatusFile(statusFile, sFinal);
        addressBookRegistry.saveAddressBookRegistryToJsonFile(addressBookFile);

        try {
            reader.join();
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    private static String shortHash(final byte[] hash) {
        if (hash == null) return "null";
        final String s = Bytes.wrap(hash).toString();
        return s.length() <= 8 ? s : s.substring(0, 8);
    }
}
