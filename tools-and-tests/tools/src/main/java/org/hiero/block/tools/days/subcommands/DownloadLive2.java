// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.subcommands;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static org.hiero.block.tools.days.downloadlive.ValidateDownloadLive.fullBlockValidate;
import static org.hiero.block.tools.mirrornode.DayBlockInfo.loadDayBlockInfoMap;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.hiero.block.tools.days.download.DownloadConstants;
import org.hiero.block.tools.days.download.DownloadDayImplV2;
import org.hiero.block.tools.days.download.DownloadDayLiveImpl;
import org.hiero.block.tools.days.listing.DayListingFileReader;
import org.hiero.block.tools.days.listing.ListingRecordFile;
import org.hiero.block.tools.days.model.AddressBookRegistry;
import org.hiero.block.tools.mirrornode.BlockInfo;
import org.hiero.block.tools.mirrornode.BlockTimeReader;
import org.hiero.block.tools.mirrornode.DayBlockInfo;
import org.hiero.block.tools.mirrornode.FetchBlockQuery;
import org.hiero.block.tools.mirrornode.MirrorNodeBlockQueryOrder;
import org.hiero.block.tools.records.model.unparsed.InMemoryFile;
import org.hiero.block.tools.utils.ConcurrentTarZstdWriter;
import org.hiero.block.tools.utils.gcp.ConcurrentDownloadManagerVirtualThreadsV3;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Live block download command that follows the chain in real-time.
 *
 * <p>Key features:
 * <ul>
 *   <li>Auto-detects "today" from mirror node if no state file exists</li>
 *   <li>Uses HTTP transport + platform threads for stability (no gRPC deadlocks)</li>
 *   <li>Downloads, validates, and appends blocks to .tar.zstd_partial</li>
 *   <li>Automatic day rollover: closes archive and starts new day at midnight</li>
 *   <li>Signature statistics tracking with CSV output</li>
 *   <li>Resumable via state file</li>
 * </ul>
 */
@Command(
        name = "download-live2",
        description = "Live block download with inline validation and automatic day rollover",
        mixinStandardHelpOptions = true)
public class DownloadLive2 implements Runnable {

    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
    private static final Duration LIVE_POLL_INTERVAL = Duration.ofSeconds(2);
    private static final int MAX_VALIDATION_RETRIES = 3;
    private static final int PROGRESS_LOG_INTERVAL = 100;

    private static final File CACHE_DIR = new File("metadata/gcp-cache");
    private static final int MIN_NODE_ACCOUNT_ID = 3;
    private static final int MAX_NODE_ACCOUNT_ID = 37;

    @Option(
            names = {"-l", "--listing-dir"},
            description = "Directory where listing files are stored (default: listingsByDay)")
    private File listingDir = new File("listingsByDay");

    @Option(
            names = {"-o", "--output-dir"},
            description = "Directory where compressed day archives are written (default: compressedDays)")
    private File outputDir = new File("compressedDays");

    @Option(
            names = {"--state-json"},
            description = "Path to state JSON file for resume (default: outputDir/validateCmdStatus.json)")
    private Path stateJsonPath; // Default set in run() after outputDir is known

    @Option(
            names = {"--address-book"},
            description = "Path to address book file for signature validation")
    private Path addressBookPath;

    @Option(
            names = {"--max-concurrency"},
            description = "Maximum concurrent downloads (default: 64)")
    private int maxConcurrency = 64;

    @Option(
            names = {"--tmp-dir"},
            description = "Temporary directory for downloads (default: tmp)")
    private File tmpDir = new File("tmp");

    @Option(
            names = {"--start-date"},
            description = "Start date in YYYY-MM-DD format (default: auto-detect from mirror node)")
    private String startDate;

    @Option(
            names = {"--stats-csv"},
            description = "Path to signature statistics CSV file (default: outputDir/signature_statistics_live.csv)")
    private Path statsCsvPath;

    /**
     * State persisted to JSON for resumability.
     * Compatible with validateCmdStatus.json format (dayDate, recordFileTime, endRunningHashHex)
     * plus blockNumber for tracking position in live download.
     */
    private static class State {
        String dayDate; // YYYY-MM-DD
        String recordFileTime; // ISO-8601
        String endRunningHashHex; // running hash as hex (matches ValidationStatus format)
        long blockNumber; // block number for resume tracking

        State() {}

        State(long blockNumber, byte[] hash, Instant recordFileTime, LocalDate dayDate) {
            this.blockNumber = blockNumber;
            this.dayDate = dayDate != null ? dayDate.toString() : null;
            this.recordFileTime = recordFileTime != null ? recordFileTime.toString() : null;
            this.endRunningHashHex = hash != null ? HexFormat.of().formatHex(hash) : null;
        }

        byte[] getHashBytes() {
            return endRunningHashHex != null ? HexFormat.of().parseHex(endRunningHashHex) : null;
        }

        LocalDate getDayDate() {
            return dayDate != null ? LocalDate.parse(dayDate) : null;
        }
    }

    /**
     * Per-day statistics for signature tracking
     */
    private static class DayStatistics {
        final LocalDate date;
        int totalBlocks = 0;
        int totalSignatures = 0;
        long totalNodeSlots = 0;
        int minSignaturesInBlock = Integer.MAX_VALUE;
        final Map<Integer, Integer> blocksBySignatureCount = new TreeMap<>();

        DayStatistics(LocalDate date) {
            this.date = date;
        }

        void recordBlock(int signatureCount, int addressBookNodeCount) {
            totalBlocks++;
            totalSignatures += signatureCount;
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

        double getMinPercentage() {
            int avgNodes = getAverageAddressBookNodeCount();
            if (totalBlocks == 0 || avgNodes == 0 || minSignaturesInBlock == Integer.MAX_VALUE) return 0.0;
            return (100.0 * minSignaturesInBlock) / avgNodes;
        }
    }

    /**
     * Statistics tracker for signature counts
     */
    private static class SignatureStats {
        private final Path csvOutputFile;
        private boolean csvHeaderWritten = false;
        private DayStatistics currentDayStats = null;

        SignatureStats(Path csvOutputFile) {
            this.csvOutputFile = csvOutputFile;
        }

        void startDay(LocalDate day, int addressBookNodeCount) {
            if (currentDayStats != null) {
                writeDayToCsv(currentDayStats);
            }
            currentDayStats = new DayStatistics(day);
            System.out.println("[STATS] Started tracking day: " + day + " (nodes=" + addressBookNodeCount + ")");
        }

        void recordBlock(List<InMemoryFile> files, int addressBookNodeCount) {
            if (currentDayStats == null) return;

            int signatureCount = 0;
            for (InMemoryFile file : files) {
                String fileName = file.path().getFileName().toString();
                if (fileName.endsWith(".rcd_sig") || fileName.endsWith(".rcs_sig")) {
                    signatureCount++;
                }
            }
            currentDayStats.recordBlock(signatureCount, addressBookNodeCount);
        }

        /**
         * Records a block's signature count directly (used by bulk download callbacks).
         */
        void recordBlock(int signatureCount, int addressBookNodeCount) {
            if (currentDayStats == null) return;
            currentDayStats.recordBlock(signatureCount, addressBookNodeCount);
        }

        void finalizeDayStats() {
            if (currentDayStats != null) {
                writeDayToCsv(currentDayStats);
                printDaySummary(currentDayStats);
            }
        }

        private void writeCsvHeader() {
            if (csvHeaderWritten || csvOutputFile == null) return;
            try {
                // If file already exists (e.g., from validate-with-stats), just append
                if (Files.exists(csvOutputFile)) {
                    csvHeaderWritten = true;
                    System.out.println("[STATS] CSV file already exists, will append: " + csvOutputFile);
                    return;
                }

                int maxSigCount = 40;
                StringBuilder header = new StringBuilder();
                header.append(
                        "date,avg_percentage,number_of_blocks,number_of_nodes,total_signatures,worst_block_coverage_pct");
                for (int i = 1; i <= maxSigCount; i++) {
                    header.append(",blocks_with_").append(i).append("_sig");
                }
                header.append("\n");

                Files.writeString(csvOutputFile, header.toString(), StandardCharsets.UTF_8, CREATE, TRUNCATE_EXISTING);
                csvHeaderWritten = true;
                System.out.println("[STATS] Created CSV file: " + csvOutputFile);
            } catch (IOException e) {
                System.err.println("[STATS] Failed to write CSV header: " + e.getMessage());
            }
        }

        private void writeDayToCsv(DayStatistics dayStats) {
            if (csvOutputFile == null || dayStats.totalBlocks == 0) return;

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
                row.append(dayStats.totalSignatures).append(",");
                row.append(String.format("%.4f", dayStats.getMinPercentage()));

                int maxSigCount = 40;
                for (int i = 1; i <= maxSigCount; i++) {
                    int blockCount = dayStats.blocksBySignatureCount.getOrDefault(i, 0);
                    row.append(",").append(blockCount);
                }
                row.append("\n");

                Files.writeString(csvOutputFile, row.toString(), StandardCharsets.UTF_8, APPEND, CREATE);
                System.out.println("[STATS] Written day " + dayStats.date + " to CSV");
            } catch (IOException e) {
                System.err.println("[STATS] Failed to write CSV for " + dayStats.date + ": " + e.getMessage());
            }
        }

        void printDaySummary(DayStatistics dayStats) {
            System.out.println("\n" + "=".repeat(70));
            System.out.println("DAY STATISTICS: " + dayStats.date);
            System.out.println("=".repeat(70));
            System.out.printf("  Total Blocks:          %,d%n", dayStats.totalBlocks);
            System.out.printf("  Total Signatures:      %,d%n", dayStats.totalSignatures);
            System.out.printf("  Avg Nodes (addr book): %d%n", dayStats.getAverageAddressBookNodeCount());
            System.out.printf("  Average Coverage:      %.2f%%%n", dayStats.getAveragePercentage());
            System.out.printf("  Worst Block Coverage:  %.2f%%%n", dayStats.getMinPercentage());

            if (!dayStats.blocksBySignatureCount.isEmpty()) {
                System.out.println("\n  Signature Distribution (top 5):");
                dayStats.blocksBySignatureCount.entrySet().stream()
                        .sorted((a, b) -> b.getValue().compareTo(a.getValue()))
                        .limit(5)
                        .forEach(entry -> {
                            double pct = (100.0 * entry.getValue()) / dayStats.totalBlocks;
                            System.out.printf(
                                    "    %d sigs: %,d blocks (%.1f%%)%n", entry.getKey(), entry.getValue(), pct);
                        });
            }
            System.out.println("=".repeat(70) + "\n");
        }
    }

    @Override
    public void run() {
        System.out.println("[download-live2] Starting live block download");
        System.out.println("Configuration:");
        System.out.println("  listingDir=" + listingDir);
        System.out.println("  outputDir=" + outputDir);
        System.out.println("  stateJsonPath=" + stateJsonPath);
        System.out.println("  addressBookPath=" + addressBookPath);
        System.out.println("  maxConcurrency=" + maxConcurrency);
        System.out.println("  startDate=" + (startDate != null ? startDate : "(auto-detect)"));

        try {
            // Create directories
            Files.createDirectories(outputDir.toPath());
            Files.createDirectories(tmpDir.toPath());

            // Set default state file path in output directory (same as validate/validate-with-stats)
            if (stateJsonPath == null) {
                stateJsonPath = outputDir.toPath().resolve("validateCmdStatus.json");
            }
            if (stateJsonPath.getParent() != null) {
                Files.createDirectories(stateJsonPath.getParent());
            }

            // Initialize address book
            final AddressBookRegistry addressBookRegistry =
                    addressBookPath != null ? new AddressBookRegistry(addressBookPath) : new AddressBookRegistry();

            // Use HTTP transport + platform threads for stability (avoids gRPC/Netty deadlocks)
            final Storage storage = StorageOptions.http()
                    .setProjectId(DownloadConstants.GCP_PROJECT_ID)
                    .build()
                    .getService();

            final ConcurrentDownloadManagerVirtualThreadsV3 downloadManager =
                    ConcurrentDownloadManagerVirtualThreadsV3.newBuilder(storage)
                            .setMaxConcurrency(maxConcurrency)
                            .build();

            final BlockTimeReader blockTimeReader = new BlockTimeReader();

            // Initialize stats CSV path (same file as validate-with-stats for consistency)
            if (statsCsvPath == null) {
                statsCsvPath = outputDir.toPath().resolve("signature_statistics.csv");
            }
            final SignatureStats stats = new SignatureStats(statsCsvPath);
            System.out.println("  statsCsvPath=" + statsCsvPath);

            // Determine starting point
            final State initialState = determineStartingPoint(blockTimeReader);
            System.out.println("[download-live2] Starting from block " + initialState.blockNumber
                    + " hash["
                    + (initialState.endRunningHashHex != null
                            ? initialState.endRunningHashHex.substring(
                                    0, Math.min(8, initialState.endRunningHashHex.length()))
                            : "null")
                    + "]"
                    + " day=" + initialState.dayDate);

            // Add shutdown hook for graceful termination
            Runtime.getRuntime()
                    .addShutdownHook(new Thread(
                            () -> {
                                System.out.println("[download-live2] Shutdown requested, finalizing stats...");
                                stats.finalizeDayStats();
                                downloadManager.close();
                            },
                            "download-live2-shutdown"));

            // Check for catch-up mode (downloading complete historical days in bulk)
            System.out.println("[download-live2] About to check catch-up mode...");
            System.out.flush();
            State currentState = initialState;
            State catchUpResult =
                    processCatchUpMode(initialState, downloadManager, blockTimeReader, stats, addressBookRegistry);
            System.out.println(
                    "[download-live2] Catch-up mode check returned: " + (catchUpResult != null ? "state" : "null"));
            System.out.flush();
            if (catchUpResult != null) {
                currentState = catchUpResult;
                System.out.println("[download-live2] Catch-up complete, continuing from block "
                        + currentState.blockNumber + " day=" + currentState.dayDate);
            }

            // Main processing loop (live mode with batch download for remaining blocks)
            processBlocks(currentState, addressBookRegistry, downloadManager, blockTimeReader, stats);

        } catch (Exception e) {
            System.err.println("[download-live2] Fatal error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Determines the starting point for block processing.
     * Priority: 1) Resume from state file, 2) Use --start-date if provided, 3) Auto-detect today from mirror node
     */
    private State determineStartingPoint(BlockTimeReader blockTimeReader) {
        // Priority 1: Resume from state file
        if (Files.exists(stateJsonPath)) {
            try {
                String json = Files.readString(stateJsonPath, StandardCharsets.UTF_8);
                State state = GSON.fromJson(json, State.class);
                if (state != null && state.blockNumber > 0) {
                    System.out.println("[download-live2] Resuming from state file: block " + state.blockNumber);
                    return state;
                }
            } catch (Exception e) {
                System.err.println("[download-live2] Warning: Failed to read state file: " + e.getMessage());
            }
        }

        // Priority 2: Use --start-date if provided
        if (startDate != null && !startDate.isBlank()) {
            LocalDate targetDay = LocalDate.parse(startDate);
            System.out.println("[download-live2] Using provided start date: " + targetDay);

            LocalDateTime startOfDay = targetDay.atStartOfDay();
            long firstBlockOfDay = blockTimeReader.getNearestBlockAfterTime(startOfDay);

            System.out.println("[download-live2] First block of " + targetDay + " is " + firstBlockOfDay);

            State state = new State();
            state.blockNumber = firstBlockOfDay - 1;
            state.dayDate = targetDay.toString();
            return state;
        }

        // Priority 3: Auto-detect today from mirror node
        System.out.println("[download-live2] Querying mirror node for current day...");
        List<BlockInfo> latestBlocks = FetchBlockQuery.getLatestBlocks(1, MirrorNodeBlockQueryOrder.DESC);

        if (latestBlocks.isEmpty()) {
            throw new RuntimeException("Failed to get latest block from mirror node");
        }

        BlockInfo latestBlock = latestBlocks.getFirst();
        String timestamp = latestBlock.timestampFrom != null ? latestBlock.timestampFrom : latestBlock.timestampTo;

        if (timestamp == null) {
            throw new RuntimeException("Latest block has no timestamp");
        }

        // Parse timestamp (format: "seconds.nanoseconds")
        String[] parts = timestamp.split("\\.");
        long epochSeconds = Long.parseLong(parts[0]);
        Instant blockInstant = Instant.ofEpochSecond(epochSeconds);
        LocalDate today = blockInstant.atZone(ZoneOffset.UTC).toLocalDate();

        System.out.println(
                "[download-live2] Mirror node latest block: " + latestBlock.number + " timestamp: " + timestamp);
        System.out.println("[download-live2] Detected current day: " + today);

        // Find first block of today
        LocalDateTime startOfDay = today.atStartOfDay();
        long firstBlockOfDay = blockTimeReader.getNearestBlockAfterTime(startOfDay);

        System.out.println("[download-live2] First block of " + today + " is " + firstBlockOfDay);

        State state = new State();
        state.blockNumber =
                firstBlockOfDay - 1; // Start from the block before, so first iteration processes firstBlockOfDay
        state.dayDate = today.toString();
        // No hash yet - will be fetched from mirror node for first block
        return state;
    }

    /**
     * Determines the current "today" date from the mirror node.
     */
    private LocalDate getTodayFromMirrorNode() {
        List<BlockInfo> latestBlocks = FetchBlockQuery.getLatestBlocks(1, MirrorNodeBlockQueryOrder.DESC);
        if (latestBlocks.isEmpty()) {
            return LocalDate.now(ZoneOffset.UTC);
        }
        BlockInfo latestBlock = latestBlocks.getFirst();
        String timestamp = latestBlock.timestampFrom != null ? latestBlock.timestampFrom : latestBlock.timestampTo;
        if (timestamp == null) {
            return LocalDate.now(ZoneOffset.UTC);
        }
        String[] parts = timestamp.split("\\.");
        long epochSeconds = Long.parseLong(parts[0]);
        return Instant.ofEpochSecond(epochSeconds).atZone(ZoneOffset.UTC).toLocalDate();
    }

    /**
     * Processes catch-up mode: downloads complete days in bulk when behind live edge.
     * Returns the updated state after catch-up, or null if no catch-up was needed.
     *
     * <p>Catch-up mode is active when:
     * <ul>
     *   <li>startDay is before (today - 1)</li>
     *   <li>There are complete days to download</li>
     * </ul>
     *
     * @param initialState the initial state from resume or start date
     * @param downloadManager the download manager for bulk downloads
     * @param blockTimeReader the block time reader
     * @param stats the signature statistics tracker
     * @param addressBookRegistry the address book registry
     * @return the updated state after catch-up, or null if no catch-up was performed
     */
    private State processCatchUpMode(
            State initialState,
            ConcurrentDownloadManagerVirtualThreadsV3 downloadManager,
            BlockTimeReader blockTimeReader,
            SignatureStats stats,
            AddressBookRegistry addressBookRegistry)
            throws Exception {

        System.out.println("[CATCH-UP] Checking catch-up mode...");
        System.out.flush();
        LocalDate today = getTodayFromMirrorNode();
        LocalDate startDay = initialState.getDayDate();
        System.out.println("[CATCH-UP] startDay=" + startDay + ", today=" + today);
        System.out.flush();

        // Only catch up if we're at least 1 full day behind
        if (startDay == null || !startDay.isBefore(today)) {
            System.out.println("[CATCH-UP] No catch-up needed, already at or past " + today);
            return null;
        }

        System.out.println("=".repeat(70));
        System.out.println("[CATCH-UP] Catch-up mode activated");
        System.out.println("[CATCH-UP] Start day: " + startDay + ", Today: " + today);
        System.out.println("=".repeat(70));

        // Load day block info for catch-up
        final Map<LocalDate, DayBlockInfo> daysInfo = loadDayBlockInfoMap();

        // Calculate days to download (from startDay up to but not including today)
        final List<LocalDate> daysToDownload = startDay.datesUntil(today).toList();

        if (daysToDownload.isEmpty()) {
            System.out.println("[CATCH-UP] No complete days to download in catch-up mode");
            return null;
        }

        System.out.println("[CATCH-UP] Will download " + daysToDownload.size() + " complete days in bulk mode");

        byte[] previousRecordHash = initialState.getHashBytes();
        long overallStartMillis = System.currentTimeMillis();
        long totalDays = daysToDownload.size();
        LocalDate lastProcessedDay = startDay;
        long lastBlockNumber = initialState.blockNumber;
        Instant lastRecordFileTime = null;

        for (int i = 0; i < daysToDownload.size(); i++) {
            LocalDate dayDate = daysToDownload.get(i);
            DayBlockInfo dayBlockInfo = daysInfo.get(dayDate);

            if (dayBlockInfo == null) {
                System.out.println("[CATCH-UP] No day block info for " + dayDate + ", stopping catch-up");
                break;
            }

            // Check if day archive already exists
            Path dayArchive = outputDir.toPath().resolve(dayDate + ".tar.zstd");
            if (Files.exists(dayArchive)) {
                System.out.println("[CATCH-UP] Skipping " + dayDate + " - archive already exists");
                // Update state to end of this day for proper resume
                lastBlockNumber = dayBlockInfo.lastBlockNumber;
                lastProcessedDay = dayDate;
                // Get the hash from mirror node for this day's last block
                if (dayBlockInfo.lastBlockHash != null) {
                    String hexStr = dayBlockInfo.lastBlockHash.startsWith("0x")
                            ? dayBlockInfo.lastBlockHash.substring(2)
                            : dayBlockInfo.lastBlockHash;
                    previousRecordHash = HexFormat.of().parseHex(hexStr);
                }
                continue;
            }

            // Refresh listings for this day
            refreshListingsForDay(dayDate);

            System.out.println("[CATCH-UP] Downloading day " + dayDate + " (day " + (i + 1) + "/" + totalDays
                    + ", blocks " + dayBlockInfo.firstBlockNumber + "-" + dayBlockInfo.lastBlockNumber + ")");

            // Start stats for this day
            int nodeCount =
                    addressBookRegistry.getCurrentAddressBook().nodeAddress().size();
            stats.startDay(dayDate, nodeCount);

            long dayStartMillis = System.currentTimeMillis();

            try {
                // Download all blocks for the day with full validation
                previousRecordHash = downloadDayWithFullValidation(
                        dayDate,
                        dayBlockInfo,
                        previousRecordHash,
                        downloadManager,
                        blockTimeReader,
                        addressBookRegistry,
                        stats);

                lastBlockNumber = dayBlockInfo.lastBlockNumber;
                lastProcessedDay = dayDate;

                // Get timestamp for last block of day
                LocalDateTime lastBlockTime = blockTimeReader.getBlockLocalDateTime(lastBlockNumber);
                lastRecordFileTime = lastBlockTime.atZone(ZoneOffset.UTC).toInstant();

                long dayDurationSec = (System.currentTimeMillis() - dayStartMillis) / 1000;
                long blocksInDay = dayBlockInfo.lastBlockNumber - dayBlockInfo.firstBlockNumber + 1;
                double blocksPerSec = blocksInDay / Math.max(1.0, dayDurationSec);

                System.out.println("[CATCH-UP] Completed " + dayDate + " in " + formatDuration(dayDurationSec) + " ("
                        + blocksInDay + " blocks, " + String.format("%.1f", blocksPerSec) + " blocks/sec)");

                // Finalize day stats
                stats.finalizeDayStats();

                // Save state after each day
                saveState(new State(lastBlockNumber, previousRecordHash, lastRecordFileTime, lastProcessedDay));

            } catch (Exception e) {
                System.err.println("[CATCH-UP] Error downloading day " + dayDate + ": " + e.getMessage());
                e.printStackTrace();
                // Save state at the last successfully completed day
                if (lastRecordFileTime != null) {
                    saveState(new State(lastBlockNumber, previousRecordHash, lastRecordFileTime, lastProcessedDay));
                }
                throw e;
            }
        }

        long totalDurationSec = (System.currentTimeMillis() - overallStartMillis) / 1000;
        System.out.println("=".repeat(70));
        System.out.println("[CATCH-UP] Catch-up complete: processed " + daysToDownload.size() + " days in "
                + formatDuration(totalDurationSec));
        System.out.println("[CATCH-UP] Last block: " + lastBlockNumber + ", switching to live mode for " + today);
        System.out.println("=".repeat(70));

        // Return updated state to continue with live mode
        return new State(lastBlockNumber, previousRecordHash, lastRecordFileTime, lastProcessedDay);
    }

    /**
     * Downloads all blocks for a day with full validation (hash chain + signatures).
     * Uses parallel download (via DownloadDayLiveImpl.downloadDay) then adds signature validation.
     *
     * @return the running hash after processing all blocks
     */
    private byte[] downloadDayWithFullValidation(
            LocalDate dayDate,
            DayBlockInfo dayBlockInfo,
            byte[] previousRecordHash,
            ConcurrentDownloadManagerVirtualThreadsV3 downloadManager,
            BlockTimeReader blockTimeReader,
            AddressBookRegistry addressBookRegistry,
            SignatureStats stats)
            throws Exception {

        // Refresh listings for this day
        refreshListingsForDay(dayDate);

        // Load listings for the day
        List<ListingRecordFile> dayListings = DayListingFileReader.loadRecordsFileForDay(
                listingDir.toPath(), dayDate.getYear(), dayDate.getMonthValue(), dayDate.getDayOfMonth());

        if (dayListings.isEmpty()) {
            throw new IllegalStateException("No listing files found for " + dayDate);
        }

        long firstBlock = dayBlockInfo.firstBlockNumber;
        long lastBlock = dayBlockInfo.lastBlockNumber;
        long totalBlocks = lastBlock - firstBlock + 1;

        System.out.println("[CATCH-UP] Day " + dayDate + ": " + totalBlocks + " blocks, " + dayListings.size()
                + " listing entries (using V2 parallel bulk download)");

        // Get node count for stats tracking
        final int nodeCount =
                addressBookRegistry.getCurrentAddressBook().nodeAddress().size();

        // Use the parallel bulk download from DownloadDayImplV2 (same as download-days-v3)
        // This downloads all blocks in parallel, validates hash chain, and writes to archive
        // Pass a callback to track signature statistics for each block
        return DownloadDayImplV2.downloadDay(
                downloadManager,
                dayBlockInfo,
                blockTimeReader,
                listingDir.toPath(),
                outputDir.toPath(),
                dayDate.getYear(),
                dayDate.getMonthValue(),
                dayDate.getDayOfMonth(),
                previousRecordHash,
                1, // totalDays (just this day for progress)
                0, // dayIndex
                System.currentTimeMillis(),
                false, // don't skip validation
                (blockNumber, signatureCount) -> stats.recordBlock(signatureCount, nodeCount));
    }

    /**
     * Result of a batch download operation.
     */
    private static class BatchDownloadResult {
        final int blocksDownloaded;
        final byte[] finalHash;

        BatchDownloadResult(int blocksDownloaded, byte[] finalHash) {
            this.blocksDownloaded = blocksDownloaded;
            this.finalHash = finalHash;
        }
    }

    /**
     * Downloads remaining blocks of the current day in batch mode.
     * This is used when we're on the current day but behind the live edge.
     *
     * @return the batch download result with count and final hash, or null if at live edge
     */
    private BatchDownloadResult downloadCurrentDayBlocksBatch(
            long startBlockNumber,
            LocalDate currentDay,
            byte[] currentHash,
            ConcurrentDownloadManagerVirtualThreadsV3 downloadManager,
            BlockTimeReader blockTimeReader,
            AddressBookRegistry addressBookRegistry,
            SignatureStats stats,
            ConcurrentTarZstdWriter writer,
            List<ListingRecordFile> cachedListingFiles)
            throws Exception {

        // Get all available blocks from listings
        Map<LocalDateTime, List<ListingRecordFile>> filesByBlock = cachedListingFiles.stream()
                .collect(java.util.stream.Collectors.groupingBy(ListingRecordFile::timestamp));

        // Find blocks we need to download
        List<Long> blocksToDownload = new java.util.ArrayList<>();
        for (long blockNum = startBlockNumber + 1; ; blockNum++) {
            try {
                LocalDateTime blockTime = blockTimeReader.getBlockLocalDateTime(blockNum);
                if (!blockTime.toLocalDate().equals(currentDay)) {
                    break; // Moved to next day
                }
                if (filesByBlock.containsKey(blockTime)) {
                    blocksToDownload.add(blockNum);
                } else {
                    break; // No more blocks available in listings
                }
            } catch (Exception e) {
                break; // Block not in BlockTimeReader yet
            }
        }

        if (blocksToDownload.isEmpty()) {
            return null; // At live edge
        }

        System.out.println("[BATCH] Downloading " + blocksToDownload.size() + " blocks in batch mode (blocks "
                + blocksToDownload.getFirst() + "-" + blocksToDownload.getLast() + ")");

        byte[] hash = currentHash;
        int downloadedCount = 0;

        for (Long blockNum : blocksToDownload) {
            LocalDateTime blockTime = blockTimeReader.getBlockLocalDateTime(blockNum);
            int year = blockTime.getYear();
            int month = blockTime.getMonthValue();
            int day = blockTime.getDayOfMonth();

            try {
                DownloadDayLiveImpl.BlockDownloadResult result = DownloadDayLiveImpl.downloadSingleBlockForLive(
                        downloadManager,
                        null,
                        blockTimeReader,
                        listingDir.toPath(),
                        year,
                        month,
                        day,
                        blockNum,
                        hash,
                        false); // don't skip hash validation

                if (result != null) {
                    // Full block validation
                    Instant recordFileTime = blockTime.atZone(ZoneOffset.UTC).toInstant();
                    boolean valid = fullBlockValidate(addressBookRegistry, hash, recordFileTime, result, null);

                    if (!valid) {
                        System.err.println("[BATCH] WARNING: Full block validation failed for block " + blockNum);
                    }

                    // Record block statistics
                    int nodeCount = addressBookRegistry
                            .getAddressBookForBlock(recordFileTime)
                            .nodeAddress()
                            .size();
                    stats.recordBlock(result.files, nodeCount);

                    // Write block to archive
                    for (InMemoryFile file : result.files) {
                        writer.putEntry(file);
                    }

                    hash = result.newPreviousRecordFileHash;
                    downloadedCount++;

                    if (downloadedCount % PROGRESS_LOG_INTERVAL == 0) {
                        System.out.println("[BATCH] Downloaded " + downloadedCount + "/" + blocksToDownload.size()
                                + " blocks (current: " + blockNum + ")");
                    }
                }
            } catch (Exception e) {
                System.err.println("[BATCH] Error downloading block " + blockNum + ": " + e.getMessage());
                throw e;
            }
        }

        System.out.println("[BATCH] Completed batch download: " + downloadedCount + " blocks");
        return new BatchDownloadResult(downloadedCount, hash);
    }

    /**
     * Main block processing loop.
     */
    private void processBlocks(
            State initialState,
            AddressBookRegistry addressBookRegistry,
            ConcurrentDownloadManagerVirtualThreadsV3 downloadManager,
            BlockTimeReader blockTimeReader,
            SignatureStats stats)
            throws Exception {

        long currentBlockNumber = initialState.blockNumber;
        byte[] currentHash = initialState.getHashBytes();
        LocalDate currentDay = initialState.getDayDate();

        ConcurrentTarZstdWriter currentDayWriter = null;
        long blocksProcessedTotal = 0;
        long blocksProcessedToday = 0;
        long dayStartTime = System.currentTimeMillis();

        // Cache for listing files - reload only when day changes
        List<ListingRecordFile> cachedListingFiles = null;
        LocalDate cachedListingDay = null;

        try {
            while (true) {
                long nextBlockNumber = currentBlockNumber + 1;

                // Get block timestamp
                LocalDateTime blockTime;
                try {
                    blockTime = blockTimeReader.getBlockLocalDateTime(nextBlockNumber);
                } catch (Exception e) {
                    // Block not in BlockTimeReader yet - we're at the live edge
                    System.out.println("[LIVE] Block " + nextBlockNumber + " not in BlockTimeReader yet, waiting...");
                    Thread.sleep(LIVE_POLL_INTERVAL.toMillis());
                    continue;
                }

                LocalDate blockDay = blockTime.toLocalDate();

                // Handle day change
                if (!blockDay.equals(currentDay)) {
                    // Finalize previous day stats
                    if (blocksProcessedToday > 0) {
                        stats.finalizeDayStats();
                    }

                    // Finalize previous day archive
                    if (currentDayWriter != null) {
                        currentDayWriter.close();
                        System.out.println("[download-live2] Day completed: " + currentDay
                                + " (" + blocksProcessedToday + " blocks in "
                                + formatDuration((System.currentTimeMillis() - dayStartTime) / 1000) + ")");
                    }

                    // Check if output file already exists for new day
                    Path dayArchive = outputDir.toPath().resolve(blockDay + ".tar.zstd");
                    if (Files.exists(dayArchive)) {
                        System.out.println(
                                "[download-live2] Archive already exists for " + blockDay + ", skipping day...");
                        // Find first block of next day to skip past this completed day
                        LocalDateTime startOfNextDay = blockDay.plusDays(1).atStartOfDay();
                        try {
                            long firstBlockOfNextDay = blockTimeReader.getNearestBlockAfterTime(startOfNextDay);
                            currentBlockNumber = firstBlockOfNextDay - 1; // Will be incremented at start of loop
                            currentDay = blockDay.plusDays(1);
                            blocksProcessedToday = 0;
                            dayStartTime = System.currentTimeMillis();
                            continue;
                        } catch (Exception e) {
                            // Can't find next day's first block - we're at the live edge, wait and retry
                            System.out.println(
                                    "[LIVE] At live edge - waiting for blocks from " + blockDay.plusDays(1) + "...");
                            Thread.sleep(LIVE_POLL_INTERVAL.toMillis());
                            continue;
                        }
                    }

                    // Start new day
                    currentDay = blockDay;
                    currentDayWriter = new ConcurrentTarZstdWriter(dayArchive);
                    blocksProcessedToday = 0;
                    dayStartTime = System.currentTimeMillis();
                    cachedListingFiles = null; // Force reload of listings
                    cachedListingDay = null;

                    // Start stats for new day
                    int nodeCount = addressBookRegistry
                            .getCurrentAddressBook()
                            .nodeAddress()
                            .size();
                    stats.startDay(currentDay, nodeCount);

                    System.out.println("[download-live2] Started new day: " + currentDay);
                }

                // Initialize writer if needed (first block)
                if (currentDayWriter == null) {
                    Path dayArchive = outputDir.toPath().resolve(currentDay + ".tar.zstd");
                    if (Files.exists(dayArchive)) {
                        System.out.println(
                                "[download-live2] Archive already exists for " + currentDay + ", finding next day...");
                        currentDay = currentDay.plusDays(1);
                        continue;
                    }
                    currentDayWriter = new ConcurrentTarZstdWriter(dayArchive);
                    dayStartTime = System.currentTimeMillis();

                    // Start stats for this day
                    int nodeCount = addressBookRegistry
                            .getCurrentAddressBook()
                            .nodeAddress()
                            .size();
                    stats.startDay(currentDay, nodeCount);

                    System.out.println("[download-live2] Started day: " + currentDay);
                }

                // Refresh listings if day changed
                if (!blockDay.equals(cachedListingDay)) {
                    refreshListingsForDay(blockDay);
                    int year = blockTime.getYear();
                    int month = blockTime.getMonthValue();
                    int day = blockTime.getDayOfMonth();
                    cachedListingFiles =
                            DayListingFileReader.loadRecordsFileForDay(listingDir.toPath(), year, month, day);
                    cachedListingDay = blockDay;
                    System.out.println("[download-live2] Loaded " + cachedListingFiles.size() + " listing entries for "
                            + blockDay);
                }

                // If no hash yet, fetch previous hash from mirror node
                if (currentHash == null && nextBlockNumber > 0) {
                    System.out.println(
                            "[download-live2] Fetching previous hash from mirror node for block " + nextBlockNumber);
                    try {
                        var prevHashBytes = FetchBlockQuery.getPreviousHashForBlock(nextBlockNumber);
                        currentHash = prevHashBytes.toByteArray();
                        System.out.println("[download-live2] Got previous hash: "
                                + HexFormat.of().formatHex(currentHash).substring(0, 16) + "...");
                    } catch (Exception e) {
                        System.err.println(
                                "[download-live2] Warning: Could not fetch previous hash: " + e.getMessage());
                    }
                }

                // Try batch download first - downloads all available blocks at once
                BatchDownloadResult batchResult = downloadCurrentDayBlocksBatch(
                        currentBlockNumber,
                        blockDay,
                        currentHash,
                        downloadManager,
                        blockTimeReader,
                        addressBookRegistry,
                        stats,
                        currentDayWriter,
                        cachedListingFiles);

                if (batchResult != null && batchResult.blocksDownloaded > 0) {
                    // Batch download succeeded - update state and continue
                    long lastDownloadedBlock = currentBlockNumber + batchResult.blocksDownloaded;
                    LocalDateTime lastBlockTime = blockTimeReader.getBlockLocalDateTime(lastDownloadedBlock);
                    Instant lastRecordFileTime =
                            lastBlockTime.atZone(ZoneOffset.UTC).toInstant();

                    currentBlockNumber = lastDownloadedBlock;
                    currentHash = batchResult.finalHash;
                    blocksProcessedTotal += batchResult.blocksDownloaded;
                    blocksProcessedToday += batchResult.blocksDownloaded;

                    // Save state after batch
                    saveState(new State(currentBlockNumber, currentHash, lastRecordFileTime, currentDay));

                    System.out.println("[download-live2] Batch downloaded " + batchResult.blocksDownloaded
                            + " blocks, now at block " + currentBlockNumber);

                    // Refresh listings to check for more blocks
                    refreshListingsForDay(blockDay);
                    cachedListingFiles = DayListingFileReader.loadRecordsFileForDay(
                            listingDir.toPath(),
                            blockTime.getYear(),
                            blockTime.getMonthValue(),
                            blockTime.getDayOfMonth());
                    continue;
                }

                // Batch returned null (at live edge), try single block download
                DownloadDayLiveImpl.BlockDownloadResult result = downloadBlockWithRetry(
                        nextBlockNumber, blockTime, currentHash, cachedListingFiles, downloadManager, blockTimeReader);

                if (result == null) {
                    // Block not available yet in GCS - we're at the live edge
                    System.out.println("[LIVE] Waiting for block " + nextBlockNumber + " in GCS...");
                    Thread.sleep(LIVE_POLL_INTERVAL.toMillis());
                    // Refresh listings in case new files appeared
                    refreshListingsForDay(blockDay);
                    cachedListingFiles = DayListingFileReader.loadRecordsFileForDay(
                            listingDir.toPath(),
                            blockTime.getYear(),
                            blockTime.getMonthValue(),
                            blockTime.getDayOfMonth());
                    continue;
                }

                // Full block validation
                Instant recordFileTime = blockTime.atZone(ZoneOffset.UTC).toInstant();
                boolean valid = fullBlockValidate(addressBookRegistry, currentHash, recordFileTime, result, null);

                if (!valid) {
                    System.err.println("[download-live2] WARNING: Full block validation failed for block "
                            + nextBlockNumber + ", continuing anyway...");
                }

                // Record block statistics
                int nodeCount = addressBookRegistry
                        .getAddressBookForBlock(recordFileTime)
                        .nodeAddress()
                        .size();
                stats.recordBlock(result.files, nodeCount);

                // Write block to archive
                for (InMemoryFile file : result.files) {
                    currentDayWriter.putEntry(file);
                }

                // Update state
                currentBlockNumber = nextBlockNumber;
                currentHash = result.newPreviousRecordFileHash;
                blocksProcessedTotal++;
                blocksProcessedToday++;

                // Save state periodically
                if (blocksProcessedTotal % 100 == 0) {
                    saveState(new State(currentBlockNumber, currentHash, recordFileTime, currentDay));
                }

                // Log progress
                if (blocksProcessedTotal % PROGRESS_LOG_INTERVAL == 0) {
                    long elapsed = System.currentTimeMillis() - dayStartTime;
                    double blocksPerSec = blocksProcessedToday / (elapsed / 1000.0);
                    System.out.printf(
                            "[download-live2] Block %d (%s) - %.1f blocks/sec - %d blocks today%n",
                            currentBlockNumber, blockTime, blocksPerSec, blocksProcessedToday);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("[download-live2] Interrupted, saving state...");
        } finally {
            // Save final state
            if (currentHash != null) {
                LocalDateTime finalBlockTime = blockTimeReader.getBlockLocalDateTime(currentBlockNumber);
                Instant finalRecordTime = finalBlockTime.atZone(ZoneOffset.UTC).toInstant();
                saveState(new State(currentBlockNumber, currentHash, finalRecordTime, currentDay));
                System.out.println("[download-live2] Saved state at block " + currentBlockNumber);
            }
            if (currentDayWriter != null) {
                try {
                    currentDayWriter.close();
                    System.out.println("[download-live2] Closed archive for " + currentDay);
                } catch (Exception e) {
                    System.err.println("[download-live2] Error closing writer: " + e.getMessage());
                }
            }
        }
    }

    /**
     * Downloads a block with retry logic.
     */
    private DownloadDayLiveImpl.BlockDownloadResult downloadBlockWithRetry(
            long blockNumber,
            LocalDateTime blockTime,
            byte[] previousHash,
            List<ListingRecordFile> cachedListingFiles,
            ConcurrentDownloadManagerVirtualThreadsV3 downloadManager,
            BlockTimeReader blockTimeReader)
            throws IOException {

        Exception lastException = null;

        for (int attempt = 1; attempt <= MAX_VALIDATION_RETRIES; attempt++) {
            try {
                int year = blockTime.getYear();
                int month = blockTime.getMonthValue();
                int day = blockTime.getDayOfMonth();

                // Filter cached listing files for this specific block
                List<ListingRecordFile> blockFiles = cachedListingFiles.stream()
                        .filter(f -> f.timestamp().equals(blockTime))
                        .toList();

                if (blockFiles.isEmpty()) {
                    // Block not available yet in GCS
                    return null;
                }

                // Download the block
                return DownloadDayLiveImpl.downloadSingleBlockForLive(
                        downloadManager,
                        null, // dayBlockInfo not needed for live mode
                        blockTimeReader,
                        listingDir.toPath(),
                        year,
                        month,
                        day,
                        blockNumber,
                        previousHash,
                        false); // don't skip hash validation

            } catch (Exception e) {
                lastException = e;
                if (attempt < MAX_VALIDATION_RETRIES) {
                    System.err.println("[download-live2] Download failed for block " + blockNumber + ", attempt "
                            + attempt + "/" + MAX_VALIDATION_RETRIES + ": " + e.getMessage());
                    try {
                        Thread.sleep(1000L * attempt); // exponential backoff
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Interrupted during retry backoff", ie);
                    }
                }
            }
        }

        throw new IOException(
                "Failed to download block " + blockNumber + " after " + MAX_VALIDATION_RETRIES + " attempts",
                lastException);
    }

    /**
     * Refreshes GCS listings for a day.
     */
    private void refreshListingsForDay(LocalDate day) {
        UpdateDayListingsCommand.updateDayListings(
                listingDir.toPath(),
                CACHE_DIR.toPath(),
                true, // forceRefresh
                MIN_NODE_ACCOUNT_ID,
                MAX_NODE_ACCOUNT_ID,
                DownloadConstants.GCP_PROJECT_ID);
    }

    /**
     * Saves state to JSON file.
     */
    private void saveState(State state) {
        try {
            Path parentDir = stateJsonPath.getParent();
            if (parentDir != null && !Files.exists(parentDir)) {
                Files.createDirectories(parentDir);
            }
            String json = GSON.toJson(state);
            Files.writeString(stateJsonPath, json, StandardCharsets.UTF_8);
        } catch (IOException e) {
            System.err.println("[download-live2] Warning: Failed to save state: " + e.getMessage());
        }
    }

    /**
     * Formats a duration in seconds to human-readable format.
     */
    private String formatDuration(long seconds) {
        if (seconds < 60) {
            return seconds + "s";
        } else if (seconds < 3600) {
            return String.format("%dm %ds", seconds / 60, seconds % 60);
        } else if (seconds < 86400) {
            return String.format("%dh %dm", seconds / 3600, (seconds % 3600) / 60);
        } else {
            return String.format("%dd %dh", seconds / 86400, (seconds % 86400) / 3600);
        }
    }
}
