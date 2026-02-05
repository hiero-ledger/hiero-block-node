// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.subcommands;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static org.hiero.block.tools.days.downloadlive.ValidateDownloadLive.fullBlockValidate;
import static org.hiero.block.tools.mirrornode.DayBlockInfo.loadDayBlockInfoMap;
import static org.hiero.block.tools.utils.Md5Checker.checkMd5;

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
import org.hiero.block.tools.metadata.MetadataFiles;
import org.hiero.block.tools.mirrornode.BlockInfo;
import org.hiero.block.tools.mirrornode.BlockTimeReader;
import org.hiero.block.tools.mirrornode.DayBlockInfo;
import org.hiero.block.tools.mirrornode.FetchBlockQuery;
import org.hiero.block.tools.mirrornode.FixBlockTime;
import org.hiero.block.tools.mirrornode.MirrorNodeBlockQueryOrder;
import org.hiero.block.tools.mirrornode.UpdateBlockData;
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
    private static final int PROGRESS_LOG_INTERVAL = 100;

    /** Number of blocks to download at a time in batch mode (when 1000+ blocks available) */
    private static final int BATCH_SIZE = 1000;
    /** Number of blocks to download at a time in live mode (when <1000 but >=100 blocks available) */
    private static final int LIVE_BATCH_SIZE = 100;
    /** Minimum interval between block time refreshes from mirror node (in milliseconds) */
    private static final long MIN_BLOCK_TIME_REFRESH_INTERVAL_MS = 30_000;

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

        /**
         * Write current progress to CSV (called after each batch for safety).
         * Overwrites the current day's row if it exists.
         */
        void writeProgress() {
            if (currentDayStats != null && currentDayStats.totalBlocks > 0) {
                writeDayToCsv(currentDayStats);
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
            State catchUpResult = processCatchUpMode(initialState, downloadManager, stats, addressBookRegistry);
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
     * @param initialState the initial state from resume or start date
     * @param downloadManager the download manager for bulk downloads
     * @param stats the signature statistics tracker
     * @param addressBookRegistry the address book registry
     * @return the updated state after catch-up, or null if no catch-up was performed
     */
    private State processCatchUpMode(
            State initialState,
            ConcurrentDownloadManagerVirtualThreadsV3 downloadManager,
            SignatureStats stats,
            AddressBookRegistry addressBookRegistry)
            throws Exception {

        System.out.println("[CATCH-UP] Checking catch-up mode...");
        LocalDate today = getTodayFromMirrorNode();
        LocalDate startDay = initialState.getDayDate();
        System.out.println("[CATCH-UP] startDay=" + startDay + ", today=" + today);

        if (startDay == null || !startDay.isBefore(today)) {
            System.out.println("[CATCH-UP] No catch-up needed, already at or past " + today);
            return null;
        }

        printCatchUpHeader(startDay, today);
        UpdateBlockData.updateMirrorNodeData(MetadataFiles.BLOCK_TIMES_FILE, MetadataFiles.DAY_BLOCKS_FILE);

        // Reload BlockTimeReader after updating block data to pick up new block times
        final BlockTimeReader updatedBlockTimeReader = new BlockTimeReader(MetadataFiles.BLOCK_TIMES_FILE);

        final Map<LocalDate, DayBlockInfo> daysInfo = loadDayBlockInfoMap();
        final List<LocalDate> daysToDownload = startDay.datesUntil(today).toList();

        if (daysToDownload.isEmpty()) {
            System.out.println("[CATCH-UP] No complete days to download in catch-up mode");
            return null;
        }

        System.out.println("[CATCH-UP] Will download " + daysToDownload.size() + " complete days in bulk mode");

        CatchUpState catchUpState = new CatchUpState(initialState);
        long overallStartMillis = System.currentTimeMillis();

        for (int i = 0; i < daysToDownload.size(); i++) {
            LocalDate dayDate = daysToDownload.get(i);
            DayBlockInfo dayBlockInfo = daysInfo.get(dayDate);

            if (dayBlockInfo == null) {
                System.out.println("[CATCH-UP] No day block info for " + dayDate + ", stopping catch-up");
                break;
            }

            if (skipExistingDayArchive(dayDate, dayBlockInfo, catchUpState)) {
                continue;
            }

            processCatchUpDay(
                    dayDate,
                    dayBlockInfo,
                    i,
                    daysToDownload.size(),
                    downloadManager,
                    updatedBlockTimeReader,
                    stats,
                    addressBookRegistry,
                    catchUpState);
        }

        printCatchUpSummary(daysToDownload.size(), catchUpState.lastBlockNumber, today, overallStartMillis);
        return new State(
                catchUpState.lastBlockNumber,
                catchUpState.previousRecordHash,
                catchUpState.lastRecordFileTime,
                catchUpState.lastProcessedDay);
    }

    /** Mutable state holder for catch-up processing. */
    private class CatchUpState {
        byte[] previousRecordHash;
        long lastBlockNumber;
        LocalDate lastProcessedDay;
        Instant lastRecordFileTime;

        CatchUpState(State initial) {
            this.previousRecordHash = initial.getHashBytes();
            this.lastBlockNumber = initial.blockNumber;
            this.lastProcessedDay = initial.getDayDate();
            this.lastRecordFileTime = null;
        }
    }

    private void printCatchUpHeader(LocalDate startDay, LocalDate today) {
        System.out.println("=".repeat(70));
        System.out.println("[CATCH-UP] Catch-up mode activated");
        System.out.println("[CATCH-UP] Start day: " + startDay + ", Today: " + today);
        System.out.println("=".repeat(70));
        System.out.println("[CATCH-UP] Updating block data from mirror node...");
    }

    private void printCatchUpSummary(int daysCount, long lastBlock, LocalDate today, long startMillis) {
        long totalDurationSec = (System.currentTimeMillis() - startMillis) / 1000;
        System.out.println("=".repeat(70));
        System.out.println("[CATCH-UP] Catch-up complete: processed " + daysCount + " days in "
                + formatDuration(totalDurationSec));
        System.out.println("[CATCH-UP] Last block: " + lastBlock + ", switching to live mode for " + today);
        System.out.println("=".repeat(70));
    }

    private boolean skipExistingDayArchive(LocalDate dayDate, DayBlockInfo dayBlockInfo, CatchUpState state) {
        Path dayArchive = outputDir.toPath().resolve(dayDate + ".tar.zstd");
        if (Files.exists(dayArchive)) {
            System.out.println("[CATCH-UP] Skipping " + dayDate + " - archive already exists");
            state.lastBlockNumber = dayBlockInfo.lastBlockNumber;
            state.lastProcessedDay = dayDate;
            if (dayBlockInfo.lastBlockHash != null) {
                String hexStr = dayBlockInfo.lastBlockHash.startsWith("0x")
                        ? dayBlockInfo.lastBlockHash.substring(2)
                        : dayBlockInfo.lastBlockHash;
                state.previousRecordHash = HexFormat.of().parseHex(hexStr);
            }
            return true;
        }
        return false;
    }

    private void processCatchUpDay(
            LocalDate dayDate,
            DayBlockInfo dayBlockInfo,
            int dayIndex,
            int totalDays,
            ConcurrentDownloadManagerVirtualThreadsV3 downloadManager,
            BlockTimeReader blockTimeReader,
            SignatureStats stats,
            AddressBookRegistry addressBookRegistry,
            CatchUpState state)
            throws Exception {

        refreshListingsForDay(dayDate);
        System.out.println("[CATCH-UP] Downloading day " + dayDate + " (day " + (dayIndex + 1) + "/" + totalDays
                + ", blocks " + dayBlockInfo.firstBlockNumber + "-" + dayBlockInfo.lastBlockNumber + ")");

        int nodeCount =
                addressBookRegistry.getCurrentAddressBook().nodeAddress().size();
        stats.startDay(dayDate, nodeCount);
        long dayStartMillis = System.currentTimeMillis();

        try {
            state.previousRecordHash = downloadDayWithFullValidation(
                    dayDate,
                    dayBlockInfo,
                    state.previousRecordHash,
                    downloadManager,
                    blockTimeReader,
                    addressBookRegistry,
                    stats);
            finalizeCatchUpDay(dayDate, dayBlockInfo, blockTimeReader, stats, state, dayStartMillis, false);
        } catch (Exception e) {
            if (handleCatchUpError(
                    e, dayDate, dayBlockInfo, downloadManager, addressBookRegistry, stats, state, dayStartMillis)) {
                return; // Successfully retried
            }
            throw e;
        }
    }

    private void finalizeCatchUpDay(
            LocalDate dayDate,
            DayBlockInfo dayBlockInfo,
            BlockTimeReader blockTimeReader,
            SignatureStats stats,
            CatchUpState state,
            long dayStartMillis,
            boolean afterFix)
            throws IOException {
        state.lastBlockNumber = dayBlockInfo.lastBlockNumber;
        state.lastProcessedDay = dayDate;
        LocalDateTime lastBlockTime = blockTimeReader.getBlockLocalDateTime(state.lastBlockNumber);
        state.lastRecordFileTime = lastBlockTime.atZone(ZoneOffset.UTC).toInstant();

        long dayDurationSec = (System.currentTimeMillis() - dayStartMillis) / 1000;
        long blocksInDay = dayBlockInfo.lastBlockNumber - dayBlockInfo.firstBlockNumber + 1;
        double blocksPerSec = blocksInDay / Math.max(1.0, dayDurationSec);

        String suffix = afterFix ? " (after fix)" : "";
        System.out.println("[CATCH-UP] Completed " + dayDate + suffix + " in " + formatDuration(dayDurationSec) + " ("
                + blocksInDay + " blocks, " + String.format("%.1f", blocksPerSec) + " blocks/sec)");

        stats.finalizeDayStats();
        saveState(new State(
                state.lastBlockNumber, state.previousRecordHash, state.lastRecordFileTime, state.lastProcessedDay));
    }

    private boolean handleCatchUpError(
            Exception e,
            LocalDate dayDate,
            DayBlockInfo dayBlockInfo,
            ConcurrentDownloadManagerVirtualThreadsV3 downloadManager,
            AddressBookRegistry addressBookRegistry,
            SignatureStats stats,
            CatchUpState state,
            long dayStartMillis)
            throws Exception {
        String errorMsg = e.getMessage() != null ? e.getMessage() : "";
        Throwable cause = e.getCause();
        String causeMsg = cause != null && cause.getMessage() != null ? cause.getMessage() : "";

        if (!errorMsg.contains("Missing record files for block number")
                && !causeMsg.contains("Missing record files for block number")) {
            System.err.println("[CATCH-UP] Error downloading day " + dayDate + ": " + e.getMessage());
            e.printStackTrace();
            if (state.lastRecordFileTime != null) {
                saveState(new State(
                        state.lastBlockNumber,
                        state.previousRecordHash,
                        state.lastRecordFileTime,
                        state.lastProcessedDay));
            }
            return false;
        }

        System.out.println("[CATCH-UP] Detected stale block times data, running FixBlockTime...");
        long fixStartBlock = dayBlockInfo.firstBlockNumber;
        long fixEndBlock = dayBlockInfo.lastBlockNumber + 100;

        try {
            FixBlockTime.fixBlockTimeRange(MetadataFiles.BLOCK_TIMES_FILE, fixStartBlock, fixEndBlock);
            BlockTimeReader newReader = new BlockTimeReader(MetadataFiles.BLOCK_TIMES_FILE);
            System.out.println("[CATCH-UP] Block times fixed, retrying day " + dayDate + "...");

            state.previousRecordHash = downloadDayWithFullValidation(
                    dayDate,
                    dayBlockInfo,
                    state.previousRecordHash,
                    downloadManager,
                    newReader,
                    addressBookRegistry,
                    stats);
            finalizeCatchUpDay(dayDate, dayBlockInfo, newReader, stats, state, dayStartMillis, true);
            return true;
        } catch (Exception retryEx) {
            System.err.println("[CATCH-UP] Retry failed after FixBlockTime: " + retryEx.getMessage());
            retryEx.printStackTrace();
            return false;
        }
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
     * Configuration for batch download operations.
     */
    private record BatchDownloadConfig(int minBlocksRequired, int maxBlocksToDownload, String modeLabel) {}

    /**
     * Context for batch download operations containing shared services and resources.
     */
    private record BatchDownloadContext(
            ConcurrentDownloadManagerVirtualThreadsV3 downloadManager,
            BlockTimeReader blockTimeReader,
            AddressBookRegistry addressBookRegistry,
            SignatureStats stats,
            ConcurrentTarZstdWriter writer) {}

    /** Helper container for pending block downloads (same pattern as DownloadDayImplV2) */
    private static final class BlockWork {
        final long blockNumber;
        final LocalDateTime blockTime;
        final List<ListingRecordFile> orderedFiles;
        final List<java.util.concurrent.CompletableFuture<InMemoryFile>> futures = new java.util.ArrayList<>();

        BlockWork(long blockNumber, LocalDateTime blockTime, List<ListingRecordFile> orderedFiles) {
            this.blockNumber = blockNumber;
            this.blockTime = blockTime;
            this.orderedFiles = orderedFiles;
        }
    }

    /**
     * Downloads remaining blocks of the current day in batch mode.
     * This is used when we're on the current day but behind the live edge.
     *
     * <p>Performance optimization: Uses async downloads like DownloadDayImplV2 -
     * queues all file downloads upfront, then processes as they complete.
     *
     * @param context shared services and resources for batch download
     * @param config batch configuration (minBlocksRequired, maxBlocksToDownload, modeLabel)
     * @return the batch download result with count and final hash, or null if fewer than minBlocksRequired blocks available
     */
    private BatchDownloadResult downloadCurrentDayBlocksBatch(
            long startBlockNumber,
            LocalDate currentDay,
            byte[] currentHash,
            List<ListingRecordFile> cachedListingFiles,
            BatchDownloadContext context,
            BatchDownloadConfig config)
            throws Exception {

        Map<LocalDateTime, List<ListingRecordFile>> filesByBlock = cachedListingFiles.stream()
                .collect(java.util.stream.Collectors.groupingBy(ListingRecordFile::timestamp));

        List<Long> allAvailableBlocks =
                findAvailableBlocks(startBlockNumber, currentDay, context.blockTimeReader(), filesByBlock);

        if (allAvailableBlocks.size() < config.minBlocksRequired()) {
            // Check if we're at end of day - sample blocks ahead at increasing offsets
            // to detect day boundary even when there are gaps in listings
            boolean endOfDay = false;
            if (!allAvailableBlocks.isEmpty()) {
                long lastAvailable = allAvailableBlocks.getLast();
                for (long offset : new long[] {1, 10, 50, 100, 500}) {
                    try {
                        LocalDateTime checkTime =
                                context.blockTimeReader().getBlockLocalDateTime(lastAvailable + offset);
                        if (!checkTime.toLocalDate().equals(currentDay)) {
                            endOfDay = true;
                            break;
                        }
                    } catch (Exception e) {
                        // Block time not available yet - at live edge
                        break;
                    }
                }
            }

            if (endOfDay && !allAvailableBlocks.isEmpty()) {
                // End of day - download whatever we have to allow day rollover
                System.out.println("[" + config.modeLabel() + "] End of day detected, downloading remaining "
                        + allAvailableBlocks.size() + " blocks...");
            } else {
                System.out.println("[" + config.modeLabel() + "] Only " + allAvailableBlocks.size()
                        + " blocks available (day=" + currentDay + ", listings=" + filesByBlock.size()
                        + "), waiting for " + config.minBlocksRequired() + "...");
                return null;
            }
        }

        List<Long> blocksToDownload =
                allAvailableBlocks.subList(0, Math.min(config.maxBlocksToDownload(), allAvailableBlocks.size()));

        System.out.println("[" + config.modeLabel() + "] Downloading " + blocksToDownload.size() + " blocks (blocks "
                + blocksToDownload.getFirst() + "-" + blocksToDownload.getLast() + ", "
                + (allAvailableBlocks.size() - blocksToDownload.size()) + " more available)");

        long downloadStartTime = System.currentTimeMillis();
        java.util.Set<ListingRecordFile> mostCommonFiles =
                buildMostCommonFilesSet(blocksToDownload, context.blockTimeReader(), filesByBlock);

        java.util.concurrent.LinkedBlockingDeque<BlockWork> pending = queueBlockDownloads(
                blocksToDownload,
                context.blockTimeReader(),
                filesByBlock,
                context.downloadManager(),
                config.modeLabel());

        byte[] hash = processQueuedDownloads(
                pending,
                currentHash,
                mostCommonFiles,
                context.addressBookRegistry(),
                context.stats(),
                context.writer(),
                blocksToDownload.size(),
                downloadStartTime,
                config.modeLabel());

        long totalTime = System.currentTimeMillis() - downloadStartTime;
        int processedCount = blocksToDownload.size();
        double blocksPerSec = processedCount / (totalTime / 1000.0);
        System.out.println("[" + config.modeLabel() + "] Completed: " + processedCount + " blocks in " + totalTime
                + "ms (" + String.format("%.1f", blocksPerSec) + " blocks/sec)");

        return new BatchDownloadResult(processedCount, hash);
    }

    private List<Long> findAvailableBlocks(
            long startBlockNumber,
            LocalDate currentDay,
            BlockTimeReader blockTimeReader,
            Map<LocalDateTime, List<ListingRecordFile>> filesByBlock) {
        List<Long> availableBlocks = new java.util.ArrayList<>();
        int consecutiveGaps = 0;
        int maxConsecutiveGaps = 5; // Allow up to 5 consecutive missing blocks before stopping
        for (long blockNum = startBlockNumber + 1; ; blockNum++) {
            try {
                LocalDateTime blockTime = blockTimeReader.getBlockLocalDateTime(blockNum);
                if (!blockTime.toLocalDate().equals(currentDay)) {
                    if (availableBlocks.size() < 10) {
                        System.out.println(
                                "[DEBUG] Block " + blockNum + " time " + blockTime + " is on different day than "
                                        + currentDay + " (found " + availableBlocks.size() + " so far)");
                    }
                    break;
                }
                if (filesByBlock.containsKey(blockTime)) {
                    availableBlocks.add(blockNum);
                    consecutiveGaps = 0; // Reset gap counter
                } else {
                    consecutiveGaps++;
                    if (consecutiveGaps >= maxConsecutiveGaps) {
                        if (availableBlocks.size() < 10) {
                            System.out.println("[DEBUG] Block " + blockNum + " time " + blockTime
                                    + " not found in listings after " + maxConsecutiveGaps
                                    + " consecutive gaps (found " + availableBlocks.size() + " so far)");
                        }
                        break;
                    }
                    // Skip gap and continue looking
                }
            } catch (Exception e) {
                if (availableBlocks.size() < 10) {
                    System.out.println("[DEBUG] Block " + blockNum + " not in BlockTimeReader: " + e.getMessage());
                }
                break;
            }
        }
        return availableBlocks;
    }

    private java.util.Set<ListingRecordFile> buildMostCommonFilesSet(
            List<Long> blocksToDownload,
            BlockTimeReader blockTimeReader,
            Map<LocalDateTime, List<ListingRecordFile>> filesByBlock) {
        java.util.Set<ListingRecordFile> mostCommonFiles = new java.util.HashSet<>();
        for (Long blockNum : blocksToDownload) {
            LocalDateTime blockTime = blockTimeReader.getBlockLocalDateTime(blockNum);
            List<ListingRecordFile> group = filesByBlock.get(blockTime);
            if (group != null) {
                ListingRecordFile record = org.hiero.block.tools.records.RecordFileUtils.findMostCommonByType(
                        group, ListingRecordFile.Type.RECORD);
                ListingRecordFile sidecar = org.hiero.block.tools.records.RecordFileUtils.findMostCommonByType(
                        group, ListingRecordFile.Type.RECORD_SIDECAR);
                if (record != null) mostCommonFiles.add(record);
                if (sidecar != null) mostCommonFiles.add(sidecar);
            }
        }
        return mostCommonFiles;
    }

    private java.util.concurrent.LinkedBlockingDeque<BlockWork> queueBlockDownloads(
            List<Long> blocksToDownload,
            BlockTimeReader blockTimeReader,
            Map<LocalDateTime, List<ListingRecordFile>> filesByBlock,
            ConcurrentDownloadManagerVirtualThreadsV3 downloadManager,
            String modeLabel) {
        System.out.println("[" + modeLabel + "] Phase 1: Queuing downloads for all blocks...");
        java.util.concurrent.LinkedBlockingDeque<BlockWork> pending = new java.util.concurrent.LinkedBlockingDeque<>();

        for (Long blockNum : blocksToDownload) {
            LocalDateTime blockTime = blockTimeReader.getBlockLocalDateTime(blockNum);
            List<ListingRecordFile> group = filesByBlock.get(blockTime);

            if (group == null || group.isEmpty()) {
                System.err.println("[" + modeLabel + "] WARNING: No files for block " + blockNum);
                continue;
            }

            ListingRecordFile mostCommonRecord = org.hiero.block.tools.records.RecordFileUtils.findMostCommonByType(
                    group, ListingRecordFile.Type.RECORD);
            ListingRecordFile[] mostCommonSidecars =
                    org.hiero.block.tools.records.RecordFileUtils.findMostCommonSidecars(group);
            List<ListingRecordFile> orderedFiles =
                    DownloadDayLiveImpl.computeFilesToDownload(mostCommonRecord, mostCommonSidecars, group);

            BlockWork bw = new BlockWork(blockNum, blockTime, orderedFiles);
            for (ListingRecordFile lr : orderedFiles) {
                String blobName = DownloadConstants.BUCKET_PATH_PREFIX + lr.path();
                bw.futures.add(downloadManager.downloadAsync(DownloadConstants.BUCKET_NAME, blobName));
            }
            pending.add(bw);
        }

        System.out.println("[" + modeLabel + "] Queued " + pending.size() + " blocks for download");
        return pending;
    }

    private byte[] processQueuedDownloads(
            java.util.concurrent.LinkedBlockingDeque<BlockWork> pending,
            byte[] currentHash,
            java.util.Set<ListingRecordFile> mostCommonFiles,
            AddressBookRegistry addressBookRegistry,
            SignatureStats stats,
            ConcurrentTarZstdWriter writer,
            int totalBlocks,
            long downloadStartTime,
            String modeLabel)
            throws Exception {
        System.out.println("[" + modeLabel + "] Phase 2: Processing downloads...");
        byte[] hash = currentHash;
        int processedCount = 0;

        while (!pending.isEmpty()) {
            BlockWork ready = pending.pollFirst();
            if (ready == null) continue;

            hash = processSingleBlock(ready, hash, mostCommonFiles, addressBookRegistry, stats, writer, modeLabel);
            processedCount++;

            if (processedCount % PROGRESS_LOG_INTERVAL == 0) {
                long elapsed = System.currentTimeMillis() - downloadStartTime;
                double blocksPerSec = processedCount / (elapsed / 1000.0);
                System.out.println("[" + modeLabel + "] Processed " + processedCount + "/" + totalBlocks + " blocks ("
                        + String.format("%.1f", blocksPerSec) + " blocks/sec)");
            }
        }
        return hash;
    }

    private byte[] processSingleBlock(
            BlockWork ready,
            byte[] hash,
            java.util.Set<ListingRecordFile> mostCommonFiles,
            AddressBookRegistry addressBookRegistry,
            SignatureStats stats,
            ConcurrentTarZstdWriter writer,
            String modeLabel)
            throws Exception {
        try {
            java.util.concurrent.CompletableFuture.allOf(
                            ready.futures.toArray(new java.util.concurrent.CompletableFuture[0]))
                    .join();

            List<InMemoryFile> inMemoryFiles = convertDownloadedFiles(ready, mostCommonFiles, modeLabel);
            byte[] newHash = DownloadDayLiveImpl.validateBlockHashes(ready.blockNumber, inMemoryFiles, hash, null);

            DownloadDayLiveImpl.BlockDownloadResult result =
                    new DownloadDayLiveImpl.BlockDownloadResult(ready.blockNumber, inMemoryFiles, newHash);
            Instant recordFileTime = ready.blockTime.atZone(ZoneOffset.UTC).toInstant();
            boolean valid = fullBlockValidate(addressBookRegistry, hash, recordFileTime, result, null);

            if (!valid) {
                System.err.println(
                        "[" + modeLabel + "] WARNING: Full block validation failed for block " + ready.blockNumber);
            }

            int nodeCount = addressBookRegistry
                    .getAddressBookForBlock(recordFileTime)
                    .nodeAddress()
                    .size();
            stats.recordBlock(result.files, nodeCount);

            for (InMemoryFile file : inMemoryFiles) {
                writer.putEntry(file);
            }
            return newHash;

        } catch (java.util.concurrent.CompletionException ce) {
            System.err.println(
                    "[" + modeLabel + "] Download failed for block " + ready.blockNumber + ": " + ce.getMessage());
            throw new IllegalStateException("Download failed for block " + ready.blockNumber, ce.getCause());
        }
    }

    private List<InMemoryFile> convertDownloadedFiles(
            BlockWork ready, java.util.Set<ListingRecordFile> mostCommonFiles, String modeLabel) throws IOException {
        List<InMemoryFile> inMemoryFiles = new java.util.ArrayList<>();
        for (int i = 0; i < ready.orderedFiles.size(); i++) {
            ListingRecordFile lr = ready.orderedFiles.get(i);
            String filename = lr.path().substring(lr.path().lastIndexOf('/') + 1);

            try {
                InMemoryFile downloadedFile = ready.futures.get(i).join();

                boolean md5Valid = checkMd5(lr.md5Hex(), downloadedFile.data());
                if (!md5Valid) {
                    System.err.println("[" + modeLabel + "] MD5 mismatch for " + lr.path() + ", skipping");
                    continue;
                }

                byte[] contentBytes = downloadedFile.data();
                if (filename.endsWith(".gz")) {
                    contentBytes = org.hiero.block.tools.utils.Gzip.ungzipInMemory(contentBytes);
                    filename = filename.replaceAll("\\.gz$", "");
                }

                Path newFilePath = DownloadDayLiveImpl.computeNewFilePath(lr, mostCommonFiles, filename);
                inMemoryFiles.add(new InMemoryFile(newFilePath, contentBytes));
            } catch (java.io.EOFException eofe) {
                System.err.println("[" + modeLabel + "] Skipping corrupted file: " + filename);
            } catch (Exception e) {
                System.err.println("[" + modeLabel + "] Error processing file " + filename + ": " + e.getMessage());
            }
        }
        return inMemoryFiles;
    }

    /**
     * Main block processing loop.
     */
    private void processBlocks(
            State initialState,
            AddressBookRegistry addressBookRegistry,
            ConcurrentDownloadManagerVirtualThreadsV3 downloadManager,
            BlockTimeReader initialBlockTimeReader,
            SignatureStats stats)
            throws Exception {

        long currentBlockNumber = initialState.blockNumber;
        byte[] currentHash = initialState.getHashBytes();
        LocalDate currentDay = initialState.getDayDate();
        BlockTimeReader blockTimeReader = initialBlockTimeReader;

        ConcurrentTarZstdWriter currentDayWriter = null;
        long blocksProcessedTotal = 0;
        long blocksProcessedToday = 0;
        long dayStartTime = System.currentTimeMillis();

        // Cache for listing files - reload only when day changes
        List<ListingRecordFile> cachedListingFiles = null;
        LocalDate cachedListingDay = null;

        // Counter for stale timestamp retries to prevent infinite loops
        int staleTimestampRetries = 0;
        long lastStaleBlock = -1;

        // Track last block time refresh to avoid excessive mirror node queries
        long lastBlockTimeRefreshMs = 0;

        try {
            while (true) {
                long nextBlockNumber = currentBlockNumber + 1;

                // Get block timestamp
                LocalDateTime blockTime;
                try {
                    blockTime = blockTimeReader.getBlockLocalDateTime(nextBlockNumber);
                } catch (Exception e) {
                    // Block not in BlockTimeReader yet - we're at the live edge
                    // Refresh block times if enough time has passed since last refresh
                    long now = System.currentTimeMillis();
                    if (now - lastBlockTimeRefreshMs >= MIN_BLOCK_TIME_REFRESH_INTERVAL_MS) {
                        System.out.println(
                                "[LIVE] Block " + nextBlockNumber + " not in BlockTimeReader, refreshing...");
                        UpdateBlockData.updateMirrorNodeData(
                                MetadataFiles.BLOCK_TIMES_FILE, MetadataFiles.DAY_BLOCKS_FILE);
                        blockTimeReader = new BlockTimeReader(MetadataFiles.BLOCK_TIMES_FILE);
                        lastBlockTimeRefreshMs = now;
                    } else {
                        System.out.println(
                                "[LIVE] Block " + nextBlockNumber + " not in BlockTimeReader yet, waiting...");
                    }
                    Thread.sleep(LIVE_POLL_INTERVAL.toMillis());
                    continue;
                }

                LocalDate blockDay = blockTime.toLocalDate();

                // Sanity check: If blockDay is BEFORE currentDay, we have stale/corrupted block time data
                // This should never happen at the live edge - newer blocks should have newer timestamps
                if (blockDay.isBefore(currentDay)) {
                    // Reset retry counter if it's a different block
                    if (nextBlockNumber != lastStaleBlock) {
                        staleTimestampRetries = 0;
                        lastStaleBlock = nextBlockNumber;
                    }
                    staleTimestampRetries++;

                    System.out.println("[download-live2] WARNING: Block " + nextBlockNumber + " has timestamp on "
                            + blockDay + " but we're on " + currentDay + ". (attempt " + staleTimestampRetries + ")");

                    if (staleTimestampRetries <= 3) {
                        // Try refreshing block times from mirror node
                        System.out.println("[download-live2] Refreshing block times from mirror node...");
                        UpdateBlockData.updateMirrorNodeData(
                                MetadataFiles.BLOCK_TIMES_FILE, MetadataFiles.DAY_BLOCKS_FILE);
                        blockTimeReader = new BlockTimeReader(MetadataFiles.BLOCK_TIMES_FILE);
                        continue; // Retry with fresh data
                    } else {
                        // After 3 retries, try fixBlockTime to query individual block timestamps
                        System.out.println(
                                "[download-live2] Trying fixBlockTime to correct individual block entries...");
                        FixBlockTime.fixBlockTimeRange(
                                MetadataFiles.BLOCK_TIMES_FILE, nextBlockNumber, nextBlockNumber + 100);
                        blockTimeReader = new BlockTimeReader(MetadataFiles.BLOCK_TIMES_FILE);
                        staleTimestampRetries = 0; // Reset after fix attempt
                        continue;
                    }
                } else {
                    // Reset stale timestamp counters when we have valid data
                    staleTimestampRetries = 0;
                    lastStaleBlock = -1;
                }

                // Handle day change
                if (!blockDay.equals(currentDay)) {
                    // Finalize previous day stats and write to file
                    if (blocksProcessedToday > 0) {
                        stats.finalizeDayStats();
                        stats.writeProgress();
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
                    int year = blockTime.getYear();
                    int month = blockTime.getMonthValue();
                    int day = blockTime.getDayOfMonth();

                    // Retry loop for live edge - listings may not be available yet
                    // Wait 15 minutes between attempts, retry indefinitely
                    int attempt = 0;
                    while (true) {
                        attempt++;
                        refreshListingsForDay(blockDay);
                        try {
                            cachedListingFiles =
                                    DayListingFileReader.loadRecordsFileForDay(listingDir.toPath(), year, month, day);
                            cachedListingDay = blockDay;
                            System.out.println("[download-live2] Loaded " + cachedListingFiles.size()
                                    + " listing entries for " + blockDay);
                            break;
                        } catch (java.nio.file.NoSuchFileException e) {
                            System.out.println("[download-live2] Listings not available yet for " + blockDay
                                    + ", waiting 15 minutes... (attempt " + attempt + ")");
                            Thread.sleep(15 * 60 * 1000); // 15 minutes
                        }
                    }
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

                // Update block times before batch mode to ensure we have current timestamps
                System.out.println("[download-live2] Updating block times from mirror node...");
                UpdateBlockData.updateMirrorNodeData(MetadataFiles.BLOCK_TIMES_FILE, MetadataFiles.DAY_BLOCKS_FILE);
                blockTimeReader = new BlockTimeReader(MetadataFiles.BLOCK_TIMES_FILE);

                // Sanity check: verify next block's timestamp is on expected day or later
                // If timestamp goes backwards (e.g., 2026-01-16 when we're on 2026-02-01), data is corrupt
                try {
                    long nextBlock = currentBlockNumber + 1;
                    LocalDateTime nextBlockTime = blockTimeReader.getBlockLocalDateTime(nextBlock);
                    if (nextBlockTime.toLocalDate().isBefore(blockDay)) {
                        System.out.println("[download-live2] Detected stale block times (block " + nextBlock
                                + " shows " + nextBlockTime.toLocalDate() + " but expected " + blockDay
                                + "), running fixBlockTime...");
                        long endBlock = currentBlockNumber + BATCH_SIZE;
                        FixBlockTime.fixBlockTimeRange(MetadataFiles.BLOCK_TIMES_FILE, nextBlock, endBlock);
                        blockTimeReader = new BlockTimeReader(MetadataFiles.BLOCK_TIMES_FILE);
                    }
                } catch (Exception e) {
                    // Block time not available yet - that's fine, will be handled by download logic
                }

                // Try batch download - if hash mismatch, fix block times and retry
                BatchDownloadResult batchResult = null;
                try {
                    BatchDownloadContext batchContext = new BatchDownloadContext(
                            downloadManager, blockTimeReader, addressBookRegistry, stats, currentDayWriter);
                    batchResult = downloadCurrentDayBlocksBatch(
                            currentBlockNumber,
                            blockDay,
                            currentHash,
                            cachedListingFiles,
                            batchContext,
                            new BatchDownloadConfig(BATCH_SIZE, BATCH_SIZE, "BATCH"));
                } catch (Exception e) {
                    if (e.getMessage() != null && e.getMessage().contains("hash mismatch")) {
                        // Hash mismatch detected - fix block times and retry
                        System.out.println("[download-live2] Hash mismatch detected, fixing block times...");
                        long estimatedEndBlock = currentBlockNumber + BATCH_SIZE + 100;
                        FixBlockTime.fixBlockTimeRange(
                                MetadataFiles.BLOCK_TIMES_FILE, currentBlockNumber + 1, estimatedEndBlock);
                        blockTimeReader = new BlockTimeReader(MetadataFiles.BLOCK_TIMES_FILE);

                        // Retry the batch
                        System.out.println("[download-live2] Retrying batch after fix...");
                        BatchDownloadContext retryContext = new BatchDownloadContext(
                                downloadManager, blockTimeReader, addressBookRegistry, stats, currentDayWriter);
                        batchResult = downloadCurrentDayBlocksBatch(
                                currentBlockNumber,
                                blockDay,
                                currentHash,
                                cachedListingFiles,
                                retryContext,
                                new BatchDownloadConfig(BATCH_SIZE, BATCH_SIZE, "BATCH"));
                    } else {
                        throw e; // Re-throw non-hash-mismatch errors
                    }
                }

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

                    // Refresh listings to check for more blocks
                    refreshListingsForDay(blockDay);
                    cachedListingFiles = DayListingFileReader.loadRecordsFileForDay(
                            listingDir.toPath(),
                            blockTime.getYear(),
                            blockTime.getMonthValue(),
                            blockTime.getDayOfMonth());
                    continue;
                }

                // Try live batch mode (100-999 blocks available -> download 100)
                // Wait until we have at least 100 blocks to avoid small/single block downloads
                BatchDownloadContext liveContext = new BatchDownloadContext(
                        downloadManager, blockTimeReader, addressBookRegistry, stats, currentDayWriter);
                BatchDownloadResult liveResult = downloadCurrentDayBlocksBatch(
                        currentBlockNumber,
                        blockDay,
                        currentHash,
                        cachedListingFiles,
                        liveContext,
                        new BatchDownloadConfig(LIVE_BATCH_SIZE, LIVE_BATCH_SIZE, "LIVE"));

                if (liveResult != null && liveResult.blocksDownloaded > 0) {
                    // Live batch download succeeded - update state
                    long lastDownloadedBlock = currentBlockNumber + liveResult.blocksDownloaded;
                    LocalDateTime lastBlockTime = blockTimeReader.getBlockLocalDateTime(lastDownloadedBlock);
                    Instant lastRecordFileTime =
                            lastBlockTime.atZone(ZoneOffset.UTC).toInstant();

                    currentBlockNumber = lastDownloadedBlock;
                    currentHash = liveResult.finalHash;
                    blocksProcessedTotal += liveResult.blocksDownloaded;
                    blocksProcessedToday += liveResult.blocksDownloaded;

                    // Save state after live batch
                    saveState(new State(currentBlockNumber, currentHash, lastRecordFileTime, currentDay));

                    // Refresh listings to check for more blocks
                    refreshListingsForDay(blockDay);
                    cachedListingFiles = DayListingFileReader.loadRecordsFileForDay(
                            listingDir.toPath(),
                            blockTime.getYear(),
                            blockTime.getMonthValue(),
                            blockTime.getDayOfMonth());
                    continue;
                }

                // Less than 100 blocks available - we're at the live edge
                // Check if we have block times ahead but no GCS files (GCS is behind)
                boolean hasBlockTimesAhead = false;
                try {
                    blockTimeReader.getBlockLocalDateTime(currentBlockNumber + 1);
                    hasBlockTimesAhead = true;
                } catch (Exception e) {
                    // No block time for next block
                }

                long now = System.currentTimeMillis();
                long timeSinceLastRefresh = now - lastBlockTimeRefreshMs;
                long waitTime = MIN_BLOCK_TIME_REFRESH_INTERVAL_MS - timeSinceLastRefresh;

                if (waitTime > 0) {
                    if (hasBlockTimesAhead) {
                        System.out.println("[LIVE] GCS listings behind, waiting " + (waitTime / 1000)
                                + "s for next refresh (current: " + currentBlockNumber + ")...");
                    } else {
                        System.out.println("[LIVE] Waiting " + (waitTime / 1000) + "s for next refresh (current: "
                                + currentBlockNumber + ")...");
                    }
                    Thread.sleep(waitTime);
                }

                now = System.currentTimeMillis();
                if (hasBlockTimesAhead) {
                    // We have block times but no GCS files - only refresh listings, skip mirror node
                    // Skip historical days at live edge - only refresh today
                    System.out.println("[LIVE] Refreshing GCS listings only (block times already available)...");
                    refreshListingsForDay(blockDay, false);
                    cachedListingFiles = DayListingFileReader.loadRecordsFileForDay(
                            listingDir.toPath(),
                            blockTime.getYear(),
                            blockTime.getMonthValue(),
                            blockTime.getDayOfMonth());
                    lastBlockTimeRefreshMs = now;
                } else {
                    // No block times ahead - need to refresh from mirror node
                    System.out.println("[LIVE] Refreshing block times from mirror node...");
                    UpdateBlockData.updateMirrorNodeData(MetadataFiles.BLOCK_TIMES_FILE, MetadataFiles.DAY_BLOCKS_FILE);
                    blockTimeReader = new BlockTimeReader(MetadataFiles.BLOCK_TIMES_FILE);
                    lastBlockTimeRefreshMs = now;

                    // Refresh listings in case new files appeared - skip historical days at live edge
                    refreshListingsForDay(blockDay, false);
                    cachedListingFiles = DayListingFileReader.loadRecordsFileForDay(
                            listingDir.toPath(),
                            blockTime.getYear(),
                            blockTime.getMonthValue(),
                            blockTime.getDayOfMonth());
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("[download-live2] Interrupted, saving state...");
        } finally {
            // Save final state and stats
            if (currentHash != null) {
                LocalDateTime finalBlockTime = blockTimeReader.getBlockLocalDateTime(currentBlockNumber);
                Instant finalRecordTime = finalBlockTime.atZone(ZoneOffset.UTC).toInstant();
                saveState(new State(currentBlockNumber, currentHash, finalRecordTime, currentDay));
                System.out.println("[download-live2] Saved state at block " + currentBlockNumber);
            }
            // Write stats on exit (captures partial day progress)
            stats.writeProgress();
            System.out.println("[download-live2] Saved stats progress");
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
     * Refreshes GCS listings for a specific day.
     * First updates all historical days (up to yesterday) via updateDayListings,
     * then fetches listings for the specific target day (including today).
     */
    private void refreshListingsForDay(LocalDate day) {
        refreshListingsForDay(day, true);
    }

    private void refreshListingsForDay(LocalDate day, boolean includeHistoricalDays) {
        LocalDate today = LocalDate.now(ZoneOffset.UTC);
        boolean isToday = day.equals(today);

        // Update historical days only if requested (skip at live edge for efficiency)
        if (includeHistoricalDays) {
            UpdateDayListingsCommand.updateDayListings(
                    listingDir.toPath(),
                    CACHE_DIR.toPath(),
                    true, // cacheEnabled for historical days
                    MIN_NODE_ACCOUNT_ID,
                    MAX_NODE_ACCOUNT_ID,
                    DownloadConstants.GCP_PROJECT_ID);
        }

        // For today's date, disable cache to get fresh listings from GCS
        UpdateDayListingsCommand.updateListingsForSingleDay(
                listingDir.toPath(),
                CACHE_DIR.toPath(),
                !isToday, // disable cache for today, enable for past days
                MIN_NODE_ACCOUNT_ID,
                MAX_NODE_ACCOUNT_ID,
                DownloadConstants.GCP_PROJECT_ID,
                day);
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
