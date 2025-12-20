// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.subcommands;

import static org.hiero.block.tools.days.downloadlive.ValidateDownloadLive.fullBlockValidate;

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
import org.hiero.block.tools.days.download.DownloadConstants;
import org.hiero.block.tools.days.download.DownloadDayLiveImpl;
import org.hiero.block.tools.days.listing.DayListingFileReader;
import org.hiero.block.tools.days.listing.ListingRecordFile;
import org.hiero.block.tools.days.model.AddressBookRegistry;
import org.hiero.block.tools.mirrornode.BlockTimeReader;
import org.hiero.block.tools.records.model.unparsed.InMemoryFile;
import org.hiero.block.tools.utils.ConcurrentTarZstdWriter;
import org.hiero.block.tools.utils.gcp.ConcurrentDownloadManagerVirtualThreads;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Hash-driven live download command that validates blocks inline as they're downloaded.
 *
 * <p>Key features:
 * <ul>
 *   <li>Uses block number + BlockTimeReader to discover next blocks</li>
 *   <li>Validates hash chain and performs full block validation inline</li>
 *   <li>Retry with exponential backoff on validation failures</li>
 *   <li>Real-time tar append with day rollover</li>
 *   <li>Adaptive catch-up vs live mode</li>
 * </ul>
 */
@Command(
        name = "download-live2",
        description = "Hash-driven live block download with inline validation",
        mixinStandardHelpOptions = true)
public class DownloadLive2 implements Runnable {

    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
    private static final Duration LIVE_POLL_INTERVAL = Duration.ofSeconds(1);
    private static final int MAX_VALIDATION_RETRIES = 3;
    private static final int CATCH_UP_CHECK_INTERVAL = 10; // check mode every N blocks

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
            description = "Path to state JSON file for resume (default: state/download-live2.json)")
    private Path stateJsonPath = Path.of("state/download-live2.json");

    @Option(
            names = {"--start-date"},
            description = "Start from this date (YYYY-MM-DD), ignored if state file exists")
    private String startDate;

    @Option(
            names = {"--start-block"},
            description = "Start from this block number, ignored if state file exists or start-date is set")
    private Long startBlock;

    @Option(
            names = {"--address-book"},
            description = "Path to address book file for signature validation")
    private Path addressBookPath;

    @Option(
            names = {"--max-concurrency"},
            description = "Maximum concurrent downloads (default: 32)")
    private int maxConcurrency = 32;

    @Option(
            names = {"--skip-validation-during-catchup"},
            description = "Skip expensive signature validation during catch-up for faster processing (default: false)")
    private boolean skipValidationDuringCatchup = false;

    @Option(
            names = {"--catchup-threshold-seconds"},
            description = "Seconds behind current time to be considered 'caught up' (default: 60)")
    private long catchupThresholdSeconds = 60;

    @Option(
            names = {"--tmp-dir"},
            description = "Temporary directory for downloads (default: tmp)")
    private File tmpDir = new File("tmp");

    /**
     * State persisted to JSON for resumability
     */
    private static class State {
        long blockNumber;
        String blockHash; // hex
        String recordFileTime; // ISO-8601
        String dayDate; // YYYY-MM-DD

        State() {}

        State(long blockNumber, byte[] blockHash, Instant recordFileTime, LocalDate dayDate) {
            this.blockNumber = blockNumber;
            this.blockHash = HexFormat.of().formatHex(blockHash);
            this.recordFileTime = recordFileTime.toString();
            this.dayDate = dayDate.toString();
        }

        byte[] getHashBytes() {
            return blockHash != null ? HexFormat.of().parseHex(blockHash) : null;
        }

        LocalDate getDayDate() {
            return dayDate != null ? LocalDate.parse(dayDate) : null;
        }

        Instant getRecordFileTime() {
            return recordFileTime != null ? Instant.parse(recordFileTime) : null;
        }
    }

    @Override
    public void run() {
        System.out.println("[download-live2] Starting hash-driven live download");
        System.out.println("Configuration:");
        System.out.println("  listingDir=" + listingDir);
        System.out.println("  outputDir=" + outputDir);
        System.out.println("  stateJsonPath=" + stateJsonPath);
        System.out.println("  startDate=" + startDate);
        System.out.println("  startBlock=" + startBlock);
        System.out.println("  addressBookPath=" + addressBookPath);
        System.out.println("  maxConcurrency=" + maxConcurrency);
        System.out.println("  skipValidationDuringCatchup=" + skipValidationDuringCatchup);
        System.out.println("  catchupThresholdSeconds=" + catchupThresholdSeconds);

        try {
            // Create directories
            Files.createDirectories(outputDir.toPath());
            Files.createDirectories(tmpDir.toPath());
            if (stateJsonPath.getParent() != null) {
                Files.createDirectories(stateJsonPath.getParent());
            }

            // Initialize components
            final AddressBookRegistry addressBookRegistry =
                    addressBookPath != null ? new AddressBookRegistry(addressBookPath) : new AddressBookRegistry();

            final Storage storage = StorageOptions.grpc()
                    .setAttemptDirectPath(false)
                    .setProjectId(DownloadConstants.GCP_PROJECT_ID)
                    .build()
                    .getService();

            final ConcurrentDownloadManagerVirtualThreads downloadManager =
                    ConcurrentDownloadManagerVirtualThreads.newBuilder(storage)
                            .setMaxConcurrency(maxConcurrency)
                            .build();

            final BlockTimeReader blockTimeReader = new BlockTimeReader();

            // Determine starting point
            final State initialState = determineStartingPoint(blockTimeReader);
            System.out.println("[download-live2] Starting from block " + initialState.blockNumber + " hash["
                    + (initialState.blockHash != null ? initialState.blockHash.substring(0, 8) : "null") + "]");

            // Main processing loop
            processBlocks(initialState, addressBookRegistry, downloadManager, blockTimeReader);

        } catch (Exception e) {
            System.err.println("[download-live2] Fatal error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private State determineStartingPoint(BlockTimeReader blockTimeReader) throws IOException {
        // Priority 1: Resume from state file
        if (Files.exists(stateJsonPath)) {
            try {
                String json = Files.readString(stateJsonPath, StandardCharsets.UTF_8);
                State state = GSON.fromJson(json, State.class);
                System.out.println("[download-live2] Resuming from state file: block " + state.blockNumber);
                return state;
            } catch (Exception e) {
                System.err.println("[download-live2] Warning: Failed to read state file: " + e.getMessage());
            }
        }

        // Priority 2: Start from date
        if (startDate != null) {
            try {
                LocalDate date = LocalDate.parse(startDate);
                LocalDateTime startTime = date.atStartOfDay();
                long blockNumber = blockTimeReader.getNearestBlockAfterTime(startTime);
                System.out.println("[download-live2] Starting from date " + startDate + " → block " + blockNumber);
                State state = new State();
                state.blockNumber = blockNumber;
                state.dayDate = date.toString();
                return state;
            } catch (Exception e) {
                throw new RuntimeException("Failed to parse start date: " + startDate, e);
            }
        }

        // Priority 3: Start from block number
        if (startBlock != null) {
            System.out.println("[download-live2] Starting from explicit block number: " + startBlock);
            State state = new State();
            state.blockNumber = startBlock;
            return state;
        }

        throw new RuntimeException("No starting point specified. Use --state-json, --start-date, or --start-block");
    }

    private void processBlocks(
            State initialState,
            AddressBookRegistry addressBookRegistry,
            ConcurrentDownloadManagerVirtualThreads downloadManager,
            BlockTimeReader blockTimeReader)
            throws Exception {

        long currentBlockNumber = initialState.blockNumber;
        byte[] currentHash = initialState.getHashBytes();
        LocalDate currentDay = initialState.getDayDate();

        ConcurrentTarZstdWriter currentDayWriter = null;
        long blocksProcessedInCatchUp = 0;
        long catchUpStartTime = System.currentTimeMillis();

        // Cache for listing files - reload only when day changes
        List<ListingRecordFile> cachedListingFiles = null;
        LocalDate cachedListingDay = null;

        try {
            while (true) {
                long nextBlockNumber = currentBlockNumber + 1;

                // Get block timestamp
                LocalDateTime blockTime = blockTimeReader.getBlockLocalDateTime(nextBlockNumber);
                LocalDate blockDay = blockTime.toLocalDate();

                // Handle day change: refresh listings, cache files, and initialize/rollover writer
                if (!blockDay.equals(currentDay) || currentDayWriter == null) {
                    // Refresh GCS listings and load cached files for new day
                    if (!blockDay.equals(cachedListingDay)) {
                        refreshListingsForDay(blockDay);
                        int year = blockTime.getYear();
                        int month = blockTime.getMonthValue();
                        int day = blockTime.getDayOfMonth();
                        cachedListingFiles =
                                DayListingFileReader.loadRecordsFileForDay(listingDir.toPath(), year, month, day);
                        cachedListingDay = blockDay;
                        System.out.println("[download-live2] Loaded " + cachedListingFiles.size()
                                + " listing entries for " + blockDay);
                    }

                    // Handle tar writer rollover
                    if (!blockDay.equals(currentDay)) {
                        if (currentDayWriter != null) {
                            currentDayWriter.close();
                            System.out.println("[download-live2] Day completed: " + currentDay);
                        }
                        currentDay = blockDay;
                        Path dayArchive = outputDir.toPath().resolve(blockDay + ".tar.zstd");
                        currentDayWriter = new ConcurrentTarZstdWriter(dayArchive);
                        System.out.println("[download-live2] Started new day: " + currentDay);
                    } else if (currentDayWriter == null) {
                        // Initialize writer for first block
                        currentDay = blockDay;
                        Path dayArchive = outputDir.toPath().resolve(blockDay + ".tar.zstd");
                        currentDayWriter = new ConcurrentTarZstdWriter(dayArchive);
                        System.out.println("[download-live2] Started new day: " + currentDay);
                    }
                }

                // Determine if we should skip validation (during catch-up mode)
                boolean shouldSkipValidation = false;
                if (skipValidationDuringCatchup) {
                    LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
                    Duration timeLag = Duration.between(blockTime, now);
                    long secondsBehind = timeLag.getSeconds();

                    if (secondsBehind > catchupThresholdSeconds) {
                        shouldSkipValidation = true;
                        if (blocksProcessedInCatchUp % 100 == 0) {
                            System.out.println("[CATCH-UP] Skipping validation for block " + nextBlockNumber + " ("
                                    + (secondsBehind / 60) + "m behind)");
                        }
                    } else if (blocksProcessedInCatchUp > 0) {
                        System.out.println("[CATCH-UP] Caught up! Re-enabling validation for block " + nextBlockNumber);
                    }
                }

                // Try to download block with retry (passing skip flag)
                DownloadDayLiveImpl.BlockDownloadResult result = downloadBlockWithRetry(
                        nextBlockNumber,
                        blockTime,
                        currentHash,
                        cachedListingFiles,
                        downloadManager,
                        blockTimeReader,
                        shouldSkipValidation);

                if (result == null) {
                    // Block not available yet - we're caught up to live
                    System.out.println("[LIVE] Waiting for block " + nextBlockNumber + "...");
                    Thread.sleep(LIVE_POLL_INTERVAL.toMillis());
                    continue;
                }

                // Convert block time to Instant for state saving
                Instant recordFileTime = blockTime.atZone(ZoneOffset.UTC).toInstant();

                // Perform full block validation inline (if not skipped)
                if (!shouldSkipValidation) {
                    boolean valid = fullBlockValidate(addressBookRegistry, currentHash, recordFileTime, result, null);

                    if (!valid) {
                        throw new RuntimeException("Full block validation failed for block " + nextBlockNumber);
                    }
                }

                // Write block to archive
                for (InMemoryFile file : result.files) {
                    currentDayWriter.putEntry(file);
                }

                // Update state
                currentBlockNumber = nextBlockNumber;
                currentHash = result.newPreviousRecordFileHash;
                blocksProcessedInCatchUp++;

                // Save state periodically
                if (currentBlockNumber % 100 == 0) {
                    saveState(new State(currentBlockNumber, currentHash, recordFileTime, blockDay));
                }

                // Check if we're catching up or live
                if (blocksProcessedInCatchUp % CATCH_UP_CHECK_INTERVAL == 0) {
                    long elapsed = System.currentTimeMillis() - catchUpStartTime;
                    double blocksPerSec = blocksProcessedInCatchUp / (elapsed / 1000.0);

                    // Calculate ETA to catch up
                    String etaString = calculateCatchUpEta(currentBlockNumber, blockTime, blocksPerSec);

                    System.out.printf(
                            "[CATCH-UP] Block %d, %.1f blocks/sec%s%n", currentBlockNumber, blocksPerSec, etaString);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("[download-live2] Interrupted");
        } finally {
            // Save final state
            if (currentHash != null) {
                LocalDateTime finalBlockTime = blockTimeReader.getBlockLocalDateTime(currentBlockNumber);
                Instant finalRecordTime = finalBlockTime.atZone(ZoneOffset.UTC).toInstant();
                saveState(new State(currentBlockNumber, currentHash, finalRecordTime, currentDay));
            }
            if (currentDayWriter != null) {
                try {
                    currentDayWriter.close();
                } catch (Exception e) {
                    System.err.println("[download-live2] Error closing writer: " + e.getMessage());
                }
            }
        }
    }

    private DownloadDayLiveImpl.BlockDownloadResult downloadBlockWithRetry(
            long blockNumber,
            LocalDateTime blockTime,
            byte[] previousHash,
            List<ListingRecordFile> cachedListingFiles,
            ConcurrentDownloadManagerVirtualThreads downloadManager,
            BlockTimeReader blockTimeReader,
            boolean skipHashValidation)
            throws IOException {

        Exception lastException = null;

        for (int attempt = 1; attempt <= MAX_VALIDATION_RETRIES; attempt++) {
            try {
                // Try to download the block
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
                DownloadDayLiveImpl.BlockDownloadResult result = DownloadDayLiveImpl.downloadSingleBlockForLive(
                        downloadManager,
                        null, // dayBlockInfo not needed for live mode
                        blockTimeReader,
                        listingDir.toPath(),
                        year,
                        month,
                        day,
                        blockNumber,
                        previousHash,
                        skipHashValidation);

                // Validate hash chain
                byte[] blockHash = result.newPreviousRecordFileHash;
                if (previousHash != null) {
                    // Check that the block's previous hash matches what we expect
                    byte[] blockPreviousHash = result.files
                            .get(0)
                            .data(); // This is simplified - actual validation happens in DownloadDayUtil
                    // The actual validation is done in DownloadDayLiveImpl.downloadSingleBlockForLive via
                    // validateBlockHashes
                }

                return result;

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

    private void refreshListingsForDay(LocalDate day) {
        System.out.println("[download-live2] Refreshing GCS listings for " + day);
        UpdateDayListingsCommand.updateDayListings(
                listingDir.toPath(),
                CACHE_DIR.toPath(),
                true, // forceRefresh
                MIN_NODE_ACCOUNT_ID,
                MAX_NODE_ACCOUNT_ID,
                DownloadConstants.GCP_PROJECT_ID);
    }

    private void saveState(State state) {
        try {
            // Create parent directory if it doesn't exist
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
     * Calculates ETA to catch up to current time based on block processing speed.
     *
     * @param currentBlockNumber the block number we're currently at
     * @param currentBlockTime the timestamp of the current block
     * @param blocksPerSec the current processing speed
     * @return formatted ETA string, or empty if we're caught up
     */
    private String calculateCatchUpEta(long currentBlockNumber, LocalDateTime currentBlockTime, double blocksPerSec) {
        try {
            // Get current wall clock time
            LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);

            // Calculate time lag
            Duration timeLag = Duration.between(currentBlockTime, now);
            long secondsBehind = timeLag.getSeconds();

            // If we're less than 10 seconds behind, we're essentially caught up
            if (secondsBehind < 10) {
                return " - CAUGHT UP";
            }

            // Estimate blocks behind (assuming ~2 second block time on average)
            // This is approximate but good enough for ETA
            double avgBlockTime = 2.0; // seconds per block
            long estimatedBlocksBehind = (long) (secondsBehind / avgBlockTime);

            // Calculate ETA
            if (blocksPerSec > 0) {
                double secondsToCompletion = estimatedBlocksBehind / blocksPerSec;
                String eta = formatDuration((long) secondsToCompletion);
                return String.format(" - %s behind, ETA: %s", formatTimeLag(secondsBehind), eta);
            } else {
                return String.format(" - %s behind", formatTimeLag(secondsBehind));
            }
        } catch (Exception e) {
            return ""; // Silently ignore errors in ETA calculation
        }
    }

    /**
     * Formats a duration in seconds into human-readable format.
     */
    private String formatDuration(long seconds) {
        if (seconds < 60) {
            return seconds + "s";
        } else if (seconds < 3600) {
            long minutes = seconds / 60;
            long secs = seconds % 60;
            return String.format("%dm %ds", minutes, secs);
        } else if (seconds < 86400) {
            long hours = seconds / 3600;
            long minutes = (seconds % 3600) / 60;
            return String.format("%dh %dm", hours, minutes);
        } else {
            long days = seconds / 86400;
            long hours = (seconds % 86400) / 3600;
            return String.format("%dd %dh", days, hours);
        }
    }

    /**
     * Formats time lag in seconds into human-readable format.
     */
    private String formatTimeLag(long seconds) {
        if (seconds < 60) {
            return seconds + "s";
        } else if (seconds < 3600) {
            long minutes = seconds / 60;
            return String.format("%dm", minutes);
        } else if (seconds < 86400) {
            long hours = seconds / 3600;
            long minutes = (seconds % 3600) / 60;
            return String.format("%dh %dm", hours, minutes);
        } else {
            long days = seconds / 86400;
            long hours = (seconds % 86400) / 3600;
            return String.format("%dd %dh", days, hours);
        }
    }
}
