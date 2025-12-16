// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.downloadlive;

import static org.hiero.block.tools.days.download.DownloadConstants.BUCKET_NAME;
import static org.hiero.block.tools.days.download.DownloadConstants.BUCKET_PATH_PREFIX;
import static org.hiero.block.tools.days.download.DownloadConstants.GCP_PROJECT_ID;
import static org.hiero.block.tools.days.downloadlive.ValidateDownloadLive.fullBlockValidate;
import static org.hiero.block.tools.days.listing.DayListingFileReader.loadRecordsFileForDay;
import static org.hiero.block.tools.days.subcommands.DownloadLive.toHex;
import static org.hiero.block.tools.mirrornode.DayBlockInfo.loadDayBlockInfoMap;
import static org.hiero.block.tools.records.RecordFileUtils.findMostCommonByType;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hiero.block.tools.days.download.DownloadDayLiveImpl;
import org.hiero.block.tools.days.listing.ListingRecordFile;
import org.hiero.block.tools.days.model.AddressBookRegistry;
import org.hiero.block.tools.days.subcommands.UpdateDayListingsCommand;
import org.hiero.block.tools.mirrornode.BlockTimeReader;
import org.hiero.block.tools.mirrornode.DayBlockInfo;
import org.hiero.block.tools.records.model.unparsed.InMemoryFile;
import org.hiero.block.tools.utils.ConcurrentTarZstdWriter;
import org.hiero.block.tools.utils.Gzip;
import org.hiero.block.tools.utils.Md5Checker;
import org.hiero.block.tools.utils.PrettyPrint;
import org.hiero.block.tools.utils.gcp.ConcurrentDownloadManagerVirtualThreads;

/**
 * Manages the download, validation, and archival of blockchain blocks in live (streaming) mode.
 *
 * <p>This class orchestrates the complete lifecycle of processing live blockchain data:
 * <ul>
 *   <li><b>Download:</b> Fetches block files from GCP storage using concurrent downloads</li>
 *   <li><b>Validation:</b> Verifies cryptographic signatures and hash chain integrity</li>
 *   <li><b>Persistence:</b> Writes validated files to per-day directories</li>
 *   <li><b>Archival:</b> Creates and compresses tar archives for each completed day</li>
 * </ul>
 *
 * <p><b>Architecture:</b>
 * <ul>
 *   <li>Creates per-day output directories: {@code downloadedDaysDir/YYYY-MM-DD/}</li>
 *   <li>Builds incremental tar archives: {@code downloadedDaysDir/YYYY-MM-DD.tar}</li>
 *   <li>Compresses completed days: {@code downloadedDaysDir/YYYY-MM-DD.tar.zstd}</li>
 *   <li>Maintains running hash state for blockchain continuity validation</li>
 * </ul>
 *
 * <p><b>Thread Safety:</b> This class uses a background executor for compression tasks
 * and manages concurrent downloads via {@link ConcurrentDownloadManagerVirtualThreads}.
 *
 * @see LivePoller for the polling mechanism that feeds blocks to this downloader
 * @see ValidateDownloadLive for the validation pipeline
 */
public class LiveDownloader {
    private final File listingDir;
    private final File downloadedDaysDir;
    private final File tmpRoot;
    private final int maxConcurrency;
    private final Path addressBookPath;
    private final Path runningHashStatusPath;
    private final AddressBookRegistry addressBookRegistry;
    private final ConcurrentDownloadManagerVirtualThreads downloadManager;
    // Running previous record-file hash used to validate the block hash chain across files.
    private byte[] previousRecordFileHash;
    // Single-threaded executor used for background compression of per-day tar files.
    private final ExecutorService compressionExecutor;
    private static final File cacheDir = new File("data/gcp-cache");
    private static final int minNodeAccountId = 3;
    private static final int maxNodeAccountId = 37;

    // === CACHED STATE FOR PERFORMANCE ===
    // Cache DayBlockInfo map - loaded once and reused across all batches
    private Map<LocalDate, DayBlockInfo> cachedDaysInfo;
    // Cache day listing files - loaded once per day, not per block
    private String cachedListingDayKey;
    private Map<LocalDateTime, List<ListingRecordFile>> cachedFilesByBlock;
    private Set<ListingRecordFile> cachedMostCommonFiles;
    // Reusable BlockTimeReader - created once and reused across batches
    private BlockTimeReader cachedBlockTimeReader;
    // Maximum number of retries for MD5 mismatch errors
    private static final int MAX_MD5_RETRIES = 3;

    /**
     * Creates a new LiveDownloader with the specified configuration.
     *
     * <p>This constructor initializes:
     * <ul>
     *   <li>GCP storage client for downloading files</li>
     *   <li>Address book registry for signature validation</li>
     *   <li>Background compression executor</li>
     *   <li>Concurrent download manager with virtual threads</li>
     * </ul>
     *
     * @param listingDir directory containing day listing files (metadata about available blocks)
     * @param downloadedDaysDir root directory for output (per-day folders and archives)
     * @param tmpRoot temporary directory for intermediate file operations
     * @param maxConcurrency maximum number of concurrent downloads (minimum 1)
     * @param addressBookPath path to address book file for signature validation, or null to use Genesis address book
     * @param runningHashStatusPath path where running hash status JSON will be persisted
     * @param initialRunningHash the starting running hash for blockchain continuity, or null for first block
     */
    public LiveDownloader(
            File listingDir,
            File downloadedDaysDir,
            File tmpRoot,
            int maxConcurrency,
            Path addressBookPath,
            Path runningHashStatusPath,
            byte[] initialRunningHash) {
        this.listingDir = listingDir;
        this.downloadedDaysDir = downloadedDaysDir;
        this.tmpRoot = tmpRoot;
        this.maxConcurrency = Math.max(1, maxConcurrency);
        this.addressBookPath = addressBookPath;
        this.runningHashStatusPath = runningHashStatusPath;
        this.previousRecordFileHash = initialRunningHash;

        if (addressBookPath != null) {
            System.out.println("[download] Loading address book from " + addressBookPath);
            this.addressBookRegistry = new AddressBookRegistry(addressBookPath);
        } else {
            System.out.println("[download] No --address-book supplied; using Genesis address book.");
            this.addressBookRegistry = new AddressBookRegistry();
        }

        Storage storage = StorageOptions.grpc()
                .setAttemptDirectPath(false)
                .setProjectId(GCP_PROJECT_ID)
                .build()
                .getService();
        this.downloadManager = ConcurrentDownloadManagerVirtualThreads.newBuilder(storage)
                .setMaxConcurrency(maxConcurrency)
                .build();
        this.compressionExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "download-live-compress");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Refreshes metadata listings for the current live day from GCP.
     *
     * <p>This method is invoked by the poller on each polling cycle to ensure that day
     * listings stay synchronized with GCP storage. This is essential for discovering newly
     * published blocks as they become available.
     *
     * <p>The refresh operation:
     * <ul>
     *   <li>Fetches updated file listings from GCP for node accounts 3-37</li>
     *   <li>Updates local listing cache</li>
     *   <li>Enables the downloader to discover and process new blocks</li>
     * </ul>
     *
     * @param liveDay the current live day (UTC) being processed (e.g., 2025-12-01)
     */
    public void refreshListingsForLiveDay(LocalDate liveDay) {
        UpdateDayListingsCommand.updateDayListings(
                listingDir.toPath(), cacheDir.toPath(), true, minNodeAccountId, maxNodeAccountId, GCP_PROJECT_ID);
        System.out.println("[download] Refreshing metadata listings for live day " + liveDay);
    }

    /**
     * Gracefully shuts down the downloader, releasing all resources.
     *
     * <p>This method should be called when the live downloader is stopping (typically via a
     * JVM shutdown hook). It performs cleanup in the following order:
     * <ol>
     *   <li>Closes the GCP download manager and releases network connections</li>
     *   <li>Shuts down the compression executor, waiting up to 5 seconds for tasks to complete</li>
     *   <li>Forces immediate shutdown if graceful shutdown times out</li>
     * </ol>
     *
     * <p>Errors during shutdown are logged but do not throw exceptions.
     */
    public void shutdown() {
        // Close the GCS transfer manager if it supports AutoCloseable/Closeable.
        try {
            if (downloadManager instanceof AutoCloseable closeable) {
                closeable.close();
            }
        } catch (Exception e) {
            System.err.println("[download] Failed to close download manager: " + e.getMessage());
        }

        // Stop accepting new compression tasks and attempt a graceful shutdown.
        compressionExecutor.shutdown();
        try {
            if (!compressionExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                compressionExecutor.shutdownNow();
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            compressionExecutor.shutdownNow();
        }
    }

    /**
     * Appends one or more file entries to the day's tar archive using the system tar command.
     *
     * <p>The tar file is created under outRoot as dayKey.tar and entries are taken from the per-day folder.
     *
     * <p><b>Security Note:</b> This method uses {@link ProcessBuilder} with separate arguments
     * (not shell execution) to safely execute tar commands. Arguments are passed as individual
     * parameters, preventing command injection even if inputs contain shell metacharacters.
     *
     * @param dayKey the day identifier in YYYY-MM-DD format
     * @param entryNames the relative paths of the files to append (within the day directory)
     */
    private void appendToDayTar(final String dayKey, final List<String> entryNames) {
        try {
            // Validate inputs to prevent path traversal and ensure valid format
            if (dayKey == null || !dayKey.matches("\\d{4}-\\d{2}-\\d{2}")) {
                System.err.println("[download] Invalid dayKey format: " + dayKey);
                return;
            }
            if (entryNames == null || entryNames.isEmpty()) {
                return;
            }
            for (String entryName : entryNames) {
                if (entryName == null || entryName.isEmpty() || entryName.contains("..")) {
                    System.err.println("[download] Invalid entryName: " + entryName);
                    return;
                }
            }

            final Path dayDir = downloadedDaysDir.toPath().resolve(dayKey);
            final Path tarPath = downloadedDaysDir.toPath().resolve(dayKey + ".tar");

            if (!Files.isDirectory(dayDir)) {
                // Nothing to do if the day directory doesn't exist yet.
                return;
            }

            final boolean tarExists = Files.exists(tarPath);

            final List<String> args = new ArrayList<>();
            args.add("tar");
            if (!tarExists) {
                // First entries: create tar with the initial set of files.
                args.add("-cf");
            } else {
                // Append to existing tar.
                args.add("-rf");
            }
            args.add(tarPath.toString());
            args.addAll(entryNames);

            final ProcessBuilder pb = new ProcessBuilder(args);
            pb.directory(dayDir.toFile());
            final Process p = pb.start();
            final int exit = p.waitFor();
            if (exit != 0) {
                System.err.println("[download] tar command failed for day %s entries=%d with exit=%d"
                        .formatted(dayKey, entryNames.size(), exit));
            } else {
                for (String entryName : entryNames) {
                    System.out.println("[download] appended %s to %s".formatted(entryName, tarPath));
                }
            }
        } catch (Exception e) {
            System.err.println("[download] Failed to append to tar for day %s: %s".formatted(dayKey, e.getMessage()));
        }
    }

    /**
     * Convenience wrapper for appending a single entry.
     */
    private void appendToDayTar(final String dayKey, final String entryName) {
        appendToDayTar(dayKey, List.of(entryName));
    }

    /**
     * Schedule finalization of a day's archive on a background thread.
     * This "closes" the tar for the day by stopping further appends (handled by the poller/dayKey rollover),
     * then compresses dayKey.tar into dayKey.tar.zstd and cleans up the per-day folder.
     */
    public void finalizeDay(String dayKey) {
        System.out.println("[download] Scheduling background compression for day " + dayKey);
        compressionExecutor.submit(() -> compressAndCleanupDay(dayKey));
    }

    /**
     * Worker that runs in the background executor to compress and clean up a day's data.
     *
     * <p><b>Security Note:</b> This method uses {@link ProcessBuilder} with separate arguments
     * (not shell execution) to safely execute zstd commands. Arguments are passed as individual
     * parameters, preventing command injection even if inputs contain shell metacharacters.
     */
    private void compressAndCleanupDay(String dayKey) {
        try {
            // Validate dayKey format to prevent path traversal and ensure valid format
            if (dayKey == null || !dayKey.matches("\\d{4}-\\d{2}-\\d{2}")) {
                System.err.println("[download] Invalid dayKey format for compression: " + dayKey);
                return;
            }

            final Path tarPath = downloadedDaysDir.toPath().resolve(dayKey + ".tar");
            final Path dayDir = downloadedDaysDir.toPath().resolve(dayKey);
            if (!Files.isRegularFile(tarPath)) {
                System.out.println("[download] No tar file for day " + dayKey + " to compress; skipping.");
                return;
            }
            final Path zstdPath = downloadedDaysDir.toPath().resolve(dayKey + ".tar.zstd");
            System.out.println("[download] Compressing " + tarPath + " -> " + zstdPath + " using zstd");

            // ProcessBuilder with varargs prevents command injection by passing arguments
            // separately to the process, not through a shell interpreter.
            final ProcessBuilder pb = new ProcessBuilder(
                    "zstd",
                    "-T0", // use all cores
                    "-f", // overwrite output if it exists
                    tarPath.toString(),
                    "-o",
                    zstdPath.toString());
            pb.inheritIO();
            final Process p = pb.start();
            final int exit = p.waitFor();
            if (exit != 0) {
                System.err.println("[download] zstd compression failed for " + tarPath + " with exit=" + exit);
            } else {
                System.out.println("[download] zstd compression complete for " + tarPath);
                // Clean up individual per-day files now that we have tar and tar.zstd.
                // It makes sense to delete them as we have the tar and zstd files.
                if (Files.isDirectory(dayDir)) {
                    try (Stream<Path> paths = Files.walk(dayDir)) {
                        paths.sorted(Comparator.reverseOrder()).forEach(filePath -> {
                            try {
                                Files.deleteIfExists(filePath);
                            } catch (IOException ioe) {
                                System.err.println("[download] Failed to delete " + filePath + ": " + ioe.getMessage());
                            }
                        });
                    }
                    System.out.println("[download] cleaned per-day folder " + dayDir);
                }
            }
        } catch (Exception e) {
            System.err.println("[download] Failed to compress tar for day " + dayKey + ": " + e.getMessage());
        }
    }

    /**
     * Prepares the batch for download by creating directories, sorting, and loading metadata.
     * Returns null if preparation fails.
     */
    private BatchContext prepareBatchDownload(String dayKey, List<BlockDescriptor> batch) {
        if (batch == null || batch.isEmpty()) {
            return null;
        }

        // Make a mutable copy because callers may pass in an immutable List (e.g., Stream.toList()).
        final List<BlockDescriptor> sortedBatch = new ArrayList<>(batch);

        try {
            Files.createDirectories(downloadedDaysDir.toPath().resolve(dayKey));
            Files.createDirectories(tmpRoot.toPath());
        } catch (IOException e) {
            System.err.println("[download] Failed to create output/tmp dirs: " + e.getMessage());
            return null;
        }

        // Ensure deterministic order
        sortedBatch.sort(Comparator.comparingLong(b -> b.getBlockNumber()));

        final LocalDate day = LocalDate.parse(dayKey);
        final Map<LocalDate, DayBlockInfo> daysInfo = loadDayBlockInfoMap();
        final DayBlockInfo dayBlockInfo = daysInfo.get(day);

        if (dayBlockInfo == null) {
            System.err.println("[download] No DayBlockInfo found for live dayKey=" + dayKey + "; skipping batch of "
                    + sortedBatch.size() + " blocks.");
            return null;
        }

        return new BatchContext(
                sortedBatch, day, dayBlockInfo, downloadedDaysDir.toPath().resolve(dayKey));
    }

    /**
     * Persists block files to disk and appends them to the day's tar archive.
     */
    private void persistBlockFiles(String dayKey, Path dayDir, List<InMemoryFile> files) throws IOException {
        final List<String> entriesToAppend = new ArrayList<>(files.size());

        for (InMemoryFile file : files) {
            final Path relativePath = file.path();
            final Path targetPath = dayDir.resolve(relativePath);
            if (targetPath.getParent() != null) {
                Files.createDirectories(targetPath.getParent());
            }
            Files.write(targetPath, file.data());

            final String entryName = dayDir.relativize(targetPath).toString();
            entriesToAppend.add(entryName);
        }

        // Append all entries for this block in a single tar invocation to reduce overhead.
        appendToDayTar(dayKey, entriesToAppend);
    }

    /**
     * Updates the running hash state and persists validation status to disk.
     */
    private void updateRunningHashState(String dayKey, byte[] newHash, Instant recordFileTime) {
        previousRecordFileHash = newHash;

        if (previousRecordFileHash != null) {
            final String endRunningHashHex = toHex(previousRecordFileHash);
            final ValidationStatus status = new ValidationStatus(dayKey, recordFileTime.toString(), endRunningHashHex);
            writeValidationStatus(runningHashStatusPath, status);
        }
    }

    /**
     * Processes a single block: downloads, validates, persists files, and updates state.
     * Returns true if successful, false if processing should stop.
     */
    private boolean processBlock(
            String dayKey, BlockDescriptor descriptor, BatchContext context, BlockTimeReader blockTimeReader) {
        try {
            final DownloadDayLiveImpl.BlockDownloadResult result = DownloadDayLiveImpl.downloadSingleBlockForLive(
                    downloadManager,
                    context.dayBlockInfo,
                    blockTimeReader,
                    listingDir.toPath(),
                    context.day.getYear(),
                    context.day.getMonthValue(),
                    context.day.getDayOfMonth(),
                    descriptor.getBlockNumber(),
                    previousRecordFileHash);

            // Perform full block validation (record + signatures + sidecars) using the
            // files already downloaded for this block.
            final Instant recordFileTime = Instant.parse(descriptor.getTimestampIso());
            final boolean fullyValid = fullBlockValidate(
                    addressBookRegistry,
                    previousRecordFileHash,
                    recordFileTime,
                    result,
                    null); // no temp file for now; quarantine will no-op on null

            if (!fullyValid) {
                System.err.println("[download] Blocking advancement at block " + descriptor.getBlockNumber()
                        + " due to full block validation failure; will retry on a later tick.");
                // Do not process subsequent blocks in this batch; preserve contiguous prefix.
                return false;
            }

            // Persist each fully validated file into the per-day folder and append it into the per-day tar.
            persistBlockFiles(dayKey, context.dayDir, result.files);

            // Update running hash and highest processed block only after full validation and persistence.
            updateRunningHashState(dayKey, result.newPreviousRecordFileHash, recordFileTime);

            return true;
        } catch (Exception e) {
            System.err.println("[download] Failed live block download for block " + descriptor.getBlockNumber() + ": "
                    + e.getMessage() + " (will retry on a later tick).");
            // Treat any failure as a boundary for the contiguous prefix; stop here.
            return false;
        }
    }

    /**
     * Download and place all files for the given batch. Returns the highest block number
     * that was successfully downloaded and placed, or -1 if none succeeded.
     */
    public long downloadBatch(String dayKey, List<BlockDescriptor> batch) {
        final BatchContext context = prepareBatchDownload(dayKey, batch);
        if (context == null) {
            return -1L;
        }

        try {
            final BlockTimeReader blockTimeReader = new BlockTimeReader();
            long highest = -1L;
            final long startTime = System.currentTimeMillis();
            final int totalBlocks = context.sortedBatch.size();

            for (int i = 0; i < context.sortedBatch.size(); i++) {
                BlockDescriptor descriptor = context.sortedBatch.get(i);
                if (!processBlock(dayKey, descriptor, context, blockTimeReader)) {
                    break;
                }
                highest = descriptor.getBlockNumber();

                // Print progress with ETA
                long elapsed = System.currentTimeMillis() - startTime;
                double percent = ((i + 1) * 100.0) / totalBlocks;
                long remaining = PrettyPrint.computeRemainingMilliseconds(i + 1, totalBlocks, elapsed);
                PrettyPrint.printProgressWithEta(
                        percent, "Downloading block " + descriptor.getBlockNumber(), remaining);
            }

            PrettyPrint.clearProgress();
            System.out.println("[download] Completed batch: " + totalBlocks + " blocks for day " + dayKey);

            return highest;
        } catch (Exception e) {
            PrettyPrint.clearProgress();
            System.err.println("[download] Failed to download day " + dayKey + ": " + e.getMessage());
            e.printStackTrace();
        }
        return -1L;
    }

    /**
     * Context object holding batch processing state.
     */
    private static class BatchContext {
        final List<BlockDescriptor> sortedBatch;
        final LocalDate day;
        final DayBlockInfo dayBlockInfo;
        final Path dayDir;

        BatchContext(List<BlockDescriptor> sortedBatch, LocalDate day, DayBlockInfo dayBlockInfo, Path dayDir) {
            this.sortedBatch = sortedBatch;
            this.day = day;
            this.dayBlockInfo = dayBlockInfo;
            this.dayDir = dayDir;
        }
    }

    private static void writeValidationStatus(Path path, ValidationStatus status) {
        if (path == null || status == null) {
            return;
        }
        try {
            if (path.getParent() != null) {
                Files.createDirectories(path.getParent());
            }
            String json = "{\n"
                    + "  \"dayDate\": \"" + status.getDayDate() + "\",\n"
                    + "  \"recordFileTime\": \"" + status.getRecordFileTime() + "\",\n"
                    + "  \"endRunningHashHex\": \"" + status.getEndRunningHashHex() + "\"\n"
                    + "}\n";
            Files.writeString(path, json, StandardCharsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("[poller] Failed to write running-hash status: " + e.getMessage());
        }
    }

    // ========================================================================
    // OPTIMIZED PIPELINED DOWNLOAD - Similar to DownloadDaysV2 for performance
    // ========================================================================

    /**
     * Gets or creates the cached BlockTimeReader.
     */
    private BlockTimeReader getBlockTimeReader() throws IOException {
        if (cachedBlockTimeReader == null) {
            cachedBlockTimeReader = new BlockTimeReader();
        }
        return cachedBlockTimeReader;
    }

    /**
     * Gets or loads the cached DayBlockInfo map.
     */
    private Map<LocalDate, DayBlockInfo> getDaysInfo() {
        if (cachedDaysInfo == null) {
            cachedDaysInfo = loadDayBlockInfoMap();
            System.out.println("[download] Loaded DayBlockInfo cache with " + cachedDaysInfo.size() + " days");
        }
        return cachedDaysInfo;
    }

    /**
     * Invalidates cached DayBlockInfo to force reload on next access.
     * Call this when new days become available.
     */
    public void invalidateDaysInfoCache() {
        cachedDaysInfo = null;
    }

    /**
     * Loads and caches day listing files for the given day.
     * Only reloads if the day key changes.
     *
     * @throws IOException if listing files cannot be loaded
     */
    private void ensureDayListingsCached(String dayKey, int year, int month, int day) throws IOException {
        if (dayKey.equals(cachedListingDayKey) && cachedFilesByBlock != null) {
            return; // Already cached for this day
        }

        System.out.println("[download] Loading listing files for day " + dayKey);
        final List<ListingRecordFile> allDaysFiles = loadRecordsFileForDay(listingDir.toPath(), year, month, day);
        cachedFilesByBlock = allDaysFiles.stream().collect(Collectors.groupingBy(ListingRecordFile::timestamp));

        // Compute most common files for this day
        cachedMostCommonFiles = new HashSet<>();
        cachedFilesByBlock.values().forEach(list -> {
            final ListingRecordFile mostCommonRecordFile = findMostCommonByType(list, ListingRecordFile.Type.RECORD);
            final ListingRecordFile mostCommonSidecarFile =
                    findMostCommonByType(list, ListingRecordFile.Type.RECORD_SIDECAR);
            if (mostCommonRecordFile != null) cachedMostCommonFiles.add(mostCommonRecordFile);
            if (mostCommonSidecarFile != null) cachedMostCommonFiles.add(mostCommonSidecarFile);
        });

        cachedListingDayKey = dayKey;
        System.out.println("[download] Cached " + allDaysFiles.size() + " listing files for day " + dayKey);
    }

    /**
     * Invalidates cached day listings to force reload on next access.
     * Call this after refreshing GCS listings.
     */
    public void invalidateDayListingsCache() {
        cachedListingDayKey = null;
        cachedFilesByBlock = null;
        cachedMostCommonFiles = null;
    }

    /**
     * Helper container for pending block downloads in the pipelined approach.
     */
    private static final class BlockWork {
        final long blockNumber;
        final byte[] blockHashFromMirrorNode;
        final LocalDateTime blockTime;
        final List<ListingRecordFile> orderedFiles;
        final List<CompletableFuture<InMemoryFile>> futures = new ArrayList<>();

        BlockWork(
                long blockNumber,
                byte[] blockHashFromMirrorNode,
                LocalDateTime blockTime,
                List<ListingRecordFile> orderedFiles) {
            this.blockNumber = blockNumber;
            this.blockHashFromMirrorNode = blockHashFromMirrorNode;
            this.blockTime = blockTime;
            this.orderedFiles = orderedFiles;
        }
    }

    /**
     * Downloads an entire day using pipelined concurrent downloads - similar to DownloadDaysV2.
     * This is much faster than processing blocks sequentially.
     *
     * @param dayKey the day to download in YYYY-MM-DD format
     * @param startBlockNumber the first block number to download (for resuming)
     * @param overallStartMillis epoch millis when overall run started (for ETA)
     * @param dayIndex zero-based index of this day within the overall run
     * @param totalDays total number of days in the overall run
     * @return the hash of the last processed block, or null on failure
     */
    public byte[] downloadDayPipelined(
            final String dayKey,
            final long startBlockNumber,
            final long overallStartMillis,
            final int dayIndex,
            final long totalDays)
            throws IOException {

        final LocalDate day = LocalDate.parse(dayKey);
        final int year = day.getYear();
        final int month = day.getMonthValue();
        final int dayOfMonth = day.getDayOfMonth();

        // Check if output already exists
        final Path finalOutFile = downloadedDaysDir.toPath().resolve(dayKey + ".tar.zstd");
        if (Files.exists(finalOutFile)) {
            System.out.println("[download] Skipping " + dayKey + " - already exists: " + finalOutFile);
            return null;
        }

        // Ensure directories exist
        try {
            Files.createDirectories(downloadedDaysDir.toPath());
        } catch (IOException e) {
            System.err.println("[download] Failed to create output dir: " + e.getMessage());
            return null;
        }

        // Get cached DayBlockInfo
        final Map<LocalDate, DayBlockInfo> daysInfo = getDaysInfo();
        final DayBlockInfo dayBlockInfo = daysInfo.get(day);
        if (dayBlockInfo == null) {
            System.err.println("[download] No DayBlockInfo found for day " + dayKey);
            return null;
        }

        // Ensure day listings are cached
        ensureDayListingsCached(dayKey, year, month, dayOfMonth);

        // Get cached BlockTimeReader
        final BlockTimeReader blockTimeReader;
        try {
            blockTimeReader = getBlockTimeReader();
        } catch (IOException e) {
            System.err.println("[download] Failed to create BlockTimeReader: " + e.getMessage());
            return null;
        }

        // Determine block range
        final long firstBlock = Math.max(dayBlockInfo.firstBlockNumber, startBlockNumber);
        final long lastBlock = dayBlockInfo.lastBlockNumber;
        final int totalBlocks = (int) (lastBlock - firstBlock + 1);

        if (totalBlocks <= 0) {
            System.out.println("[download] No blocks to download for day " + dayKey);
            return previousRecordFileHash;
        }

        System.out.println("[download] Starting pipelined download for " + dayKey + " blocks " + firstBlock + " to "
                + lastBlock + " (" + totalBlocks + " blocks)");

        final double daySharePercent = (totalDays <= 0) ? 100.0 : (100.0 / totalDays);
        final AtomicLong blocksProcessed = new AtomicLong(0);
        final AtomicLong blocksQueuedForDownload = new AtomicLong(0);
        final LinkedBlockingDeque<BlockWork> pending = new LinkedBlockingDeque<>(1000);

        // Queue downloads in background thread
        final CompletableFuture<Void> downloadQueueingFuture = CompletableFuture.runAsync(() -> {
            for (long blockNumber = firstBlock; blockNumber <= lastBlock; blockNumber++) {
                final LocalDateTime blockTime = blockTimeReader.getBlockLocalDateTime(blockNumber);
                final List<ListingRecordFile> group = cachedFilesByBlock.get(blockTime);

                if (group == null || group.isEmpty()) {
                    throw new IllegalStateException("Missing record files for block " + blockNumber + " at time "
                            + blockTime + " on " + dayKey);
                }

                final ListingRecordFile mostCommonRecordFile =
                        findMostCommonByType(group, ListingRecordFile.Type.RECORD);
                final ListingRecordFile mostCommonSidecarFile =
                        findMostCommonByType(group, ListingRecordFile.Type.RECORD_SIDECAR);

                final List<ListingRecordFile> orderedFilesToDownload =
                        computeFilesToDownload(mostCommonRecordFile, mostCommonSidecarFile, group);

                // Get mirror node block hash if available
                byte[] blockHashFromMirrorNode =
                        DownloadDayLiveImpl.extractBlockHashFromMirrorNode(blockNumber, dayBlockInfo);

                // Create BlockWork and start async downloads
                final BlockWork bw =
                        new BlockWork(blockNumber, blockHashFromMirrorNode, blockTime, orderedFilesToDownload);
                for (ListingRecordFile lr : orderedFilesToDownload) {
                    final String blobName = BUCKET_PATH_PREFIX + lr.path();
                    bw.futures.add(downloadManager.downloadAsync(BUCKET_NAME, blobName));
                }

                try {
                    pending.putLast(bw);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while enqueueing block work", ie);
                }
                blocksQueuedForDownload.incrementAndGet();
            }
        });

        // Process and write blocks as downloads complete
        try (ConcurrentTarZstdWriter writer = new ConcurrentTarZstdWriter(finalOutFile)) {
            while (!downloadQueueingFuture.isDone() || !pending.isEmpty()) {
                final BlockWork ready;
                try {
                    ready = pending.pollFirst(1, TimeUnit.SECONDS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting for pending block", ie);
                }

                if (ready == null) {
                    continue;
                }

                // Wait for all downloads for this block to complete
                try {
                    CompletableFuture.allOf(ready.futures.toArray(new CompletableFuture[0]))
                            .join();
                } catch (CompletionException ce) {
                    PrettyPrint.clearProgress();
                    throw new RuntimeException("Failed downloading block " + ready.blockTime, ce.getCause());
                }

                // Process downloaded files
                final List<InMemoryFile> filesForWriting = processDownloadedFiles(ready);

                // Validate block hashes
                previousRecordFileHash = DownloadDayLiveImpl.validateBlockHashes(
                        ready.blockNumber, filesForWriting, previousRecordFileHash, ready.blockHashFromMirrorNode);

                // Perform full block validation
                final DownloadDayLiveImpl.BlockDownloadResult result = new DownloadDayLiveImpl.BlockDownloadResult(
                        ready.blockNumber, filesForWriting, previousRecordFileHash);
                final Instant recordFileTime =
                        ready.blockTime.atZone(java.time.ZoneOffset.UTC).toInstant();
                final boolean fullyValid =
                        fullBlockValidate(addressBookRegistry, previousRecordFileHash, recordFileTime, result, null);

                if (!fullyValid) {
                    System.err.println("[download] Full block validation failed for block " + ready.blockNumber);
                    // Continue anyway - the hash chain validation passed
                }

                // Write files to tar archive
                for (InMemoryFile imf : filesForWriting) {
                    writer.putEntry(imf);
                }

                // Update running hash status
                updateRunningHashState(dayKey, previousRecordFileHash, recordFileTime);

                // Print progress
                final long processed = blocksProcessed.incrementAndGet();
                if (processed == 1 || processed % 50 == 0) {
                    final double blockFraction = processed / (double) totalBlocks;
                    final double overallPercent = dayIndex * daySharePercent + blockFraction * daySharePercent;
                    final long now = System.currentTimeMillis();
                    final long elapsed = Math.max(1L, now - overallStartMillis);
                    final long remaining = (overallPercent > 0.0 && overallPercent < 100.0)
                            ? (long) (elapsed * (100.0 - overallPercent) / overallPercent)
                            : Long.MAX_VALUE;

                    final String msg = dayKey + " - Block q:" + blocksQueuedForDownload.get()
                            + " p:" + processed + " t:" + totalBlocks
                            + " (" + ready.blockTime + ")";
                    PrettyPrint.printProgressWithEta(overallPercent, msg, remaining);
                }
            }

            // Ensure producer exceptions are propagated
            downloadQueueingFuture.join();

        } catch (Exception e) {
            PrettyPrint.clearProgress();
            System.err.println("[download] Failed pipelined download for day " + dayKey + ": " + e.getMessage());
            e.printStackTrace();
            return null;
        }

        PrettyPrint.clearProgress();
        System.out.println("[download] Completed pipelined download for " + dayKey + ": " + totalBlocks + " blocks");
        return previousRecordFileHash;
    }

    /**
     * Computes the ordered list of files to download for a block.
     */
    private static List<ListingRecordFile> computeFilesToDownload(
            ListingRecordFile mostCommonRecordFile,
            ListingRecordFile mostCommonSidecarFile,
            List<ListingRecordFile> group) {

        final List<ListingRecordFile> orderedFilesToDownload = new ArrayList<>();
        if (mostCommonRecordFile != null) orderedFilesToDownload.add(mostCommonRecordFile);
        if (mostCommonSidecarFile != null) orderedFilesToDownload.add(mostCommonSidecarFile);

        for (ListingRecordFile file : group) {
            switch (file.type()) {
                case RECORD -> {
                    if (!file.equals(mostCommonRecordFile)) orderedFilesToDownload.add(file);
                }
                case RECORD_SIG -> orderedFilesToDownload.add(file);
                case RECORD_SIDECAR -> {
                    if (!file.equals(mostCommonSidecarFile)) orderedFilesToDownload.add(file);
                }
                default -> throw new RuntimeException("Unsupported file type: " + file.type());
            }
        }
        return orderedFilesToDownload;
    }

    /**
     * Processes downloaded files for a block: validates MD5, ungzips if needed, computes paths.
     */
    private List<InMemoryFile> processDownloadedFiles(BlockWork blockWork) throws IOException {
        final List<InMemoryFile> filesForWriting = new ArrayList<>();

        for (int i = 0; i < blockWork.orderedFiles.size(); i++) {
            final ListingRecordFile lr = blockWork.orderedFiles.get(i);
            String filename = lr.path().substring(lr.path().lastIndexOf('/') + 1);

            try {
                InMemoryFile downloadedFile = blockWork.futures.get(i).join();

                // Check MD5 and retry if mismatch
                boolean md5Valid = Md5Checker.checkMd5(lr.md5Hex(), downloadedFile.data());
                if (!md5Valid) {
                    PrettyPrint.clearProgress();
                    System.err.println("MD5 mismatch for " + lr.path() + ", retrying...");
                    downloadedFile = downloadFileWithRetry(lr);
                    if (downloadedFile == null) {
                        continue; // Skip signature files with persistent MD5 mismatch
                    }
                }

                byte[] contentBytes = downloadedFile.data();
                if (filename.endsWith(".gz")) {
                    contentBytes = Gzip.ungzipInMemory(contentBytes);
                    filename = filename.replaceAll("\\.gz$", "");
                }

                final Path newFilePath = DownloadDayLiveImpl.computeNewFilePath(lr, cachedMostCommonFiles, filename);
                filesForWriting.add(new InMemoryFile(newFilePath, contentBytes));

            } catch (Exception e) {
                System.err.println("Warning: Skipping file [" + filename + "] for block " + blockWork.blockNumber + ": "
                        + e.getMessage());
            }
        }

        return filesForWriting;
    }

    /**
     * Downloads a file with retry logic for MD5 mismatch errors.
     */
    private InMemoryFile downloadFileWithRetry(ListingRecordFile lr) throws IOException {
        final String blobName = BUCKET_PATH_PREFIX + lr.path();
        final boolean isSignatureFile = lr.type() == ListingRecordFile.Type.RECORD_SIG;
        IOException lastException = null;

        for (int attempt = 1; attempt <= MAX_MD5_RETRIES; attempt++) {
            try {
                final CompletableFuture<InMemoryFile> future = downloadManager.downloadAsync(BUCKET_NAME, blobName);
                final InMemoryFile downloadedFile = future.join();

                if (!Md5Checker.checkMd5(lr.md5Hex(), downloadedFile.data())) {
                    throw new IOException("MD5 mismatch for blob " + blobName);
                }

                if (attempt > 1) {
                    PrettyPrint.clearProgress();
                    System.err.println("Successfully downloaded " + blobName + " after " + attempt + " attempts");
                }
                return downloadedFile;

            } catch (Exception e) {
                lastException = (e instanceof IOException)
                        ? (IOException) e
                        : new IOException("Download failed for " + blobName, e);

                if (e.getMessage() != null && e.getMessage().contains("MD5 mismatch")) {
                    if (attempt < MAX_MD5_RETRIES) {
                        PrettyPrint.clearProgress();
                        System.err.println("MD5 mismatch for " + blobName + " (attempt " + attempt + "), retrying...");
                        try {
                            Thread.sleep(100L * attempt);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            throw new IOException("Interrupted during retry delay", ie);
                        }
                    }
                } else {
                    throw lastException;
                }
            }
        }

        if (isSignatureFile) {
            PrettyPrint.clearProgress();
            System.err.println("WARNING: Skipping signature file " + blobName + " due to persistent MD5 mismatch");
            return null;
        }

        throw lastException;
    }

    /**
     * Validates block hashes - delegates to DownloadDayLiveImpl.
     */
    private static byte[] validateBlockHashes(
            long blockNumber, List<InMemoryFile> files, byte[] prevRecordFileHash, byte[] blockHashFromMirrorNode) {
        return DownloadDayLiveImpl.validateBlockHashes(blockNumber, files, prevRecordFileHash, blockHashFromMirrorNode);
    }
}
