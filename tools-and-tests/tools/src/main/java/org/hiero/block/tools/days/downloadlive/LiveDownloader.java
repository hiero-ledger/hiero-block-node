// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.downloadlive;

import static org.hiero.block.tools.days.download.DownloadConstants.GCP_PROJECT_ID;
import static org.hiero.block.tools.days.downloadlive.ValidateDownloadLive.fullBlockValidate;
import static org.hiero.block.tools.days.subcommands.DownloadLive.toHex;
import static org.hiero.block.tools.mirrornode.DayBlockInfo.loadDayBlockInfoMap;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.hiero.block.tools.days.download.DownloadDayLiveImpl;
import org.hiero.block.tools.days.model.AddressBookRegistry;
import org.hiero.block.tools.days.subcommands.UpdateDayListingsCommand;
import org.hiero.block.tools.mirrornode.BlockTimeReader;
import org.hiero.block.tools.mirrornode.DayBlockInfo;
import org.hiero.block.tools.records.model.unparsed.InMemoryFile;
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
}
