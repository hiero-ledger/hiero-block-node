// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.download;

import static org.hiero.block.tools.days.download.DownloadConstants.BUCKET_NAME;
import static org.hiero.block.tools.days.download.DownloadConstants.BUCKET_PATH_PREFIX;
import static org.hiero.block.tools.days.listing.DayListingFileReader.loadRecordsFileForDay;
import static org.hiero.block.tools.records.RecordFileUtils.extractRecordFileTimeStrFromPath;
import static org.hiero.block.tools.records.RecordFileUtils.findMostCommonByType;
import static org.hiero.block.tools.records.RecordFileUtils.findMostCommonSidecars;
import static org.hiero.block.tools.utils.PrettyPrint.clearProgress;
import static org.hiero.block.tools.utils.PrettyPrint.prettyPrintFileSize;
import static org.hiero.block.tools.utils.PrettyPrint.printProgressWithEta;

import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.hiero.block.tools.days.listing.ListingRecordFile;
import org.hiero.block.tools.mirrornode.BlockTimeReader;
import org.hiero.block.tools.mirrornode.DayBlockInfo;
import org.hiero.block.tools.records.InMemoryFile;
import org.hiero.block.tools.records.RecordFileInfo;
import org.hiero.block.tools.utils.ConcurrentTarZstdWriter;
import org.hiero.block.tools.utils.Gzip;
import org.hiero.block.tools.utils.Md5Checker;
import org.hiero.block.tools.utils.gcp.ConcurrentDownloadManager;

/**
 * Download all record files for a given day from GCP, group by block, deduplicate, validate,
 * and write into a single .tar.zstd file.
 */
@SuppressWarnings({"CallToPrintStackTrace", "DuplicatedCode"})
public class DownloadDayImplV2 {

    /** Maximum number of retries for MD5 mismatch errors. */
    private static final int MAX_MD5_RETRIES = 3;

    // small helper container for pending block downloads
    private static final class BlockWork {
        final long blockNumber;
        /** Optional block hash from mirror node listing, may be null. Only set for first and last blocks of the day */
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
     *  Download all record files for a given day from GCP, group by block, deduplicate, validate,
     *  and write into a single .tar.zstd file.
     *
     * @param downloadManager the concurrent download manager to use
     * @param dayBlockInfo the block info for the day to download
     * @param blockTimeReader the block time reader to get block times
     * @param listingDir directory where listing files are stored
     * @param downloadedDaysDir directory where downloaded .tar.zstd files are stored
     * @param year the year (e.g., 2023) to download
     * @param month the month (1-12) to download
     * @param day the day of month (1-31) to download
     * @param totalDays total number of days in the overall download run (used to split 100% across days)
     * @param dayIndex zero-based index of this day within the overall run (0..totalDays-1)
     * @param overallStartMillis epoch millis when overall run started (for ETA calculations)
     * @return the hash of the last most common record file for this day, to be passed as previousRecordFileHash for next day
     * @throws Exception on any error
     */
    public static byte[] downloadDay(
            final ConcurrentDownloadManager downloadManager,
            final DayBlockInfo dayBlockInfo,
            final BlockTimeReader blockTimeReader,
            final Path listingDir,
            final Path downloadedDaysDir,
            final int year,
            final int month,
            final int day,
            final byte[] previousRecordFileHash,
            final long totalDays,
            final int dayIndex,
            final long overallStartMillis)
            throws Exception {
        // the running blockchain hash from previous record file, null means unknown (first block of chain, or starting
        // mid-chain)
        byte[] prevRecordFileHash = previousRecordFileHash;
        // load record file listings and group by ListingRecordFile.timestamp
        final List<ListingRecordFile> allDaysFiles = loadRecordsFileForDay(listingDir, year, month, day);
        final Map<LocalDateTime, List<ListingRecordFile>> filesByBlock =
                allDaysFiles.stream().collect(Collectors.groupingBy(ListingRecordFile::timestamp));

        // prepare output files and early exit if already present
        final String dayString = String.format("%04d-%02d-%02d", year, month, day);
        final Path finalOutFile = downloadedDaysDir.resolve(dayString + ".tar.zstd");
        final Path partialOutFile = downloadedDaysDir.resolve(dayString + ".tar.zstd_partial");
        if (Files.exists(finalOutFile)) {
            double daySharePercent = (totalDays <= 0) ? 100.0 : (100.0 / totalDays);
            double overallPercent = dayIndex * daySharePercent + daySharePercent; // this day done
            long remaining = Long.MAX_VALUE;
            long now = System.currentTimeMillis();
            long elapsed = Math.max(1L, now - overallStartMillis);
            if (overallPercent > 0.0 && overallPercent < 100.0) {
                remaining = (long) (elapsed * (100.0 - overallPercent) / overallPercent);
            }
            printProgressWithStats(
                    downloadManager,
                    overallPercent,
                    dayString + " :: Skipping as exists " + allDaysFiles.size() + " files",
                    remaining);
            return null;
        }
        if (!Files.exists(downloadedDaysDir)) Files.createDirectories(downloadedDaysDir);
        try {
            Files.deleteIfExists(partialOutFile);
        } catch (IOException ignored) {
        }

        double daySharePercent = (totalDays <= 0) ? 100.0 : (100.0 / totalDays);
        double startingPercent = dayIndex * daySharePercent;
        long remainingMillisUnknown = Long.MAX_VALUE;
        printProgressWithStats(
                downloadManager,
                startingPercent,
                dayString + " :: Processing " + allDaysFiles.size() + " files",
                remainingMillisUnknown);

        // sets for most common files
        final Set<ListingRecordFile> mostCommonFiles = new HashSet<>();
        filesByBlock.values().forEach(list -> {
            final ListingRecordFile mostCommonRecordFile = findMostCommonByType(list, ListingRecordFile.Type.RECORD);
            final ListingRecordFile mostCommonSidecarFile =
                    findMostCommonByType(list, ListingRecordFile.Type.RECORD_SIDECAR);
            if (mostCommonRecordFile != null) mostCommonFiles.add(mostCommonRecordFile);
            if (mostCommonSidecarFile != null) mostCommonFiles.add(mostCommonSidecarFile);
        });

        // prepare ordered block numbers for this day
        final long firstBlock = dayBlockInfo.firstBlockNumber;
        final long lastBlock = dayBlockInfo.lastBlockNumber;
        final int totalBlocks = (int) (lastBlock - firstBlock + 1);
        final AtomicLong blocksProcessed = new AtomicLong(0);
        final AtomicLong blocksQueuedForDownload = new AtomicLong(0);

        final LinkedBlockingDeque<BlockWork> pending = new LinkedBlockingDeque<>(1000);

        // in background thread iterate blocks in numeric order, queue downloads for each block's files
        CompletableFuture<Void> downloadQueueingFuture = CompletableFuture.runAsync(() -> {
            for (long blockNumber = firstBlock; blockNumber <= lastBlock; blockNumber++) {
                final LocalDateTime blockTime = blockTimeReader.getBlockLocalDateTime(blockNumber);
                final List<ListingRecordFile> group = filesByBlock.get(blockTime);
                if (group == null || group.isEmpty()) {
                    throw new IllegalStateException("Missing record files for block number " + blockNumber + " at time "
                            + blockTime + " on " + year + "-" + month + "-" + day);
                }
                final ListingRecordFile mostCommonRecordFile =
                        findMostCommonByType(group, ListingRecordFile.Type.RECORD);
                final ListingRecordFile[] mostCommonSidecarFiles = findMostCommonSidecars(group);
                // build ordered list of files to download for this block
                final List<ListingRecordFile> orderedFilesToDownload =
                        computeFilesToDownload(mostCommonRecordFile, mostCommonSidecarFiles, group);
                // get mirror node block hash if available (only for first and last blocks of day)
                byte[] blockHashFromMirrorNode = null;
                if (blockNumber == firstBlock && dayBlockInfo.firstBlockHash != null) {
                    String hexStr = dayBlockInfo.firstBlockHash.startsWith("0x")
                            ? dayBlockInfo.firstBlockHash.substring(2)
                            : dayBlockInfo.firstBlockHash;
                    blockHashFromMirrorNode = HexFormat.of().parseHex(hexStr);
                } else if (blockNumber == lastBlock && dayBlockInfo.lastBlockHash != null) {
                    String hexStr = dayBlockInfo.lastBlockHash.startsWith("0x")
                            ? dayBlockInfo.lastBlockHash.substring(2)
                            : dayBlockInfo.lastBlockHash;
                    blockHashFromMirrorNode = HexFormat.of().parseHex(hexStr);
                }
                // create BlockWork and start downloads for its files
                final BlockWork bw =
                        new BlockWork(blockNumber, blockHashFromMirrorNode, blockTime, orderedFilesToDownload);
                for (ListingRecordFile lr : orderedFilesToDownload) {
                    final String blobName = BUCKET_PATH_PREFIX + lr.path();
                    bw.futures.add(downloadManager.downloadAsync(BUCKET_NAME, blobName));
                }
                try {
                    // block if queue is full to provide backpressure
                    pending.putLast(bw);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while enqueueing block work", ie);
                }
                blocksQueuedForDownload.incrementAndGet();
            }
        });

        // validate and write completed blocks in order as they finish downloading
        try (ConcurrentTarZstdWriter writer = new ConcurrentTarZstdWriter(finalOutFile)) {
            // process pending blocks while the producer is still running or while there is work in the queue
            while (!downloadQueueingFuture.isDone() || !pending.isEmpty()) {
                // wait up to 1s for a block; if none available and producer still running, loop again
                final BlockWork ready;
                try {
                    ready = pending.pollFirst(1, TimeUnit.SECONDS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting for pending block", ie);
                }
                if (ready == null) {
                    // no work available right now; retry loop condition
                    continue;
                }
                // wait for its downloads to complete for this block
                try {
                    CompletableFuture.allOf(ready.futures.toArray(new CompletableFuture[0]))
                            .join();
                } catch (CompletionException ce) {
                    clearProgress();
                    ce.printStackTrace();
                    throw new RuntimeException("Failed downloading block " + ready.blockTime, ce.getCause());
                }
                // convert the downloaded files into InMemoryFiles with destination paths, unzipped if needed and
                //  validate md5 hashes with retry logic
                final List<InMemoryFile> inMemoryFilesForWriting = new ArrayList<>();
                for (int i = 0; i < ready.orderedFiles.size(); i++) {
                    final ListingRecordFile lr = ready.orderedFiles.get(i);
                    String filename = lr.path().substring(lr.path().lastIndexOf('/') + 1);
                    try {
                        InMemoryFile downloadedFile = ready.futures.get(i).join();

                        // Check MD5 and retry if mismatch
                        boolean md5Valid = Md5Checker.checkMd5(lr.md5Hex(), downloadedFile.data());
                        if (!md5Valid) {
                            clearProgress();
                            System.err.println(
                                    "MD5 mismatch for " + (BUCKET_PATH_PREFIX + lr.path()) + ", retrying download...");
                            // Retry download with built-in retry logic
                            downloadedFile = downloadFileWithRetry(downloadManager, lr);
                            // If still null after retries (signature file with persistent MD5 mismatch), skip this file
                            if (downloadedFile == null) {
                                continue; // Skip this file and move to next
                            }
                        }

                        byte[] contentBytes = downloadedFile.data();
                        if (filename.endsWith(".gz")) {
                            contentBytes = Gzip.ungzipInMemory(contentBytes);
                            filename = filename.replaceAll("\\.gz$", "");
                        }
                        final Path newFilePath = computeNewFilePath(lr, mostCommonFiles, filename);
                        inMemoryFilesForWriting.add(new InMemoryFile(newFilePath, contentBytes));
                    } catch (EOFException eofe) {
                        // ignore corrupted gzip files
                        System.err.println("Warning: Skipping corrupted gzip file [" + filename + "] for block "
                                + ready.blockNumber + " time " + ready.blockTime + ": " + eofe.getMessage());
                    }
                }
                // validate block hashes
                prevRecordFileHash = validateBlockHashes(
                        ready.blockNumber, inMemoryFilesForWriting, prevRecordFileHash, ready.blockHashFromMirrorNode);
                // write files to output tar.zstd
                for (InMemoryFile imf : inMemoryFilesForWriting) writer.putEntry(imf);
                // print progress
                printProgress(
                        blocksProcessed,
                        blocksQueuedForDownload,
                        totalBlocks,
                        dayIndex,
                        daySharePercent,
                        overallStartMillis,
                        dayString,
                        ready.blockTime,
                        downloadManager);
            }
            // Ensure producer exceptions are propagated instead of being silently ignored.
            downloadQueueingFuture.join();
        } catch (Exception e) {
            clearProgress();
            e.printStackTrace();
            try {
                Files.deleteIfExists(partialOutFile);
            } catch (IOException ignored) {
            }
            throw e;
        }
        return prevRecordFileHash;
    }

    /**
     * Download a file with retry logic for MD5 mismatch errors.
     * For signature files (.rcd_sig), returns null if MD5 validation fails after all retries,
     * allowing the download process to continue since only 2/3rds of signature files are needed.
     *
     * @param downloadManager the concurrent download manager to use
     * @param lr the listing record file to download
     * @return the downloaded in-memory file, or null if signature file failed MD5 check after all retries
     * @throws IOException if download or MD5 validation fails after all retries (for non-signature files)
     */
    private static InMemoryFile downloadFileWithRetry(
            final ConcurrentDownloadManager downloadManager, final ListingRecordFile lr) throws IOException {
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

                // Success - return the file
                if (attempt > 1) {
                    clearProgress();
                    System.err.println("Successfully downloaded " + blobName + " after " + attempt + " attempts");
                }
                return downloadedFile;

            } catch (Exception e) {
                final IOException ioException = (e instanceof IOException)
                        ? (IOException) e
                        : new IOException("Download failed for blob " + blobName, e);

                lastException = ioException;

                // Only retry on MD5 mismatch
                if (e.getMessage() != null && e.getMessage().contains("MD5 mismatch")) {
                    if (attempt < MAX_MD5_RETRIES) {
                        clearProgress();
                        System.err.println("MD5 mismatch for " + blobName + " (attempt " + attempt + "/"
                                + MAX_MD5_RETRIES + "), retrying...");
                        // Small delay before retry
                        try {
                            Thread.sleep(100 * attempt); // Exponential backoff: 100ms, 200ms, 300ms
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            throw new IOException("Interrupted during retry delay", ie);
                        }
                    } else {
                        clearProgress();
                        System.err.println(
                                "MD5 mismatch for " + blobName + " failed after " + MAX_MD5_RETRIES + " attempts");
                    }
                } else {
                    // Non-MD5 errors should not be retried
                    throw ioException;
                }
            }
        }

        // All retries exhausted
        // For signature files, we can tolerate MD5 failures since only 2/3 are needed for validation
        if (isSignatureFile) {
            clearProgress();
            System.err.println("WARNING: Skipping signature file " + blobName
                    + " due to persistent MD5 mismatch after " + MAX_MD5_RETRIES
                    + " retries. Only 2/3 of signature files are required for block validation.");
            return null; // Return null to allow the download process to continue
        }

        // For non-signature files, throw the exception
        throw lastException;
    }

    /**
     * Validate block hashes for the given block's record files.
     *
     * @param blockNum the block number
     * @param inMemoryFilesForWriting the list of in-memory record files for this block
     * @param prevRecordFileHash the previous record file hash to validate against (can be null)
     * @param blockHashFromMirrorNode the expected block hash from mirror node listing (can be null)
     * @return the computed block hash from this block's record file
     * @throws IllegalStateException if any hash validation fails
     */
    private static byte[] validateBlockHashes(
            final long blockNum,
            final List<InMemoryFile> inMemoryFilesForWriting,
            final byte[] prevRecordFileHash,
            final byte[] blockHashFromMirrorNode) {
        final InMemoryFile mostCommonRecordFileInMem = inMemoryFilesForWriting.getFirst();
        final RecordFileInfo recordFileInfo = RecordFileInfo.parse(mostCommonRecordFileInMem.data());
        byte[] readPreviousBlockHash = recordFileInfo.previousBlockHash().toByteArray();
        byte[] computedBlockHash = recordFileInfo.blockHash().toByteArray();
        if (blockHashFromMirrorNode != null && !Arrays.equals(blockHashFromMirrorNode, computedBlockHash)) {
            throw new IllegalStateException(
                    "Block[" + blockNum + "] hash mismatch with mirror node listing. " + ", Expected: "
                            + HexFormat.of().formatHex(blockHashFromMirrorNode).substring(0, 8)
                            + ", Found: "
                            + HexFormat.of().formatHex(computedBlockHash).substring(0, 8) + "\n"
                            + "Context mostCommonRecordFile:"
                            + mostCommonRecordFileInMem.path() + " computedHash:"
                            + HexFormat.of().formatHex(computedBlockHash).substring(0, 8));
        }
        if (prevRecordFileHash != null && !Arrays.equals(prevRecordFileHash, readPreviousBlockHash)) {
            throw new IllegalStateException("Block[" + blockNum + "] previous block hash mismatch. " + ", Expected: "
                    + HexFormat.of().formatHex(prevRecordFileHash).substring(0, 8)
                    + ", Found: "
                    + HexFormat.of().formatHex(readPreviousBlockHash).substring(0, 8) + "\n"
                    + "Context mostCommonRecordFile:"
                    + mostCommonRecordFileInMem.path() + " computedHash:"
                    + HexFormat.of().formatHex(computedBlockHash).substring(0, 8));
        }
        return computedBlockHash;
    }

    /**
     * Compute the new file path for a record file within the output tar.zstd archive.
     *
     * @param lr the listing record file
     * @param mostCommonFiles the set of most common files
     * @param filename the original filename
     * @return the new file path within the archive
     * @throws IOException if an unsupported file type is encountered
     */
    private static Path computeNewFilePath(
            ListingRecordFile lr, Set<ListingRecordFile> mostCommonFiles, String filename) throws IOException {
        String parentDir = lr.path();
        int lastSlash = parentDir.lastIndexOf('/');
        if (lastSlash > 0) parentDir = parentDir.substring(0, lastSlash);
        String nodeDir = parentDir.substring(parentDir.lastIndexOf('/') + 1).replace("record", "");
        String targetFileName;
        if (lr.type() == ListingRecordFile.Type.RECORD || lr.type() == ListingRecordFile.Type.RECORD_SIDECAR) {
            if (mostCommonFiles.contains(lr)) {
                targetFileName = filename;
            } else {
                targetFileName = filename.replaceAll("\\.rcd$", "_node_" + nodeDir + ".rcd");
            }
        } else if (lr.type() == ListingRecordFile.Type.RECORD_SIG) {
            targetFileName = "node_" + nodeDir + ".rcs_sig";
        } else {
            throw new IOException("Unsupported file type: " + lr.type());
        }
        String dateDirName = extractRecordFileTimeStrFromPath(Path.of(filename));
        String entryName = dateDirName + "/" + targetFileName;
        return Path.of(entryName);
    }

    /**
     * Compute the ordered list of files to download for a block, prioritizing most common files.
     *
     * @param mostCommonRecordFile the most common record file for the block
     * @param mostCommonSidecarFiles the most common sidecar files for the block
     * @param group the full list of listing record files for the block
     * @return the ordered list of files to download
     */
    private static List<ListingRecordFile> computeFilesToDownload(
            ListingRecordFile mostCommonRecordFile,
            ListingRecordFile[] mostCommonSidecarFiles,
            List<ListingRecordFile> group) {
        final List<ListingRecordFile> orderedFilesToDownload = new ArrayList<>();
        if (mostCommonRecordFile != null) orderedFilesToDownload.add(mostCommonRecordFile);
        orderedFilesToDownload.addAll(Arrays.asList(mostCommonSidecarFiles));
        for (ListingRecordFile file : group) {
            switch (file.type()) {
                case RECORD -> {
                    if (!file.equals(mostCommonRecordFile)) orderedFilesToDownload.add(file);
                }
                case RECORD_SIG -> orderedFilesToDownload.add(file);
                case RECORD_SIDECAR -> {
                    boolean isMostCommon = false;
                    for (ListingRecordFile f : mostCommonSidecarFiles)
                        if (file.equals(f)) {
                            isMostCommon = true;
                            break;
                        }
                    if (!isMostCommon) orderedFilesToDownload.add(file);
                }
                default -> throw new RuntimeException("Unsupported file type: " + file.type());
            }
        }
        return orderedFilesToDownload;
    }

    /**
     * Print progress for the day download.
     *
     * @param blocksProcessed the atomic long tracking number of blocks processed
     * @param blocksQueuedForDownload the atomic long tracking number of blocks queued for download
     * @param totalBlocks the total number of blocks to process
     * @param dayIndex the zero-based index of the day within the overall run
     * @param daySharePercent the percent share of this day within the overall run
     * @param overallStartMillis epoch millis when overall run started (for ETA calculations)
     * @param dayString the string representation of the day (e.g., "2023-01-15")
     * @param ready the LocalDateTime of the block just processed
     * @param downloadManager the concurrent download manager (may be null)
     */
    private static void printProgress(
            AtomicLong blocksProcessed,
            final AtomicLong blocksQueuedForDownload,
            int totalBlocks,
            int dayIndex,
            double daySharePercent,
            long overallStartMillis,
            String dayString,
            LocalDateTime ready,
            ConcurrentDownloadManager downloadManager) {
        long processed = blocksProcessed.incrementAndGet();
        double blockFraction = processed / (double) totalBlocks;
        double overallPercent = dayIndex * daySharePercent + blockFraction * daySharePercent;
        long now = System.currentTimeMillis();
        long elapsed = Math.max(1L, now - overallStartMillis);
        long remaining = Long.MAX_VALUE;
        if (overallPercent > 0.0 && overallPercent < 100.0) {
            remaining = (long) (elapsed * (100.0 - overallPercent) / overallPercent);
        } else if (overallPercent >= 100.0) {
            remaining = 0L;
        }
        String msg = dayString + " -Blk q " + blocksQueuedForDownload.get() + " p " + processed + " t " + totalBlocks
                + " (" + ready + ")";
        if (processed == 1 || processed % 50 == 0) {
            printProgressWithStats(downloadManager, overallPercent, msg, remaining);
        }
    }

    /**
     * Print progress including ConcurrentDownloadManager statistics.
     *
     * @param mgr the download manager (maybe null)
     * @param overallPercent overall progress percent
     * @param msg the base message to print
     * @param remaining estimated remaining millis
     */
    private static void printProgressWithStats(
            final ConcurrentDownloadManager mgr, final double overallPercent, final String msg, final long remaining) {
        final String stats;
        if (mgr == null) {
            stats = "";
        } else {
            stats = String.format(
                    " [dl=%s, files=%d, threads=%d/%d]",
                    prettyPrintFileSize(mgr.getBytesDownloaded()),
                    mgr.getObjectsCompleted(),
                    mgr.getCurrentConcurrency(),
                    mgr.getMaxConcurrency());
        }
        printProgressWithEta(overallPercent, msg + stats, remaining);
    }
}
