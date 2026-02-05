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
import java.time.format.DateTimeFormatter;
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
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import org.hiero.block.tools.days.listing.ListingRecordFile;
import org.hiero.block.tools.mirrornode.BlockTimeReader;
import org.hiero.block.tools.mirrornode.DayBlockInfo;
import org.hiero.block.tools.records.model.parsed.ParsedRecordFile;
import org.hiero.block.tools.records.model.unparsed.InMemoryFile;
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

    /** Maximum number of retries for parsing/validation failures (e.g., corrupted downloads). */
    private static final int MAX_PARSE_RETRIES = 3;

    /** Minimum node account ID to try when fetching missing record files */
    private static final int MIN_NODE_ID = 3;

    /** Maximum node account ID to try when fetching missing record files */
    private static final int MAX_NODE_ID = 37;

    /** Formatter for GCS record file timestamps: 2026-02-02T23_58_48.420820000Z */
    private static final DateTimeFormatter GCS_TIMESTAMP_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH_mm_ss.nnnnnnnnn'Z'");

    /** small helper container for pending block downloads */
    private static final class BlockWork {
        /** The block number for this block. */
        final long blockNumber;
        /** Optional block hash from mirror node listing, may be null. Only set for first and last blocks of the day */
        final byte[] blockHashFromMirrorNode;
        /** The block time for this block. */
        final LocalDateTime blockTime;
        /** The ordered list of files to download for this block. The first is the most common record file*/
        final List<ListingRecordFile> orderedFiles;
        /** The futures for the downloads of the files for this block, in the same order as orderedFiles */
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
     * Exception thrown when a block has signature files but no record files in the listing.
     * This typically indicates the GCS listing is stale and needs to be refreshed.
     */
    public static class MissingRecordFileException extends RuntimeException {
        public MissingRecordFileException(String message) {
            super(message);
        }
    }

    /**
     * Checks if an exception is recoverable (partial file should be kept for retry).
     * Recoverable errors include missing files in listing (can be fixed by refreshing listings).
     * Unrecoverable errors include corruption, hash mismatches, etc.
     *
     * @param e the exception to check
     * @return true if the error is recoverable and partial file should be kept
     */
    private static boolean isRecoverableDownloadError(Exception e) {
        // Check the exception and its cause chain for recoverable error types
        Throwable current = e;
        while (current != null) {
            if (current instanceof MissingRecordFileException) {
                return true;
            }
            String msg = current.getMessage();
            if (msg != null) {
                // "Missing record files for block number" is recoverable (stale listing)
                if (msg.contains("Missing record files for block number")) {
                    return true;
                }
                // "no files in listing" is recoverable
                if (msg.contains("no files in listing")) {
                    return true;
                }
            }
            current = current.getCause();
        }
        return false;
    }

    /**
     * Counts how many blocks already exist in the temp directory (for resume progress display).
     */
    private static long countExistingBlocks(Path tempDir) throws IOException {
        if (!Files.exists(tempDir)) return 0;
        try (var files = Files.list(tempDir)) {
            return files.filter(p -> p.getFileName().toString().endsWith(".block"))
                    .count();
        }
    }

    /**
     * Checks if a block's files already exist in the temp directory.
     */
    private static boolean blockExistsInTempDir(Path tempDir, long blockNumber) {
        // Single file per block: {blockNumber}.block
        return Files.exists(tempDir.resolve(blockNumber + ".block"));
    }

    /**
     * Writes all block files to a single temp file for this block.
     * Format: [numFiles:4][filename1Len:4][filename1:bytes][data1Len:4][data1:bytes]...
     */
    private static void writeBlockToTempDir(Path tempDir, long blockNumber, List<InMemoryFile> files)
            throws IOException {
        Path blockFile = tempDir.resolve(blockNumber + ".block");
        try (var dos =
                new java.io.DataOutputStream(new java.io.BufferedOutputStream(Files.newOutputStream(blockFile)))) {
            dos.writeInt(files.size());
            for (InMemoryFile imf : files) {
                String filename = imf.path().getFileName().toString();
                byte[] filenameBytes = filename.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                dos.writeInt(filenameBytes.length);
                dos.write(filenameBytes);
                dos.writeInt(imf.data().length);
                dos.write(imf.data());
            }
        }
    }

    /**
     * Reads a block file and returns the list of InMemoryFiles.
     */
    private static List<InMemoryFile> readBlockFile(Path blockFile) throws IOException {
        List<InMemoryFile> files = new ArrayList<>();
        try (var dis = new java.io.DataInputStream(new java.io.BufferedInputStream(Files.newInputStream(blockFile)))) {
            int numFiles = dis.readInt();
            for (int i = 0; i < numFiles; i++) {
                int filenameLen = dis.readInt();
                byte[] filenameBytes = new byte[filenameLen];
                dis.readFully(filenameBytes);
                String filename = new String(filenameBytes, java.nio.charset.StandardCharsets.UTF_8);
                int dataLen = dis.readInt();
                byte[] data = new byte[dataLen];
                dis.readFully(data);
                files.add(new InMemoryFile(Path.of(filename), data));
            }
        }
        return files;
    }

    /**
     * Combines all block files from temp directory into a tar.zstd archive.
     */
    private static void combineTempDirToTarZstd(Path tempDir, Path outputFile) throws Exception {
        System.out.println("[DOWNLOAD] Combining temp files into " + outputFile.getFileName());
        try (ConcurrentTarZstdWriter writer = new ConcurrentTarZstdWriter(outputFile)) {
            // Get all block files sorted by block number
            List<Path> blockFiles;
            try (var files = Files.list(tempDir)) {
                blockFiles = files.filter(p -> p.getFileName().toString().endsWith(".block"))
                        .sorted((a, b) -> {
                            String nameA = a.getFileName().toString();
                            String nameB = b.getFileName().toString();
                            long blockA = Long.parseLong(nameA.replace(".block", ""));
                            long blockB = Long.parseLong(nameB.replace(".block", ""));
                            return Long.compare(blockA, blockB);
                        })
                        .toList();
            }

            for (Path blockFile : blockFiles) {
                List<InMemoryFile> blockContents = readBlockFile(blockFile);
                for (InMemoryFile imf : blockContents) {
                    writer.putEntry(imf);
                }
            }
        }
        System.out.println("[DOWNLOAD] Combined " + countExistingBlocks(tempDir) + " blocks into archive");
    }

    /**
     * Deletes a directory and all its contents.
     */
    private static void deleteDirectory(Path dir) throws IOException {
        if (!Files.exists(dir)) return;
        try (var files = Files.walk(dir)) {
            files.sorted((a, b) -> -a.compareTo(b)) // reverse order to delete files before dirs
                    .forEach(p -> {
                        try {
                            Files.delete(p);
                        } catch (IOException e) {
                            System.err.println("Failed to delete: " + p);
                        }
                    });
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
        return downloadDay(
                downloadManager,
                dayBlockInfo,
                blockTimeReader,
                listingDir,
                downloadedDaysDir,
                year,
                month,
                day,
                previousRecordFileHash,
                totalDays,
                dayIndex,
                overallStartMillis,
                false); // validate by default
    }

    /**
     *  Download all record files for a given day from GCP, group by block, deduplicate, optionally validate,
     *  and write into a single .tar.zstd file.
     *
     * @param skipValidation if true, skip hash chain validation (useful for catch-up when BlockTimeReader may be stale)
     * @return the hash of the last most common record file for this day, or null if validation was skipped
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
            final long overallStartMillis,
            final boolean skipValidation)
            throws Exception {
        return downloadDay(
                downloadManager,
                dayBlockInfo,
                blockTimeReader,
                listingDir,
                downloadedDaysDir,
                year,
                month,
                day,
                previousRecordFileHash,
                totalDays,
                dayIndex,
                overallStartMillis,
                skipValidation,
                null); // no block callback
    }

    /**
     *  Download all record files for a given day from GCP, group by block, deduplicate, optionally validate,
     *  and write into a single .tar.zstd file.
     *
     * @param skipValidation if true, skip hash chain validation (useful for catch-up when BlockTimeReader may be stale)
     * @param blockCallback optional callback invoked for each block with (blockNumber, signatureCount)
     *                      for signature statistics tracking. Pass null to skip.
     * @return the hash of the last most common record file for this day, or null if validation was skipped
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
            final long overallStartMillis,
            final boolean skipValidation,
            final BiConsumer<Long, Integer> blockCallback)
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
        final Path tempDir = downloadedDaysDir.resolve(dayString + "_temp");
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
        // ensure download directory and temp directory exist
        if (!Files.exists(downloadedDaysDir)) Files.createDirectories(downloadedDaysDir);
        if (!Files.exists(tempDir)) {
            Files.createDirectories(tempDir);
        } else {
            // Temp dir exists from previous failed attempt - will resume
            long existingBlocks = countExistingBlocks(tempDir);
            System.out.println(
                    "[DOWNLOAD] Resuming from temp dir with " + existingBlocks + " blocks already downloaded");
        }
        // print starting progress
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
            // for each block in the day
            for (long blockNumber = firstBlock; blockNumber <= lastBlock; blockNumber++) {
                // Skip blocks that already exist in temp directory (resume support)
                if (blockExistsInTempDir(tempDir, blockNumber)) {
                    blocksQueuedForDownload.incrementAndGet();
                    continue;
                }
                // get block time, from block number
                final LocalDateTime blockTime = blockTimeReader.getBlockLocalDateTime(blockNumber);
                // get list of all the files for this block
                List<ListingRecordFile> group = filesByBlock.get(blockTime);

                // If no files at all in listing for this block, try to fetch directly from GCS
                if (group == null || group.isEmpty()) {
                    System.out.println("[DOWNLOAD] Block " + blockNumber + " has no files in listing, "
                            + "trying to fetch directly from GCS...");
                    ListingRecordFile recordFile = tryFetchRecordFileFromGcs(blockTime, downloadManager);
                    if (recordFile == null) {
                        throw new MissingRecordFileException("Block " + blockNumber + " at time " + blockTime
                                + " has no files in listing and no record file could be found on any node.");
                    }
                    System.out.println("[DOWNLOAD] Found record file: " + recordFile.path());
                    // Create a synthetic group with just the record file
                    group = new ArrayList<>();
                    group.add(recordFile);
                }

                ListingRecordFile mostCommonRecordFile = findMostCommonByType(group, ListingRecordFile.Type.RECORD);
                // Check if there are any RECORD files in the group - if not, try to fetch dynamically
                if (mostCommonRecordFile == null) {
                    boolean hasAnyRecordFile = group.stream().anyMatch(f -> f.type() == ListingRecordFile.Type.RECORD);
                    if (!hasAnyRecordFile) {
                        // Try to dynamically fetch the record file from GCS
                        System.out.println("[DOWNLOAD] Block " + blockNumber + " has no record files in listing, "
                                + "trying to fetch directly from GCS...");
                        mostCommonRecordFile = tryFetchRecordFileFromGcs(blockTime, downloadManager);
                        if (mostCommonRecordFile == null) {
                            long sigCount = group.stream()
                                    .filter(f -> f.type() == ListingRecordFile.Type.RECORD_SIG)
                                    .count();
                            throw new MissingRecordFileException(
                                    "Block " + blockNumber + " at time " + blockTime + " has " + sigCount
                                            + " signature files but no record files could be found on any node.");
                        }
                        System.out.println("[DOWNLOAD] Found record file: " + mostCommonRecordFile.path());
                    }
                }
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
        // Write to temp directory for resumability, combine into tar.zstd at the end
        try {
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
                List<InMemoryFile> inMemoryFilesForWriting = null;
                Exception lastParseException = null;

                for (int parseAttempt = 1; parseAttempt <= MAX_PARSE_RETRIES; parseAttempt++) {
                    try {
                        inMemoryFilesForWriting = new ArrayList<>();
                        for (int i = 0; i < ready.orderedFiles.size(); i++) {
                            final ListingRecordFile lr = ready.orderedFiles.get(i);
                            String filename = lr.path().substring(lr.path().lastIndexOf('/') + 1);
                            try {
                                InMemoryFile downloadedFile =
                                        ready.futures.get(i).join();

                                // Check MD5 and retry if mismatch
                                boolean md5Valid = Md5Checker.checkMd5(lr.md5Hex(), downloadedFile.data());
                                if (!md5Valid) {
                                    clearProgress();
                                    System.err.println("MD5 mismatch for " + (BUCKET_PATH_PREFIX + lr.path())
                                            + ", retrying download...");
                                    // Retry download with built-in retry logic
                                    downloadedFile = downloadFileWithRetry(downloadManager, lr);
                                    if (downloadedFile == null) {
                                        throw new IOException("MD5 mismatch persists after retries for: " + lr.path());
                                    }
                                }

                                byte[] contentBytes = downloadedFile.data();
                                if (filename.endsWith(".gz")) {
                                    // Log compressed data info for debugging
                                    if (lr.type() == ListingRecordFile.Type.RECORD && parseAttempt > 1) {
                                        System.err.println("[DEBUG] Compressed data size: " + contentBytes.length
                                                + ", first 8 bytes: " + bytesToHex(contentBytes, 8));
                                    }
                                    contentBytes = Gzip.ungzipInMemory(contentBytes);
                                    // Log decompressed data info for debugging
                                    if (lr.type() == ListingRecordFile.Type.RECORD && parseAttempt > 1) {
                                        System.err.println("[DEBUG] Decompressed data size: " + contentBytes.length
                                                + ", first 8 bytes: " + bytesToHex(contentBytes, 8));
                                    }
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
                        // validate block hashes (this will throw if parsing fails due to corruption)
                        prevRecordFileHash = validateBlockHashes(
                                ready.blockNumber,
                                inMemoryFilesForWriting,
                                prevRecordFileHash,
                                ready.blockHashFromMirrorNode);
                        // Success - break out of retry loop
                        break;
                    } catch (Exception e) {
                        lastParseException = e;
                        String errorMsg = e.getMessage() != null ? e.getMessage() : "";
                        Throwable cause = e.getCause();
                        String causeMsg = (cause != null && cause.getMessage() != null) ? cause.getMessage() : "";

                        // Check if this is a parsing/corruption error that's worth retrying
                        boolean isParseError = errorMsg.contains("Unsupported record format version")
                                || causeMsg.contains("Unsupported record format version")
                                || errorMsg.contains("Invalid")
                                || causeMsg.contains("Invalid");

                        if (isParseError && parseAttempt < MAX_PARSE_RETRIES) {
                            clearProgress();
                            System.err.println("Parse error for block " + ready.blockNumber + " (attempt "
                                    + parseAttempt + "/" + MAX_PARSE_RETRIES + "): " + errorMsg);

                            // Log the corrupted data info on first failure
                            if (parseAttempt == 1 && !inMemoryFilesForWriting.isEmpty()) {
                                InMemoryFile firstFile = inMemoryFilesForWriting.get(0);
                                System.err.println("[DEBUG] Failed file path: " + firstFile.path());
                                System.err.println("[DEBUG] Failed file size: " + firstFile.data().length
                                        + ", first 8 bytes: " + bytesToHex(firstFile.data(), 8));
                            }

                            // On first retry, re-download the same file (transient error)
                            // On subsequent retries, try an alternate record file from a different node
                            if (parseAttempt == 1) {
                                System.err.println("Re-downloading most common record file and retrying...");
                                if (!ready.orderedFiles.isEmpty()) {
                                    ListingRecordFile mostCommon = ready.orderedFiles.get(0);
                                    String blobName = BUCKET_PATH_PREFIX + mostCommon.path();
                                    try {
                                        CompletableFuture<InMemoryFile> newFuture =
                                                downloadManager.downloadAsync(BUCKET_NAME, blobName);
                                        ready.futures.set(0, newFuture);
                                        newFuture.join();
                                    } catch (Exception redownloadEx) {
                                        System.err.println("Failed to re-download: " + redownloadEx.getMessage());
                                    }
                                }
                            } else {
                                // Find an alternate RECORD file from a different node
                                int alternateIndex = findAlternateRecordFile(ready.orderedFiles, parseAttempt - 1);
                                if (alternateIndex > 0) {
                                    ListingRecordFile alternate = ready.orderedFiles.get(alternateIndex);
                                    System.err.println(
                                            "Trying alternate record file from different node: " + alternate.path());
                                    String blobName = BUCKET_PATH_PREFIX + alternate.path();
                                    try {
                                        CompletableFuture<InMemoryFile> newFuture =
                                                downloadManager.downloadAsync(BUCKET_NAME, blobName);
                                        // Swap the alternate with position 0 so it becomes the "most common"
                                        // for this block's processing
                                        ready.futures.set(0, newFuture);
                                        ready.orderedFiles.set(0, alternate);
                                        newFuture.join();
                                    } catch (Exception redownloadEx) {
                                        System.err.println(
                                                "Failed to download alternate: " + redownloadEx.getMessage());
                                    }
                                } else {
                                    System.err.println("No alternate record files available, retrying same file...");
                                    if (!ready.orderedFiles.isEmpty()) {
                                        ListingRecordFile mostCommon = ready.orderedFiles.get(0);
                                        String blobName = BUCKET_PATH_PREFIX + mostCommon.path();
                                        try {
                                            CompletableFuture<InMemoryFile> newFuture =
                                                    downloadManager.downloadAsync(BUCKET_NAME, blobName);
                                            ready.futures.set(0, newFuture);
                                            newFuture.join();
                                        } catch (Exception redownloadEx) {
                                            System.err.println("Failed to re-download: " + redownloadEx.getMessage());
                                        }
                                    }
                                }
                            }

                            // Small delay before retry
                            try {
                                Thread.sleep(100 * parseAttempt);
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                                throw new IllegalStateException("Interrupted during retry delay", ie);
                            }
                        } else {
                            // Not a parse error or exhausted retries - rethrow
                            throw e;
                        }
                    }
                }

                // If we exhausted all retries without success, throw the last exception
                if (inMemoryFilesForWriting == null && lastParseException != null) {
                    throw new IllegalStateException(
                            "Failed to process block " + ready.blockNumber + " after " + MAX_PARSE_RETRIES
                                    + " attempts",
                            lastParseException);
                }
                // write files to temp directory (for resume support)
                writeBlockToTempDir(tempDir, ready.blockNumber, inMemoryFilesForWriting);
                // invoke block callback for signature statistics if provided
                if (blockCallback != null) {
                    int signatureCount = 0;
                    for (InMemoryFile imf : inMemoryFilesForWriting) {
                        String fileName = imf.path().getFileName().toString();
                        if (fileName.endsWith(".rcd_sig") || fileName.endsWith(".rcs_sig")) {
                            signatureCount++;
                        }
                    }
                    blockCallback.accept(ready.blockNumber, signatureCount);
                    // Debug log for first block of each day
                    if (blocksProcessed.get() == 0) {
                        System.out.println("[STATS-DEBUG] First block callback: block=" + ready.blockNumber + " sigs="
                                + signatureCount + " files=" + inMemoryFilesForWriting.size());
                    }
                }
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
            // Only delete temp directory for corruption/unrecoverable errors
            // Keep temp dir for MissingRecordFileException (recoverable by refreshing listings)
            boolean isRecoverableError = isRecoverableDownloadError(e);
            if (!isRecoverableError) {
                try {
                    deleteDirectory(tempDir);
                    System.out.println("[DOWNLOAD] Deleted temp directory due to unrecoverable error");
                } catch (IOException ignored) {
                }
            } else {
                System.out.println("[DOWNLOAD] Keeping temp directory - error is recoverable, retry will resume");
            }
            throw e;
        }
        // Combine temp files into final tar.zstd archive
        combineTempDirToTarZstd(tempDir, finalOutFile);
        // Clean up temp directory on success
        deleteDirectory(tempDir);
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
        // Find the actual record file (not signature files)
        final InMemoryFile mostCommonRecordFileInMem = inMemoryFilesForWriting.stream()
                .filter(f -> {
                    String name = f.path().getFileName().toString();
                    return name.endsWith(".rcd") || name.endsWith(".rcd.gz");
                })
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "No record file found for block " + blockNum + ", only signature files present"));
        final ParsedRecordFile recordFileInfo = ParsedRecordFile.parse(mostCommonRecordFileInMem);
        byte[] readPreviousBlockHash = recordFileInfo.previousBlockHash();
        byte[] computedBlockHash = recordFileInfo.blockHash();
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
     * Convert first N bytes to hex string for debugging.
     */
    private static String bytesToHex(byte[] bytes, int maxBytes) {
        if (bytes == null || bytes.length == 0) return "(empty)";
        int len = Math.min(bytes.length, maxBytes);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) {
            sb.append(String.format("%02x", bytes[i]));
        }
        return sb.toString();
    }

    /**
     * Find an alternate RECORD file from a different node for retry purposes.
     * The orderedFiles list contains the most common record file first, followed by
     * sidecars, then alternate record files, then signature files.
     *
     * @param orderedFiles the ordered list of files for the block
     * @param alternateIndex which alternate to pick (1-based, e.g., 1 for first alternate)
     * @return the index of an alternate RECORD file, or -1 if none found
     */
    private static int findAlternateRecordFile(List<ListingRecordFile> orderedFiles, int alternateIndex) {
        int foundCount = 0;
        for (int i = 1; i < orderedFiles.size(); i++) { // Start from 1 to skip the most common
            if (orderedFiles.get(i).type() == ListingRecordFile.Type.RECORD) {
                foundCount++;
                if (foundCount == alternateIndex) {
                    return i;
                }
            }
        }
        // If we didn't find the requested alternate index, return the first alternate if any
        for (int i = 1; i < orderedFiles.size(); i++) {
            if (orderedFiles.get(i).type() == ListingRecordFile.Type.RECORD) {
                return i;
            }
        }
        return -1;
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
            targetFileName = "node_" + nodeDir + ".rcd_sig";
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
     * Try to fetch a record file directly from GCS when the listing doesn't have it.
     * This handles the case where GCS listing is eventually consistent but the file exists.
     *
     * @param blockTime the block timestamp
     * @param downloadManager the download manager to use
     * @return a ListingRecordFile if found, or null if not found on any node
     */
    private static ListingRecordFile tryFetchRecordFileFromGcs(
            LocalDateTime blockTime, ConcurrentDownloadManager downloadManager) {
        // Convert blockTime to GCS path format: 2026-02-02T23_58_48.420820000Z
        String timestamp = blockTime.format(GCS_TIMESTAMP_FORMATTER);

        // Try each node from MIN to MAX
        for (int nodeId = MIN_NODE_ID; nodeId <= MAX_NODE_ID; nodeId++) {
            // Try both .rcd.gz and .rcd formats
            String[] extensions = {".rcd.gz", ".rcd"};
            for (String ext : extensions) {
                String relativePath = "record0.0." + nodeId + "/" + timestamp + ext;
                String blobPath = BUCKET_PATH_PREFIX + relativePath;

                try {
                    CompletableFuture<InMemoryFile> future = downloadManager.downloadAsync(BUCKET_NAME, blobPath);
                    InMemoryFile file = future.get(10, TimeUnit.SECONDS);

                    if (file != null && file.data() != null && file.data().length > 0) {
                        // Calculate MD5 hash (required for GCS compatibility)
                        String md5Hex = Md5Checker.computeMd5Hex(file.data());

                        System.out.println("[DOWNLOAD] Found record file on node 0.0." + nodeId + ": " + relativePath);
                        return new ListingRecordFile(relativePath, blockTime, file.data().length, md5Hex);
                    }
                } catch (Exception e) {
                    // File not found on this node, try next
                }
            }
        }

        return null;
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
