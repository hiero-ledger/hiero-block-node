// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.download;

import static org.hiero.block.tools.days.download.DownloadConstants.BUCKET_NAME;
import static org.hiero.block.tools.days.download.DownloadConstants.BUCKET_PATH_PREFIX;
import static org.hiero.block.tools.days.download.DownloadDayUtil.validateBlockHashes;
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
public class DownloadDayLiveImpl {

    /** Maximum number of retries for MD5 mismatch errors. */
    private static final int MAX_MD5_RETRIES = 3;

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
        return Files.exists(tempDir.resolve(blockNumber + ".block"));
    }

    /**
     * Writes all block files to a single temp file for this block.
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
            files.sorted((a, b) -> -a.compareTo(b)).forEach(p -> {
                try {
                    Files.delete(p);
                } catch (IOException e) {
                    System.err.println("Failed to delete: " + p);
                }
            });
        }
    }

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
     * Result of downloading and validating a single block for live mode.
     */
    public static final class BlockDownloadResult {
        public final long blockNumber;
        public final List<InMemoryFile> files;
        public final byte[] newPreviousRecordFileHash;

        public BlockDownloadResult(long blockNumber, List<InMemoryFile> files, byte[] newPreviousRecordFileHash) {
            this.blockNumber = blockNumber;
            this.files = files;
            this.newPreviousRecordFileHash = newPreviousRecordFileHash;
        }
    }

    /**
     * Context for processing a single block download.
     */
    private static final class BlockDownloadContext {
        final long blockNumber;
        final LocalDateTime blockTime;
        final List<ListingRecordFile> group;
        final ListingRecordFile mostCommonRecordFile;
        final ListingRecordFile[] mostCommonSidecarFiles;
        final Set<ListingRecordFile> mostCommonFiles;
        final byte[] blockHashFromMirrorNode;

        BlockDownloadContext(
                long blockNumber,
                LocalDateTime blockTime,
                List<ListingRecordFile> group,
                ListingRecordFile mostCommonRecordFile,
                ListingRecordFile[] mostCommonSidecarFiles,
                Set<ListingRecordFile> mostCommonFiles,
                byte[] blockHashFromMirrorNode) {
            this.blockNumber = blockNumber;
            this.blockTime = blockTime;
            this.group = group;
            this.mostCommonRecordFile = mostCommonRecordFile;
            this.mostCommonSidecarFiles = mostCommonSidecarFiles;
            this.mostCommonFiles = mostCommonFiles;
            this.blockHashFromMirrorNode = blockHashFromMirrorNode;
        }
    }

    /**
     * Loads and groups file listings for a specific block.
     *
     * @return the list of listing files for the requested block
     * @throws IllegalStateException if no files are found for the block
     */
    private static List<ListingRecordFile> loadBlockFilesMetadata(
            final Path listingDir,
            final BlockTimeReader blockTimeReader,
            final int year,
            final int month,
            final int day,
            final long blockNumber)
            throws Exception {
        final List<ListingRecordFile> allDaysFiles = loadRecordsFileForDay(listingDir, year, month, day);
        final Map<LocalDateTime, List<ListingRecordFile>> filesByBlock =
                allDaysFiles.stream().collect(Collectors.groupingBy(ListingRecordFile::timestamp));

        final LocalDateTime blockTime = blockTimeReader.getBlockLocalDateTime(blockNumber);
        final List<ListingRecordFile> group = filesByBlock.get(blockTime);

        if (group == null || group.isEmpty()) {
            throw new IllegalStateException("Missing record files for block number " + blockNumber + " at time "
                    + blockTime + " on " + year + "-" + month + "-" + day);
        }

        return group;
    }

    /**
     * Selects the most common record and sidecar files from a group of listings.
     *
     * @return a context object containing the selected files and metadata
     */
    private static BlockDownloadContext selectMostCommonFiles(
            final List<ListingRecordFile> group,
            final long blockNumber,
            final LocalDateTime blockTime,
            final DayBlockInfo dayBlockInfo) {

        final ListingRecordFile mostCommonRecordFile = findMostCommonByType(group, ListingRecordFile.Type.RECORD);

        // Mirror historic behaviour: use a single most common sidecar candidate
        final ListingRecordFile mostCommonSidecarFile =
                findMostCommonByType(group, ListingRecordFile.Type.RECORD_SIDECAR);
        final ListingRecordFile[] mostCommonSidecarFiles;

        if (mostCommonSidecarFile == null) {
            mostCommonSidecarFiles = new ListingRecordFile[0];
        } else {
            mostCommonSidecarFiles = new ListingRecordFile[] {mostCommonSidecarFile};
        }

        final Set<ListingRecordFile> mostCommonFiles = new HashSet<>();
        if (mostCommonRecordFile != null) {
            mostCommonFiles.add(mostCommonRecordFile);
        }
        if (mostCommonSidecarFile != null) {
            mostCommonFiles.add(mostCommonSidecarFile);
        }

        // Extract block hash from mirror node (only for first/last blocks)
        final byte[] blockHashFromMirrorNode = extractBlockHashFromMirrorNode(blockNumber, dayBlockInfo);

        return new BlockDownloadContext(
                blockNumber,
                blockTime,
                group,
                mostCommonRecordFile,
                mostCommonSidecarFiles,
                mostCommonFiles,
                blockHashFromMirrorNode);
    }

    /**
     * Extracts the expected block hash from mirror node metadata.
     *
     * @return the block hash as bytes, or null if not available
     */
    public static byte[] extractBlockHashFromMirrorNode(final long blockNumber, final DayBlockInfo dayBlockInfo) {
        if (dayBlockInfo == null) {
            return null;
        }

        final long firstBlock = dayBlockInfo.firstBlockNumber;
        final long lastBlock = dayBlockInfo.lastBlockNumber;

        String hashHex = null;
        if (blockNumber == firstBlock && dayBlockInfo.firstBlockHash != null) {
            hashHex = dayBlockInfo.firstBlockHash;
        } else if (blockNumber == lastBlock && dayBlockInfo.lastBlockHash != null) {
            hashHex = dayBlockInfo.lastBlockHash;
        }

        if (hashHex == null) {
            return null;
        }

        final String hexStr = hashHex.startsWith("0x") ? hashHex.substring(2) : hashHex;
        return HexFormat.of().parseHex(hexStr);
    }

    /**
     * Downloads and processes all files for a block (MD5 validation, ungzip, path computation).
     *
     * @return list of processed in-memory files ready for validation and writing
     */
    private static List<InMemoryFile> downloadAndProcessFiles(
            final ConcurrentDownloadManager downloadManager, final BlockDownloadContext context) throws IOException {

        final List<ListingRecordFile> orderedFilesToDownload =
                computeFilesToDownload(context.mostCommonRecordFile, context.mostCommonSidecarFiles, context.group);

        final List<InMemoryFile> inMemoryFilesForWriting = new ArrayList<>();
        for (ListingRecordFile lr : orderedFilesToDownload) {
            String filename = lr.path().substring(lr.path().lastIndexOf('/') + 1);
            try {
                InMemoryFile downloadedFile = downloadFileWithRetry(downloadManager, lr);
                if (downloadedFile == null) {
                    // Signature file with persistent MD5 mismatch, skip it
                    continue;
                }

                byte[] contentBytes = downloadedFile.data();
                if (filename.endsWith(".gz")) {
                    contentBytes = Gzip.ungzipInMemory(contentBytes);
                    filename = filename.replaceAll("\\.gz$", "");
                }

                final Path newFilePath = computeNewFilePath(lr, context.mostCommonFiles, filename);
                inMemoryFilesForWriting.add(new InMemoryFile(newFilePath, contentBytes));
            } catch (EOFException eofe) {
                System.err.println("Warning: Skipping corrupted gzip file [" + filename + "] for block "
                        + context.blockNumber + " time " + context.blockTime + ": " + eofe.getMessage());
            }
        }

        return inMemoryFilesForWriting;
    }

    /**
     * Validates the block hash chain and returns the new running hash.
     *
     * @return the updated running hash to be used for the next block
     * @throws Exception if validation fails
     */
    private static byte[] validateBlockHashChain(
            final long blockNumber,
            final List<InMemoryFile> files,
            final byte[] previousRecordFileHash,
            final byte[] blockHashFromMirrorNode)
            throws Exception {

        try {
            return validateBlockHashes(blockNumber, files, previousRecordFileHash, blockHashFromMirrorNode);
        } catch (Exception e) {
            System.err.println(
                    "[download-live] Hash-chain validation failed for block " + blockNumber + ": " + e.getMessage());
            throw e;
        }
    }

    /**
     * Download and validate a single block for a given day using the same listing-based
     * grouping and MD5/ungzip logic as {@link #downloadDay}, but without writing into
     * the day's tar.zstd. This is intended for live mode, where the caller will decide
     * how to persist the validated files (e.g. writing into a per-day folder, appending
     * to a tar file, etc.).
     *
     * This method:
     *  - loads the day's listing file
     *  - finds the ListingRecordFile group for the given blockNumber based on block time
     *  - selects the most common record + sidecar files
     *  - downloads all required files with MD5 validation and retry
     *  - ungzips .gz files and computes canonical entry names via {@link #computeNewFilePath}
     *  - validates block hashes via {@link DownloadDayUtil#validateBlockHashes}
     *
     * On success it returns the in-memory files and the updated previousRecordFileHash
     * that should be passed into the next block/day.
     *
     * @param downloadManager the concurrent download manager to use
     * @param dayBlockInfo the block info for the day
     * @param blockTimeReader the block time reader to get block times
     * @param listingDir directory where listing files are stored
     * @param year the year (e.g., 2023) to download
     * @param month the month (1-12) to download
     * @param day the day of month (1-31) to download
     * @param blockNumber the block number to download and validate
     * @param previousRecordFileHash the running hash from the previous record file (may be null for first block)
     * @return a {@link BlockDownloadResult} containing the in-memory files and updated running hash
     * @throws Exception on any error
     */
    public static BlockDownloadResult downloadSingleBlockForLive(
            final ConcurrentDownloadManager downloadManager,
            final DayBlockInfo dayBlockInfo,
            final BlockTimeReader blockTimeReader,
            final Path listingDir,
            final int year,
            final int month,
            final int day,
            final long blockNumber,
            final byte[] previousRecordFileHash)
            throws Exception {
        return downloadSingleBlockForLive(
                downloadManager,
                dayBlockInfo,
                blockTimeReader,
                listingDir,
                year,
                month,
                day,
                blockNumber,
                previousRecordFileHash,
                true);
    }

    /**
     * Download and optionally validate a single block for a given day.
     *
     * @param skipHashValidation if true, skip hash chain validation for faster catch-up
     * @see #downloadSingleBlockForLive(ConcurrentDownloadManager, DayBlockInfo, BlockTimeReader, Path, int, int, int, long, byte[])
     */
    public static BlockDownloadResult downloadSingleBlockForLive(
            final ConcurrentDownloadManager downloadManager,
            final DayBlockInfo dayBlockInfo,
            final BlockTimeReader blockTimeReader,
            final Path listingDir,
            final int year,
            final int month,
            final int day,
            final long blockNumber,
            final byte[] previousRecordFileHash,
            final boolean skipHashValidation)
            throws Exception {

        // Step 1: Load file metadata for the block
        final List<ListingRecordFile> group =
                loadBlockFilesMetadata(listingDir, blockTimeReader, year, month, day, blockNumber);

        // Step 2: Select most common files and build context
        final LocalDateTime blockTime = blockTimeReader.getBlockLocalDateTime(blockNumber);
        final BlockDownloadContext context = selectMostCommonFiles(group, blockNumber, blockTime, dayBlockInfo);

        // Step 3: Download and process all files
        final List<InMemoryFile> inMemoryFilesForWriting = downloadAndProcessFiles(downloadManager, context);

        // Step 4: Validate block hash chain (if not skipped)
        final byte[] newPrevRecordFileHash;
        if (skipHashValidation) {
            // Skip validation - just return the previous hash
            newPrevRecordFileHash = previousRecordFileHash;
        } else {
            newPrevRecordFileHash = validateBlockHashChain(
                    blockNumber, inMemoryFilesForWriting, previousRecordFileHash, context.blockHashFromMirrorNode);
        }

        return new BlockDownloadResult(blockNumber, inMemoryFilesForWriting, newPrevRecordFileHash);
    }

    /**
     * Context object holding day download state and configuration.
     */
    private static class DayDownloadContext {
        final ConcurrentDownloadManager downloadManager;
        final DayBlockInfo dayBlockInfo;
        final BlockTimeReader blockTimeReader;
        final Map<LocalDateTime, List<ListingRecordFile>> filesByBlock;
        final Set<ListingRecordFile> mostCommonFiles;
        final String dayString;
        final Path finalOutFile;
        final Path tempDir;
        final long totalDays;
        final int dayIndex;
        final long overallStartMillis;
        final double daySharePercent;
        final int totalBlocks;

        DayDownloadContext(
                ConcurrentDownloadManager downloadManager,
                DayBlockInfo dayBlockInfo,
                BlockTimeReader blockTimeReader,
                Map<LocalDateTime, List<ListingRecordFile>> filesByBlock,
                Set<ListingRecordFile> mostCommonFiles,
                String dayString,
                Path tempDir,
                Path finalOutFile,
                long totalDays,
                int dayIndex,
                long overallStartMillis) {
            this.downloadManager = downloadManager;
            this.dayBlockInfo = dayBlockInfo;
            this.blockTimeReader = blockTimeReader;
            this.filesByBlock = filesByBlock;
            this.mostCommonFiles = mostCommonFiles;
            this.dayString = dayString;
            this.tempDir = tempDir;
            this.finalOutFile = finalOutFile;
            this.totalDays = totalDays;
            this.dayIndex = dayIndex;
            this.overallStartMillis = overallStartMillis;
            this.daySharePercent = (totalDays <= 0) ? 100.0 : (100.0 / totalDays);
            this.totalBlocks = (int) (dayBlockInfo.lastBlockNumber - dayBlockInfo.firstBlockNumber + 1);
        }
    }

    /**
     * Prepares the day download by loading files, checking if already exists, and setting up output.
     * Returns null if the day should be skipped (already exists).
     */
    private static DayDownloadContext prepareDayDownload(
            final ConcurrentDownloadManager downloadManager,
            final DayBlockInfo dayBlockInfo,
            final BlockTimeReader blockTimeReader,
            final Path listingDir,
            final Path downloadedDaysDir,
            final int year,
            final int month,
            final int day,
            final long totalDays,
            final int dayIndex,
            final long overallStartMillis)
            throws Exception {
        // Load record file listings and group by timestamp
        final List<ListingRecordFile> allDaysFiles = loadRecordsFileForDay(listingDir, year, month, day);
        final Map<LocalDateTime, List<ListingRecordFile>> filesByBlock =
                allDaysFiles.stream().collect(Collectors.groupingBy(ListingRecordFile::timestamp));

        // Prepare output files
        final String dayString = String.format("%04d-%02d-%02d", year, month, day);
        final Path finalOutFile = downloadedDaysDir.resolve(dayString + ".tar.zstd");
        final Path tempDir = downloadedDaysDir.resolve(dayString + "_temp");

        // Early exit if already present
        if (Files.exists(finalOutFile)) {
            double daySharePercent = (totalDays <= 0) ? 100.0 : (100.0 / totalDays);
            double overallPercent = dayIndex * daySharePercent + daySharePercent;
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

        // Setup output directory and temp directory
        if (!Files.exists(downloadedDaysDir)) Files.createDirectories(downloadedDaysDir);
        if (!Files.exists(tempDir)) {
            Files.createDirectories(tempDir);
        } else {
            // Temp dir exists from previous failed attempt - will resume
            long existingBlocks = countExistingBlocks(tempDir);
            System.out.println(
                    "[DOWNLOAD] Resuming from temp dir with " + existingBlocks + " blocks already downloaded");
        }

        double daySharePercent = (totalDays <= 0) ? 100.0 : (100.0 / totalDays);
        double startingPercent = dayIndex * daySharePercent;
        printProgressWithStats(
                downloadManager,
                startingPercent,
                dayString + " :: Processing " + allDaysFiles.size() + " files",
                Long.MAX_VALUE);

        // Compute most common files
        final Set<ListingRecordFile> mostCommonFiles = computeMostCommonFiles(filesByBlock);

        return new DayDownloadContext(
                downloadManager,
                dayBlockInfo,
                blockTimeReader,
                filesByBlock,
                mostCommonFiles,
                dayString,
                tempDir,
                finalOutFile,
                totalDays,
                dayIndex,
                overallStartMillis);
    }

    /**
     * Computes the set of most common record and sidecar files across all blocks.
     */
    private static Set<ListingRecordFile> computeMostCommonFiles(
            Map<LocalDateTime, List<ListingRecordFile>> filesByBlock) {
        final Set<ListingRecordFile> mostCommonFiles = new HashSet<>();
        filesByBlock.values().forEach(list -> {
            final ListingRecordFile mostCommonRecordFile = findMostCommonByType(list, ListingRecordFile.Type.RECORD);
            final ListingRecordFile mostCommonSidecarFile =
                    findMostCommonByType(list, ListingRecordFile.Type.RECORD_SIDECAR);
            if (mostCommonRecordFile != null) mostCommonFiles.add(mostCommonRecordFile);
            if (mostCommonSidecarFile != null) mostCommonFiles.add(mostCommonSidecarFile);
        });
        return mostCommonFiles;
    }

    /**
     * Queues block downloads in the background, creating BlockWork entries and starting async downloads.
     */
    private static CompletableFuture<Void> queueBlockDownloads(
            final DayDownloadContext context,
            final int year,
            final int month,
            final int day,
            final LinkedBlockingDeque<BlockWork> pending,
            final AtomicLong blocksQueuedForDownload) {
        return CompletableFuture.runAsync(() -> {
            for (long blockNumber = context.dayBlockInfo.firstBlockNumber;
                    blockNumber <= context.dayBlockInfo.lastBlockNumber;
                    blockNumber++) {
                // Skip blocks that already exist in temp directory (resume support)
                if (blockExistsInTempDir(context.tempDir, blockNumber)) {
                    blocksQueuedForDownload.incrementAndGet();
                    continue;
                }
                final LocalDateTime blockTime = context.blockTimeReader.getBlockLocalDateTime(blockNumber);
                final List<ListingRecordFile> group = context.filesByBlock.get(blockTime);
                if (group == null || group.isEmpty()) {
                    throw new IllegalStateException("Missing record files for block number " + blockNumber + " at time "
                            + blockTime + " on " + year + "-" + month + "-" + day);
                }
                final ListingRecordFile mostCommonRecordFile =
                        findMostCommonByType(group, ListingRecordFile.Type.RECORD);
                final ListingRecordFile[] mostCommonSidecarFiles = findMostCommonSidecars(group);
                final List<ListingRecordFile> orderedFilesToDownload =
                        computeFilesToDownload(mostCommonRecordFile, mostCommonSidecarFiles, group);

                // Get mirror node block hash if available (only for first and last blocks of day)
                byte[] blockHashFromMirrorNode = null;
                if (blockNumber == context.dayBlockInfo.firstBlockNumber
                        && context.dayBlockInfo.firstBlockHash != null) {
                    String hexStr = context.dayBlockInfo.firstBlockHash.startsWith("0x")
                            ? context.dayBlockInfo.firstBlockHash.substring(2)
                            : context.dayBlockInfo.firstBlockHash;
                    blockHashFromMirrorNode = HexFormat.of().parseHex(hexStr);
                } else if (blockNumber == context.dayBlockInfo.lastBlockNumber
                        && context.dayBlockInfo.lastBlockHash != null) {
                    String hexStr = context.dayBlockInfo.lastBlockHash.startsWith("0x")
                            ? context.dayBlockInfo.lastBlockHash.substring(2)
                            : context.dayBlockInfo.lastBlockHash;
                    blockHashFromMirrorNode = HexFormat.of().parseHex(hexStr);
                }

                // Create BlockWork and start downloads for its files
                final BlockWork bw =
                        new BlockWork(blockNumber, blockHashFromMirrorNode, blockTime, orderedFilesToDownload);
                for (ListingRecordFile lr : orderedFilesToDownload) {
                    final String blobName = BUCKET_PATH_PREFIX + lr.path();
                    bw.futures.add(context.downloadManager.downloadAsync(BUCKET_NAME, blobName));
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
    }

    /**
     * Processes downloaded files for a block: validates MD5, ungzips if needed, computes new file paths.
     */
    private static List<InMemoryFile> processDownloadedFiles(
            final BlockWork blockWork,
            final Set<ListingRecordFile> mostCommonFiles,
            final ConcurrentDownloadManager downloadManager)
            throws IOException {
        final List<InMemoryFile> inMemoryFilesForWriting = new ArrayList<>();
        for (int i = 0; i < blockWork.orderedFiles.size(); i++) {
            final ListingRecordFile lr = blockWork.orderedFiles.get(i);
            String filename = lr.path().substring(lr.path().lastIndexOf('/') + 1);
            try {
                InMemoryFile downloadedFile = blockWork.futures.get(i).join();

                // Check MD5 and retry if mismatch
                boolean md5Valid = Md5Checker.checkMd5(lr.md5Hex(), downloadedFile.data());
                if (!md5Valid) {
                    clearProgress();
                    System.err.println(
                            "MD5 mismatch for " + (BUCKET_PATH_PREFIX + lr.path()) + ", retrying download...");
                    downloadedFile = downloadFileWithRetry(downloadManager, lr);
                    if (downloadedFile == null) {
                        continue; // Skip signature files with persistent MD5 mismatch
                    }
                }

                byte[] contentBytes = downloadedFile.data();
                if (filename.endsWith(".gz")) {
                    contentBytes = Gzip.ungzipInMemory(contentBytes);
                    filename = filename.replaceAll("\\.gz$", "");
                }
                final Path newFilePath = computeNewFilePath(lr, mostCommonFiles, filename);
                inMemoryFilesForWriting.add(new InMemoryFile(newFilePath, contentBytes));
            } catch (Exception eofe) {
                System.err.println("Warning: Skipping corrupted gzip file [" + filename + "] for block "
                        + blockWork.blockNumber + " time " + blockWork.blockTime + ": " + eofe.getMessage());
            }
        }
        return inMemoryFilesForWriting;
    }

    /**
     * Validates block hashes and writes files to the tar archive.
     */
    private static byte[] validateAndWriteBlock(
            final BlockWork blockWork,
            final List<InMemoryFile> files,
            final byte[] prevRecordFileHash,
            final ConcurrentTarZstdWriter writer) {
        final byte[] newPrevHash = validateBlockHashes(
                blockWork.blockNumber, files, prevRecordFileHash, blockWork.blockHashFromMirrorNode);
        for (InMemoryFile imf : files) writer.putEntry(imf);
        return newPrevHash;
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
        // Prepare the day download (loads files, checks if exists, sets up output)
        final DayDownloadContext context = prepareDayDownload(
                downloadManager,
                dayBlockInfo,
                blockTimeReader,
                listingDir,
                downloadedDaysDir,
                year,
                month,
                day,
                totalDays,
                dayIndex,
                overallStartMillis);

        // Early exit if day already exists
        if (context == null) {
            return null;
        }

        byte[] prevRecordFileHash = previousRecordFileHash;
        final AtomicLong blocksProcessed = new AtomicLong(0);
        final AtomicLong blocksQueuedForDownload = new AtomicLong(0);
        final LinkedBlockingDeque<BlockWork> pending = new LinkedBlockingDeque<>(1000);

        // Queue downloads in background thread
        final CompletableFuture<Void> downloadQueueingFuture =
                queueBlockDownloads(context, year, month, day, pending, blocksQueuedForDownload);

        // Process and write blocks as they complete to temp directory
        try {
            while (!downloadQueueingFuture.isDone() || !pending.isEmpty()) {
                // Poll for next completed block
                final BlockWork ready;
                try {
                    ready = pending.pollFirst(1, TimeUnit.SECONDS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting for pending block", ie);
                }
                if (ready == null) {
                    continue; // No work available right now
                }

                // Wait for downloads to complete for this block
                try {
                    CompletableFuture.allOf(ready.futures.toArray(new CompletableFuture[0]))
                            .join();
                } catch (CompletionException ce) {
                    clearProgress();
                    ce.printStackTrace();
                    throw new RuntimeException("Failed downloading block " + ready.blockTime, ce.getCause());
                }

                // Process downloaded files (MD5 validation, ungzip, compute paths)
                final List<InMemoryFile> inMemoryFilesForWriting =
                        processDownloadedFiles(ready, context.mostCommonFiles, downloadManager);

                // Validate block hashes and write to temp directory
                prevRecordFileHash = validateBlockHashes(
                        ready.blockNumber, inMemoryFilesForWriting, prevRecordFileHash, ready.blockHashFromMirrorNode);
                writeBlockToTempDir(context.tempDir, ready.blockNumber, inMemoryFilesForWriting);

                // Print progress
                printProgress(
                        blocksProcessed,
                        blocksQueuedForDownload,
                        context.totalBlocks,
                        context.dayIndex,
                        context.daySharePercent,
                        context.overallStartMillis,
                        context.dayString,
                        ready.blockTime,
                        context.downloadManager);
            }
            // Ensure producer exceptions are propagated
            downloadQueueingFuture.join();
        } catch (Exception e) {
            clearProgress();
            e.printStackTrace();
            // Keep temp directory for resume on next attempt
            System.out.println("[DOWNLOAD] Keeping temp directory for resume: " + context.tempDir);
            throw e;
        }

        // Combine temp files into final tar.zstd archive
        combineTempDirToTarZstd(context.tempDir, context.finalOutFile);
        // Clean up temp directory on success
        deleteDirectory(context.tempDir);

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
     * Validates block hashes using the DownloadDayUtil utility.
     * This is a public wrapper for external callers.
     *
     * @param blockNumber the block number being validated
     * @param files the in-memory files for the block
     * @param previousRecordFileHash the running hash from the previous block (may be null for first block)
     * @param blockHashFromMirrorNode the expected block hash from mirror node (may be null)
     * @return the updated running hash to be used for the next block
     */
    public static byte[] validateBlockHashes(
            final long blockNumber,
            final List<InMemoryFile> files,
            final byte[] previousRecordFileHash,
            final byte[] blockHashFromMirrorNode) {
        return DownloadDayUtil.validateBlockHashes(blockNumber, files, previousRecordFileHash, blockHashFromMirrorNode);
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
    public static Path computeNewFilePath(ListingRecordFile lr, Set<ListingRecordFile> mostCommonFiles, String filename)
            throws IOException {
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
    public static List<ListingRecordFile> computeFilesToDownload(
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
