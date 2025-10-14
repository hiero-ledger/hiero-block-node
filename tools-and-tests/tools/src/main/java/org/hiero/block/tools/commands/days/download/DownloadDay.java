// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands.days.download;

import static org.hiero.block.tools.commands.days.download.DownloadConstants.BUCKET_NAME;
import static org.hiero.block.tools.commands.days.download.DownloadConstants.BUCKET_PATH_PREFIX;
import static org.hiero.block.tools.commands.days.download.DownloadConstants.GCP_PROJECT_ID;
import static org.hiero.block.tools.commands.days.listing.DayListingFileReader.loadRecordsFileForDay;
import static org.hiero.block.tools.records.RecordFileUtils.extractRecordFileTimeStrFromPath;
import static org.hiero.block.tools.records.RecordFileUtils.findMostCommonByType;
import static org.hiero.block.tools.records.RecordFileUtils.findMostCommonSidecars;
import static org.hiero.block.tools.utils.PrettyPrint.clearProgress;
import static org.hiero.block.tools.utils.PrettyPrint.printProgressWithEta;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.hiero.block.tools.commands.days.listing.ListingRecordFile;
import org.hiero.block.tools.records.InMemoryFile;
import org.hiero.block.tools.records.RecordFileInfo;
import org.hiero.block.tools.utils.ConcurrentTarZstdWriter;
import org.hiero.block.tools.utils.Gzip;
import org.hiero.block.tools.utils.Md5Checker;

/**
 * Download all record files for a given day from GCP, group by block, deduplicate, validate,
 * and write into a single .tar.zstd file.
 */
@SuppressWarnings("CallToPrintStackTrace")
public class DownloadDay {
    /** GCP BlobSourceOption to use userProject for billing */
    public static final com.google.cloud.storage.Storage.BlobSourceOption BLOB_SOURCE_OPTION =
            com.google.cloud.storage.Storage.BlobSourceOption.userProject(DownloadConstants.GCP_PROJECT_ID);

    /**
     *  Download all record files for a given day from GCP, group by block, deduplicate, validate,
     *  and write into a single .tar.zstd file.
     *
     * @param listingDir directory where listing files are stored
     * @param downloadedDaysDir directory where downloaded .tar.zstd files are stored
     * @param year the year (e.g., 2023) to download
     * @param month the month (1-12) to download
     * @param day the day of month (1-31) to download
     * @param totalDays total number of days in the overall download run (used to split 100% across days)
     * @param dayIndex zero-based index of this day within the overall run (0..totalDays-1)
     * @param threads number of threads to use for processing blocks in parallel
     * @param previousRecordFileHash the hash of the previous block's most common record file, null if first
     *                               being processed
     * @param overallStartMillis epoch millis when overall run started (for ETA calculations)
     * @return the hash of the last most common record file for this day, to be passed as previousRecordFileHash for next day
     * @throws Exception on any error
     */
    public static byte[] downloadDay(
            final Path listingDir,
            final Path downloadedDaysDir,
            final int year,
            final int month,
            final int day,
            final long totalDays,
            final int dayIndex,
            final int threads,
            final byte[] previousRecordFileHash,
            final long overallStartMillis)
            throws Exception {
        byte[] prevRecordFileHash = previousRecordFileHash;
        final List<ListingRecordFile> allDaysFiles = loadRecordsFileForDay(listingDir, year, month, day);
        // group by RecordFile.block and process each group
        final Map<LocalDateTime, List<ListingRecordFile>> filesByBlock =
                allDaysFiles.stream().collect(Collectors.groupingBy(ListingRecordFile::timestamp));
        // for each group, download the files and write them into a single .tar.zstd file
        final String dayString = String.format("%04d-%02d-%02d", year, month, day);
        // target output tar.zstd path
        final Path finalOutFile = downloadedDaysDir.resolve(dayString + ".tar.zstd");
        final Path partialOutFile = downloadedDaysDir.resolve(dayString + ".tar.zstd_partial");
        // If the final output already exists, bail out early (higher-level command may also check)
        if (Files.exists(finalOutFile)) {
            // compute percent for this day as fully complete
            double daySharePercent = (totalDays <= 0) ? 100.0 : (100.0 / totalDays);
            double overallPercent = dayIndex * daySharePercent + daySharePercent; // this day done
            long remaining = Long.MAX_VALUE;
            long now = System.currentTimeMillis();
            long elapsed = Math.max(1L, now - overallStartMillis);
            if (overallPercent > 0.0 && overallPercent < 100.0) {
                remaining = (long) (elapsed * (100.0 - overallPercent) / overallPercent);
            }
            printProgressWithEta(
                    overallPercent, dayString + " :: Skipping as exists " + allDaysFiles.size() + " files", remaining);
            return null;
        }
        // ensure output dir exists
        if (!Files.exists(downloadedDaysDir)) Files.createDirectories(downloadedDaysDir);
        // remove stale partial
        try {
            Files.deleteIfExists(partialOutFile);
        } catch (IOException ignored) {
        }
        // print starting message (showing day share percent and unknown ETA initially)
        double daySharePercent = (totalDays <= 0) ? 100.0 : (100.0 / totalDays);
        double startingPercent = dayIndex * daySharePercent;
        long remainingMillisUnknown = Long.MAX_VALUE;
        printProgressWithEta(
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
        // prepare list of blocks sorted
        final List<LocalDateTime> sortedBlocks =
                filesByBlock.keySet().stream().sorted().toList();
        // Use Storage client to stream each blob into memory, check MD5, (ungzip if needed), and write to tar
        final Storage storage =
                StorageOptions.newBuilder().setProjectId(GCP_PROJECT_ID).build().getService();
        // precompute total blocks count to drive per-block progress
        int totalBlocks = sortedBlocks.size();
        AtomicLong blocksProcessed = new AtomicLong(0);
        // create executor for parallel downloads
        // download, validate, and write files in block order
        try (final ExecutorService exec = Executors.newFixedThreadPool(Math.max(1, threads));
                ConcurrentTarZstdWriter writer = new ConcurrentTarZstdWriter(finalOutFile)) {
            final Map<LocalDateTime, Future<List<InMemoryFile>>> futures = new ConcurrentHashMap<>();
            // submit a task per block to download and prepare in-memory files
            for (LocalDateTime ts : sortedBlocks) {
                final List<ListingRecordFile> group = filesByBlock.get(ts);
                if (group == null || group.isEmpty()) continue;
                final ListingRecordFile mostCommonRecordFile =
                        findMostCommonByType(group, ListingRecordFile.Type.RECORD);
                final ListingRecordFile[] mostCommonSidecarFiles = findMostCommonSidecars(group);
                // build ordered list of files to download, including the most common ones first, then all signatures,
                // and other uncommon files
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
                // submit downloader task that returns in-memory files for this block
                futures.put(ts, exec.submit(() -> downloadBlock(orderedFilesToDownload, storage, mostCommonFiles)));
            }

            // iterate blocks in order, wait for downloads to complete, validate, compute hash, and enqueue entries
            for (int blockIndex = 0; blockIndex < sortedBlocks.size(); blockIndex++) {
                final LocalDateTime ts = sortedBlocks.get(blockIndex);
                final Future<List<InMemoryFile>> f = futures.get(ts);
                if (f == null) throw new Exception("no files for this block: " + ts); // should not happen
                List<InMemoryFile> resultInMemFiles;
                try {
                    resultInMemFiles = f.get();
                } catch (ExecutionException ee) {
                    // clear progress so stacktrace prints cleanly
                    clearProgress();
                    ee.getCause().printStackTrace();
                    throw new RuntimeException("Failed downloading block " + ts, ee.getCause());
                }
                // first is always the most common record file
                final InMemoryFile mostCommonRecordFileInMem = resultInMemFiles.getFirst();
                // validate time period
                prevRecordFileHash = validateBlock(prevRecordFileHash, resultInMemFiles, mostCommonRecordFileInMem);
                // enqueue entries in the same block order to preserve tar ordering
                final String blockStr = ts.toString();
                for (InMemoryFile imf : resultInMemFiles) {
                    writer.putEntry(imf);
                }

                // update blocks processed and print progress with ETA, showing every block
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
                String msg = dayString + " :: Block " + (blockIndex + 1) + "/" + totalBlocks + " (" + blockStr + ")";
                if (blockIndex % 500 == 0) printProgressWithEta(overallPercent, msg, remaining);
            }
        } catch (Exception e) {
            // clear any active progress line before printing errors
            clearProgress();
            e.printStackTrace();
            // on any error, delete partial file
            try {
                Files.deleteIfExists(partialOutFile);
            } catch (IOException ignored) {
            }
            throw e;
        }
        return prevRecordFileHash;
    }

    /**
     * Download the given list of files from GCP, check MD5, ungzip if needed, and prepare in-memory files with
     * correct target paths.
     *
     * @param orderedFilesToDownload the list of ListingRecordFile objects to download, in the order to write them. The
     *                               first file in the list is always the most common record file for this block.
     * @param storage the GCP Storage client
     * @param mostCommonFiles the set of most common files to use for naming
     * @return a list of in-memory files, the first is allways the most common record file
     * @throws Exception on any error
     */
    private static List<InMemoryFile> downloadBlock(
            final List<ListingRecordFile> orderedFilesToDownload,
            Storage storage,
            Set<ListingRecordFile> mostCommonFiles)
            throws Exception {
        final List<InMemoryFile> memFiles = new ArrayList<>();
        for (ListingRecordFile lr : orderedFilesToDownload) {
            final String blobName = BUCKET_PATH_PREFIX + lr.path();
            final byte[] rawBytes = storage.readAllBytes(BUCKET_NAME, blobName, BLOB_SOURCE_OPTION);
            // MD5 check
            if (!Md5Checker.checkMd5(lr.md5Hex(), rawBytes)) {
                throw new IOException("MD5 mismatch for blob " + blobName);
            }
            // unzip if needed
            byte[] contentBytes = rawBytes;
            String filename = lr.path().substring(lr.path().lastIndexOf('/') + 1);
            if (filename.endsWith(".gz")) {
                contentBytes = Gzip.ungzipInMemory(contentBytes);
                filename = filename.replaceAll("\\.gz$", "");
            }
            // determine target path within tar
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
            // use block dir from one of the files in this group (they all share the same block)
            String dateDirName = extractRecordFileTimeStrFromPath(Path.of(filename));
            String entryName = dateDirName + "/" + targetFileName;
            memFiles.add(new InMemoryFile(Path.of(entryName), contentBytes));
        }
        return memFiles;
    }

    /**
     * Validate the downloaded files hash correctly and form a valid Hedera blockchain.
     *
     * @param previousRecordFileHash the SHA384 hash of the previous block's most common record file, null if first
     *                               block of day
     * @param files the list of InMemoryFile objects for the current block
     * @param mostCommonRecordFile the most common record file for the current block
     * @return computed SHA384 hash of the current block's most common record file, to be passed as
     *         previousRecordFileHash for next block, or null if validation failed
     * @throws IllegalStateException if validation fails
     */
    private static byte[] validateBlock(
            byte[] previousRecordFileHash, List<InMemoryFile> files, final InMemoryFile mostCommonRecordFile) {

        final RecordFileInfo recordFileInfo = RecordFileInfo.parse(mostCommonRecordFile.data());
        byte[] readPreviousBlockHash = recordFileInfo.previousBlockHash().toByteArray();
        byte[] computedBlockHash = recordFileInfo.blockHash().toByteArray();
        // check computed previousRecordFileHash matches one read from file
        if (previousRecordFileHash != null && !Arrays.equals(previousRecordFileHash, readPreviousBlockHash)) {
            throw new IllegalStateException("Previous block hash mismatch. Expected: "
                    + HexFormat.of().formatHex(previousRecordFileHash).substring(0, 8)
                    + ", Found: "
                    + HexFormat.of().formatHex(readPreviousBlockHash).substring(0, 8));
        }
        // TODO validate signatures and sidecars
        return computedBlockHash;
    }
}
