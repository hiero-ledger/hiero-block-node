package org.hiero.block.tools.commands.days.download;

import static org.hiero.block.tools.commands.days.download.DownloadConstants.BUCKET_NAME;
import static org.hiero.block.tools.commands.days.download.DownloadConstants.BUCKET_PATH_PREFIX;
import static org.hiero.block.tools.commands.days.download.DownloadConstants.GCP_PROJECT_ID;
import static org.hiero.block.tools.commands.days.listing.DayListingFileReader.loadRecordsFileForDay;
import static org.hiero.block.tools.records.RecordFileUtils.findMostCommonSidecars;
import static org.hiero.block.tools.utils.PrettyPrint.printProgress;
import static org.hiero.block.tools.records.RecordFileUtils.extractRecordFileTimeStrFromPath;
import static org.hiero.block.tools.records.RecordFileUtils.findMostCommonByType;

import com.github.luben.zstd.ZstdOutputStream;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobGetOption;
import com.google.cloud.storage.Storage.BlobSourceOption;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.hiero.block.tools.commands.days.listing.ListingRecordFile;
import org.hiero.block.tools.utils.Gzip;
import org.hiero.block.tools.utils.Md5Checker;

public class DownloadDay {

    public static final Storage.BlobSourceOption BLOB_SOURCE_OPTION = BlobSourceOption.userProject(
        DownloadConstants.GCP_PROJECT_ID);

    public static void downloadDay(final Path listingDir, final Path downloadedDaysDir,
            final int year, final int month, final int day,
            final long progressTotal, final long progressStart, final int threads) throws Exception {
        final List<ListingRecordFile> allDaysFiles = loadRecordsFileForDay(listingDir, year, month, day);
        // group by RecordFile.timestamp and process each group
        final Map<LocalDateTime, List<ListingRecordFile>> filesByTimestamp = allDaysFiles.stream()
                .collect(Collectors.groupingBy(ListingRecordFile::timestamp));
        // for each group, download the files and write them into a single .tar.zstd file
        final String dayString = String.format("%04d-%02d-%02d", year, month, day);
        // target output tar.zstd path
        final Path finalOutFile = downloadedDaysDir.resolve(dayString + ".tar.zstd");
        final Path partialOutFile = downloadedDaysDir.resolve(dayString + ".tar.zstd_partial");
        // If the final output already exists, bail out early (higher-level command may also check)
        if (Files.exists(finalOutFile)) {
            printProgress(progressStart, progressTotal, dayString + " :: Skipping as exists " + allDaysFiles.size() + " files");
            return;
        }
        // ensure output dir exists
        if (!Files.exists(downloadedDaysDir)) Files.createDirectories(downloadedDaysDir);
        // remove stale partial
        try { Files.deleteIfExists(partialOutFile); } catch (IOException ignored) {}
        // print starting message
        printProgress(progressStart, progressTotal, dayString + " :: Processing " + allDaysFiles.size() + " files");
        // sets for most common files
        final Set<ListingRecordFile> mostCommonFiles = new HashSet<>();
        filesByTimestamp.values().forEach(list -> {
            final ListingRecordFile mostCommonRecordFile = findMostCommonByType(list, ListingRecordFile.Type.RECORD);
            final ListingRecordFile mostCommonSidecarFile = findMostCommonByType(list, ListingRecordFile.Type.RECORD_SIDECAR);
            if (mostCommonRecordFile != null) mostCommonFiles.add(mostCommonRecordFile);
            if (mostCommonSidecarFile != null) mostCommonFiles.add(mostCommonSidecarFile);
        });
        // prepare list of timestamps sorted
        final List<LocalDateTime> sortedTimestamps = filesByTimestamp.keySet().stream().sorted().toList();
        // Use Storage client to stream each blob into memory, check MD5, (ungzip if needed), and write to tar
        final Storage storage = StorageOptions.newBuilder()
            .setProjectId(GCP_PROJECT_ID)
            .build().getService();
        // precompute total files count cheaply to drive progress; reuse earlier count logic
        int totalFiles = allDaysFiles.size();
        AtomicLong writtenCounter = new AtomicLong(0);
        // Open ZstdOutputStream -> TarArchiveOutputStream to write entries directly
        try (OutputStream fout = Files.newOutputStream(partialOutFile);
             ZstdOutputStream zOut = new ZstdOutputStream(fout, /*level*/6);
             TarArchiveOutputStream tar = new TarArchiveOutputStream(zOut)) {
            tar.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
            // iterate timestamps in order
            for (LocalDateTime ts : sortedTimestamps) {
                final List<ListingRecordFile> group = filesByTimestamp.get(ts);
                if (group == null || group.isEmpty()) continue;
                // find most common files for this timestamp
                final ListingRecordFile mostCommonRecordFile = findMostCommonByType(group, ListingRecordFile.Type.RECORD);
                final ListingRecordFile[] mostCommonSidecarFiles = findMostCommonSidecars(group);
                // build ordered list of ListingRecordFile objects to write for this timestamp
                final List<ListingRecordFile> ordered = new ArrayList<>();
                if (mostCommonRecordFile != null) ordered.add(mostCommonRecordFile);
                ordered.addAll(Arrays.asList(mostCommonSidecarFiles));
                for (ListingRecordFile file : group) {
                    switch (file.type()) {
                        case RECORD -> { if (!file.equals(mostCommonRecordFile)) ordered.add(file); }
                        case RECORD_SIG -> ordered.add(file);
                        case RECORD_SIDECAR -> {
                            // check if this sidecar is already in mostCommonSidecarFiles
                            boolean isMostCommon = false;
                            for (ListingRecordFile f : mostCommonSidecarFiles) {
                                if (file.equals(f)) { isMostCommon = true; break; }
                            }
                            if (!isMostCommon) ordered.add(file);
                        }
                        default -> throw new RuntimeException("Unsupported file type: " + file.type());
                    }
                }
                // For each file in ordered list, stream from GCS into memory, check MD5, optionally ungzip, then write into tar
                for (ListingRecordFile lr : ordered) {
                    final String blobName = BUCKET_PATH_PREFIX + lr.path();
                    final byte[] rawBytes = storage.readAllBytes(BUCKET_NAME, blobName, BLOB_SOURCE_OPTION);

                    // compute md5 hex of raw bytes (when blob is gzip compressed the md5 in listing corresponds to gz content)
                    if (!Md5Checker.checkMd5(lr.md5Hex(), rawBytes)) {
                        throw new IOException("MD5 mismatch for blob " + blobName);
                    }

                    // if gz compressed, ungzip in-memory
                    byte[] contentBytes = rawBytes;
                    String filename = lr.path().substring(lr.path().lastIndexOf('/') + 1);
                    if (filename.endsWith(".gz")) {
                        contentBytes = Gzip.ungzipInMemory(contentBytes);
                        // strip .gz from filename for tar entry target unless existing code expects original name: original moved code removed .gz
                        filename = filename.replaceAll("\\.gz$", "");
                    }

                    // determine nodeId from parent directory in path (e.g., .../record0.0.18/<file>)
                    String parentDir = lr.path();
                    int lastSlash = parentDir.lastIndexOf('/');
                    if (lastSlash > 0) parentDir = parentDir.substring(0, lastSlash);
                    String nodeDir = parentDir.substring(parentDir.lastIndexOf('/') + 1).replace("record", "");

                    // determine target filename inside tar
                    String targetFileName;
                    if (lr.type() == ListingRecordFile.Type.RECORD || lr.type() == ListingRecordFile.Type.RECORD_SIDECAR) {
                        if (mostCommonFiles.contains(lr)) {
                            targetFileName = filename;
                        } else {
                            // insert _node_<id> before extension .rcd
                            targetFileName = filename.replaceAll("\\.rcd$", "_node_" + nodeDir + ".rcd");
                        }
                    } else if (lr.type() == ListingRecordFile.Type.RECORD_SIG) {
                        targetFileName = "node_" + nodeDir + ".rcs_sig";
                    } else {
                        throw new IOException("Unsupported file type: " + lr.type());
                    }

                    // compute date directory name for this entry
                    String dateDirName = extractRecordFileTimeStrFromPath(Path.of(filename));
                    String entryName = dateDirName + "/" + targetFileName;

                    // write tar entry
                    TarArchiveEntry entry = new TarArchiveEntry(entryName);
                    entry.setSize(contentBytes.length);
                    tar.putArchiveEntry(entry);
                    tar.write(contentBytes);
                    tar.closeArchiveEntry();

                    long idx = writtenCounter.getAndIncrement();
                    if (idx % 1000 == 0 || idx == totalFiles - 1) {
                        printProgress(progressStart + idx, progressTotal, dayString + " :: Writing " + entryName);
                    }
                }
            }
            tar.finish();
        }

        // move partial to final
        try {
            Files.move(partialOutFile, finalOutFile, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            Files.move(partialOutFile, finalOutFile, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    /**
     * Given the most common record file for a timestamp, compute its SHA384 hash based on Hedera record stream hashing
     * logic and record file version.
     * @param mostCommonRecordFile
     * @return
     */
    private static byte[] computeRecordFileHash(ListingRecordFile mostCommonRecordFile) {
        // TODO implement hash computation
        return new byte[48];
    }

    private static boolean validateTimePeriod(byte[] previousRecordFileHash, List<ListingRecordFile> files,ListingRecordFile mostCommonRecordFile) {
        // TODO implement validation logic
        return true;
    }
}
