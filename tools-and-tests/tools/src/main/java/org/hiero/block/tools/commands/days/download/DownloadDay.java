package org.hiero.block.tools.commands.days.download;

import static org.hiero.block.tools.commands.days.listing.DayListingFileReader.loadRecordsFileForDay;
import static org.hiero.block.tools.utils.FilesUtils.deleteDirectory;
import static org.hiero.block.tools.utils.Gzip.ungzip;
import static org.hiero.block.tools.utils.Md5Checker.checkMd5;
import static org.hiero.block.tools.utils.PrettyPrint.printProgress;
import static org.hiero.block.tools.utils.RecordFileUtils.extractRecordFileTimeStrFromPath;
import static org.hiero.block.tools.utils.RecordFileUtils.findMostCommonByType;
import static org.hiero.block.tools.commands.days.download.DownloadConstants.*;

import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage.BlobSourceOption;
import com.google.cloud.storage.transfermanager.DownloadResult;
import com.google.cloud.storage.transfermanager.ParallelDownloadConfig;
import com.google.cloud.storage.transfermanager.TransferManager;
import com.google.cloud.storage.transfermanager.TransferManagerConfig;
import com.google.cloud.storage.transfermanager.TransferStatus;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hiero.block.tools.commands.days.listing.ListingRecordFile;

public class DownloadDay {

    public static void downloadDay(final Path listingDir, final Path downloadedDaysDir,
            final int year, final int month, final int day,
            final long progressTotal, final long progressStart) throws Exception {
        final List<ListingRecordFile> allDaysFiles = loadRecordsFileForDay(listingDir, year, month, day);
        // group by RecordFile.timestamp and process each group
        final Map<LocalDateTime, List<ListingRecordFile>> filesByTimestamp = allDaysFiles.stream()
                .collect(Collectors.groupingBy(ListingRecordFile::timestamp));
        // for each group, download the files and compress them into a single .tar.gz file
        // use OUTPUT_DIR/year/month/day/timestamp.tar.gz as the output file
        final String dayString = String.format("%04d-%02d-%02d", year, month, day);
        // create directories if they don't exist
        final Path dayDir = downloadedDaysDir.resolve(dayString);
        if (Files.exists(dayDir)) {
            printProgress(progressStart, progressTotal,dayString+" :: Skipping as exists " + allDaysFiles.size() + " files");
        } else {
            Files.createDirectories(dayDir);
            printProgress(progressStart, progressTotal,dayString+" :: Processing " + allDaysFiles.size() + " files");
            // sets for most common files
            final Set<ListingRecordFile> mostCommonFiles = new HashSet<>();
            // process each record file period
            final List<BlobInfo> blobsToDownload = filesByTimestamp.entrySet().stream()
                    .flatMap(entry -> {
                        final List<ListingRecordFile> files = entry.getValue();
                        // find most common record file and sidecar file
                        final ListingRecordFile mostCommonRecordFile = findMostCommonByType(files, ListingRecordFile.Type.RECORD);
                        final ListingRecordFile mostCommonSidecarFile = findMostCommonByType(files, ListingRecordFile.Type.RECORD_SIDECAR);
                        if (mostCommonRecordFile != null) mostCommonFiles.add(mostCommonRecordFile);
                        if (mostCommonSidecarFile != null) mostCommonFiles.add(mostCommonSidecarFile);
                        return getOneDaysFileToDownload(entry.getKey(), files, mostCommonRecordFile, mostCommonSidecarFile);
                    })
                    .toList();
            printProgress(progressStart, progressTotal,dayString+" :: Downloading " + blobsToDownload.size() + " files");
            // create temp directory in output dir
            final Path tempDir = dayDir.resolve(String.format("%04d-%02d-%02d_tmp", year, month, day));
            Files.createDirectories(tempDir);
            // download all files for this day
            downloadFiles(blobsToDownload, tempDir, dayDir, allDaysFiles, mostCommonFiles, progressTotal, progressStart, dayString);
        }
    }

    private static Stream<BlobInfo> getOneDaysFileToDownload(final LocalDateTime timestamp,
            final List<ListingRecordFile> files, final ListingRecordFile mostCommonRecordFile,
            final ListingRecordFile mostCommonSidecarFile) {
        // check we found at least one record file
        if (mostCommonRecordFile == null) {
            System.err.println("No record files found for timestamp " + timestamp + ", skipping. All files:\n    " +
                    files.stream().map(ListingRecordFile::toString).collect(Collectors.joining(",\n    ")));
            return Stream.empty();
        }
        // first collect all files to download
        final List<String> filesToDownload = new ArrayList<>();
        // add the common record and sidecar files first
        filesToDownload.add(mostCommonRecordFile.path());
        if (mostCommonSidecarFile != null) {
            filesToDownload.add(mostCommonSidecarFile.path());
        }
        for (ListingRecordFile file : files) {
            switch (file.type()) {
                case RECORD -> {
                    if (!file.equals(mostCommonRecordFile)) {
                        filesToDownload.add(file.path());
                    }
                }
                case RECORD_SIG -> {
                    // always add sig files
                    filesToDownload.add(file.path());
                }
                case RECORD_SIDECAR -> {
                    if (!file.equals(mostCommonSidecarFile)) {
                        filesToDownload.add(file.path());
                    }
                }
                default -> throw new RuntimeException("Unsupported file type: " + file.type());
            }
        }
        // create bucket infos for all files
        return filesToDownload.stream()
                .map(path -> BlobInfo.newBuilder(BUCKET_NAME, BUCKET_PATH_PREFIX + path).build());
    }


    public static void downloadFiles(final List<BlobInfo> blobs, final Path tempDir, final Path dayDir,
            final List<ListingRecordFile> files, final Set<ListingRecordFile> mostCommonFiles,
            long progressTotal, long progressStart, String dayString) throws Exception {
        try (TransferManager transferManager = TransferManagerConfig.newBuilder()
                .setStorageOptions(REQUESTER_PAYS_STORAGE_OPTIONS)
                .build()
                .getService()) {
            final ParallelDownloadConfig parallelDownloadConfig = ParallelDownloadConfig.newBuilder()
                    .setOptionsPerRequest(List.of(BlobSourceOption.userProject(GCP_PROJECT_ID)))
                    .setBucketName(BUCKET_NAME)
                    .setDownloadDirectory(tempDir)
                    .build();
            final List<DownloadResult> results = transferManager
                    .downloadBlobs(blobs, parallelDownloadConfig)
                    .getDownloadResults();

            final AtomicLong progress = new AtomicLong(0);

            results.stream()
                    .sorted(Comparator.comparing(r -> r.getOutputDestination().getFileName().toString()))
                    .parallel()
                    .forEach(result -> {
                        if (result.getStatus() != TransferStatus.SUCCESS) {
                            throw new RuntimeException("Failed to download file: " + result);
                        }
                        // verify the file exists
                        final Path downloadedFile = result.getOutputDestination();
                        if (!Files.exists(downloadedFile)) {
                            throw new RuntimeException("Downloaded file does not exist: " + downloadedFile);
                        }
                        // print progress
                        final int i = (int) progress.getAndIncrement();
                        if( (i % 1000) == 0 || i == results.size()-1) {
                            printProgress(progressStart + i, progressTotal, dayString + " :: Moving " + downloadedFile);
                        }
                        try {
                            // find the RecordFile for this path
                            final String blobPath = result.getInput().getName();
                            final ListingRecordFile recordFile = files.stream()
                                    .filter(f -> blobPath.endsWith(f.path()))
                                    .findFirst()
                                    .orElseThrow(
                                            () -> new IOException("No RecordFile found for blob path: " + blobPath));
                            // verify the file MD5 hash
                            if (!checkMd5(recordFile.md5Hex(), downloadedFile)) {
                                throw new IOException("MD5 hash mismatch for file: " + downloadedFile);
                            }
                            // for signature files extract the node ID from the parent directory like "record0.0.18" becomes "0.0.18"
                            final String nodeId = downloadedFile.getParent().getFileName().toString()
                                    .replace("record", "");
                            // create directory for this record file set timestamp
                            final Path dateDir = dayDir.resolve(extractRecordFileTimeStrFromPath(downloadedFile));
                            Files.createDirectories(dateDir);
                            // move the file to the dateDir
                            switch (recordFile.type()) {
                                case RECORD, RECORD_SIDECAR -> {
                                    // if the file ends with ".gz" then decompress it
                                    Path theFile = downloadedFile;
                                    if (downloadedFile.getFileName().toString().endsWith(".gz")) {
                                        theFile = ungzip(downloadedFile);
                                    }
                                    // find if the file is the common file that most of the nodes have or an unique one
                                    if (mostCommonFiles.contains(recordFile)) {
                                        // move and keep file name as original
                                        final Path targetFile = dateDir.resolve(theFile.getFileName());
                                        Files.move(theFile, targetFile);
                                    } else {
                                        // for unique files add the node ID before the .rcd or .rcd.sig extension and move to dateDir
                                        final String newFileName = theFile.getFileName().toString()
                                                .replaceAll("\\.rcd$", "_node_" + nodeId + ".rcd");
                                        final Path targetFile = dateDir.resolve(newFileName);
                                        Files.move(theFile, targetFile);
                                    }
                                }
                                case RECORD_SIG -> {
                                    // add the node ID before the .rcd.sig extension and move to dateDir
                                    final Path targetFile = dateDir.resolve("node_" + nodeId + ".rcs_sig");
                                    Files.move(downloadedFile, targetFile);
                                }
                                default -> throw new IOException("Unsupported file type: " + recordFile.type());
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
            // delete tempDir and all its contents
            deleteDirectory(tempDir);
        }
    }
}
