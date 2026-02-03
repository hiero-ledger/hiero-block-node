// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.mirrornode;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.transfermanager.DownloadResult;
import com.google.cloud.storage.transfermanager.ParallelDownloadConfig;
import com.google.cloud.storage.transfermanager.TransferManager;
import com.google.cloud.storage.transfermanager.TransferManagerConfig;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Download mirror node record table CSV dump from GCP bucket
 */
@SuppressWarnings({"CallToPrintStackTrace", "unused"})
@Command(name = "fetchRecordsCsv", description = "Download mirror node record table CSV dump from GCP bucket")
public class FetchMirrorNodeRecordsCsv implements Runnable {
    /** The GCP bucket name that contains CSV dumps of mirror node */
    private static final String bucketName = "mirrornode-db-export";

    /** The path download the record table CSVs from mirror node to, gzipped. */
    @Option(
            names = {"--record-dir"},
            description = "Path to download the record table CSVs from mirror node to, gzipped.")
    private Path recordsCsvDir = Path.of("data/mirror_node_record_files");

    /**
     * Download the record table CSV from mirror node GCP bucket
     */
    @Override
    public void run() {
        try {
            // Load the current credentials
            GoogleCredentials.getApplicationDefault();

            // Get the project ID from the credentials
            String projectId = ServiceOptions.getDefaultProjectId();

            if (projectId != null) {
                System.out.println("Project ID: " + projectId);
            } else {
                System.out.println("Project ID not found.");
                System.exit(1);
            }

            // create the records CSV directory if it doesn't exist
            if (!recordsCsvDir.toFile().exists()) {
                //noinspection ResultOfMethodCallIgnored
                recordsCsvDir.toFile().mkdirs();
            }

            // Instantiates a GCP Storage client
            final Storage storage = StorageOptions.grpc()
                    .setAttemptDirectPath(false)
                    .setProjectId(projectId)
                    .build()
                    .getService();

            // list bucket root, find latest version subdirectory like "0.136.0" or "0.25.0"
            // Equivalent to delimiter="/" and prefix=""
            // remove trailing slash
            String latestVersion = storage.list(
                            bucketName,
                            BlobListOption.currentDirectory(), // Equivalent to delimiter="/" and prefix=""
                            BlobListOption.userProject(projectId))
                    .streamAll()
                    .map(Blob::getName)
                    .map(name -> name.replaceAll("/$", "")) // remove trailing slash
                    .filter(name -> MirrorNodeUtils.SYMANTIC_VERSION_PATTERN
                            .matcher(name)
                            .matches())
                    .max(Comparator.comparingLong(MirrorNodeUtils::parseSymantecVersion))
                    .orElseThrow();
            System.out.println("Latest version: " + latestVersion);

            // list all blobs in that version directory and subdirector "record_files/"
            String prefix = latestVersion + "/record_file/";
            // example gs://mirrornode-db-export/0.136.0/record_file/record_file_p2019_09.csv.gz
            List<BlobInfo> recordsCsvFileBlobInfos = storage.list(
                            bucketName, BlobListOption.prefix(prefix), BlobListOption.userProject(projectId))
                    .streamAll()
                    .filter(blob -> blob.getName().contains("record_file")
                            && blob.getName().endsWith(".csv.gz"))
                    .map(Blob::asBlobInfo)
                    .toList();
            System.out.println("Found " + recordsCsvFileBlobInfos.size() + " record CSV files to download.");
            // bulk download all files
            try (TransferManager transferManager = TransferManagerConfig.newBuilder()
                    .setAllowDivideAndConquerDownload(true)
                    .build()
                    .getService()) {
                ParallelDownloadConfig parallelDownloadConfig = ParallelDownloadConfig.newBuilder()
                        .setBucketName(bucketName)
                        .setOptionsPerRequest(List.of(Storage.BlobSourceOption.userProject(projectId)))
                        .setDownloadDirectory(recordsCsvDir)
                        .build();
                List<DownloadResult> results = transferManager
                        .downloadBlobs(recordsCsvFileBlobInfos, parallelDownloadConfig)
                        .getDownloadResults();
                for (DownloadResult result : results) {
                    System.out.println("Download of " + result.getInput().getName() + " completed with status "
                            + result.getStatus());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
