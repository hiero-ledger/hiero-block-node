// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands.record2blocks.mirrornode;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobGetOption;
import com.google.cloud.storage.StorageOptions;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
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

    /** The path to the record table CSV in bucket */
    private static final String objectPath = "0.113.2/record_file.csv.gz";

    /** The path to the record table CSV from mirror node, gzipped. */
    @Option(
            names = {"--record-csv"},
            description = "Path to the record table CSV from mirror node, gzipped.")
    private Path recordsCsvFile = Path.of("data/record_file.csv.gz");

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

            // Instantiates a GCP Storage client
            final Storage storage = StorageOptions.getDefaultInstance().getService();
            // Read the object from the bucket with requester pays option
            BlobId blobId = BlobId.of(bucketName, objectPath);
            Blob blob = storage.get(blobId, BlobGetOption.userProject(projectId));
            // print error if file already exists
            if (Files.exists(recordsCsvFile)) {
                System.err.println("Output file already exists: " + recordsCsvFile);
                System.exit(1);
            }
            // create parent directories
            //noinspection ResultOfMethodCallIgnored
            recordsCsvFile.toFile().getParentFile().mkdirs();
            // download file
            try (ProgressOutputStream out = new ProgressOutputStream(
                    new BufferedOutputStream(new FileOutputStream(recordsCsvFile.toFile()), 1024 * 1024 * 32),
                    blob.getSize(),
                    recordsCsvFile.getFileName().toString())) {
                blob.downloadTo(out);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * A simple output stream that prints progress to the console.
     */
    public static class ProgressOutputStream extends FilterOutputStream {
        private static final long MB = 1024 * 1024;
        private final long size;
        private final String name;
        private long bytesWritten = 0;

        /**
         * Create new progress output stream.
         *
         * @param out the output stream to wrap
         * @param size the size of the output stream
         * @param name the name of the output stream
         */
        public ProgressOutputStream(OutputStream out, long size, String name) {
            super(out);
            this.size = size;
            this.name = name;
        }

        /**
         * Write a byte to the output stream.
         *
         * @param b the byte to write
         * @throws IOException if an error occurs writing the byte
         */
        @Override
        public void write(int b) throws IOException {
            super.write(b);
            bytesWritten++;
            printProgress();
        }

        /**
         * Write a byte array to the output stream.
         *
         * @param b the byte array to write
         * @param off the offset in the byte array to start writing
         * @param len the number of bytes to write
         * @throws IOException if an error occurs writing the byte array
         */
        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            super.write(b, off, len);
            bytesWritten += len;
            printProgress();
        }

        /**
         * Print the progress of the output stream to the console.
         */
        private void printProgress() {
            if (bytesWritten % MB == 0) {
                System.out.printf(
                        "\rProgress: %.0f%% - %,d MB written of %s",
                        (bytesWritten / (double) size) * 100d, bytesWritten / MB, name);
            }
        }
    }
}
