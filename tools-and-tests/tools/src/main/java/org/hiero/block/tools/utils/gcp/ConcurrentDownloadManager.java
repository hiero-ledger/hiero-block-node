package org.hiero.block.tools.utils.gcp;

import static org.hiero.block.tools.commands.days.download.DownloadConstants.GCP_PROJECT_ID;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.util.concurrent.CompletableFuture;

/**
 * Manages concurrent downloads from GCP Storage. Handling maximum concurrent connections, retries and backoff.
 */
public class ConcurrentDownloadManager implements AutoCloseable {
    private Storage storage;

    public ConcurrentDownloadManager() {
        storage = StorageOptions.grpc()
                    .setAttemptDirectPath(false)
                    .setProjectId(GCP_PROJECT_ID)
                    .build().getService();
    }

    public CompletableFuture<byte[]> downloadAsync(String bucketName, String objectName) {
        return null;
    }

    @Override
    public void close() throws Exception {
        if (storage != null) {
            storage.close();
            storage = null;
        }
    }
}
