// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.utils.gcp;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import org.hiero.block.tools.records.model.unparsed.InMemoryFile;

/**
 * Manager for concurrent downloads from GCS with built-in concurrency.
 */
public interface ConcurrentDownloadManager extends AutoCloseable, Closeable {

    /**
     * Asynchronously download a small object fully into memory.
     *
     * <p>This is the primary API for small-object flows where the caller needs to stage
     * the bytes for ordered, batched processing before writing transformed output.</p>
     *
     * <pre>{@code
     * List<BlobId> batch = ...;
     * List<CompletableFuture<InMemoryFile>> futures = new ArrayList<>();
     * for (BlobId id : batch) {
     *   futures.add(manager.downloadAsync(id.getBucket(), id.getName()));
     * }
     * CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
     * // Now process in-order:
     * for (int i = 0; i < batch.size(); i++) {
     *   InMemoryFile data = futures.get(i).join();
     *   // process data...
     * }
     * }</pre>
     *
     * @param bucketName GCS bucket
     * @param objectName GCS object name
     * @return future completing with object bytes
     * @throws IllegalStateException if the manager is closed
     */
    CompletableFuture<InMemoryFile> downloadAsync(String bucketName, String objectName);

    // ====== Helpers for progress ======

    /** @return bytes downloaded so far (best-effort) */
    long getBytesDownloaded();

    /** @return objects completed so far */
    long getObjectsCompleted();

    /** @return current concurrency limit */
    int getCurrentConcurrency();

    /** @return max concurrency configured */
    int getMaxConcurrency();
}
