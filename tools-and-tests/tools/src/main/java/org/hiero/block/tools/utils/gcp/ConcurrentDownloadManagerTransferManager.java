// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.utils.gcp;

import com.google.cloud.ServiceOptions;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.transfermanager.DownloadResult;
import com.google.cloud.storage.transfermanager.ParallelDownloadConfig;
import com.google.cloud.storage.transfermanager.TransferManager;
import com.google.cloud.storage.transfermanager.TransferManagerConfig;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import org.hiero.block.tools.days.download.DownloadConstants;
import org.hiero.block.tools.records.InMemoryFile;

/**
 * Manager for concurrent downloads from GCS with built-in concurrency using TransferManager.
 */
@SuppressWarnings("unused")
public class ConcurrentDownloadManagerTransferManager implements ConcurrentDownloadManager {
    private static final Logger LOGGER = Logger.getLogger(ConcurrentDownloadManagerTransferManager.class.getName());
    private static final int BATCH_SIZE = 128;

    private record Request(String bucketName, String objectName, CompletableFuture<InMemoryFile> future) {}

    private final TransferManagerConfig config;
    private final TransferManager transferManager;
    private final FileSystem fs;
    private final Path downloadDir;
    private final AtomicLong bytesDownloaded = new AtomicLong(0);
    private final AtomicLong objectsCompleted = new AtomicLong(0);
    private final LinkedBlockingQueue<Request> requests = new LinkedBlockingQueue<>();
    private final ScheduledExecutorService executor;
    private volatile ScheduledFuture<?> scheduledFuture;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Constructor
     */
    public ConcurrentDownloadManagerTransferManager() {
        config = TransferManagerConfig.newBuilder()
                .setStorageOptions(StorageOptions.newBuilder()
                        .setProjectId(DownloadConstants.GCP_PROJECT_ID)
                        .build())
                .setAllowDivideAndConquerDownload(true)
                .build();
        transferManager = config.getService();
        fs = Jimfs.newFileSystem(Configuration.unix());
        downloadDir = fs.getPath("/downloads");
        try {
            Files.createDirectory(downloadDir);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        executor = Executors.newScheduledThreadPool(1);
    }

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
    @Override
    public CompletableFuture<InMemoryFile> downloadAsync(String bucketName, String objectName) {
        if (closed.get()) {
            throw new IllegalStateException("ConcurrentDownloadManager is closed");
        }
        final CompletableFuture<InMemoryFile> future = new CompletableFuture<>();
        final Request request = new Request(bucketName, objectName, future);
        requests.add(request);
        if (requests.size() > BATCH_SIZE) {
            List<Request> batch = new ArrayList<>();
            requests.drainTo(batch, BATCH_SIZE);
            // schedule download on executor to avoid blocking the caller thread
            executor.submit(() -> downloadBatch(batch));
        }
        // set a timer to flush remaining requests after a short delay if no new requests arrive
        // as it is possible the last few requests will never fill a full batch
        if (scheduledFuture != null && !scheduledFuture.isDone()) {
            scheduledFuture.cancel(false);
        }
        scheduledFuture = executor.schedule(
                () -> {
                    List<Request> batch = new ArrayList<>();
                    requests.drainTo(batch, BATCH_SIZE);
                    if (!batch.isEmpty()) {
                        downloadBatch(batch);
                    }
                },
                5,
                TimeUnit.SECONDS);
        return future;
    }

    /** Download a batch of requests */
    private void downloadBatch(List<Request> batchRequests) {
        if (batchRequests.isEmpty()) {
            return;
        }
        // prepare blob infos
        final List<BlobInfo> recordsCsvFileBlobInfos = batchRequests.stream()
                .map(req -> Blob.newBuilder(req.bucketName(), req.objectName()).build())
                .toList();
        // prepare download config
        final ParallelDownloadConfig parallelDownloadConfig = ParallelDownloadConfig.newBuilder()
                .setBucketName(batchRequests.getFirst().bucketName())
                .setOptionsPerRequest(
                        List.of(Storage.BlobSourceOption.userProject(ServiceOptions.getDefaultProjectId())))
                .setDownloadDirectory(downloadDir)
                .build();
        List<DownloadResult> results;
        try {
            results = transferManager
                    .downloadBlobs(recordsCsvFileBlobInfos, parallelDownloadConfig)
                    .getDownloadResults();
        } catch (Exception e) {
            // complete all futures exceptionally if the transfer call fails
            for (Request req : batchRequests) {
                req.future().completeExceptionally(e);
            }
            return;
        }
        for (int i = 0; i < results.size(); i++) {
            final DownloadResult result = results.get(i);
            final Request req = batchRequests.get(i);
            try {
                final Path filePath = result.getOutputDestination();
                final byte[] data = Files.readAllBytes(filePath);
                bytesDownloaded.addAndGet(data.length);
                objectsCompleted.incrementAndGet();
                req.future().complete(new InMemoryFile(Path.of(req.objectName()), data));
                // delete the temp file if present
                try {
                    Files.deleteIfExists(filePath);
                } catch (IOException ignore) {
                    // best-effort cleanup
                }
            } catch (Exception e) {
                req.future().completeExceptionally(e);
            }
        }
    }

    /** @return bytes downloaded so far (best-effort) */
    @Override
    public long getBytesDownloaded() {
        return bytesDownloaded.get();
    }

    /** @return objects completed so far */
    @Override
    public long getObjectsCompleted() {
        return objectsCompleted.get();
    }

    /** @return current concurrency limit */
    @Override
    public int getCurrentConcurrency() {
        return config.getMaxWorkers();
    }

    /** @return max concurrency configured */
    @Override
    public int getMaxConcurrency() {
        return config.getMaxWorkers();
    }

    /**
     * Closes this resource
     */
    @Override
    public void close() throws IOException {
        Exception firstException = null;
        // mark closed first so no new requests are accepted
        closed.set(true);
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }
        // drain any pending requests and complete their futures exceptionally
        List<Request> remaining = new ArrayList<>();
        requests.drainTo(remaining);
        for (Request req : remaining) {
            req.future().completeExceptionally(new IllegalStateException("ConcurrentDownloadManager is closed"));
        }
        executor.shutdown();
        try {
            final boolean terminated = executor.awaitTermination(10, TimeUnit.SECONDS);
            if (!terminated) {
                final var notRan = executor.shutdownNow();
                LOGGER.warning(() -> "Executor did not terminate in time; cancelled " + notRan.size() + " tasks");
                final boolean terminatedAfterShutdownNow = executor.awaitTermination(5, TimeUnit.SECONDS);
                if (!terminatedAfterShutdownNow) {
                    LOGGER.warning("Executor still did not terminate after shutdownNow");
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.warning("Interrupted while awaiting executor termination");
        }
        try {
            transferManager.close();
        } catch (Exception e) {
            firstException = e;
        }
        try {
            fs.close();
        } catch (IOException e) {
            if (firstException == null) {
                firstException = e;
            }
        }
        if (firstException != null) {
            throw new IOException(firstException);
        }
    }
}
