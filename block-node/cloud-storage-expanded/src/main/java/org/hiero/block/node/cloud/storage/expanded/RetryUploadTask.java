// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.expanded;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.TRACE;
import static java.lang.System.Logger.Level.WARNING;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.util.concurrent.Callable;
import org.hiero.block.node.cloud.storage.expanded.SingleBlockStoreTask.UploadResult;
import org.hiero.block.node.cloud.storage.expanded.SingleBlockStoreTask.UploadStatus;
import org.hiero.block.node.spi.blockmessaging.BlockSource;

/// Callable task that re-uploads a block's already-compressed bytes read back from
/// {@link RetryStagingManager}. Unlike {@link SingleBlockStoreTask}, no compression step is
/// needed — the bytes were compressed once when the block was originally staged.
///
/// `stagedForRetry` is always `false` on the returned {@link UploadResult}: the retry pipeline in
/// `ExpandedCloudStoragePlugin` handles staging bookkeeping itself via
/// {@link RetryStagingManager#unstage} / {@link RetryStagingManager#recordFailure} rather
/// than re-staging an already-staged block on repeated failure.
class RetryUploadTask implements Callable<UploadResult> {

    private static final String CONTENT_TYPE = "application/octet-stream";
    private static final System.Logger LOGGER = System.getLogger(RetryUploadTask.class.getName());

    private final long blockNumber;
    private final byte[] compressedBytes;
    private final S3UploadClient s3Client;
    private final String objectKey;
    private final String storageClass;
    private final BlockSource blockSource;

    /// Constructs a new retry task.
    ///
    /// @param blockNumber    the block number being retried
    /// @param compressedBytes the previously-compressed block bytes, read back from disk
    /// @param s3Client       the upload client to use for the upload
    /// @param objectKey      the fully-qualified S3 object key
    /// @param storageClass   the S3 storage class
    /// @param blockSource    the source of the block (for the `PersistedNotification`)
    RetryUploadTask(
            final long blockNumber,
            @NonNull final byte[] compressedBytes,
            @NonNull final S3UploadClient s3Client,
            @NonNull final String objectKey,
            @NonNull final String storageClass,
            @NonNull final BlockSource blockSource) {
        this.blockNumber = blockNumber;
        this.compressedBytes = compressedBytes;
        this.s3Client = s3Client;
        this.objectKey = objectKey;
        this.storageClass = storageClass;
        this.blockSource = blockSource;
    }

    /// Re-uploads the staged compressed bytes to S3.
    ///
    /// @return the upload result (never `null`)
    @Override
    public UploadResult call() {
        final long uploadStartNs = System.nanoTime();
        try {
            s3Client.uploadFile(objectKey, storageClass, new PayloadIterator(compressedBytes), CONTENT_TYPE);
            LOGGER.log(TRACE, "Block {0}: retry upload succeeded for {1}", blockNumber, objectKey);
            return new UploadResult(
                    blockNumber,
                    UploadStatus.SUCCESS,
                    compressedBytes.length,
                    blockSource,
                    System.nanoTime() - uploadStartNs,
                    false);
        } catch (final UploadException e) {
            LOGGER.log(DEBUG, "Block {0}: S3 retry upload failed", blockNumber, e);
            return new UploadResult(
                    blockNumber, UploadStatus.S3_ERROR, 0L, blockSource, System.nanoTime() - uploadStartNs, false);
        } catch (final IOException e) {
            LOGGER.log(DEBUG, "Block {0}: I/O error during retry upload", blockNumber, e);
            return new UploadResult(
                    blockNumber, UploadStatus.IO_ERROR, 0L, blockSource, System.nanoTime() - uploadStartNs, false);
        } catch (final RuntimeException e) {
            LOGGER.log(WARNING, "Block {0}: unexpected error during retry upload", blockNumber, e);
            return new UploadResult(
                    blockNumber,
                    UploadStatus.UNEXPECTED_ERROR,
                    0L,
                    blockSource,
                    System.nanoTime() - uploadStartNs,
                    false);
        }
    }
}
