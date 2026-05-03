// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.expanded;

import static java.lang.System.Logger.Level.TRACE;
import static java.lang.System.Logger.Level.WARNING;

import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.node.spi.blockmessaging.BlockSource;

/// Callable task that compresses a single verified block and uploads it to S3 as a
/// `.blk.zstd` object.
///
/// Each task is responsible for exactly one block. The {@link ExpandedCloudStoragePlugin}
/// submits tasks to a {@link java.util.concurrent.CompletionService} and drains results
/// before each new verification notification.
///
/// ## Object key format
/// ```
/// {prefix}/AAAA/BBBB/CCCC/DDDD/EEE.blk.zstd
/// ```
/// The 19-digit zero-padded block number is split into 4-digit folder groups (4/4/4/4/3)
/// for lexicographic ordering and S3 prefix partitioning.
/// ```
/// Block          1 → {prefix}/0000/0000/0000/0000/001.blk.zstd
/// Block  108273182 → {prefix}/0000/0000/0010/8273/182.blk.zstd
/// ```
public class SingleBlockStoreTask implements Callable<SingleBlockStoreTask.UploadResult> {

    private static final String CONTENT_TYPE = "application/octet-stream";
    private static final System.Logger LOGGER = System.getLogger(SingleBlockStoreTask.class.getName());

    /// Outcome of a single block upload attempt.
    ///
    /// Distinguishes between compression failure, S3 service errors, and I/O errors so
    /// callers can apply different retry or alerting strategies per failure type.
    public enum UploadStatus {
        /// Block was compressed and uploaded successfully.
        SUCCESS,
        /// Compression produced empty bytes — upload was skipped.
        /// In practice this is not triggered by the ZSTD streaming path; retained as a guard.
        COMPRESSION_ERROR,
        /// S3 service returned an error (4xx / 5xx HTTP response or client init failure).
        S3_ERROR,
        /// An I/O error occurred while streaming bytes to S3.
        IO_ERROR
    }

    /// The outcome of a single block upload attempt.
    ///
    /// @param blockNumber     the block number that was uploaded
    /// @param status          the upload outcome; use {@link #succeeded()} for a binary check
    /// @param bytesUploaded   compressed bytes transferred; 0 on any failure
    /// @param blockSource     origin of the block (forwarded to {@code PersistedNotification})
    /// @param uploadDurationNs wall-clock time of the upload call in nanoseconds
    public record UploadResult(
            long blockNumber, UploadStatus status, long bytesUploaded, BlockSource blockSource, long uploadDurationNs) {

        /// Returns `true` if the upload completed successfully.
        public boolean succeeded() {
            return status == UploadStatus.SUCCESS;
        }
    }

    private final long blockNumber;
    private final BlockUnparsed block;
    private final S3UploadClient s3Client;
    private final String objectKey;
    private final String storageClass;
    private final BlockSource blockSource;

    /// Constructs a new task.
    ///
    /// @param blockNumber  the block number being uploaded
    /// @param block        the verified block payload
    /// @param s3Client     the upload client to use for the upload
    /// @param objectKey    the fully-qualified S3 object key
    /// @param storageClass the S3 storage class
    /// @param blockSource  the source of the block (for the `PersistedNotification`)
    public SingleBlockStoreTask(
            final long blockNumber,
            @NonNull final BlockUnparsed block,
            @NonNull final S3UploadClient s3Client,
            @NonNull final String objectKey,
            @NonNull final String storageClass,
            @NonNull final BlockSource blockSource) {
        this.blockNumber = blockNumber;
        this.block = block;
        this.s3Client = s3Client;
        this.objectKey = objectKey;
        this.storageClass = storageClass;
        this.blockSource = blockSource;
    }

    /// Compresses the block to ZSTD protobuf bytes and uploads it to S3.
    ///
    /// Calls `S3UploadClient.uploadFile()` directly, relying on S3 SDK
    /// connection/socket timeouts. Failures are captured as `succeeded=false` results
    /// rather than thrown exceptions so the {@link java.util.concurrent.CompletionService}
    /// always receives a result.
    ///
    /// @return the upload result (never `null`)
    @Override
    public UploadResult call() {
        final long uploadStartNs = System.nanoTime();
        try {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (final OutputStream zstdOut = CompressionType.ZSTD.wrapStream(baos)) {
                BlockUnparsed.PROTOBUF.write(block, new WritableStreamingData(zstdOut));
            }
            final byte[] compressed = baos.toByteArray();

            if (compressed.length == 0) {
                LOGGER.log(WARNING, "Block {0}: compressed bytes are empty, skipping upload.", blockNumber);
                return new UploadResult(
                        blockNumber,
                        UploadStatus.COMPRESSION_ERROR,
                        0L,
                        blockSource,
                        System.nanoTime() - uploadStartNs);
            }

            s3Client.uploadFile(objectKey, storageClass, new PayloadIterator(compressed), CONTENT_TYPE);
            LOGGER.log(TRACE, "Block {0}: uploaded to {1}", blockNumber, objectKey);
            return new UploadResult(
                    blockNumber,
                    UploadStatus.SUCCESS,
                    compressed.length,
                    blockSource,
                    System.nanoTime() - uploadStartNs);

        } catch (final UploadException e) {
            final String msg = "Block " + blockNumber + ": S3 upload failed";
            LOGGER.log(WARNING, msg, e);
            return new UploadResult(
                    blockNumber, UploadStatus.S3_ERROR, 0L, blockSource, System.nanoTime() - uploadStartNs);
        } catch (final IOException e) {
            final String msg = "Block " + blockNumber + ": I/O error during upload";
            LOGGER.log(WARNING, msg, e);
            return new UploadResult(
                    blockNumber, UploadStatus.IO_ERROR, 0L, blockSource, System.nanoTime() - uploadStartNs);
        }
    }

    /// Single-use iterator that delivers one byte array and then reports exhausted.
    private static final class PayloadIterator implements Iterator<byte[]> {
        private final byte[] payload;
        private boolean delivered = false;

        PayloadIterator(final byte[] payload) {
            this.payload = payload;
        }

        @Override
        public boolean hasNext() {
            return !delivered;
        }

        @Override
        public byte[] next() {
            if (delivered) throw new NoSuchElementException();
            delivered = true;
            return payload;
        }
    }
}
