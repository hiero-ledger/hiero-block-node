// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.expanded.cloud.storage;

import static java.lang.System.Logger.Level.TRACE;
import static java.lang.System.Logger.Level.WARNING;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.node.spi.blockmessaging.BlockSource;

/**
 * Callable task that compresses a single verified block and uploads it to S3 as a
 * {@code .blk.zstd} object.
 *
 * <p>Each task is responsible for exactly one block. The {@link ExpandedCloudStoragePlugin}
 * submits tasks to a {@link java.util.concurrent.CompletionService} and drains results
 * before each new verification notification.
 *
 * <h2>Object key format</h2>
 * <pre>
 * {prefix}/AAAA/BBBB/CCCC/DDDD/EEE.blk.zstd
 * </pre>
 * The 19-digit zero-padded block number is split into 4-digit folder groups (4/4/4/4/3)
 * for lexicographic ordering and S3 prefix partitioning.
 * <pre>
 * Block          1 → {prefix}/0000/0000/0000/0000/001.blk.zstd
 * Block  108273182 → {prefix}/0000/0000/0010/8273/182.blk.zstd
 * </pre>
 *
 * @param result if non-null, the upload outcome; populated by {@link #call()}
 */
public class SingleBlockStoreTask implements Callable<SingleBlockStoreTask.UploadResult> {

    private static final String CONTENT_TYPE = "application/octet-stream";
    private static final System.Logger LOGGER = System.getLogger(SingleBlockStoreTask.class.getName());

    /** The outcome of a single block upload attempt. */
    public record UploadResult(long blockNumber, boolean succeeded, BlockSource blockSource) {}

    private final long blockNumber;
    private final BlockUnparsed block;
    private final S3Client s3Client;
    private final String objectKey;
    private final String storageClass;
    private final int uploadTimeoutSeconds;
    private final BlockSource blockSource;

    /**
     * Constructs a new task.
     *
     * @param blockNumber          the block number being uploaded
     * @param block                the verified block payload
     * @param s3Client             the S3 client to use for the upload
     * @param objectKey            the fully-qualified S3 object key
     * @param storageClass         the S3 storage class
     * @param uploadTimeoutSeconds max seconds to allow before treating the upload as failed
     * @param blockSource          the source of the block (for the {@code PersistedNotification})
     */
    public SingleBlockStoreTask(
            final long blockNumber,
            @NonNull final BlockUnparsed block,
            @NonNull final S3Client s3Client,
            @NonNull final String objectKey,
            @NonNull final String storageClass,
            final int uploadTimeoutSeconds,
            @NonNull final BlockSource blockSource) {
        this.blockNumber = blockNumber;
        this.block = block;
        this.s3Client = s3Client;
        this.objectKey = objectKey;
        this.storageClass = storageClass;
        this.uploadTimeoutSeconds = uploadTimeoutSeconds;
        this.blockSource = blockSource;
    }

    /**
     * Compresses the block to ZSTD protobuf bytes and uploads it to S3.
     *
     * <p>The task is submitted to a virtual-thread executor; the upload runs to completion or
     * times out after {@code uploadTimeoutSeconds}. Failures are captured as
     * {@code succeeded=false} results rather than thrown exceptions so the
     * {@link java.util.concurrent.CompletionService} always receives a result.
     *
     * @return the upload result (never {@code null})
     */
    @Override
    public UploadResult call() {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(uploadTimeoutSeconds);
        try {
            final byte[] protoBytes = BlockUnparsed.PROTOBUF.toBytes(block).toByteArray();
            final byte[] compressed = CompressionType.ZSTD.compress(protoBytes);

            if (compressed.length == 0) {
                LOGGER.log(WARNING, "Block {0}: compressed bytes are empty, skipping upload.", blockNumber);
                return new UploadResult(blockNumber, false, blockSource);
            }

            if (System.nanoTime() > deadline) {
                LOGGER.log(
                        WARNING,
                        "Block {0}: compression exceeded upload timeout ({1}s), skipping upload.",
                        blockNumber,
                        uploadTimeoutSeconds);
                return new UploadResult(blockNumber, false, blockSource);
            }

            s3Client.uploadFile(
                    objectKey,
                    storageClass,
                    Collections.singletonList(compressed).iterator(),
                    CONTENT_TYPE);

            LOGGER.log(TRACE, "Block {0}: uploaded to {1}", blockNumber, objectKey);
            return new UploadResult(blockNumber, true, blockSource);

        } catch (final TimeoutException e) {
            LOGGER.log(WARNING, "Block {0}: upload timed out after {1}s: {2}", blockNumber, uploadTimeoutSeconds, e.getMessage());
            return new UploadResult(blockNumber, false, blockSource);
        } catch (final S3ClientException | IOException e) {
            LOGGER.log(WARNING, "Block {0}: upload failed: {1}", blockNumber, e.getMessage());
            return new UploadResult(blockNumber, false, blockSource);
        }
    }
}
