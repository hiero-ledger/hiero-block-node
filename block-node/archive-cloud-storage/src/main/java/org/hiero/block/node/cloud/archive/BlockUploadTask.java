// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.archive;

import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.TRACE;

import com.hedera.bucky.S3Client;
import com.hedera.bucky.S3ResponseException;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;

/// Archives a single contiguous batch of verified blocks into one `tar` file on S3-compatible
/// cloud storage via a multipart upload.
///
/// Implements [Callable] so that exceptions are not silently swallowed by the thread pool, and
/// callers can inspect the outcome or error through the returned [java.util.concurrent.Future].
///
/// Blocks are fed through a [BlockingQueue] shared with [ArchiveCloudStoragePlugin].  The task
/// polls the queue in block-number order, accumulates tar-encoded bytes in an in-memory buffer,
/// and flushes a fixed-size chunk to S3 whenever the buffer reaches
/// [ArchiveCloudStorageConfig#partSizeMb()].
///
/// **Cancellation** is handled via standard thread interruption: calling
/// [java.util.concurrent.Future#cancel(boolean)] with `true` interrupts the virtual thread, which
/// causes [BlockingQueue#take] to throw [InterruptedException].
///
/// The [S3Client] lives entirely inside a try-with-resources block, so it is always closed,
/// even on exceptions, without any extra cleanup code.
public class BlockUploadTask implements Callable<BlockUploadTask.UploadResult> {

    private static final System.Logger LOGGER = System.getLogger(BlockUploadTask.class.getName());
    private static final String CONTENT_TYPE = "application/x-tar";

    private final ArchiveCloudStorageConfig config;
    private final BlockMessagingFacility blockMessaging;

    private final long firstBlock;
    private final long groupSize;
    private final int partSizeBytes;

    /// The S3 object key computed once in the constructor from [firstBlock] and
    /// [ArchiveCloudStorageConfig#groupingLevel()].
    private final String key;
    private final BlockingQueue<BlockUnparsed> blockQueue;

    BlockUploadTask(
            @NonNull ArchiveCloudStorageConfig config,
            @NonNull BlockMessagingFacility blockMessaging,
            long firstBlock,
            long groupSize,
            @NonNull BlockingQueue<BlockUnparsed> blockQueue) {
        this.config = config;
        this.blockMessaging = blockMessaging;
        this.firstBlock = firstBlock;
        this.groupSize = groupSize;
        this.partSizeBytes = config.partSizeMb() * 1024 * 1024;
        this.blockQueue = blockQueue;
        this.key = computeKey();
    }

    /// Runs the full upload lifecycle for this batch:
    /// 1. Opens the S3 client and initiates a multipart-upload session.
    /// 2. Loops [groupSize] times, taking each block from [blockQueue].
    /// 3. Accumulates tar-encoded bytes; flushes a part to S3 when the buffer is full.
    /// 4. Uploads the final partial buffer and completes the multipart upload.
    ///
    /// If a part upload fails, failed [PersistedNotification]s are sent for all blocks whose bytes
    /// were in that part, and the exception is rethrown so the [java.util.concurrent.Future]
    /// records it.
    @Override
    public UploadResult call() throws Exception {
        try (S3Client s3 = new S3Client(
                config.regionName(),
                config.endpointUrl(),
                config.bucketName(),
                config.accessKey(),
                config.secretKey())) {
            // Start the multipart upload session
            final String uploadId =
                    s3.createMultipartUpload(key, config.storageClass().name(), CONTENT_TYPE);
            final List<String> etags = new ArrayList<>();

            // Initialize the buffer to hold tar bytes of a single part
            byte[] buffer = new byte[0];

            // Tracks block numbers whose tar bytes are still accumulating in the buffer. A block is
            // removed (and its persisted notification sent) when the part containing its last byte
            // is uploaded.
            final List<Long> blocksInBuffer = new ArrayList<>();

            for (long blockNum = firstBlock; blockNum < firstBlock + groupSize; blockNum++) {
                // Handle cancellation at the start of every iteration, in addition to the
                // InterruptedException that blockQueue.take() throws on interruption. This additional
                // prerequisite is necessary in case when the queue is filled (e.g. by stash replay) and
                // at this point plugin is stopped. The blockQueue.take() call will not throw interrupted
                // exception until the queue is drained.
                if (Thread.interrupted()) {
                    if (blockNum == firstBlock) {
                        s3.abortMultipartUpload(key, uploadId);
                    }
                    throw new InterruptedException();
                }

                // Take the next block from the queue (which was placed there by the plugin's verification handling)
                // and convert it to a tar entry
                final BlockUnparsed block = blockQueue.take();
                final byte[] tarEntry = BlockToTarEntry.toTarEntry(block, blockNum);

                // Append the new tar entry to the accumulation buffer
                final byte[] extended = new byte[buffer.length + tarEntry.length];
                System.arraycopy(buffer, 0, extended, 0, buffer.length);
                System.arraycopy(tarEntry, 0, extended, buffer.length, tarEntry.length);
                buffer = extended;

                // Flush a fixed-size part to S3 once the buffer reaches the threshold
                if (buffer.length >= partSizeBytes) {
                    try {
                        buffer = uploadPart(buffer, s3, uploadId, etags);
                    } catch (Exception e) {
                        // Part upload failed — notify all buffered blocks AND the current block
                        // (whose bytes were also in the failed part) as not persisted, then abort
                        blocksInBuffer.forEach(bn -> blockMessaging.sendBlockPersisted(
                                new PersistedNotification(bn, false, 1_000, BlockSource.UNKNOWN)));
                        blockMessaging.sendBlockPersisted(
                                new PersistedNotification(blockNum, false, 1_000, BlockSource.UNKNOWN));
                        LOGGER.log(
                                INFO,
                                "Failed to upload part containing blocks %d to %d"
                                        .formatted(blocksInBuffer.getFirst(), blockNum),
                                e);
                        return UploadResult.FAILED;
                    }

                    // If the current block's entry exactly fills the part (zero remainder),
                    // include it in the notification batch before clearing
                    if (buffer.length == 0) {
                        blocksInBuffer.add(blockNum);
                    }

                    // Send persisted notifications for all blocks whose bytes are now durably uploaded
                    blocksInBuffer.forEach(bn -> blockMessaging.sendBlockPersisted(
                            new PersistedNotification(bn, true, 1_000, BlockSource.UNKNOWN)));
                    blocksInBuffer.clear();
                }

                // If the current block's bytes are (partly) still in the buffer, notification about it will be
                // sent on the next part upload or at completeUpload
                if (buffer.length > 0) {
                    blocksInBuffer.add(blockNum);
                }
            }

            // Upload the remaining bytes as the final part
            if (buffer.length > 0) {
                final int partNumber = etags.size() + 1;
                final String etag = s3.multipartUploadPart(key, uploadId, partNumber, buffer);
                etags.add(etag);
                blocksInBuffer.forEach(bn -> blockMessaging.sendBlockPersisted(
                        new PersistedNotification(bn, true, 1_000, BlockSource.UNKNOWN)));
                blocksInBuffer.clear();
            }

            // Complete the multipart upload before the task is done
            s3.completeMultipartUpload(key, uploadId, etags);
        }
        return UploadResult.SUCCESS;
    }

    @NonNull
    byte[] uploadPart(byte[] buffer, S3Client s3, String uploadId, List<String> etags)
            throws S3ResponseException, IOException {
        // Split the buffer into a part and a remainder
        final byte[] part = new byte[partSizeBytes];
        final byte[] remainder = new byte[buffer.length - partSizeBytes];
        System.arraycopy(buffer, 0, part, 0, partSizeBytes);
        System.arraycopy(buffer, partSizeBytes, remainder, 0, remainder.length);

        // Upload the part to S3 and record the etag
        final int partNumber = etags.size() + 1;
        final String etag = s3.multipartUploadPart(key, uploadId, partNumber, part);
        etags.add(etag);
        LOGGER.log(TRACE, "Uploaded part {0}, etag {1}", partNumber, etag);

        return remainder;
    }

    /// Computes the S3 object key for this task's tar archive.
    ///
    /// The key encodes the first block number of the batch as a hierarchical path of 4-digit
    /// segments, with the precision controlled by [ArchiveCloudStorageConfig#groupingLevel()].
    ///
    /// **Algorithm:**
    /// 1. Zero-pad [firstBlockNumber] to 19 digits (the maximum decimal width of a `long`).
    /// 2. Drop the last `groupingLevel` digits — those digits vary within a single tar batch and
    ///    are therefore not part of the key.
    /// 3. Split the remaining prefix into 4-character segments.
    /// 4. Strip leading zeros from the final segment (cosmetic: `0012` → `12`).
    /// 5. Join all segments with `/` and append `.tar`.
    ///
    /// **Examples** (groupingLevel = 1, batches of 10 blocks):
    /// - block 0 → `0000/0000/0000/0000/0.tar`
    /// - block 10 → `0000/0000/0000/0000/1.tar`
    /// - block 1_234_567_890 → `0000/0000/0123/4567/89.tar`
    ///
    /// @return the S3 object key for this tar archive, never `null`
    @NonNull
    private String computeKey() {
        final String truncated = String.format("%019d", firstBlock).substring(0, 19 - config.groupingLevel());
        final List<String> parts = new ArrayList<>();
        for (int i = 0; i < truncated.length(); i += 4) {
            parts.add(truncated.substring(i, Math.min(i + 4, truncated.length())));
        }
        parts.set(parts.size() - 1, String.valueOf(Long.parseLong(parts.getLast())));
        return String.join("/", parts) + ".tar";
    }

    enum UploadResult {
        SUCCESS,
        FAILED
    }
}
