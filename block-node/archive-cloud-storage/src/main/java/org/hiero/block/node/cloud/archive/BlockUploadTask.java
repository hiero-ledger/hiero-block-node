// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.archive;

import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.TRACE;

import com.hedera.bucky.S3Client;
import com.hedera.bucky.S3ClientInitializationException;
import com.hedera.bucky.S3ResponseException;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListMap;
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

    /// Plugin configuration providing S3 credentials, bucket, storage class, and tuning knobs.
    private final ArchiveCloudStorageConfig config;
    /// Facility used to publish [PersistedNotification]s after each block is durably stored.
    private final BlockMessagingFacility blockMessaging;

    /// The block number of the first block in this batch.
    private final long firstBlock;
    /// The total number of blocks in this batch (always a power of ten).
    private final long groupSize;
    /// The multipart-upload part size in bytes, derived from [ArchiveCloudStorageConfig#partSizeMb()].
    private final int partSizeBytes;

    /// The S3 object key computed once in the constructor from [firstBlock] and
    /// [ArchiveCloudStorageConfig#groupingLevel()].
    private final String key;
    /// The queue from which this task takes [BlockWithSource] pairs in block-number order.
    /// Shared with [ArchiveCloudStoragePlugin], which enqueues pairs as verified blocks arrive.
    private final BlockingQueue<BlockWithSource> blockQueue;

    /// Creates a new task that will upload [groupSize] blocks starting at [firstBlock].
    ///
    /// @param config         plugin configuration
    /// @param blockMessaging facility for publishing [PersistedNotification]s
    /// @param firstBlock     block number of the first block in this batch
    /// @param groupSize      number of blocks to archive (must equal `10 ^ groupingLevel`)
    /// @param blockQueue     queue from which blocks are taken in ascending block-number order
    BlockUploadTask(
            @NonNull ArchiveCloudStorageConfig config,
            @NonNull BlockMessagingFacility blockMessaging,
            long firstBlock,
            long groupSize,
            @NonNull BlockingQueue<BlockWithSource> blockQueue) {
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
    public UploadResult call()
            throws S3ClientInitializationException, S3ResponseException, IOException, InterruptedException {
        UploadResult result = UploadResult.SUCCESS;
        try (S3Client s3 = new S3Client(
                config.regionName(),
                config.endpointUrl(),
                config.bucketName(),
                config.accessKey(),
                config.secretKey())) {
            final String uploadId =
                    s3.createMultipartUpload(key, config.storageClass().name(), CONTENT_TYPE);
            final List<String> etags = new ArrayList<>();
            // Maps each buffered block number to its source, in insertion order.
            final SortedMap<Long, BlockSource> blocksInBuffer = new ConcurrentSkipListMap<>();

            final UploadBlocksOutput uploadOutput = uploadBlocks(s3, uploadId, etags, blocksInBuffer);
            result = uploadOutput.result();

            // Upload any bytes that didn't fill a complete part during the loop.
            if (result == UploadResult.SUCCESS && uploadOutput.remainderBuffer().length > 0) {
                result = uploadFinalPart(uploadOutput.remainderBuffer(), s3, uploadId, etags, blocksInBuffer);
            }

            if (result == UploadResult.SUCCESS) {
                s3.completeMultipartUpload(key, uploadId, etags);
            }
        }
        return result;
    }

    /// Loops through [groupSize] blocks starting at [firstBlock], accumulating tar-encoded bytes
    /// into a buffer and flushing fixed-size parts to S3 as the buffer fills.
    ///
    /// @param s3             the S3 client for this upload session
    /// @param uploadId       the multipart upload ID
    /// @param etags          the list of part ETags collected so far (mutated in place)
    /// @param blocksInBuffer map of block number -> source for blocks whose bytes remain in the
    ///                       buffer after the loop completes (mutated in place)
    /// @return a [UploadBlocksOutput] with the final [UploadResult] and any leftover buffer bytes;
    ///         [UploadBlocksOutput#remainderBuffer] is always non-null
    /// @throws InterruptedException if the thread is interrupted while waiting for the next block
    /// @throws IOException          if a block cannot be serialized to a tar entry
    private UploadBlocksOutput uploadBlocks(
            S3Client s3, String uploadId, List<String> etags, SortedMap<Long, BlockSource> blocksInBuffer)
            throws InterruptedException, IOException {
        // Start with an empty array rather than null so arraycopy calls in accumulateBlock
        // always work without null checks.
        byte[] buffer = new byte[0];
        UploadResult result = UploadResult.SUCCESS;
        for (long blockNum = firstBlock;
                blockNum < firstBlock + groupSize && result == UploadResult.SUCCESS;
                blockNum++) {
            try {
                if (Thread.interrupted()) {
                    // Re-set the flag so that blockQueue.take() detects it and throws InterruptedException,
                    // which the caller's catch block handles (including cleanupEmptyUpload when etags is empty).
                    Thread.currentThread().interrupt();
                }
                final BlockWithSource item = blockQueue.take();
                final byte[] tarEntry = BlockToTarEntry.toTarEntry(item.block(), blockNum);
                final byte[] extended = new byte[buffer.length + tarEntry.length];
                System.arraycopy(buffer, 0, extended, 0, buffer.length);
                System.arraycopy(tarEntry, 0, extended, buffer.length, tarEntry.length);
                final BlockData blockData = new BlockData(extended, item.source());
                buffer = blockData.buffer();
                buffer = flushPartIfNeeded(blockNum, blockData.source(), buffer, s3, uploadId, etags, blocksInBuffer);
                if (buffer == null) {
                    result = UploadResult.FAILED;
                    buffer = new byte[0];
                } else if (buffer.length > 0) {
                    // Only track blockNum when its bytes are still in the buffer.  If flushPartIfNeeded
                    // found an exact fit (remainder is empty), it already added blockNum to blocksInBuffer
                    // and sent notifications — adding it again here would cause a duplicate notification.
                    blocksInBuffer.put(blockNum, blockData.source());
                }
            } catch (InterruptedException e) {
                LOGGER.log(TRACE, "Block upload task interrupted", e);
                cleanupEmptyUpload(s3, uploadId, etags);
                // TODO(1166) Do something with the result of the cleanup (if it is false)
                throw e;
            } catch (IOException e) {
                LOGGER.log(TRACE, "Failed to accumulate block %d".formatted(blockNum), e);
                throw e;
            }
        }
        return new UploadBlocksOutput(result, buffer);
    }

    /// Flushes a fixed-size part to S3 when [buffer] has reached [partSizeBytes], then sends
    /// [PersistedNotification]s for every block whose bytes are now durably stored.
    ///
    /// @param blockNum      the block number just accumulated (used for failure notifications and logging)
    /// @param blockSource   the [BlockSource] of the block just accumulated
    /// @param buffer        the current accumulation buffer
    /// @param s3            the S3 client for this upload session
    /// @param uploadId      the multipart upload ID
    /// @param etags         the list of part ETags collected so far (mutated in place)
    /// @param blocksInBuffer map of block number → source for blocks whose bytes are in [buffer]
    ///                       (mutated in place)
    /// @return the remainder buffer after the flush, or `null` if the part upload failed
    ///         (false [PersistedNotification]s have already been sent for all affected blocks)
    private byte[] flushPartIfNeeded(
            long blockNum,
            BlockSource blockSource,
            byte[] buffer,
            S3Client s3,
            String uploadId,
            List<String> etags,
            SortedMap<Long, BlockSource> blocksInBuffer) {
        try {
            byte[] remainder = buffer;

            if (buffer.length >= partSizeBytes) {
                remainder = uploadPart(buffer, s3, uploadId, etags);

                // If the current block's bytes exactly fill the part, include it before sending notifications
                if (remainder.length == 0) {
                    blocksInBuffer.put(blockNum, blockSource);
                }
                final Map.Entry<Long, BlockSource> last = blocksInBuffer.lastEntry();
                blockMessaging.sendBlockPersisted(
                        new PersistedNotification(last.getKey(), true, 1_000, last.getValue()));
                // Reset the map so the caller starts fresh for the next part's worth of blocks.
                blocksInBuffer.clear();
            }

            return remainder;
        } catch (S3ResponseException | IOException e) {
            final long firstBlockNum = blocksInBuffer.isEmpty()
                    ? blockNum
                    : blocksInBuffer.firstEntry().getKey();
            final BlockSource firstBlockSource = blocksInBuffer.isEmpty()
                    ? blockSource
                    : blocksInBuffer.firstEntry().getValue();
            blockMessaging.sendBlockPersisted(new PersistedNotification(firstBlockNum, false, 1_000, firstBlockSource));
            LOGGER.log(INFO, "Failed to upload part containing blocks %d to %d".formatted(firstBlockNum, blockNum), e);
            return null;
        }
    }

    /// Uploads [buffer] as the final multipart part and sends a [PersistedNotification] for the
    /// last block in [blocksInBuffer].
    ///
    /// Called after the main loop when leftover bytes did not fill a complete part. Assumes
    /// [buffer] is non-empty and that the upload has not already failed.
    ///
    /// @param buffer         the leftover bytes that did not fill a complete part during the loop
    /// @param s3             the S3 client for this upload session
    /// @param uploadId       the multipart upload ID
    /// @param etags          the list of part ETags collected so far (mutated in place)
    /// @param blocksInBuffer map of block number → source for blocks whose bytes are in [buffer]
    /// @return [UploadResult#SUCCESS] if the part was uploaded and a success notification was sent,
    ///         [UploadResult#FAILED] otherwise (a failure notification has already been sent)
    private UploadResult uploadFinalPart(
            byte[] buffer,
            S3Client s3,
            String uploadId,
            List<String> etags,
            SortedMap<Long, BlockSource> blocksInBuffer) {
        UploadResult partResult = UploadResult.SUCCESS;
        try {
            doUploadPart(buffer, s3, uploadId, etags);
        } catch (S3ResponseException | IOException e) {
            final Map.Entry<Long, BlockSource> first = blocksInBuffer.firstEntry();
            blockMessaging.sendBlockPersisted(
                    new PersistedNotification(first.getKey(), false, 1_000, first.getValue()));
            LOGGER.log(INFO, "Failed to upload final part for key {0}", key, e);
            partResult = UploadResult.FAILED;
            // TODO(1166) Make sure that we do our best to upload this block
        }
        if (partResult == UploadResult.SUCCESS) {
            final Map.Entry<Long, BlockSource> last = blocksInBuffer.lastEntry();
            blockMessaging.sendBlockPersisted(new PersistedNotification(last.getKey(), true, 1_000, last.getValue()));
        }
        return partResult;
    }

    /// Aborts the multipart upload if no parts have been uploaded yet (i.e., [etags] is empty).
    /// Called from the [InterruptedException] handler in [call] to avoid leaving incomplete
    /// multipart uploads in S3 when the task is cancelled before writing any data.
    ///
    /// @param s3       the S3 client for this upload session
    /// @param uploadId the multipart upload ID to abort
    /// @param etags    the list of part ETags uploaded so far; abort is skipped if non-empty
    private boolean cleanupEmptyUpload(S3Client s3, String uploadId, List<String> etags) {
        try {
            if (etags.isEmpty()) {
                s3.abortMultipartUpload(key, uploadId);
            }
            return true;
        } catch (S3ResponseException | IOException e) {
            LOGGER.log(INFO, "Failed to abort multipart upload after interruption", e);
            return false;
        }
    }

    /// Sends a single part to S3 and records its ETag.  Every S3 part upload — whether a
    /// fixed-size chunk flushed during the loop or the final partial buffer — goes through this
    /// method.
    ///
    /// @param buffer   the bytes to upload as one part; may be any length
    /// @param s3       the S3 client for this upload session
    /// @param uploadId the multipart upload ID
    /// @param etags    the list of part ETags collected so far (mutated in place)
    void doUploadPart(byte[] buffer, S3Client s3, String uploadId, List<String> etags)
            throws S3ResponseException, IOException {
        // S3 part numbers are 1-based, so the next part number is the current list size plus one.
        final int partNumber = etags.size() + 1;
        final String etag = s3.multipartUploadPart(key, uploadId, partNumber, buffer);
        etags.add(etag);
        LOGGER.log(TRACE, "Uploaded part {0}, etag {1}", partNumber, etag);
        // TODO(1166) catch exception in the method body and return false as method result (otherwise true)
    }

    /// Splits `buffer` into a [partSizeBytes]-sized part and a remainder, delegates the upload to
    /// [doUploadPart(byte[], S3Client, String, List)], and returns the remainder.
    ///
    /// @param buffer   the accumulation buffer; must be at least [partSizeBytes] long
    /// @param s3       the S3 client for this upload session
    /// @param uploadId the multipart upload ID
    /// @param etags    the list of part ETags collected so far (mutated in place)
    /// @return the bytes that did not fit in the uploaded part
    @NonNull
    private byte[] uploadPart(byte[] buffer, S3Client s3, String uploadId, List<String> etags)
            throws S3ResponseException, IOException {
        // Split the buffer into a part and a remainder
        final byte[] part = new byte[partSizeBytes];
        final byte[] remainder = new byte[buffer.length - partSizeBytes];
        System.arraycopy(buffer, 0, part, 0, partSizeBytes);
        System.arraycopy(buffer, partSizeBytes, remainder, 0, remainder.length);

        doUploadPart(part, s3, uploadId, etags);

        return remainder;
    }

    /// Computes the S3 object key for this task's tar archive.
    ///
    /// The key encodes the first block number of the batch as a hierarchical path of 4-digit
    /// segments, with the precision controlled by [ArchiveCloudStorageConfig#groupingLevel()].
    ///
    /// **Algorithm:**
    /// 1. Zero-pad [firstBlock] to 19 digits (the maximum decimal width of a `long`).
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

    /// Carries the extended accumulation buffer and the [BlockSource] of the block just taken from
    /// the queue out of [accumulateBlock] as a single return value.
    private record BlockData(byte[] buffer, BlockSource source) {}

    /// Carries the [UploadResult] and leftover buffer bytes out of [uploadBlocks].
    /// [remainderBuffer] is always non-null; it is empty when [result] is [UploadResult#FAILED].
    private record UploadBlocksOutput(UploadResult result, byte[] remainderBuffer) {}

    /// The outcome of a completed [BlockUploadTask].
    enum UploadResult {
        /// All blocks were successfully archived and persisted notifications were sent.
        SUCCESS,
        /// A part upload failed; false persisted notifications were sent for the affected blocks.
        FAILED
    }
}
