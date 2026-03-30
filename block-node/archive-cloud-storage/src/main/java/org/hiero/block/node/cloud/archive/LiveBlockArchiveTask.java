// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.archive;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.TRACE;

import com.hedera.bucky.S3Client;
import com.hedera.bucky.S3ClientException;
import com.hedera.bucky.S3ResponseException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;

public class LiveBlockArchiveTask implements BlockArchiveTask {

    private static final System.Logger LOGGER = System.getLogger(LiveBlockArchiveTask.class.getName());

    // Block node infrastructure
    private final ArchiveCloudStorageConfig config;
    private final BlockMessagingFacility blockMessaging;

    // This is the first block number of this task's range
    private final long firstBlockNumber;
    // This is the last block number of this task's range
    private final long lastBlockNumber;
    // The number of blocks a tar archive should contain
    private final long maxBlocksPerTar;

    // We are uploading the content of the tar archive in parts (multipart upload). Each part has a fixed size.
    // This is the size of the part in bytes
    private final int partSizeBytes;

    // Keep track of the block number that was last processed. Not the block number that was last received.
    private long currentBlockNumber;
    // Keep track of the number of blocks that were received. If this becomes equal to maxBlocksPerTar,
    // the caller (the plugin) will start a new tar archive task
    private int blocksReceived = 0;
    // Keep track of the number of blocks that were processed. If this becomes equal to maxBlocksPerTar,
    // the task itself should consider itself finished
    private int blocksProcessed = 0;

    // It is a blocking queue per se, implemented in a map, because we need to support scenarios where blocks arrive
    // in wrong order, but at the same time be able to pull the blocks in the correct order. Both producer and consumer
    // of this map will check whether the entry with a block number is present. If the producer is faster, then it will
    // create the future and submit it to the map. If the consumer is faster, then it will wait for the future to be
    // submited
    // and will "join" it
    private final Map<Long, CompletableFuture<BlockUnparsed>> blocks = new ConcurrentHashMap<>();
    // This is the buffer that will contain the blocks that are currently being processed. It is extended with every new
    // block that is received. Once the buffer reaches a predefined size, that size is uploaded to S3 and the buffer is
    // cut and what remains is put in the front of the buffer
    byte[] taredBlocksBuffer = new byte[0];
    // Keep track of the block numbers of the blocks that were buffered before the last upload. This is used to send
    // persisted notifications for those blocks
    private final List<Long> blocksInBuffer = new ArrayList<>();

    // As there are two threads performing S3 client operations, we need to ensure thread safety when accessing it, so
    // that abort doesn't happen mid-operation
    private final ReentrantLock s3ClientLock = new ReentrantLock();
    // We are aborting the upload from the plugin thread. We need to know to stop the processing loop, so that
    // the task can complete
    private final AtomicBoolean aborted = new AtomicBoolean(false);

    private S3Client s3Client;
    // The key in S3. Its format is calculated based on the first block number of the range and number of blocks per tar
    // (i.e. the grouping level). Follows the trie "structure" (no directories on S3, just use slashes to simulate them)
    private final String fileName;
    // The upload ID of the multipart upload. Initialized once the first block is processed and the upload is started
    // Used for all later operations on the multipart upload, like adding a new part to it, aborting it or completing it
    private String uploadId;
    // When a part is uploaded, its etag is returned. This is used to complete the upload later
    private final List<String> partEtags = new ArrayList<>();

    LiveBlockArchiveTask(
            ArchiveCloudStorageConfig config,
            BlockMessagingFacility blockMessaging,
            long firstBlockNumber,
            long maxBlocksPerTar) {
        this.config = config;
        this.blockMessaging = blockMessaging;
        this.firstBlockNumber = firstBlockNumber;
        this.maxBlocksPerTar = maxBlocksPerTar;
        this.lastBlockNumber = firstBlockNumber + maxBlocksPerTar - 1;
        this.currentBlockNumber = firstBlockNumber;
        this.partSizeBytes = config.partSizeMb() * 1024 * 1024;
        fileName = computeKey();
    }

    public void init() throws S3ClientException, IOException {
        s3Client = new S3Client(
                config.regionName(), config.endpointUrl(), config.bucketName(), config.accessKey(), config.secretKey());
        // TODO (2030) check for already started upload
        this.uploadId = s3Client.createMultipartUpload(fileName, config.storageClass(), "x/application-tar");
    }

    @Override
    public void run() {
        try {
            while (blocksProcessed < maxBlocksPerTar && !aborted.get()) {
                processBlock();
            }

            if (blocksProcessed == maxBlocksPerTar) {
                completeUpload();
            }
        } catch (Exception e) {
            LOGGER.log(DEBUG, "Live block archive task failed", e);
        }
    }

    private void processBlock() throws S3ClientException, IOException {
        try {
            // Wait for the next block to be received, ignoring at this moment those that should be processed later
            final BlockUnparsed currentBlock = blocks.computeIfAbsent(
                            currentBlockNumber, _ -> new CompletableFuture<>())
                    .join();
            // Once the block is received, convert it to a tar entry
            byte[] taredBlock = BlockToTarEntry.toTarEntry(currentBlock, currentBlockNumber);

            // Extend the buffer to be able to accept the new block's bytes. Move around existing bytes and add the new
            // ones
            final byte[] extendedBuffer = new byte[taredBlocksBuffer.length + taredBlock.length];
            System.arraycopy(taredBlocksBuffer, 0, extendedBuffer, 0, taredBlocksBuffer.length);
            System.arraycopy(taredBlock, 0, extendedBuffer, taredBlocksBuffer.length, taredBlock.length);
            taredBlocksBuffer = extendedBuffer;

            // If the buffer is past the part size, upload the current part and start buffering the next one
            if (taredBlocksBuffer.length >= partSizeBytes) {
                final byte[] remainder = uploadBlockChunk();

                // The last block fitted exactly the part size, let's add it now to the list of blocks in buffer
                if (remainder.length == 0) {
                    blocksInBuffer.add(currentBlockNumber);
                }
                // Let's now send notification for all blocks that were buffered before the upload
                blocksInBuffer.forEach(block -> blockMessaging.sendBlockPersisted(
                        new PersistedNotification(block, true, 1_000, BlockSource.CLOUD_ARCHIVE)));
                // Let's clear the buffer so that we start buffering the next chunk
                blocksInBuffer.clear();

                // If the remainder is not empty, it means that the last block didn't fit exactly the part size,
                // so it will go with the next chunk
                taredBlocksBuffer = remainder;
            }

            // Current block is not a part of a multipart upload, so we need to remember it and send notification once
            // it is
            if (taredBlocksBuffer.length > 0) {
                blocksInBuffer.add(currentBlockNumber);
            }

            // Remove current block number from the list of blocks to be processed
            blocks.remove(currentBlockNumber);

            // Increment counters
            currentBlockNumber++;
            blocksProcessed++;
        } catch (S3ClientException | IOException e) {
            LOGGER.log(DEBUG, "Failed to upload block %d".formatted(currentBlockNumber), e);
            blockMessaging.sendBlockPersisted(
                    new PersistedNotification(currentBlockNumber, false, 1_000, BlockSource.CLOUD_ARCHIVE));
            // @TODO(1166) Maybe throw specific exception to let caller also do its handling
            throw e;
        }
    }

    private byte[] uploadBlockChunk() throws S3ResponseException, IOException {
        // We have to upload a fixed number of bytes per part (if it is not the last part). So let's take
        // the first partSizeBytes bytes from the buffer and upload them. The remainder will be uploaded later
        final byte[] partToUpload = new byte[partSizeBytes];
        final byte[] remainder = new byte[taredBlocksBuffer.length - partSizeBytes];
        System.arraycopy(taredBlocksBuffer, 0, partToUpload, 0, partSizeBytes);
        System.arraycopy(taredBlocksBuffer, partSizeBytes, remainder, 0, remainder.length);

        // Let's make sure that we are the only ones using the S3Client at the same time
        s3ClientLock.lock();
        try {
            int partNumber = partEtags.size() + 1; // part number in S3 Client API starts from 1
            final String partEtag = s3Client.multipartUploadPart(fileName, uploadId, partNumber, partToUpload);
            // Part was uploaded, we need its etag to be able to complete the upload later
            partEtags.add(partEtag);
            LOGGER.log(TRACE, "Successfully uploaded block {} and received etag {}", partNumber, partEtag);
        } finally {
            // Unlock the S3Client to allow other threads to use it
            s3ClientLock.unlock();
        }
        return remainder;
    }

    private void completeUpload() throws S3ResponseException, IOException {
        s3ClientLock.lock();
        try {
            // First, let check whether there are some block bytes left over and upload them as last part
            if (taredBlocksBuffer.length > 0) {
                final int partNumber = partEtags.size() + 1;
                final String partEtag = s3Client.multipartUploadPart(fileName, uploadId, partNumber, taredBlocksBuffer);
                partEtags.add(partEtag);
                blocksInBuffer.forEach(block -> blockMessaging.sendBlockPersisted(
                        new PersistedNotification(block, true, 1_000, BlockSource.CLOUD_ARCHIVE)));
                blocksInBuffer.clear();
                taredBlocksBuffer = new byte[0];
            }
            // Complete the multipart upload and close the client: we are done with this tar
            s3Client.completeMultipartUpload(fileName, uploadId, partEtags);
            s3Client.close();
        } catch (S3ClientException | IOException e) {
            LOGGER.log(DEBUG, "Failed to complete multipart upload: {0}", e.toString(), e);
            // @TODO(1166) Maybe throw specific exception to let caller also do its handling
            throw e;
        } finally {
            s3ClientLock.unlock();
        }
    }

    @Override
    public ArchiveResult submit(BlockUnparsed block, long blockNumber) {
        // A block is submitted for archiving, but we are not responsible for it. Let's tell the caller about it
        if (blockNumber < firstBlockNumber || blockNumber > lastBlockNumber) {
            return ArchiveResult.BLOCK_OUT_OF_RANGE;
        }

        // Let's submit this for work. There is a task monitoring the blocks map (which acts as a queue), we just give
        // it some work
        blocks.computeIfAbsent(blockNumber, _ -> new CompletableFuture<>()).complete(block);
        // Increment the counter for received blocks so that the caller (the plugin) knows when to stop feeding us
        // blocks
        blocksReceived++;
        if (blocksReceived == maxBlocksPerTar) {
            // If we have received all the blocks that we should have received, let's tell that to the caller (the
            // plugin)
            return ArchiveResult.FINISHED;
        }

        // We have successfully submitted the block for archiving and we expect more to come
        return ArchiveResult.SUCCESS;
    }

    @Override
    public void abort() {
        this.aborted.set(true);
        // If there are still tasks in the queue, we need to cancel them and to clean it up
        blocks.values().forEach(future -> future.cancel(true));
        blocks.clear();
        s3ClientLock.lock();
        try {
            // Now let's abort the multipart upload and close the client
            s3Client.abortMultipartUpload(fileName, uploadId);
            s3Client.close();
        } catch (S3ClientException | IOException e) {
            LOGGER.log(DEBUG, "Failed to abort multipart upload.", e);
        } finally {
            s3ClientLock.unlock();
        }
    }

    /// Computes the S3 object key for this task's tar archive.
    ///
    /// The key encodes the first block number of the batch as a hierarchical path of 4-digit
    /// segments, with the precision controlled by [ArchiveCloudStorageConfig#groupingLevel].
    ///
    /// **Algorithm:**
    /// 1. Zero-pad [firstBlockNumber] to 19 digits (the maximum decimal width of a {@code long}).
    /// 2. Drop the last `groupingLevel` digits — those digits vary within a single tar batch and
    ///    are therefore not part of the key.
    /// 3. Split the remaining prefix into 4-character segments.
    /// 4. Strip leading zeros from the final segment (cosmetic: `0012` → `12`).
    /// 5. Join all segments with {@code /} and append {@code .tar}.
    ///
    /// **Examples** (groupingLevel = 1, batches of 10 blocks):
    /// - block 0 → `0000/0000/0000/0000/0.tar`
    /// - block 10 → `0000/0000/0000/0000/1.tar`
    /// - block 1_234_567_890 → `0000/0000/0123/4567/89.tar`
    ///
    /// @return the S3 object key for this tar archive, never {@code null}
    private String computeKey() {
        String truncated = String.format("%019d", firstBlockNumber).substring(0, 19 - config.groupingLevel());
        List<String> parts = new ArrayList<>();
        for (int i = 0; i < truncated.length(); i += 4) {
            parts.add(truncated.substring(i, Math.min(i + 4, truncated.length())));
        }
        parts.set(parts.size() - 1, String.valueOf(Long.parseLong(parts.getLast())));
        return String.join("/", parts) + ".tar";
    }
}
