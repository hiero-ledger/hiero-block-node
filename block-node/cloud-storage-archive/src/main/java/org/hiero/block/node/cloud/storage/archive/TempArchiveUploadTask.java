// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.archive;

import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.TRACE;
import static java.util.Objects.requireNonNull;

import com.hedera.bucky.S3Client;
import com.hedera.bucky.S3ClientInitializationException;
import com.hedera.bucky.S3ResponseException;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.spi.ApplicationStateFacility;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.historicalblocks.LongRange;

/// Archives a streaming, open-ended segment of verified blocks into one temporary `.tmp` tar file
/// on S3-compatible storage via a multipart upload.
///
/// Unlike [BlockUploadTask], which handles an aligned power-of-ten group whose size is known
/// upfront, this task handles an arbitrary contiguous segment whose end is not known at
/// construction time.  Blocks are fed one by one through a live [BlockingQueue] shared with
/// [CloudStorageArchivePlugin]; the segment ends when [SEGMENT_END] is placed in the queue.
///
/// Usage:
/// 1. Construct the task with [firstBlock] and an empty [blockQueue].
/// 2. Submit the task to a virtual-thread executor — it immediately blocks on [BlockingQueue#take].
/// 3. Offer verified blocks to the queue in ascending block-number order.
/// 4. Signal the end of the segment by offering [SEGMENT_END].
///
/// On completion the task:
/// 1. Completes the S3 multipart upload for the tar content at the `.tmp` key.
/// 2. Writes a companion `.meta` object containing the decimal string `lastBlock` so that
///    [StartupRecoveryTask] can reconstruct the [TempArchiveEntry] without downloading the tar.
/// 3. Calls [ApplicationStateFacility#addStoredBlockRange] so that backfill does not re-fetch
///    the archived blocks.
/// 4. Returns a completed [TempArchiveEntry] (with `uploadId == null`).
class TempArchiveUploadTask implements Callable<TempArchiveEntry> {

    private static final System.Logger LOGGER = System.getLogger(TempArchiveUploadTask.class.getName());

    /// Sentinel placed in the queue by [CloudStorageArchivePlugin] to signal end-of-segment.
    /// Detected by reference identity (`==`) inside [call]; never serialised or uploaded.
    static final BlockWithSource SEGMENT_END = new BlockWithSource(BlockUnparsed.DEFAULT, BlockSource.UNKNOWN);

    private final CloudStorageArchiveConfig config;
    private final BlockMessagingFacility blockMessaging;
    private final ApplicationStateFacility applicationStateFacility;
    private final CloudStorageArchivePlugin.MetricsHolder metricsHolder;

    private final String s3Key;
    private final long firstBlock;
    private final int partSizeBytes;
    private final BlockingQueue<BlockWithSource> blockQueue;

    TempArchiveUploadTask(
            @NonNull CloudStorageArchiveConfig config,
            @NonNull BlockMessagingFacility blockMessaging,
            @NonNull ApplicationStateFacility applicationStateFacility,
            @NonNull CloudStorageArchivePlugin.MetricsHolder metricsHolder,
            @NonNull String s3Key,
            long firstBlock,
            @NonNull BlockingQueue<BlockWithSource> blockQueue) {
        this.config = requireNonNull(config);
        this.blockMessaging = requireNonNull(blockMessaging);
        this.applicationStateFacility = requireNonNull(applicationStateFacility);
        this.metricsHolder = requireNonNull(metricsHolder);
        this.s3Key = requireNonNull(s3Key);
        this.firstBlock = firstBlock;
        this.partSizeBytes = config.partSizeMb() * 1024 * 1024;
        this.blockQueue = requireNonNull(blockQueue);
    }

    @Override
    public TempArchiveEntry call()
            throws S3ClientInitializationException, S3ResponseException, IOException, InterruptedException {
        try (S3Client s3 = S3UploadUtils.createClient(config)) {
            final String uploadId =
                    s3.createMultipartUpload(s3Key, config.storageClass().name(), S3UploadUtils.CONTENT_TYPE);
            final List<String> etags = new ArrayList<>();
            byte[] buffer = new byte[0];
            long totalBytes = 0;
            long currentBlock = firstBlock;
            long lastProcessedBlock = firstBlock - 1;
            BlockSource lastSource = BlockSource.UNKNOWN;

            while (true) {
                try {
                    final BlockWithSource item = blockQueue.take();
                    if (item == SEGMENT_END) {
                        break;
                    }
                    final byte[] tarEntry = TarEntries.toTarEntry(item.block(), currentBlock);
                    buffer = S3UploadUtils.concat(buffer, tarEntry);
                    lastSource = item.source();
                    lastProcessedBlock = currentBlock;
                    currentBlock++;

                    if (buffer.length >= partSizeBytes) {
                        final S3UploadUtils.SplitBuffer split = S3UploadUtils.splitAtPartSize(buffer, partSizeBytes);
                        doUploadPart(split.part(), s3, uploadId, etags);
                        buffer = split.remainder();
                        totalBytes += partSizeBytes;
                    }
                } catch (InterruptedException e) {
                    LOGGER.log(TRACE, "Temp archive upload task interrupted for key {0}", s3Key, e);
                    S3UploadUtils.abortQuietly(s3, s3Key, uploadId);
                    throw e;
                } catch (S3ResponseException | IOException e) {
                    LOGGER.log(INFO, "Failed to accumulate block {0} for temp archive {1}", currentBlock, s3Key, e);
                    S3UploadUtils.abortQuietly(s3, s3Key, uploadId);
                    throw e;
                }
            }

            final long lastBlock = lastProcessedBlock;

            if (buffer.length > 0) {
                try {
                    doUploadPart(buffer, s3, uploadId, etags);
                    totalBytes += buffer.length;
                } catch (S3ResponseException | IOException e) {
                    S3UploadUtils.abortQuietly(s3, s3Key, uploadId);
                    blockMessaging.sendBlockPersisted(new PersistedNotification(firstBlock, false, 1_000, lastSource));
                    LOGGER.log(INFO, "Failed to upload final temp archive part for key {0}", s3Key, e);
                    throw e;
                }
            }

            try {
                doCompleteMultipartUpload(s3, s3Key, uploadId, etags);
            } catch (S3ResponseException | IOException e) {
                S3UploadUtils.abortQuietly(s3, s3Key, uploadId);
                blockMessaging.sendBlockPersisted(new PersistedNotification(firstBlock, false, 1_000, lastSource));
                LOGGER.log(INFO, "Failed to complete temp archive multipart upload for key {0}", s3Key, e);
                throw e;
            }
            LOGGER.log(
                    TRACE,
                    "Completed temp archive upload for key {0}, blocks [{1}, {2}]",
                    s3Key,
                    firstBlock,
                    lastBlock);

            final String metaKey = TempArchiveKey.formatMeta(firstBlock, config.objectKeyPrefix());
            try {
                doUploadTextFile(s3, metaKey, config.storageClass().name(), String.valueOf(lastBlock));
            } catch (S3ResponseException | IOException e) {
                LOGGER.log(INFO, "Failed to write temp archive meta {0}, tar is already committed", metaKey, e);
                blockMessaging.sendBlockPersisted(new PersistedNotification(lastBlock, true, 1_000, lastSource));
                applicationStateFacility.addStoredBlockRange(new LongRange(firstBlock, lastBlock));
                metricsHolder.blocksWritten().increment(lastBlock - firstBlock + 1);
                metricsHolder.storedBytes().increment(totalBytes);
                throw e;
            }
            LOGGER.log(TRACE, "Wrote temp archive meta {0}", metaKey);

            blockMessaging.sendBlockPersisted(new PersistedNotification(lastBlock, true, 1_000, lastSource));
            applicationStateFacility.addStoredBlockRange(new LongRange(firstBlock, lastBlock));
            metricsHolder.blocksWritten().increment(lastBlock - firstBlock + 1);
            metricsHolder.storedBytes().increment(totalBytes);

            return new TempArchiveEntry(s3Key, firstBlock, lastBlock, null);
        }
    }

    /// Delegates to [S3UploadUtils#uploadPart].  Overridable so tests can inject part-upload
    /// failures without a live S3 endpoint.
    void doUploadPart(byte[] buffer, S3Client s3, String uploadId, List<String> etags)
            throws S3ResponseException, IOException {
        S3UploadUtils.uploadPart(buffer, s3Key, s3, uploadId, etags);
    }

    /// Delegates to [S3Client#completeMultipartUpload].  Overridable so tests can inject completion
    /// failures without a live S3 endpoint.
    void doCompleteMultipartUpload(S3Client s3, String key, String uploadId, List<String> etags)
            throws S3ResponseException, IOException {
        s3.completeMultipartUpload(key, uploadId, etags);
    }

    /// Delegates to [S3Client#uploadTextFile].  Overridable so tests can inject meta-write failures
    /// without a live S3 endpoint.
    void doUploadTextFile(S3Client s3, String key, String storageClass, String content)
            throws S3ResponseException, IOException {
        s3.uploadTextFile(key, storageClass, content);
    }
}
