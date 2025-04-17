// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.archive;

import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;
import org.hiero.block.node.base.s3.S3Client;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor.Format;

/**
 * This a block node plugin that stores verified blocks in a cloud archive for disaster recovery and backup. It will
 * archive in batches as storing billions of small files in the cloud is non-ideal and expensive. Archive style cloud
 * storage has minimum file sizes and per file costs. So batches of compressed blocks will be more optimal. It will
 * watch persisted block notifications for when the next batch of blocks can be archived. It will then fetch those
 * blocks from persistence plugins and upload to the archive.
 */
public class ArchivePlugin implements BlockNodePlugin, BlockNotificationHandler {
    /** The logger for this class. */
    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    private static final String LATEST_ARCHIVED_BLOCK_FILE = "latestArchivedBlockNumber.txt";
    /** The block node context, for access to core facilities. */
    private BlockNodeContext context;
    /** The configuration for the archive plugin. */
    private ArchiveConfig archiveConfig;
    /** A single thread executor service for the archive plugin, background jobs. Accessed from tests. */
    final ExecutorService executorService = Executors.newSingleThreadExecutor(task -> {
        final var thread = Thread.startVirtualThread(task);
        thread.setName("ArchivePlugin");
        thread.setUncaughtExceptionHandler(
                (t, e) -> LOGGER.log(System.Logger.Level.ERROR, "Uncaught exception in thread: " + t.getName(), e));
        return thread;
    });
    /** The latest block number that has been archived. If there are no archived blocks then is UNKNOWN_BLOCK_NUMBER */
    private final AtomicLong lastArchivedBlockNumber = new AtomicLong(UNKNOWN_BLOCK_NUMBER);

    // ==== Plugin Methods =============================================================================================

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(BlockNodeContext context, ServiceBuilder serviceBuilder) {
        this.context = context;
        archiveConfig = context.configuration().getConfigData(ArchiveConfig.class);
        context.blockMessaging().registerBlockNotificationHandler(this, false, name());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start() {
        // fetch the last archived block number from the archive
        try (final S3Client s3Client = new S3Client(
                archiveConfig.regionName(),
                archiveConfig.endpointUrl(),
                archiveConfig.bucketName(),
                archiveConfig.accessKey(),
                archiveConfig.secretKey())) {
            String lastArchivedBlockNumberString = s3Client.downloadTextFile(LATEST_ARCHIVED_BLOCK_FILE);
            if (lastArchivedBlockNumberString != null && !lastArchivedBlockNumberString.isEmpty()) {
                lastArchivedBlockNumber.set(Long.parseLong(lastArchivedBlockNumberString));
            }
        } catch (Exception e) {
            LOGGER.log(System.Logger.Level.ERROR, "Failed to read latest archived block file: " + e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    // ==== BlockItemHandler Methods ===================================================================================

    /**
     * {@inheritDoc}
     */
    @Override
    public void handlePersisted(PersistedNotification notification) {
        // check if there is a new batch of blocks to archive
        final long mostRecentPersistedBlockNumber = notification.endBlockNumber();
        long mostRecentArchivedBlockNumber = lastArchivedBlockNumber.get();
        // compute the next batch of blocks to archive
        long nextBatchStartBlockNumber =
                (mostRecentArchivedBlockNumber / archiveConfig.blocksPerFile()) * archiveConfig.blocksPerFile()
                        + archiveConfig.blocksPerFile();
        long nextBatchEndBlockNumber = nextBatchStartBlockNumber + archiveConfig.blocksPerFile() - 1;
        // find if there is blocksPerFile blocks past the mostRecentArchivedBlockNumber staring from a multiple of
        // blocksPerFile
        while (nextBatchEndBlockNumber <= mostRecentPersistedBlockNumber) {
            // we have a batch of blocks to archive, so schedule it on background thread
            scheduleBatchArchiving(nextBatchStartBlockNumber, nextBatchEndBlockNumber);
            // compute the next batch of blocks to archive
            mostRecentArchivedBlockNumber = mostRecentArchivedBlockNumber + archiveConfig.blocksPerFile();
            nextBatchStartBlockNumber =
                    (mostRecentArchivedBlockNumber / archiveConfig.blocksPerFile()) * archiveConfig.blocksPerFile()
                            + archiveConfig.blocksPerFile();
            nextBatchEndBlockNumber = nextBatchStartBlockNumber + archiveConfig.blocksPerFile() - 1;
        }
    }

    // ==== Private Methods ============================================================================================

    /**
     * Schedule a batch of blocks to be archived. This is called when a batch of blocks can be archived.
     *
     * @param nextBatchStartBlockNumber the first block number to archive
     * @param nextBatchEndBlockNumber the last block number to archive, inclusive
     */
    private void scheduleBatchArchiving(final long nextBatchStartBlockNumber, final long nextBatchEndBlockNumber) {
        // we have a batch of blocks to archive
        executorService.execute(() -> {
            try (final S3Client s3Client = new S3Client(
                    archiveConfig.regionName(),
                    archiveConfig.endpointUrl(),
                    archiveConfig.bucketName(),
                    archiveConfig.accessKey(),
                    archiveConfig.secretKey())) {
                // fetch the blocks from the persistence plugins and archive
                uploadBlocksTar(s3Client, nextBatchStartBlockNumber, nextBatchEndBlockNumber);
                // update the last archived block number
                lastArchivedBlockNumber.set(nextBatchEndBlockNumber);
                // write the latest archived block number to the archive
                s3Client.uploadTextFile(
                        LATEST_ARCHIVED_BLOCK_FILE, String.valueOf(nextBatchEndBlockNumber), "text/plain");
            } catch (Exception e) {
                LOGGER.log(System.Logger.Level.ERROR, "Failed to upload archive block batch: " + e.getMessage(), e);
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Upload a batch of blocks to the tar archive in S3 bucket. This is called when a batch of blocks can
     * be archived.
     *
     * @param s3Client the S3 client to use for uploading the blocks
     * @param startBlockNumber the first block number to upload
     * @param endBlockNumber the last block number to upload, inclusive
     */
    private void uploadBlocksTar(S3Client s3Client, long startBlockNumber, long endBlockNumber) {
        // The HTTP client needs an Iterable of byte arrays, so create one from the blocks
        final Iterator<byte[]> tarBlocks = new TaredBlockIterator(
                Format.ZSTD_PROTOBUF,
                LongStream.range(startBlockNumber, endBlockNumber + 1)
                        .mapToObj(
                                blockNumber -> context.historicalBlockProvider().block(blockNumber))
                        .iterator());
        // Upload the blocks to S3
        s3Client.uploadFile(
                archiveConfig.basePath() + "/" + startBlockNumber + "-" + endBlockNumber + ".tar",
                archiveConfig.storageClass(),
                tarBlocks,
                "application/x-tar");
    }
}
