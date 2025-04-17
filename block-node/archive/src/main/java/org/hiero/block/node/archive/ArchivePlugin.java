// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.archive;

import java.util.Iterator;
import java.util.stream.LongStream;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor.Format;

/**
 * This a block node plugin that stores verified blocks in a cloud archive for disaster recovery and backup. It will
 * archive in batches as storing billions of small files in the cloud is non-ideal and expensive. Archive style cloud
 * storage has minimum file sizes and per file costs. So batches of compressed blocks will be more optimal. It will
 * watch persisted block notifications for when the next batch of blocks is available to be archived. It will then
 * fetch those blocks from persistence plugins and upload to the archive.
 */
public class ArchivePlugin implements BlockNodePlugin, BlockNotificationHandler {
    /** The logger for this class. */
    private final System.Logger LOGGER = System.getLogger(getClass().getName());
    // keep track of the last block number archived
    // have configuration for the number of blocks to archive at a time
    // on init register as a block notification handler, listen to persisted notifications
    // on persisted notification, check if the whole batch size of blocks from last block archived is available
    // if so, fetch them from the persistence plugin, compress and upload to the archive bucket
    /** The block node context, for access to core facilities. */
    private BlockNodeContext context;
    /** The configuration for the archive plugin. */
    private ArchiveConfig archiveConfig;

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
     * Upload a batch of blocks to the tar archive in S3 bucket. This is called when a batch of blocks can
     * be archived.
     *
     * @param startBlockNumber the first block number to upload
     * @param endBlockNumber the last block number to upload, inclusive
     */
    void uploadBlocksTar(long startBlockNumber, long endBlockNumber) {
        // The HTTP client needs an Iterable of byte arrays, so create one from the blocks
        final Iterator<byte[]> tarBlocks = new TaredBlockIterator(
                Format.ZSTD_PROTOBUF,
                LongStream.range(startBlockNumber, endBlockNumber + 1)
                        .mapToObj(
                                blockNumber -> context.historicalBlockProvider().block(blockNumber))
                        .iterator());
        // Upload the blocks to S3
        try (final S3Client s3Client = new S3Client(
                archiveConfig.regionName(),
                archiveConfig.endpointUrl(),
                archiveConfig.bucketName(),
                archiveConfig.accessKey(),
                archiveConfig.secretKey())) {
            s3Client.uploadFile(
                    archiveConfig.basePath() + "/" + startBlockNumber + "-" + endBlockNumber + ".tar",
                    archiveConfig.storageClass(),
                    tarBlocks,
                    "application/x-tar");
        } catch (Exception e) {
            LOGGER.log(System.Logger.Level.ERROR, "Failed to upload blocks to S3: " + e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
