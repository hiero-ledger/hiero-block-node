// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.archive;

import static org.hiero.block.node.archive.S3Upload.uploadFile;

import com.hedera.pbj.runtime.io.buffer.Bytes;
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
     * Upload a batch of blocks to the tar archive in S3 bucket. This is called when a batch of blocks is available to
     * be archived.
     *
     * @param startBlockNumber the first block number to upload
     * @param endBlockNumber the last block number to upload, inclusive
     */
    public void uploadBlocksTar(long startBlockNumber, long endBlockNumber) {
        // The HTTP client needs an Iterable of byte arrays, so create one from the blocks
        final Iterable<byte[]> tarBlocks = () -> new TaredBlockIterator(Format.ZSTD_PROTOBUF,
                LongStream.range(startBlockNumber, endBlockNumber+1)
                        .mapToObj(blockNumber -> context.historicalBlockProvider().block(blockNumber))
                        .iterator());
        // Upload the blocks to S3
        uploadFile(archiveConfig.endpointUrl(),
                archiveConfig.bucketName(),
                archiveConfig.basePath() + "/"
                        + startBlockNumber + "-" + endBlockNumber + ".tar",
                archiveConfig.accessKey(),
                archiveConfig.secretKey(),
                tarBlocks,
                "application/x-tar");
    }
}
