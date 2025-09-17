// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.archive.s3;

import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;
import static org.hiero.block.node.base.BlockFile.blockNumberFormated;

import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;
import org.hiero.block.common.utils.StringUtilities;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.base.s3.S3Client;
import org.hiero.block.node.base.s3.S3ResponseException;
import org.hiero.block.node.base.tar.TaredBlockIterator;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor.Format;

/**
 * This a block node plugin that stores verified blocks in a cloud archive for disaster recovery and backup. It will
 * archive in batches as storing billions of small files in the cloud is non-ideal and expensive. Archive style cloud
 * storage has minimum file sizes and per file costs. So batches of compressed blocks will be more optimal. It will
 * watch persisted block notifications for when the next batch of blocks can be archived. It will then fetch those
 * blocks from persistence plugins and upload to the archive.
 */
public class S3ArchivePlugin implements BlockNodePlugin, BlockNotificationHandler {
    /** The storage class used for uploading the lastest block file */
    public static final String LATEST_FILE_STORAGE_CLASS = "STANDARD";
    /** The logger for this class. */
    private final System.Logger LOGGER = System.getLogger(getClass().getName());
    /** The name of the file containing the latest archived block number. */
    static final String LATEST_ARCHIVED_BLOCK_FILE = "latestArchivedBlockNumber.txt";
    /** Format for the year/month directory structure. */
    private static final DateTimeFormatter YEAR_MONTH_FORMATTER = DateTimeFormatter.ofPattern("yyyy/MM");
    /** The format for the date-based prefix for the archive files. */
    private static final DateTimeFormatter FILE_PREFIX_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss_");
    /** The block node context, for access to core facilities. */
    private BlockNodeContext context;
    /** The configuration for the archive plugin. */
    private S3ArchiveConfig archiveConfig;
    /** Plugin enabled flag. */
    private final AtomicBoolean enabled = new AtomicBoolean(false);
    /** A single thread executor service for the archive plugin, background jobs. */
    private ExecutorService executorService;
    /** list of pending uploads. This is used to cancel uploads if the plugin is stopped and for testing. */
    final CopyOnWriteArrayList<CompletableFuture<Void>> pendingUploads = new CopyOnWriteArrayList<>();
    /** The latest block number that has been archived. If there are no archived blocks then is UNKNOWN_BLOCK_NUMBER */
    private final AtomicLong lastArchivedBlockNumber = new AtomicLong(UNKNOWN_BLOCK_NUMBER);

    // ==== Plugin Methods =============================================================================================

    /**
     * {@inheritDoc}
     */
    @NonNull
    @Override
    public List<Class<? extends Record>> configDataTypes() {
        return List.of(S3ArchiveConfig.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(final BlockNodeContext context, final ServiceBuilder serviceBuilder) {
        this.context = context;
        this.archiveConfig = context.configuration().getConfigData(S3ArchiveConfig.class);
        // check if enabled by the "endpointUrl" property being non-empty in config
        if (StringUtilities.isBlank(archiveConfig.endpointUrl())) {
            LOGGER.log(INFO, "S3 Archive plugin is disabled. No endpoint URL provided.");
            return;
        } else if (StringUtilities.isBlank(archiveConfig.accessKey())) {
            LOGGER.log(INFO, "S3 Archive plugin is disabled. No access key provided.");
            return;
        } else if (StringUtilities.isBlank(archiveConfig.secretKey())) {
            LOGGER.log(INFO, "S3 Archive plugin is disabled. No secret key provided.");
            return;
        }
        // set up the executor service
        this.executorService = context.threadPoolManager()
                .createSingleThreadExecutor(
                        "S3ArchiveRunner",
                        (t, e) -> LOGGER.log(ERROR, "Uncaught exception in thread: " + t.getName(), e));
        // plugin is enabled
        enabled.set(true);
        // register
        context.blockMessaging().registerBlockNotificationHandler(this, false, name());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start() {
        if (enabled.get()) {
            // fetch the last archived block number from the archive
            try (final S3Client s3Client = new S3Client(
                    archiveConfig.regionName(),
                    archiveConfig.endpointUrl(),
                    archiveConfig.bucketName(),
                    archiveConfig.accessKey(),
                    archiveConfig.secretKey())) {
                final String lastArchivedBlockNumberString = s3Client.downloadTextFile(LATEST_ARCHIVED_BLOCK_FILE);
                if (lastArchivedBlockNumberString != null && !lastArchivedBlockNumberString.isEmpty()) {
                    lastArchivedBlockNumber.set(Long.parseLong(lastArchivedBlockNumberString));
                }
                LOGGER.log(INFO, "Last S3 archived block number: " + lastArchivedBlockNumber);
            } catch (final Exception e) {
                LOGGER.log(ERROR, "Failed to read latest archived block file: " + e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop() {
        BlockNodePlugin.super.stop();
        // cancel all pending uploads
        for (final Future<?> future : pendingUploads) {
            future.cancel(true);
        }
    }

    // ==== BlockItemHandler Methods ===================================================================================

    /**
     * {@inheritDoc}
     */
    @Override
    public void handlePersisted(final PersistedNotification notification) {
        // get the latest persisted block number from the notification
        final long latestPersisted = notification.blockNumber();
        // compute what should be the start of the next batch to archive
        long nextStart = lastArchivedBlockNumber.get() + 1;
        // compute the end of the next batch to archive
        long nextEnd = nextStart + archiveConfig.blocksPerFile() - 1;
        // while the next batch end is less than or equal to the latest
        // persisted block number, we can schedule a batch
        while (nextEnd <= latestPersisted) {
            // schedule the batch archiving
            scheduleBatchArchiving(nextStart, nextEnd);
            // move to the next batch and try again
            nextStart = nextEnd + 1;
            nextEnd = nextStart + archiveConfig.blocksPerFile() - 1;
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
        pendingUploads.add(CompletableFuture.runAsync(
                () -> {
                    // Check the nextBatchStartBlockNumber is less than lastArchivedBlockNumber, ie is this a duplicate
                    // batch and
                    // already been archived. That can happen as the archive job has not been completed for a batch and
                    // a new
                    // batch is created.
                    if (nextBatchStartBlockNumber > lastArchivedBlockNumber.get()) {
                        // log started
                        LOGGER.log(
                                INFO,
                                "Uploading archive block batch: " + nextBatchStartBlockNumber + "-"
                                        + nextBatchEndBlockNumber);
                        try (final S3Client s3Client = new S3Client(
                                archiveConfig.regionName(),
                                archiveConfig.endpointUrl(),
                                archiveConfig.bucketName(),
                                archiveConfig.accessKey(),
                                archiveConfig.secretKey())) {
                            // fetch the blocks from the persistence plugins and archive
                            uploadBlocksTar(s3Client, nextBatchStartBlockNumber, nextBatchEndBlockNumber);
                            // write the latest archived block number to the archive
                            s3Client.uploadTextFile(
                                    LATEST_ARCHIVED_BLOCK_FILE,
                                    LATEST_FILE_STORAGE_CLASS,
                                    String.valueOf(nextBatchEndBlockNumber));
                            // update the last archived block number
                            lastArchivedBlockNumber.set(nextBatchEndBlockNumber);
                            // log completed
                            LOGGER.log(
                                    INFO,
                                    "Uploaded archive block batch: " + nextBatchStartBlockNumber + "-"
                                            + nextBatchEndBlockNumber);
                        } catch (final S3ResponseException e) {
                            // todo we could retry here
                            LOGGER.log(
                                    INFO,
                                    "Failed to upload archive block batch due to an exceptional response: "
                                            + e.getMessage(),
                                    e);
                            throw new RuntimeException(e);
                        } catch (final Exception e) {
                            LOGGER.log(ERROR, "Failed to upload archive block batch: " + e.getMessage(), e);
                            throw new RuntimeException(e);
                        }
                    }
                },
                executorService));
        // cleanup pendingUploads, remove completed futures from the pending uploads list
        pendingUploads.removeIf(future -> future.isDone() || future.isCancelled());
    }

    /**
     * Upload a batch of blocks to the tar archive in S3 bucket. This is called when a batch of blocks can
     * be archived.
     *
     * @param s3Client the S3 client to use for uploading the blocks
     * @param startBlockNumber the first block number to upload
     * @param endBlockNumber the last block number to upload, inclusive
     */
    private void uploadBlocksTar(final S3Client s3Client, final long startBlockNumber, final long endBlockNumber)
            throws IllegalStateException, S3ResponseException, IOException {
        // The HTTP client needs an Iterable of byte arrays, so create one from the blocks
        final Iterator<BlockAccessor> blocksIterator = LongStream.range(startBlockNumber, endBlockNumber + 1)
                .mapToObj(blockNumber -> context.historicalBlockProvider().block(blockNumber))
                .iterator();
        final Iterator<byte[]> tarBlocks = new TaredBlockIterator(Format.ZSTD_PROTOBUF, blocksIterator);
        // fetch the first blocks consensus time so that we can place the file in a directory based on year and month
        final BlockUnparsed firstBlock =
                context.historicalBlockProvider().block(startBlockNumber).blockUnparsed();
        final ZonedDateTime firstBlockConsensusTime;
        try {
            final Bytes headerBytes = firstBlock.blockItems().getFirst().blockHeader();
            if (headerBytes == null) {
                throw new IllegalStateException("Block header is null");
            }
            final BlockHeader header = BlockHeader.PROTOBUF.parse(headerBytes);
            if (header.blockTimestamp() == null) {
                throw new IllegalStateException("Block header firstTransactionConsensusTime is null");
            }
            firstBlockConsensusTime = ZonedDateTime.ofInstant(
                    Instant.ofEpochSecond(
                            header.blockTimestamp().seconds(),
                            header.blockTimestamp().nanos()),
                    ZoneOffset.UTC);
        } catch (final ParseException e) {
            throw new IllegalStateException("Failed to parse Block Header from first block", e);
        }

        // Upload the blocks to S3
        s3Client.uploadFile(
                archiveConfig.basePath() + "/" + YEAR_MONTH_FORMATTER.format(firstBlockConsensusTime)
                        + "/"
                        + FILE_PREFIX_FORMATTER.format(firstBlockConsensusTime) + blockNumberFormated(startBlockNumber)
                        + "-" + blockNumberFormated(endBlockNumber) + ".tar",
                archiveConfig.storageClass(),
                tarBlocks,
                "application/x-tar");
    }
}
