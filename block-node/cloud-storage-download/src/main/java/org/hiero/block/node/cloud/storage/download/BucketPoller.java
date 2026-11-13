// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.download;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;

import com.hedera.bucky.S3Client;
import com.hedera.bucky.S3ResponseException;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.hiero.block.api.BlockRange;
import org.hiero.block.api.RosterEntry;
import org.hiero.block.api.TssData;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.node.base.ParseHelper;
import org.hiero.block.node.cloud.storage.download.CloudStorageDownloadPlugin.MetricsHolder;
import org.hiero.block.node.cloud.storage.download.PersistenceAwaiter.Outcome;
import org.hiero.block.node.spi.blockmessaging.ApplicationStateNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.BackfilledBlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.StoredBlocksNotification;
import org.hiero.block.node.spi.blockmessaging.TssDataNotification;

/// The scheduled poll of the bucket: probes it for the block the bucket must hold before a pull
/// is worth it, and pulls the missing range when it does. Only the poll thread runs the poll;
/// the stored blocks and the roster arrive as application state notifications.
class BucketPoller implements Runnable, ApplicationStateNotificationHandler {
    private static final System.Logger LOGGER = System.getLogger(BucketPoller.class.getName());

    /// Returned by [#probeAndPull(long)] when no pull completed.
    private static final long NO_PULL = -1L;

    private final CloudStorageDownloadConfig config;
    private final S3Client s3Client;
    private final BlockMessagingFacility messaging;
    private final PersistenceAwaiter awaiter;

    /// Runs the downloads of one window in parallel.
    private final ExecutorService downloader;

    private final MetricsHolder metrics;

    /// Stored block ranges in ascending order, as last reported.
    private volatile List<BlockRange> storedBlocks = List.of();

    /// Node IDs of the current roster, whose bucket folders are read.
    private volatile List<Long> nodeIds = List.of();

    /// Creates a poller with no stored blocks and no roster, until the first notifications.
    ///
    /// @param config the plugin configuration
    /// @param s3Client the bucket client
    /// @param messaging where pulled blocks are submitted
    /// @param awaiter waits for the outcome of each submitted block
    /// @param downloader runs the downloads of one window in parallel
    /// @param metrics the plugin's counters
    BucketPoller(
            final CloudStorageDownloadConfig config,
            final S3Client s3Client,
            final BlockMessagingFacility messaging,
            final PersistenceAwaiter awaiter,
            final ExecutorService downloader,
            final MetricsHolder metrics) {
        this.config = config;
        this.s3Client = s3Client;
        this.messaging = messaging;
        this.awaiter = awaiter;
        this.downloader = downloader;
        this.metrics = metrics;
    }

    /// {@inheritDoc}
    @Override
    public void handleStoredBlocksUpdate(final StoredBlocksNotification notification) {
        storedBlocks = notification.storedBlocks();
    }

    /// {@inheritDoc}
    @Override
    public void handleTssDataUpdate(final TssDataNotification notification) {
        nodeIds = nodeIdsOf(notification.tssData());
    }

    /// Stored block ranges, as last reported.
    ///
    /// @return the ranges in ascending order
    List<BlockRange> storedBlocks() {
        return storedBlocks;
    }

    /// Node IDs of the current roster.
    ///
    /// @return the node IDs, empty when no roster is known
    List<Long> nodeIds() {
        return nodeIds;
    }

    /// Extracts the node IDs of the current roster of the given TSS data.
    private static List<Long> nodeIdsOf(final TssData tssData) {
        final List<Long> ids = new ArrayList<>();
        if (tssData != null && tssData.hasCurrentRoster()) {
            for (final RosterEntry entry : tssData.currentRosterOrThrow().rosterEntries()) {
                ids.add(entry.nodeId());
            }
        }
        // TODO(#4080) fall back to the node IDs in the address book history when there is no TSS roster
        return List.copyOf(ids);
    }

    /// One scheduled run. Catches every runtime exception, since one escaping would cancel all
    /// later runs.
    @Override
    public void run() {
        try {
            poll();
        } catch (final RuntimeException e) {
            LOGGER.log(WARNING, "Cloud storage download poll failed", e);
        }
    }

    /// Probes the bucket, and probes again at once after every pull that completed its whole range.
    void poll() {
        long completedUpTo = NO_PULL;
        boolean pullCompleted = true;
        // a completed pull leaves the bucket possibly still ahead: probe again without waiting
        while (pullCompleted) {
            completedUpTo = probeAndPull(completedUpTo);
            pullCompleted = completedUpTo != NO_PULL;
        }
    }

    /// The last stored block.
    ///
    /// @return the end of the last stored range, `-1` when nothing is stored
    long lastStored() {
        final List<BlockRange> ranges = storedBlocks;
        return ranges.isEmpty() ? -1L : ranges.getLast().rangeEnd();
    }

    /// Whether a block is in the stored ranges.
    ///
    /// @param blockNumber the block
    /// @return `true` when the block is stored
    boolean isStored(final long blockNumber) {
        boolean stored = false;
        for (final BlockRange range : storedBlocks) {
            stored = stored || (range.rangeStart() <= blockNumber && blockNumber <= range.rangeEnd());
        }
        return stored;
    }

    /// Probes the bucket for the probe block and pulls the missing range when the bucket is far
    /// enough ahead.
    ///
    /// @param completedUpTo the last block of the pull this poll completed just before, or
    ///     [#NO_PULL]. The stored blocks may not report it yet, and the probe must not start
    ///     before it, or it would pull the same range again.
    /// @return the last block of the range when the pull completed it, [#NO_PULL] otherwise
    private long probeAndPull(final long completedUpTo) {
        long pulledUpTo = NO_PULL;
        final List<Long> nodes = nodeIds;
        if (nodes.isEmpty()) {
            LOGGER.log(DEBUG, "No roster node IDs known, skipping the bucket probe");
        } else {
            final long last = Math.max(lastStored(), completedUpTo);
            // the bucket is far enough ahead to pull once this block is complete in it
            final long target = last + config.aheadThresholdBlocks();
            try {
                final long sourceNode = findBlock(nodes, target);
                // TODO(#4097) skip the pull when a peer block node has the target block
                if (sourceNode >= 0 && pull(last, target, sourceNode)) {
                    pulledUpTo = target;
                }
            } catch (final IOException | S3ResponseException e) {
                // the client reports an interrupt, as on stop, as an IOException
                LOGGER.log(
                        Thread.currentThread().isInterrupted() ? DEBUG : WARNING,
                        "Bucket probe for block " + target + " failed",
                        e);
            }
        }
        return pulledUpTo;
    }

    /// Looks for a block in the folder of a random roster node, then in the folder of one more
    /// random node, different from the first when there is more than one.
    ///
    /// @return the ID of the node whose folder holds the block, `-1` when neither does
    private long findBlock(final List<Long> nodes, final long block) throws IOException, S3ResponseException {
        final int size = nodes.size();
        final int first = ThreadLocalRandom.current().nextInt(size);
        long sourceNode = hasBlock(nodes.get(first), block) ? nodes.get(first) : -1L;
        if (sourceNode < 0 && size > 1) {
            final int second = (first + 1 + ThreadLocalRandom.current().nextInt(size - 1)) % size;
            if (hasBlock(nodes.get(second), block)) {
                sourceNode = nodes.get(second);
            }
        }
        return sourceNode;
    }

    /// Whether the block object exists in a node folder. The uploader writes each block with a
    /// single PUT, so an object at the key is always complete. The check lists the key instead of
    /// opening the object, so it does not start a download of the whole block.
    private boolean hasBlock(final long nodeId, final long block) throws IOException, S3ResponseException {
        final String key = BlockKeys.blockKey(config.objectKeyPrefix(), nodeId, block);
        // the listing matches by prefix, so only an exact key counts
        return s3Client.listObjectsPage(key, null, null, 1).keys().contains(key);
    }

    /// Pulls the blocks after the last stored block up to the target from the folder of the
    /// source node, in windows of `downloadWindow` blocks downloaded in parallel. Each window is
    /// submitted in block order, and each block is awaited before the next is submitted. The
    /// first block that cannot be used, fails or times out ends the pull.
    ///
    /// @param last the last stored block
    /// @param target the last block to pull
    /// @param sourceNode the node whose folder holds the blocks
    /// @return `true` when the whole range was pulled
    boolean pull(final long last, final long target, final long sourceNode) {
        LOGGER.log(INFO, "Pulling blocks {0} to {1} from the folder of node {2}", last + 1, target, sourceNode);
        final long started = System.nanoTime();
        final PullProgress progress = new PullProgress();
        long windowStart = last + 1;
        while (progress.endReason == null && windowStart <= target) {
            final long windowEnd = Math.min(target, windowStart + config.downloadWindow() - 1);
            pullWindow(windowStart, windowEnd, sourceNode, progress);
            windowStart = windowEnd + 1;
        }
        LOGGER.log(
                INFO,
                "Pull from the folder of node {0} ended after block {1}: {2} pulled, {3} already stored, in {4} ms{5}",
                sourceNode,
                progress.lastHandled,
                progress.pulled,
                progress.alreadyStored,
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started),
                progress.endReason == null ? "" : "; ended early: " + progress.endReason);
        return progress.endReason == null;
    }

    /// Downloads one window in parallel, then submits its blocks in order and awaits each.
    private void pullWindow(
            final long windowStart, final long windowEnd, final long sourceNode, final PullProgress progress) {
        final List<Future<BlockUnparsed>> downloads = new ArrayList<>();
        for (long blockNumber = windowStart; blockNumber <= windowEnd; blockNumber++) {
            downloads.add(isStored(blockNumber) ? null : downloader.submit(new Download(blockNumber, sourceNode)));
        }
        for (int i = 0; i < downloads.size(); i++) {
            final long blockNumber = windowStart + i;
            final Future<BlockUnparsed> download = downloads.get(i);
            if (progress.endReason != null || Thread.currentThread().isInterrupted()) {
                if (download != null) {
                    download.cancel(true);
                }
                progress.end("interrupted");
            } else if (download == null) {
                progress.alreadyStored(blockNumber);
            } else {
                final BlockUnparsed block = downloaded(download, blockNumber, sourceNode, progress);
                if (block != null && isStored(blockNumber)) {
                    // stored by another source while it downloaded
                    progress.alreadyStored(blockNumber);
                } else if (block != null) {
                    submit(blockNumber, block, progress);
                }
            }
        }
    }

    /// The downloaded block, or `null` when it cannot be used. In that case the pull ends, with
    /// the reason.
    // TODO(#4080) try an unusable block's copy in the other node folders
    private static BlockUnparsed downloaded(
            final Future<BlockUnparsed> download,
            final long blockNumber,
            final long sourceNode,
            final PullProgress progress) {
        BlockUnparsed block = null;
        try {
            block = download.get();
        } catch (final ExecutionException e) {
            if (e.getCause() instanceof UnusableBlockException) {
                LOGGER.log(
                        DEBUG,
                        "Block {0} from the folder of node {1} is unusable: {2}",
                        blockNumber,
                        sourceNode,
                        e.getCause().getMessage());
                progress.end(
                        "block " + blockNumber + " is unusable: " + e.getCause().getMessage());
            } else {
                // on stop, the download is interrupted along with this thread
                LOGGER.log(
                        Thread.currentThread().isInterrupted() ? DEBUG : WARNING,
                        "Download of block " + blockNumber + " from the folder of node " + sourceNode + " failed",
                        e.getCause());
                progress.end("download of block " + blockNumber + " failed");
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            progress.end("interrupted");
        }
        return block;
    }

    /// Submits one block for verification and persistence and waits for its outcome.
    private void submit(final long blockNumber, final BlockUnparsed block, final PullProgress progress) {
        awaiter.track(blockNumber);
        try {
            messaging.sendBackfilledBlockNotification(
                    new BackfilledBlockNotification(blockNumber, block, BlockSource.CLOUD_STORAGE));
            final Outcome outcome = awaiter.await(blockNumber, config.persistWaitTimeoutMs());
            if (outcome == Outcome.PERSISTED_BY_PULL) {
                progress.pulled(blockNumber);
            } else if (outcome == Outcome.PERSISTED_BY_OTHER) {
                progress.alreadyStored(blockNumber);
            } else if (outcome == Outcome.FAILED && isStored(blockNumber)) {
                // our copy failed, but another source stored the block meanwhile
                progress.alreadyStored(blockNumber);
            } else if (outcome == Outcome.FAILED) {
                // TODO(#4080) retry once from another node's copy
                progress.end("block " + blockNumber + " failed to verify or persist");
            } else {
                progress.end(
                        "block " + blockNumber + " was not persisted within " + config.persistWaitTimeoutMs() + " ms");
            }
        } catch (final RejectedExecutionException e) {
            // the messaging facility is shutting down
            LOGGER.log(DEBUG, "Submission of block {0} rejected: {1}", blockNumber, e.getMessage());
            progress.end("the node is shutting down");
        }
    }

    /// Counts of one pull and the reason it ended early, if it did; each count also increments its
    /// counter. Only the poll thread uses it.
    private final class PullProgress {
        private long lastHandled = NO_PULL;
        private long pulled;
        private long alreadyStored;
        private String endReason;

        private void pulled(final long blockNumber) {
            pulled++;
            lastHandled = blockNumber;
            metrics.blocksPulled().increment();
        }

        private void alreadyStored(final long blockNumber) {
            alreadyStored++;
            lastHandled = blockNumber;
            metrics.blocksAlreadyStored().increment();
        }

        private void end(final String reason) {
            if (endReason == null) {
                endReason = reason;
            }
        }
    }

    /// Downloads one block from the folder of the source node, decompresses and parses it, and
    /// checks that it is the block asked for.
    private final class Download implements Callable<BlockUnparsed> {
        private final long blockNumber;
        private final long sourceNode;

        private Download(final long blockNumber, final long sourceNode) {
            this.blockNumber = blockNumber;
            this.sourceNode = sourceNode;
        }

        @Override
        public BlockUnparsed call() throws IOException, S3ResponseException, ParseException, UnusableBlockException {
            final Bytes bytes;
            try (InputStream stored =
                    s3Client.openObjectStream(BlockKeys.blockKey(config.objectKeyPrefix(), sourceNode, blockNumber))) {
                if (stored == null) {
                    throw new UnusableBlockException("not in the bucket");
                }
                try (InputStream decompressed = CompressionType.ZSTD.wrapStream(stored)) {
                    bytes = Bytes.wrap(decompressed.readAllBytes());
                }
            }
            final BlockUnparsed block = ParseHelper.standardParse(BlockUnparsed.PROTOBUF, bytes, Integer.MAX_VALUE);
            final BlockItemUnparsed first =
                    block.blockItems().isEmpty() ? null : block.blockItems().getFirst();
            if (first == null || !first.hasBlockHeader()) {
                throw new UnusableBlockException("does not start with a block header");
            }
            final BlockHeader header = ParseHelper.standardParse(BlockHeader.PROTOBUF, first.blockHeaderOrThrow());
            if (header.number() != blockNumber) {
                throw new UnusableBlockException("its header has block number " + header.number());
            }
            return block;
        }
    }

    /// A downloaded object that is not the block asked for.
    private static final class UnusableBlockException extends Exception {
        private UnusableBlockException(final String message) {
            super(message);
        }
    }
}
