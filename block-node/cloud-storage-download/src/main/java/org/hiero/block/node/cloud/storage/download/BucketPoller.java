// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.download;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;

import com.hedera.bucky.S3Client;
import com.hedera.bucky.S3ResponseException;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.hiero.block.api.BlockRange;
import org.hiero.block.api.RangedAddressBookHistory;
import org.hiero.block.api.RangedNodeAddressBook;
import org.hiero.block.api.RosterEntry;
import org.hiero.block.api.TssData;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.node.base.ParseHelper;
import org.hiero.block.node.cloud.storage.download.CloudStorageDownloadPlugin.MetricsHolder;
import org.hiero.block.node.cloud.storage.download.PersistenceAwaiter.Outcome;
import org.hiero.block.node.spi.blockmessaging.AddressBookHistoryNotification;
import org.hiero.block.node.spi.blockmessaging.BackfilledBlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.StoredBlocksNotification;
import org.hiero.block.node.spi.blockmessaging.TssDataNotification;

/// The scheduled poll of the bucket: probes it for the block the bucket must hold before a pull
/// is worth it, and pulls the missing range when it does. Only the poll thread runs the poll;
/// the plugin forwards the stored blocks, the roster and the address book from the application
/// state notifications.
class BucketPoller implements Runnable {
    private static final System.Logger LOGGER = System.getLogger(BucketPoller.class.getName());

    /// Returned by [#probeAndPull(long)] when no pull completed.
    private static final long NO_PULL = -1L;

    /// Stands for no node; node IDs are never negative.
    private static final long NO_NODE = -1L;

    private final CloudStorageDownloadConfig config;
    private final S3Client s3Client;
    private final BlockMessagingFacility messaging;
    private final PersistenceAwaiter awaiter;

    /// Runs the downloads of one window in parallel.
    private final ExecutorService downloader;

    private final MetricsHolder metrics;

    /// How long the poll thread waits for one download, including its fallback to other folders,
    /// in milliseconds, before it abandons it.
    private final long downloadDeadlineMs;

    /// Stored block ranges in ascending order, as last reported.
    private volatile List<BlockRange> storedBlocks = List.of();

    /// Node IDs of the current TSS roster, empty when there is none.
    private volatile List<Long> rosterNodeIds = List.of();

    /// The address book history, as last reported; `null` when none is known.
    private volatile RangedAddressBookHistory addressBookHistory;

    /// Creates a poller with no stored blocks and no roster, until the first notifications.
    ///
    /// @param config the plugin configuration
    /// @param s3Client the bucket client
    /// @param messaging where pulled blocks are submitted
    /// @param awaiter waits for the outcome of each submitted block
    /// @param downloader runs the downloads of one window in parallel
    /// @param metrics the plugin's counters
    /// @param downloadDeadlineMs how long to wait for one download once its turn comes, in
    ///     milliseconds, before abandoning it as a storage fault
    BucketPoller(
            final CloudStorageDownloadConfig config,
            final S3Client s3Client,
            final BlockMessagingFacility messaging,
            final PersistenceAwaiter awaiter,
            final ExecutorService downloader,
            final MetricsHolder metrics,
            final long downloadDeadlineMs) {
        this.config = config;
        this.s3Client = s3Client;
        this.messaging = messaging;
        this.awaiter = awaiter;
        this.downloader = downloader;
        this.metrics = metrics;
        this.downloadDeadlineMs = downloadDeadlineMs;
    }

    /// Records the stored block ranges.
    ///
    /// @param notification the stored blocks notification
    void handleStoredBlocksUpdate(final StoredBlocksNotification notification) {
        storedBlocks = notification.storedBlocks();
    }

    /// Records the node IDs of the current roster.
    ///
    /// @param notification the TSS data notification
    void handleTssDataUpdate(final TssDataNotification notification) {
        rosterNodeIds = nodeIdsOf(notification.tssData());
    }

    /// Records the address book history.
    ///
    /// @param notification the address book history notification
    void handleAddressBookHistoryUpdate(final AddressBookHistoryNotification notification) {
        addressBookHistory = notification.rangedAddressBookHistory();
    }

    /// Stored block ranges, as last reported.
    ///
    /// @return the ranges in ascending order
    List<BlockRange> storedBlocks() {
        return storedBlocks;
    }

    /// Node IDs whose bucket folders are read: the TSS roster's, else the address book's.
    /// Computed on every call; a poll reads it once per pull.
    ///
    /// @return the node IDs, empty when neither is known
    List<Long> nodeIds() {
        List<Long> ids = rosterNodeIds;
        if (ids.isEmpty()) {
            ids = nodeIdsOf(addressBookFor(addressBookHistory, lastStored() + 1));
        }
        return ids;
    }

    /// Extracts the node IDs of the current roster of the given TSS data.
    private static List<Long> nodeIdsOf(final TssData tssData) {
        final List<Long> ids = new ArrayList<>();
        if (tssData != null && tssData.hasCurrentRoster()) {
            for (final RosterEntry entry : tssData.currentRosterOrThrow().rosterEntries()) {
                ids.add(entry.nodeId());
            }
        }
        return List.copyOf(ids);
    }

    /// Extracts the node IDs of the given address book.
    private static List<Long> nodeIdsOf(final NodeAddressBook addressBook) {
        final List<Long> ids = new ArrayList<>();
        if (addressBook != null) {
            for (final NodeAddress address : addressBook.nodeAddress()) {
                ids.add(address.nodeId());
            }
        }
        return List.copyOf(ids);
    }

    /// The address book whose block range holds the given block; the newest one when no range
    /// holds it.
    ///
    /// @return the address book, `null` when the history is unknown or empty
    private static NodeAddressBook addressBookFor(final RangedAddressBookHistory history, final long block) {
        NodeAddressBook book = null;
        if (history != null && !history.addressBooks().isEmpty()) {
            for (final RangedNodeAddressBook ranged : history.addressBooks()) {
                // an end block of -1 leaves the range open
                if (ranged.startBlock() <= block && (ranged.endBlock() == -1L || block <= ranged.endBlock())) {
                    book = ranged.addressBook();
                }
            }
            if (book == null) {
                book = history.addressBooks().getLast().addressBook();
            }
        }
        return book;
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
        final List<Long> nodes = nodeIds();
        if (nodes.isEmpty()) {
            LOGGER.log(DEBUG, "No roster node IDs known, skipping the bucket probe");
        } else {
            final long last = Math.max(lastStored(), completedUpTo);
            // the bucket is far enough ahead to pull once this block is complete in it
            final long target = last + config.aheadThresholdBlocks();
            try {
                final long sourceNode = findBlock(nodes, target);
                if (sourceNode >= 0 && pull(last, target, sourceNode, nodes)) {
                    pulledUpTo = target;
                }
            } catch (final IOException | S3ResponseException e) {
                // the client reports an interrupt, as on stop, as an IOException
                if (Thread.currentThread().isInterrupted()) {
                    LOGGER.log(DEBUG, "Bucket probe for block " + target + " interrupted", e);
                } else {
                    metrics.requestFailures().increment();
                    LOGGER.log(WARNING, "Bucket probe for block " + target + " failed", e);
                }
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
    /// submitted in block order, and each block is awaited before the next is submitted. A block
    /// whose copy in the source node's folder is missing or unusable is taken from the other
    /// node folders. The first block with no usable copy in any folder, or that fails or times
    /// out, ends the pull.
    ///
    /// @param last the last stored block
    /// @param target the last block to pull
    /// @param sourceNode the node whose folder holds the blocks
    /// @param nodes the node IDs whose folders are read, the same for the whole pull
    /// @return `true` when the whole range was pulled
    boolean pull(final long last, final long target, final long sourceNode, final List<Long> nodes) {
        LOGGER.log(INFO, "Pulling blocks {0} to {1} from the folder of node {2}", last + 1, target, sourceNode);
        final long started = System.nanoTime();
        final PullProgress progress = new PullProgress(target);
        long windowStart = last + 1;
        while (progress.endReason == null && windowStart <= target) {
            final long windowEnd = Math.min(target, windowStart + config.downloadWindow() - 1);
            pullWindow(windowStart, windowEnd, sourceNode, nodes, progress);
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
            final long windowStart,
            final long windowEnd,
            final long sourceNode,
            final List<Long> nodes,
            final PullProgress progress) {
        // the folders to fall back to when the source node's copy is missing or unusable
        final List<Long> otherNodes = new ArrayList<>();
        for (final long nodeId : nodes) {
            if (nodeId != sourceNode) {
                otherNodes.add(nodeId);
            }
        }
        // only the blocks not stored yet are downloaded
        final Map<Long, Future<Copy>> downloads = new HashMap<>();
        for (long blockNumber = windowStart; blockNumber <= windowEnd; blockNumber++) {
            if (!isStored(blockNumber)) {
                downloads.put(blockNumber, downloader.submit(new Download(blockNumber, sourceNode, otherNodes)));
            }
        }
        for (long blockNumber = windowStart; blockNumber <= windowEnd; blockNumber++) {
            final Future<Copy> download = downloads.get(blockNumber);
            if (progress.endReason != null || Thread.currentThread().isInterrupted()) {
                if (download != null) {
                    download.cancel(true);
                }
                progress.end("interrupted");
            } else if (download == null) {
                progress.alreadyStored(blockNumber);
            } else {
                final Copy copy = downloaded(download, blockNumber, progress);
                if (copy != null && isStored(blockNumber)) {
                    // stored by another source while it downloaded
                    progress.alreadyStored(blockNumber);
                } else if (copy != null && copy.block() == null) {
                    progress.noUsableCopy(blockNumber, copy.present());
                } else if (copy != null) {
                    submit(blockNumber, copy, progress);
                }
            }
        }
    }

    /// The outcome of a download, or `null` when it failed. In that case the pull ends, with
    /// the reason. A storage fault, an error response or a download that does not finish within
    /// the deadline counts as a request failure; the bucket is then likely failing as a whole, so
    /// no other folder is tried. The deadline covers the whole walk through the folders, so on a
    /// very slow bucket a long walk also ends as a request failure. The deadline is counted from
    /// when the block's turn comes, not from when its download started, so a block late in a
    /// window, which kept downloading while earlier blocks were awaited, has a longer budget.
    private Copy downloaded(final Future<Copy> download, final long blockNumber, final PullProgress progress) {
        Copy copy = null;
        try {
            copy = download.get(downloadDeadlineMs, TimeUnit.MILLISECONDS);
        } catch (final ExecutionException e) {
            if (Thread.currentThread().isInterrupted()) {
                // on stop, the download is interrupted along with this thread
                LOGGER.log(DEBUG, "Download of block " + blockNumber + " interrupted", e.getCause());
                progress.end("interrupted");
            } else {
                metrics.requestFailures().increment();
                LOGGER.log(WARNING, "Download of block " + blockNumber + " failed", e.getCause());
                progress.end("download of block " + blockNumber + " failed");
            }
        } catch (final TimeoutException e) {
            download.cancel(true);
            metrics.requestFailures().increment();
            LOGGER.log(WARNING, "Download of block {0} did not finish within {1} ms", blockNumber, downloadDeadlineMs);
            progress.end("download of block " + blockNumber + " timed out");
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            progress.end("interrupted");
        }
        return copy;
    }

    /// Submits one block for verification and persistence and waits for its outcome. A copy that
    /// fails verification is replaced once by a copy from another node's folder, which is
    /// submitted in its turn; this is the only block a pull submits twice.
    private void submit(final long blockNumber, final Copy first, final PullProgress progress) {
        Copy copy = first;
        boolean replaced = false;
        while (copy != null) {
            awaiter.track(blockNumber);
            messaging.sendBackfilledBlockNotification(
                    new BackfilledBlockNotification(blockNumber, copy.block(), BlockSource.CLOUD_STORAGE));
            final Outcome outcome = awaiter.await(blockNumber, config.persistWaitTimeoutMs());
            final Copy next = handleOutcome(blockNumber, outcome, copy, replaced, progress);
            replaced = replaced || next != null;
            copy = next;
        }
    }

    /// Records the outcome of one submitted copy of a block.
    ///
    /// @param replaced whether the copy is already the replacement of one that failed verification
    /// @return the replacement copy to submit next, `null` when the block is done
    private Copy handleOutcome(
            final long blockNumber,
            final Outcome outcome,
            final Copy copy,
            final boolean replaced,
            final PullProgress progress) {
        Copy next = null;
        if (outcome == Outcome.PERSISTED_BY_PULL) {
            progress.pulled(blockNumber);
            progress.persisted(blockNumber, copy);
        } else if (outcome == Outcome.PERSISTED_BY_OTHER) {
            progress.alreadyStored(blockNumber);
            progress.persisted(blockNumber, copy);
        } else if ((outcome == Outcome.VERIFICATION_FAILED || outcome == Outcome.FAILED) && isStored(blockNumber)) {
            // our copy failed, but another source stored the block meanwhile
            progress.alreadyStored(blockNumber);
        } else if (outcome == Outcome.VERIFICATION_FAILED) {
            metrics.copiesRejected().increment();
            LOGGER.log(
                    DEBUG,
                    "Copy of block {0} from the folder of node {1} failed verification",
                    blockNumber,
                    copy.nodeId());
            if (replaced) {
                progress.verificationFailed(blockNumber);
            } else {
                next = replacement(blockNumber, copy.untriedNodes(), progress);
            }
        } else if (outcome == Outcome.FAILED && Thread.currentThread().isInterrupted()) {
            // on stop, the wait is released as failed after this thread is interrupted
            progress.end("interrupted");
        } else if (outcome == Outcome.FAILED) {
            // the copy verified, so another copy would not persist any better
            progress.end("block " + blockNumber + " failed to persist");
        } else if (outcome == Outcome.TIMED_OUT) {
            progress.persistTimeout(blockNumber);
        }
        return next;
    }

    /// Downloads a copy of a block from the folders the download of the copy that failed
    /// verification did not read. The folders it did read had no object, an unusable one or the
    /// failed copy, so they are not read again. When there is none left, the block counts as
    /// failing verification.
    ///
    /// @param untriedNodes the node folders not read yet for this block
    /// @return the copy, `null` when there is none or the block is stored meanwhile; the outcome
    ///     of the block is then already recorded
    private Copy replacement(final long blockNumber, final List<Long> untriedNodes, final PullProgress progress) {
        Copy copy = null;
        if (untriedNodes.isEmpty()) {
            progress.verificationFailed(blockNumber);
        } else {
            copy = downloaded(
                    downloader.submit(new Download(blockNumber, NO_NODE, untriedNodes)), blockNumber, progress);
            if (copy != null && isStored(blockNumber)) {
                progress.alreadyStored(blockNumber);
                copy = null;
            } else if (copy != null && copy.block() == null) {
                progress.verificationFailed(blockNumber);
                copy = null;
            }
        }
        return copy;
    }

    /// Counts of one pull and the reason it ended early, if it did; each count also increments its
    /// counter. Only the poll thread uses it.
    private final class PullProgress {
        /// The last block of the range, the probe block.
        private final long target;

        private long lastHandled = NO_PULL;
        private long pulled;
        private long alreadyStored;
        private String endReason;

        private PullProgress(final long target) {
            this.target = target;
        }

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

        /// Sets the probe block latency when the submitted block is the probe block and its
        /// persisted notification has just arrived.
        private void persisted(final long blockNumber, final Copy copy) {
            if (blockNumber == target && copy.consensusTime() != null) {
                metrics.probeBlockLatencyMs()
                        .set(Duration.between(copy.consensusTime(), Instant.now())
                                .toMillis());
            }
        }

        /// Ends the pull at a block no folder has a usable copy of. Objects that are all corrupt
        /// or another block point to a defect or malice in the uploads, so that case is a WARNING.
        ///
        /// @param present whether any folder had the object at all
        private void noUsableCopy(final long blockNumber, final boolean present) {
            if (present) {
                metrics.blocksDownloadFailed().increment();
                LOGGER.log(WARNING, "No folder in the bucket has a usable copy of block {0}", blockNumber);
                end("no folder has a usable copy of block " + blockNumber);
            } else {
                metrics.blocksNotPresent().increment();
                end("block " + blockNumber + " is in no folder");
            }
        }

        /// Ends the pull at a block whose copies failed verification and that has no other copy.
        /// Bucket copies that fail verification point to a defect or malice in the uploads, so
        /// this is a WARNING.
        private void verificationFailed(final long blockNumber) {
            metrics.blocksVerificationFailed().increment();
            LOGGER.log(WARNING, "No copy of block {0} in the bucket passed verification", blockNumber);
            end("block " + blockNumber + " failed verification");
        }

        /// Ends the pull at a block not persisted in time.
        private void persistTimeout(final long blockNumber) {
            metrics.blocksPersistTimeout().increment();
            end("block " + blockNumber + " was not persisted within " + config.persistWaitTimeoutMs() + " ms");
        }

        private void end(final String reason) {
            if (endReason == null) {
                endReason = reason;
            }
        }
    }

    /// The outcome of downloading one block.
    ///
    /// @param block the first usable copy, `null` when no folder has one
    /// @param nodeId the node whose folder the copy came from, [#NO_NODE] when there is no copy
    /// @param present whether any folder had the object, usable or not
    /// @param consensusTime the consensus timestamp in the block header, `null` when there is no
    ///     copy or the header has none
    /// @param untriedNodes the node folders the download did not read, because it stopped at
    ///     this copy
    private record Copy(
            BlockUnparsed block, long nodeId, boolean present, Instant consensusTime, List<Long> untriedNodes) {}

    /// Downloads one block from the folder of the first node, if there is one, then from the
    /// other nodes' folders in random order, one at a time, until one has a usable copy. A
    /// missing object moves on to the next folder; so does an unusable one, which is counted as a
    /// rejected copy. A storage fault in any folder ends the download.
    private final class Download implements Callable<Copy> {
        private final long blockNumber;
        private final long firstNode;
        private final List<Long> otherNodes;

        private Download(final long blockNumber, final long firstNode, final List<Long> otherNodes) {
            this.blockNumber = blockNumber;
            this.firstNode = firstNode;
            this.otherNodes = otherNodes;
        }

        @Override
        public Copy call() throws IOException, S3ResponseException {
            final List<Long> folders = new ArrayList<>(otherNodes);
            // a random order spreads the fallback reads over the other folders
            Collections.shuffle(folders, ThreadLocalRandom.current());
            if (firstNode != NO_NODE) {
                folders.addFirst(firstNode);
            }
            Copy copy = new Copy(null, NO_NODE, false, null, List.of());
            int read = 0;
            while (copy.block() == null && read < folders.size()) {
                final long nodeId = folders.get(read);
                read++;
                final long started = System.nanoTime();
                final byte[] compressed = read(nodeId);
                if (compressed != null) {
                    metrics.copiesDownloaded().increment();
                    metrics.copyLatencyNs().increment(System.nanoTime() - started);
                    try {
                        copy = parse(compressed, nodeId);
                    } catch (final UnusableBlockException e) {
                        metrics.copiesRejected().increment();
                        LOGGER.log(
                                DEBUG,
                                "Copy of block {0} in the folder of node {1} rejected: {2}",
                                blockNumber,
                                nodeId,
                                e.getMessage());
                        copy = new Copy(null, NO_NODE, true, null, List.of());
                    }
                }
            }
            // a replacement reads only the folders this download did not
            return new Copy(
                    copy.block(),
                    copy.nodeId(),
                    copy.present(),
                    copy.consensusTime(),
                    List.copyOf(folders.subList(read, folders.size())));
        }

        /// Reads the whole object of the block in a node folder, before it is decompressed, so a
        /// failed transfer (a storage fault) is told apart from corrupt content (an unusable copy).
        ///
        /// @return the compressed bytes, `null` when the folder has no such object
        private byte[] read(final long nodeId) throws IOException, S3ResponseException {
            byte[] compressed = null;
            try (InputStream stored =
                    s3Client.openObjectStream(BlockKeys.blockKey(config.objectKeyPrefix(), nodeId, blockNumber))) {
                if (stored != null) {
                    compressed = stored.readAllBytes();
                }
            }
            return compressed;
        }

        /// Decompresses and parses a copy and checks that it is the block asked for.
        ///
        /// @return the usable copy from the folder of the given node
        private Copy parse(final byte[] compressed, final long nodeId) throws UnusableBlockException {
            final BlockUnparsed block;
            final BlockHeader header;
            try {
                block = ParseHelper.standardParse(
                        BlockUnparsed.PROTOBUF,
                        Bytes.wrap(CompressionType.ZSTD.decompress(compressed)),
                        Integer.MAX_VALUE);
                final BlockItemUnparsed first =
                        block.blockItems().isEmpty() ? null : block.blockItems().getFirst();
                if (first == null || !first.hasBlockHeader()) {
                    throw new UnusableBlockException("does not start with a block header");
                }
                header = ParseHelper.standardParse(BlockHeader.PROTOBUF, first.blockHeaderOrThrow());
            } catch (final UncheckedIOException e) {
                throw new UnusableBlockException(
                        "cannot be decompressed: " + e.getCause().getMessage());
            } catch (final ParseException e) {
                throw new UnusableBlockException("cannot be parsed: " + e.getMessage());
            }
            if (header.number() != blockNumber) {
                throw new UnusableBlockException("its header has block number " + header.number());
            }
            final Timestamp timestamp = header.blockTimestamp();
            final Instant consensusTime =
                    timestamp == null ? null : Instant.ofEpochSecond(timestamp.seconds(), timestamp.nanos());
            return new Copy(block, nodeId, true, consensusTime, List.of());
        }
    }

    /// A downloaded object that is not the block asked for.
    private static final class UnusableBlockException extends Exception {
        private UnusableBlockException(final String message) {
            super(message);
        }
    }
}
