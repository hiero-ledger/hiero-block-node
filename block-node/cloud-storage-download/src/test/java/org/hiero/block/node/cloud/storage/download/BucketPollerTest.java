// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.download;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.hedera.bucky.S3Client;
import com.hedera.hapi.block.stream.output.BlockHeader;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.hiero.block.api.BlockRange;
import org.hiero.block.api.RosterEntry;
import org.hiero.block.api.TssData;
import org.hiero.block.api.TssRoster;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.node.cloud.storage.download.CloudStorageDownloadPlugin.MetricsHolder;
import org.hiero.block.node.spi.blockmessaging.BackfilledBlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.StoredBlocksNotification;
import org.hiero.block.node.spi.blockmessaging.TssDataNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification.FailureInfo;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification.FailureType;
import org.hiero.metrics.core.MetricRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class BucketPollerTest {

    private static final String PREFIX = "blocks";
    private static final long THRESHOLD = 10L;

    private final S3Client s3Client = mock(S3Client.class);
    private final TestBlockMessagingFacility messaging = new TestBlockMessagingFacility();
    private final PersistenceAwaiter awaiter = new PersistenceAwaiter();
    private final ExecutorService downloader = Executors.newVirtualThreadPerTaskExecutor();
    private final MetricsHolder metrics =
            MetricsHolder.createMetrics(MetricRegistry.builder().build());
    private final Persistence persistence = new Persistence();
    private RecordingPoller recordingPoller;

    @BeforeEach
    void setUp() throws Exception {
        // no block is in the bucket unless a test lists it
        when(s3Client.listObjectsPage(anyString(), any(), any(), anyInt()))
                .thenReturn(new S3Client.ListPage(List.of(), null));
        messaging.registerBlockNotificationHandler(awaiter, false, "awaiter");
        messaging.registerBlockNotificationHandler(persistence, false, "persistence");
        recordingPoller = new RecordingPoller(config(10, 90_000L), s3Client, messaging, awaiter, downloader, metrics);
    }

    @AfterEach
    void tearDown() {
        downloader.shutdownNow();
    }

    private static CloudStorageDownloadConfig config(final int downloadWindow, final long persistWaitTimeoutMs) {
        return new CloudStorageDownloadConfig(
                true,
                "http://localhost:9000",
                "us-east-1",
                "blocks",
                "access",
                "secret",
                PREFIX,
                3_600_000L,
                THRESHOLD,
                downloadWindow,
                persistWaitTimeoutMs,
                10_000L);
    }

    /// A poller that really pulls, answered by [#persistence].
    private BucketPoller pullingPoller(final int downloadWindow, final long persistWaitTimeoutMs) {
        final BucketPoller poller = new BucketPoller(
                config(downloadWindow, persistWaitTimeoutMs), s3Client, messaging, awaiter, downloader, metrics);
        persistence.poller = poller;
        return poller;
    }

    /// Makes the probe of a block in a node folder find it.
    private void probeFinds(final long nodeId, final long block) throws Exception {
        final String key = BlockKeys.blockKey(PREFIX, nodeId, block);
        when(s3Client.listObjectsPage(key, null, null, 1)).thenReturn(new S3Client.ListPage(List.of(key), null));
    }

    private void verifyProbed(final long nodeId, final long block) throws Exception {
        verify(s3Client).listObjectsPage(BlockKeys.blockKey(PREFIX, nodeId, block), null, null, 1);
    }

    private void blocksPresent(final long nodeId, final long first, final long last) throws Exception {
        for (long blockNumber = first; blockNumber <= last; blockNumber++) {
            blockPresent(nodeId, blockNumber, blockNumber);
        }
    }

    /// Puts a block file under the key of `blockNumber` whose header says `headerNumber`.
    private void blockPresent(final long nodeId, final long blockNumber, final long headerNumber) throws Exception {
        final BlockUnparsed block = BlockUnparsed.newBuilder()
                .blockItems(BlockItemUnparsed.newBuilder()
                        .blockHeader(BlockHeader.PROTOBUF.toBytes(
                                BlockHeader.newBuilder().number(headerNumber).build()))
                        .build())
                .build();
        final byte[] file = CompressionType.ZSTD.compress(
                BlockUnparsed.PROTOBUF.toBytes(block).toByteArray());
        when(s3Client.openObjectStream(BlockKeys.blockKey(PREFIX, nodeId, blockNumber)))
                .thenReturn(new ByteArrayInputStream(file));
    }

    private void assertCounts(final long pulled, final long alreadyStored) {
        assertThat(metrics.blocksPulled().get()).as("pulled").isEqualTo(pulled);
        assertThat(metrics.blocksAlreadyStored().get()).as("already stored").isEqualTo(alreadyStored);
    }

    private static BlockRange range(final long start, final long end) {
        return BlockRange.newBuilder().rangeStart(start).rangeEnd(end).build();
    }

    private static StoredBlocksNotification stored(final BlockRange... ranges) {
        return new StoredBlocksNotification(List.of(ranges));
    }

    private static List<Long> blocks(final long first, final long last) {
        final List<Long> numbers = new ArrayList<>();
        for (long blockNumber = first; blockNumber <= last; blockNumber++) {
            numbers.add(blockNumber);
        }
        return numbers;
    }

    /// A TSS data notification whose current roster holds the given node IDs.
    private static TssDataNotification roster(final long... nodeIds) {
        final List<RosterEntry> entries = new ArrayList<>();
        for (final long nodeId : nodeIds) {
            entries.add(RosterEntry.newBuilder().nodeId(nodeId).build());
        }
        return new TssDataNotification(TssData.newBuilder()
                .currentRoster(TssRoster.newBuilder().rosterEntries(entries).build())
                .build());
    }

    @Test
    @DisplayName("The node IDs come from the current roster; no TSS data means none")
    void nodeIdsFromRoster() {
        recordingPoller.handleTssDataUpdate(roster(3L, 4L));
        assertThat(recordingPoller.nodeIds()).containsExactly(3L, 4L);
        recordingPoller.handleTssDataUpdate(new TssDataNotification(null));
        assertThat(recordingPoller.nodeIds()).isEmpty();
    }

    @Test
    @DisplayName("With nothing stored the probe looks for the block of the threshold minus one")
    void probeWithNothingStored() throws Exception {
        recordingPoller.handleTssDataUpdate(roster(3L));
        recordingPoller.poll();
        verifyProbed(3L, THRESHOLD - 1);
    }

    @Test
    @DisplayName("The probe follows the last stored block")
    void probeFollowsStoredBlocks() throws Exception {
        recordingPoller.handleTssDataUpdate(roster(3L));
        recordingPoller.handleStoredBlocksUpdate(stored(range(0, 49), range(60, 99)));
        assertThat(recordingPoller.lastStored()).isEqualTo(99L);
        recordingPoller.poll();
        verifyProbed(3L, 99L + THRESHOLD);
    }

    @Test
    @DisplayName("No roster node IDs: the probe does nothing")
    void noNodeIdsNoProbe() {
        recordingPoller.poll();
        verifyNoInteractions(s3Client);
        assertThat(recordingPoller.pulls).isEmpty();
    }

    @Test
    @DisplayName("Probe block absent in two folders: no pull and exactly two probe requests")
    void probeBlockAbsentInTwoFolders() throws Exception {
        recordingPoller.handleTssDataUpdate(roster(3L, 4L, 5L));
        recordingPoller.poll();
        verify(s3Client, times(2)).listObjectsPage(anyString(), any(), any(), anyInt());
        verify(s3Client, never()).openObjectStream(anyString());
        assertThat(recordingPoller.pulls).isEmpty();
    }

    @Test
    @DisplayName("Probe block only in the second folder probed: pull from that node")
    void probeBlockInSecondFolder() throws Exception {
        recordingPoller.handleTssDataUpdate(roster(3L, 4L));
        probeFinds(4L, THRESHOLD - 1);
        recordingPoller.poll();
        assertThat(recordingPoller.pulls).containsExactly(new Pull(-1L, THRESHOLD - 1, 4L));
    }

    @Test
    @DisplayName("Behind by the threshold: the range is submitted in order, only from the source node's folder")
    void pullsRangeInOrderFromSourceNode() throws Exception {
        final BucketPoller poller = pullingPoller(3, 90_000L);
        poller.handleTssDataUpdate(roster(3L, 4L));
        poller.handleStoredBlocksUpdate(stored(range(0, 4)));
        probeFinds(4L, 4 + THRESHOLD);
        blocksPresent(4L, 5, 4 + THRESHOLD);
        poller.poll();
        assertThat(persistence.submitted).containsExactlyElementsOf(blocks(5, 4 + THRESHOLD));
        assertCounts(THRESHOLD, 0);
        verify(s3Client, never()).openObjectStream(matches("blocks/3/.*\\.blk\\.zstd"));
        // the completed pull probes again at once, from the end of the range
        verifyProbed(4L, 4 + 2 * THRESHOLD);
    }

    @Test
    @DisplayName(
            "A completed pull probes again at once and does not repeat its range before the stored blocks catch up")
    void completedPullProbesAgainWithoutRepeating() throws Exception {
        final BucketPoller poller = pullingPoller(10, 90_000L);
        poller.handleTssDataUpdate(roster(3L));
        probeFinds(3L, THRESHOLD - 1);
        probeFinds(3L, 2 * THRESHOLD - 1);
        blocksPresent(3L, 0, 2 * THRESHOLD - 1);
        poller.poll();
        assertThat(persistence.submitted).containsExactlyElementsOf(blocks(0, 2 * THRESHOLD - 1));
        assertCounts(2 * THRESHOLD, 0);
    }

    @Test
    @DisplayName("A block stored by another source before its window starts is not downloaded")
    void blockStoredBeforeItsWindowIsNotDownloaded() throws Exception {
        final BucketPoller poller = pullingPoller(2, 90_000L);
        poller.handleTssDataUpdate(roster(3L));
        poller.handleStoredBlocksUpdate(stored(range(0, 4)));
        probeFinds(3L, 4 + THRESHOLD);
        blocksPresent(3L, 5, 4 + THRESHOLD);
        persistence.storeOnSubmit.put(5L, stored(range(0, 5), range(7, 7)));
        poller.poll();
        assertThat(persistence.submitted).doesNotContain(7L).contains(5L, 6L, 8L, 4 + THRESHOLD);
        assertCounts(THRESHOLD - 1, 1);
        verify(s3Client, never()).openObjectStream(BlockKeys.blockKey(PREFIX, 3L, 7L));
    }

    @Test
    @DisplayName("A block stored by another source while it downloads is not submitted")
    void blockStoredDuringDownloadIsNotSubmitted() throws Exception {
        final BucketPoller poller = pullingPoller(10, 90_000L);
        poller.handleTssDataUpdate(roster(3L));
        poller.handleStoredBlocksUpdate(stored(range(0, 4)));
        probeFinds(3L, 4 + THRESHOLD);
        blocksPresent(3L, 5, 4 + THRESHOLD);
        persistence.storeOnSubmit.put(5L, stored(range(0, 6)));
        poller.poll();
        assertThat(persistence.submitted).doesNotContain(6L).contains(5L, 7L, 4 + THRESHOLD);
        assertCounts(THRESHOLD - 1, 1);
    }

    @Test
    @DisplayName("A block the publisher persists after submission ends the wait and the pull goes on")
    void blockPersistedByOtherSourceEndsTheWait() throws Exception {
        final BucketPoller poller = pullingPoller(10, 90_000L);
        poller.handleTssDataUpdate(roster(3L));
        poller.handleStoredBlocksUpdate(stored(range(0, 4)));
        probeFinds(3L, 4 + THRESHOLD);
        blocksPresent(3L, 5, 4 + THRESHOLD);
        persistence.answers.put(5L, Answer.PERSIST_BY_PUBLISHER);
        poller.poll();
        assertThat(persistence.submitted).containsExactlyElementsOf(blocks(5, 4 + THRESHOLD));
        assertCounts(THRESHOLD - 1, 1);
    }

    @Test
    @DisplayName("A failed persist of the publisher's copy does not end the wait for ours")
    void failedPersistOfOtherSourceKeepsWaiting() throws Exception {
        final BucketPoller poller = pullingPoller(10, 90_000L);
        poller.handleTssDataUpdate(roster(3L));
        poller.handleStoredBlocksUpdate(stored(range(0, 4)));
        probeFinds(3L, 4 + THRESHOLD);
        blocksPresent(3L, 5, 4 + THRESHOLD);
        persistence.answers.put(6L, Answer.PUBLISHER_FAILS_THEN_PERSIST);
        poller.poll();
        assertThat(persistence.submitted).containsExactlyElementsOf(blocks(5, 4 + THRESHOLD));
        assertCounts(THRESHOLD, 0);
    }

    @Test
    @DisplayName(
            "A failed verification of a block another source stored meanwhile counts as stored and the pull goes on")
    void failedVerificationOfStoredBlockGoesOn() throws Exception {
        final BucketPoller poller = pullingPoller(10, 90_000L);
        poller.handleTssDataUpdate(roster(3L));
        poller.handleStoredBlocksUpdate(stored(range(0, 4)));
        probeFinds(3L, 4 + THRESHOLD);
        blocksPresent(3L, 5, 4 + THRESHOLD);
        persistence.storeOnSubmit.put(6L, stored(range(0, 6)));
        persistence.answers.put(6L, Answer.FAIL_VERIFICATION);
        poller.poll();
        assertThat(persistence.submitted).containsExactlyElementsOf(blocks(5, 4 + THRESHOLD));
        assertCounts(THRESHOLD - 1, 1);
    }

    @Test
    @DisplayName("A failed verification of a block nobody stored ends the pull")
    void failedVerificationEndsThePull() throws Exception {
        final BucketPoller poller = pullingPoller(10, 90_000L);
        poller.handleTssDataUpdate(roster(3L));
        poller.handleStoredBlocksUpdate(stored(range(0, 4)));
        probeFinds(3L, 4 + THRESHOLD);
        blocksPresent(3L, 5, 4 + THRESHOLD);
        persistence.answers.put(6L, Answer.FAIL_VERIFICATION);
        poller.poll();
        assertThat(persistence.submitted).containsExactly(5L, 6L);
        assertCounts(1, 0);
    }

    @Test
    @DisplayName("A block not persisted in time ends the pull and is not sent again")
    void persistTimeoutEndsThePull() throws Exception {
        final BucketPoller poller = pullingPoller(10, 50L);
        poller.handleTssDataUpdate(roster(3L));
        poller.handleStoredBlocksUpdate(stored(range(0, 4)));
        probeFinds(3L, 4 + THRESHOLD);
        blocksPresent(3L, 5, 4 + THRESHOLD);
        persistence.answers.put(6L, Answer.NONE);
        poller.poll();
        assertThat(persistence.submitted).containsExactly(5L, 6L);
        assertCounts(1, 0);
    }

    @Test
    @DisplayName("A block missing from the folder ends the pull; the blocks before it are submitted")
    void missingBlockEndsThePull() throws Exception {
        final BucketPoller poller = pullingPoller(10, 90_000L);
        poller.handleTssDataUpdate(roster(3L));
        poller.handleStoredBlocksUpdate(stored(range(0, 4)));
        probeFinds(3L, 4 + THRESHOLD);
        blocksPresent(3L, 5, 6);
        blocksPresent(3L, 8, 4 + THRESHOLD);
        poller.poll();
        assertThat(persistence.submitted).containsExactly(5L, 6L);
        assertCounts(2, 0);
    }

    @Test
    @DisplayName("A block whose header has another number ends the pull")
    void wrongHeaderNumberEndsThePull() throws Exception {
        final BucketPoller poller = pullingPoller(10, 90_000L);
        poller.handleTssDataUpdate(roster(3L));
        poller.handleStoredBlocksUpdate(stored(range(0, 4)));
        probeFinds(3L, 4 + THRESHOLD);
        blocksPresent(3L, 5, 4 + THRESHOLD);
        blockPresent(3L, 7L, 8L);
        poller.poll();
        assertThat(persistence.submitted).containsExactly(5L, 6L);
        assertCounts(2, 0);
    }

    private record Pull(long last, long target, long sourceNode) {}

    /// Records each pull instead of performing it, and reports it as not completed so one
    /// poll makes at most one pull.
    private static final class RecordingPoller extends BucketPoller {
        private final List<Pull> pulls = new ArrayList<>();

        private RecordingPoller(
                final CloudStorageDownloadConfig config,
                final S3Client s3Client,
                final TestBlockMessagingFacility messaging,
                final PersistenceAwaiter awaiter,
                final ExecutorService downloader,
                final MetricsHolder metrics) {
            super(config, s3Client, messaging, awaiter, downloader, metrics);
        }

        @Override
        boolean pull(final long last, final long target, final long sourceNode) {
            pulls.add(new Pull(last, target, sourceNode));
            return false;
        }
    }

    /// How [Persistence] answers a submitted block.
    private enum Answer {
        PERSIST,
        PERSIST_BY_PUBLISHER,
        PUBLISHER_FAILS_THEN_PERSIST,
        FAIL_VERIFICATION,
        NONE
    }

    /// Stands in for verification and persistence: records each submitted block and answers it
    /// on the same thread, since the test messaging facility is synchronous.
    private final class Persistence implements BlockNotificationHandler {
        private final List<Long> submitted = new CopyOnWriteArrayList<>();
        private final Map<Long, Answer> answers = new HashMap<>();
        private final Map<Long, StoredBlocksNotification> storeOnSubmit = new HashMap<>();
        private BucketPoller poller;

        @Override
        public void handleBackfilled(final BackfilledBlockNotification notification) {
            final long blockNumber = notification.blockNumber();
            submitted.add(blockNumber);
            final StoredBlocksNotification storedUpdate = storeOnSubmit.get(blockNumber);
            if (storedUpdate != null) {
                poller.handleStoredBlocksUpdate(storedUpdate);
            }
            final Answer answer = answers.getOrDefault(blockNumber, Answer.PERSIST);
            if (answer == Answer.PERSIST) {
                messaging.sendBlockPersisted(
                        new PersistedNotification(blockNumber, true, 0, BlockSource.CLOUD_STORAGE));
            } else if (answer == Answer.PERSIST_BY_PUBLISHER) {
                messaging.sendBlockPersisted(new PersistedNotification(blockNumber, true, 0, BlockSource.PUBLISHER));
            } else if (answer == Answer.PUBLISHER_FAILS_THEN_PERSIST) {
                messaging.sendBlockPersisted(new PersistedNotification(blockNumber, false, 0, BlockSource.PUBLISHER));
                messaging.sendBlockPersisted(
                        new PersistedNotification(blockNumber, true, 0, BlockSource.CLOUD_STORAGE));
            } else if (answer == Answer.FAIL_VERIFICATION) {
                messaging.sendBlockVerification(new VerificationNotification(
                        false,
                        FailureInfo.standard(FailureType.BAD_BLOCK_PROOF),
                        blockNumber,
                        null,
                        null,
                        BlockSource.CLOUD_STORAGE));
            }
        }
    }
}
