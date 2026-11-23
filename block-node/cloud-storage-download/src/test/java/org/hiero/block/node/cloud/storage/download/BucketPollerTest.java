// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.download;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.hedera.bucky.S3Client;
import com.hedera.bucky.S3ResponseException;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.Timestamp;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.hiero.block.api.BlockRange;
import org.hiero.block.api.RangedAddressBookHistory;
import org.hiero.block.api.RangedNodeAddressBook;
import org.hiero.block.api.RosterEntry;
import org.hiero.block.api.TssData;
import org.hiero.block.api.TssRoster;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.node.cloud.storage.download.CloudStorageDownloadPlugin.MetricsHolder;
import org.hiero.block.node.spi.blockmessaging.AddressBookHistoryNotification;
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
import org.mockito.InOrder;

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

    /// The download deadline of the next [#pullingPoller(int, long)].
    private long downloadDeadlineMs = 60_000L;

    /// The consensus timestamp in the header of the blocks put in the bucket; none when `null`.
    private Timestamp consensusTimestamp;

    @BeforeEach
    void setUp() throws Exception {
        // no block is in the bucket unless a test lists it
        when(s3Client.listObjectsPage(anyString(), any(), any(), anyInt()))
                .thenReturn(new S3Client.ListPage(List.of(), null));
        messaging.registerBlockNotificationHandler(new AwaiterForwarder(), false, "awaiter");
        messaging.registerUnvalidatedBlockHandler(persistence, false, "persistence");
        recordingPoller = new RecordingPoller(config(10, 90_000L), s3Client, messaging, awaiter, downloader, metrics);
    }

    @AfterEach
    void tearDown() {
        downloader.shutdownNow();
    }

    private static CloudStorageDownloadConfig config(final int downloadWindow, final long persistWaitTimeoutMs) {
        return new CloudStorageDownloadConfig(
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
                config(downloadWindow, persistWaitTimeoutMs),
                s3Client,
                messaging,
                awaiter,
                downloader,
                metrics,
                downloadDeadlineMs);
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
                        .blockHeader(BlockHeader.PROTOBUF.toBytes(BlockHeader.newBuilder()
                                .number(headerNumber)
                                .blockTimestamp(consensusTimestamp)
                                .build()))
                        .build())
                .build();
        final byte[] file = CompressionType.ZSTD.compress(
                BlockUnparsed.PROTOBUF.toBytes(block).toByteArray());
        when(s3Client.openObjectStream(BlockKeys.blockKey(PREFIX, nodeId, blockNumber)))
                .thenReturn(new ByteArrayInputStream(file));
    }

    /// Polls until a pull submits a block. With three or more roster nodes the probe looks in only
    /// two random folders, so it can miss the one folder the test made it find; a miss changes
    /// nothing, so the next poll simply probes again.
    private void pollUntilPulled(final BucketPoller poller) {
        for (int attempt = 0; attempt < 100 && persistence.submitted.isEmpty(); attempt++) {
            poller.poll();
        }
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

    /// An address book holding the given node IDs, in effect from `startBlock` to `endBlock`
    /// (`-1` for open-ended).
    private static RangedNodeAddressBook addressBook(
            final long startBlock, final long endBlock, final long... nodeIds) {
        final List<NodeAddress> addresses = new ArrayList<>();
        for (final long nodeId : nodeIds) {
            addresses.add(NodeAddress.newBuilder().nodeId(nodeId).build());
        }
        return RangedNodeAddressBook.newBuilder()
                .addressBook(NodeAddressBook.newBuilder().nodeAddress(addresses).build())
                .startBlock(startBlock)
                .endBlock(endBlock)
                .build();
    }

    private static AddressBookHistoryNotification history(final RangedNodeAddressBook... addressBooks) {
        return new AddressBookHistoryNotification(RangedAddressBookHistory.newBuilder()
                .addressBooks(List.of(addressBooks))
                .build());
    }

    @Test
    @DisplayName("Without a TSS roster the node IDs come from the address book; a later roster replaces them")
    void nodeIdsFromAddressBookUntilRoster() {
        recordingPoller.handleAddressBookHistoryUpdate(history(addressBook(0, -1, 3L, 4L, 5L)));
        assertThat(recordingPoller.nodeIds()).containsExactly(3L, 4L, 5L);
        recordingPoller.handleTssDataUpdate(roster(7L));
        assertThat(recordingPoller.nodeIds()).containsExactly(7L);
        recordingPoller.handleAddressBookHistoryUpdate(history(addressBook(0, -1, 3L, 4L, 5L, 6L)));
        assertThat(recordingPoller.nodeIds()).containsExactly(7L);
    }

    @Test
    @DisplayName("A TSS roster with no entries leaves the node IDs to the address book")
    void emptyRosterFallsBackToAddressBook() {
        recordingPoller.handleAddressBookHistoryUpdate(history(addressBook(0, -1, 3L, 4L)));
        recordingPoller.handleTssDataUpdate(roster());
        assertThat(recordingPoller.nodeIds()).containsExactly(3L, 4L);
    }

    @Test
    @DisplayName("The address book in effect for the next block to store is used, the newest one past every range")
    void addressBookForNextBlock() {
        recordingPoller.handleStoredBlocksUpdate(stored(range(0, 99)));
        recordingPoller.handleAddressBookHistoryUpdate(
                history(addressBook(0, 99, 3L), addressBook(100, 199, 4L), addressBook(250, 299, 5L)));
        assertThat(recordingPoller.nodeIds()).containsExactly(4L);
        recordingPoller.handleStoredBlocksUpdate(stored(range(0, 209)));
        assertThat(recordingPoller.nodeIds()).containsExactly(5L);
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
        persistence.answer(5L, Answer.PERSIST_BY_PUBLISHER);
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
        persistence.answer(6L, Answer.PUBLISHER_FAILS_THEN_PERSIST);
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
        persistence.answer(6L, Answer.FAIL_VERIFICATION);
        poller.poll();
        assertThat(persistence.submitted).containsExactlyElementsOf(blocks(5, 4 + THRESHOLD));
        assertCounts(THRESHOLD - 1, 1);
    }

    @Test
    @DisplayName("With one node in the roster a failed verification ends the pull, since there is no other copy")
    void failedVerificationWithOneNodeEndsThePull() throws Exception {
        final BucketPoller poller = pullingPoller(10, 90_000L);
        poller.handleTssDataUpdate(roster(3L));
        poller.handleStoredBlocksUpdate(stored(range(0, 4)));
        probeFinds(3L, 4 + THRESHOLD);
        blocksPresent(3L, 5, 4 + THRESHOLD);
        persistence.answer(6L, Answer.FAIL_VERIFICATION);
        poller.poll();
        assertThat(persistence.submitted).containsExactly(5L, 6L);
        assertCounts(1, 0);
        assertThat(metrics.blocksVerificationFailed().get()).isEqualTo(1L);
        assertThat(metrics.copiesRejected().get()).isEqualTo(1L);
    }

    @Test
    @DisplayName("A failed verification is retried once from another node's copy and the pull goes on")
    void failedVerificationIsRetriedFromAnotherNode() throws Exception {
        final BucketPoller poller = pullingPoller(10, 90_000L);
        poller.handleTssDataUpdate(roster(3L, 4L));
        poller.handleStoredBlocksUpdate(stored(range(0, 4)));
        probeFinds(3L, 4 + THRESHOLD);
        blocksPresent(3L, 5, 4 + THRESHOLD);
        blockPresent(4L, 6L, 6L);
        persistence.answer(6L, Answer.FAIL_VERIFICATION);
        poller.poll();
        final List<Long> expected = new ArrayList<>(blocks(5, 4 + THRESHOLD));
        expected.add(1, 6L);
        assertThat(persistence.submitted).containsExactlyElementsOf(expected);
        assertCounts(THRESHOLD, 0);
        assertThat(metrics.blocksVerificationFailed().get()).isZero();
        assertThat(metrics.copiesRejected().get()).isEqualTo(1L);
        verify(s3Client).openObjectStream(BlockKeys.blockKey(PREFIX, 4L, 6L));
        verify(s3Client, never()).openObjectStream(BlockKeys.blockKey(PREFIX, 4L, 7L));
    }

    @Test
    @DisplayName("A second failed verification ends the pull")
    void secondFailedVerificationEndsThePull() throws Exception {
        final BucketPoller poller = pullingPoller(10, 90_000L);
        poller.handleTssDataUpdate(roster(3L, 4L, 5L));
        poller.handleStoredBlocksUpdate(stored(range(0, 4)));
        probeFinds(3L, 4 + THRESHOLD);
        blocksPresent(3L, 5, 4 + THRESHOLD);
        blockPresent(4L, 6L, 6L);
        blockPresent(5L, 6L, 6L);
        persistence.answer(6L, Answer.FAIL_VERIFICATION, Answer.FAIL_VERIFICATION);
        pollUntilPulled(poller);
        assertThat(persistence.submitted).containsExactly(5L, 6L, 6L);
        assertCounts(1, 0);
        assertThat(metrics.blocksVerificationFailed().get()).isEqualTo(1L);
        assertThat(metrics.copiesRejected().get()).isEqualTo(2L);
    }

    @Test
    @DisplayName("A failed verification with no usable copy in the other folders ends the pull")
    void failedVerificationWithNoOtherCopyEndsThePull() throws Exception {
        final BucketPoller poller = pullingPoller(10, 90_000L);
        poller.handleTssDataUpdate(roster(3L, 4L));
        poller.handleStoredBlocksUpdate(stored(range(0, 4)));
        probeFinds(3L, 4 + THRESHOLD);
        blocksPresent(3L, 5, 4 + THRESHOLD);
        persistence.answer(6L, Answer.FAIL_VERIFICATION);
        poller.poll();
        assertThat(persistence.submitted).containsExactly(5L, 6L);
        assertThat(metrics.blocksVerificationFailed().get()).isEqualTo(1L);
        assertThat(metrics.blocksNotPresent().get()).isZero();
        verify(s3Client).openObjectStream(BlockKeys.blockKey(PREFIX, 4L, 6L));
    }

    @Test
    @DisplayName("A fallback copy that fails verification is not replaced from a folder its download already read")
    void failedVerificationOfFallbackCopySkipsFoldersAlreadyRead() throws Exception {
        final BucketPoller poller = pullingPoller(10, 90_000L);
        poller.handleTssDataUpdate(roster(3L, 4L));
        poller.handleStoredBlocksUpdate(stored(range(0, 4)));
        probeFinds(3L, 4 + THRESHOLD);
        blocksPresent(3L, 5, 4 + THRESHOLD);
        corruptPresent(3L, 6L);
        blockPresent(4L, 6L, 6L);
        persistence.answer(6L, Answer.FAIL_VERIFICATION);
        poller.poll();
        assertThat(persistence.submitted).containsExactly(5L, 6L);
        assertThat(metrics.blocksVerificationFailed().get()).isEqualTo(1L);
        // the corrupt source copy and the fallback copy that failed verification, once each
        assertThat(metrics.copiesRejected().get()).isEqualTo(2L);
        verify(s3Client).openObjectStream(BlockKeys.blockKey(PREFIX, 3L, 6L));
        verify(s3Client).openObjectStream(BlockKeys.blockKey(PREFIX, 4L, 6L));
    }

    @Test
    @DisplayName("A fallback copy that fails verification is replaced from the folder not read yet, not the source")
    void failedVerificationOfFallbackCopyIsReplacedFromUnreadFolder() throws Exception {
        final BucketPoller poller = pullingPoller(10, 90_000L);
        poller.handleTssDataUpdate(roster(3L, 4L, 5L));
        poller.handleStoredBlocksUpdate(stored(range(0, 4)));
        probeFinds(3L, 4 + THRESHOLD);
        blocksPresent(3L, 5, 4 + THRESHOLD);
        corruptPresent(3L, 6L);
        // the fallback reads 4 and 5 in a random order; whichever comes first fails verification
        blockPresent(4L, 6L, 6L);
        blockPresent(5L, 6L, 6L);
        persistence.answer(6L, Answer.FAIL_VERIFICATION, Answer.PERSIST);
        pollUntilPulled(poller);
        final List<Long> expected = new ArrayList<>(blocks(5, 4 + THRESHOLD));
        expected.add(1, 6L);
        assertThat(persistence.submitted).containsExactlyElementsOf(expected);
        assertCounts(THRESHOLD, 0);
        assertThat(metrics.blocksVerificationFailed().get()).isZero();
        // the corrupt source copy and the fallback copy that failed verification, once each
        assertThat(metrics.copiesRejected().get()).isEqualTo(2L);
        verify(s3Client).openObjectStream(BlockKeys.blockKey(PREFIX, 3L, 6L));
        verify(s3Client).openObjectStream(BlockKeys.blockKey(PREFIX, 4L, 6L));
        verify(s3Client).openObjectStream(BlockKeys.blockKey(PREFIX, 5L, 6L));
    }

    @Test
    @DisplayName("A failed persistence of our verified copy ends the pull with no retry")
    void failedPersistenceEndsThePullWithoutRetry() throws Exception {
        final BucketPoller poller = pullingPoller(10, 90_000L);
        poller.handleTssDataUpdate(roster(3L, 4L));
        poller.handleStoredBlocksUpdate(stored(range(0, 4)));
        probeFinds(3L, 4 + THRESHOLD);
        blocksPresent(3L, 5, 4 + THRESHOLD);
        blockPresent(4L, 6L, 6L);
        persistence.answer(6L, Answer.FAIL_PERSIST);
        poller.poll();
        assertThat(persistence.submitted).containsExactly(5L, 6L);
        assertThat(metrics.blocksVerificationFailed().get()).isZero();
        assertThat(metrics.copiesRejected().get()).isZero();
        verify(s3Client, never()).openObjectStream(BlockKeys.blockKey(PREFIX, 4L, 6L));
    }

    @Test
    @DisplayName("A block not persisted in time ends the pull, is not sent again and is skipped once it is stored")
    void persistTimeoutEndsThePull() throws Exception {
        final BucketPoller poller = pullingPoller(10, 50L);
        poller.handleTssDataUpdate(roster(3L));
        poller.handleStoredBlocksUpdate(stored(range(0, 4)));
        probeFinds(3L, 4 + THRESHOLD);
        blocksPresent(3L, 5, 4 + THRESHOLD);
        persistence.answer(6L, Answer.NONE);
        poller.poll();
        assertThat(persistence.submitted).containsExactly(5L, 6L);
        assertCounts(1, 0);
        assertThat(metrics.blocksPersistTimeout().get()).isEqualTo(1L);
        // the late persisted notification arrives before the next poll
        poller.handleStoredBlocksUpdate(stored(range(0, 6)));
        probeFinds(3L, 6 + THRESHOLD);
        // the first poll read the stubbed streams of its whole window
        blocksPresent(3L, 7, 6 + THRESHOLD);
        poller.poll();
        assertThat(persistence.submitted).containsOnlyOnce(6L).contains(7L, 6 + THRESHOLD);
        assertThat(metrics.blocksPersistTimeout().get()).isEqualTo(1L);
    }

    @Test
    @DisplayName("A block in no folder ends the pull as not present; the blocks before it are submitted")
    void missingBlockEndsThePull() throws Exception {
        final BucketPoller poller = pullingPoller(10, 90_000L);
        poller.handleTssDataUpdate(roster(3L, 4L));
        poller.handleStoredBlocksUpdate(stored(range(0, 4)));
        probeFinds(3L, 4 + THRESHOLD);
        blocksPresent(3L, 5, 6);
        blocksPresent(3L, 8, 4 + THRESHOLD);
        poller.poll();
        assertThat(persistence.submitted).containsExactly(5L, 6L);
        assertCounts(2, 0);
        assertThat(metrics.blocksNotPresent().get()).isEqualTo(1L);
        assertThat(metrics.blocksDownloadFailed().get()).isZero();
        assertThat(metrics.copiesRejected().get()).isZero();
        assertThat(metrics.requestFailures().get()).isZero();
        verify(s3Client).openObjectStream(BlockKeys.blockKey(PREFIX, 4L, 7L));
    }

    @Test
    @DisplayName(
            "An error response or a network error on the probe counts one request failure and probes no other folder")
    void probeStorageFaultIsCounted() throws Exception {
        recordingPoller.handleTssDataUpdate(roster(3L, 4L));
        when(s3Client.listObjectsPage(anyString(), any(), any(), anyInt()))
                .thenThrow(new S3ResponseException(500, null, null))
                .thenThrow(new IOException("connection reset"));
        recordingPoller.poll();
        assertThat(metrics.requestFailures().get()).isEqualTo(1L);
        recordingPoller.poll();
        assertThat(metrics.requestFailures().get()).isEqualTo(2L);
        verify(s3Client, times(2)).listObjectsPage(anyString(), any(), any(), anyInt());
        assertThat(recordingPoller.pulls).isEmpty();
    }

    @Test
    @DisplayName("An error response on a download counts one request failure and ends the pull")
    void downloadErrorResponseEndsThePull() throws Exception {
        final BucketPoller poller = pullingPoller(10, 90_000L);
        poller.handleTssDataUpdate(roster(3L, 4L));
        poller.handleStoredBlocksUpdate(stored(range(0, 4)));
        probeFinds(3L, 4 + THRESHOLD);
        blocksPresent(3L, 5, 4 + THRESHOLD);
        blocksPresent(4L, 5, 4 + THRESHOLD);
        when(s3Client.openObjectStream(BlockKeys.blockKey(PREFIX, 3L, 7L)))
                .thenThrow(new S3ResponseException(500, null, null));
        poller.poll();
        assertThat(persistence.submitted).containsExactly(5L, 6L);
        assertCounts(2, 0);
        assertThat(metrics.requestFailures().get()).isEqualTo(1L);
        verify(s3Client, never()).openObjectStream(matches("blocks/4/.*"));
    }

    @Test
    @DisplayName("A network error while reading a download counts one request failure and ends the pull")
    void downloadReadErrorEndsThePull() throws Exception {
        final BucketPoller poller = pullingPoller(10, 90_000L);
        poller.handleTssDataUpdate(roster(3L, 4L));
        poller.handleStoredBlocksUpdate(stored(range(0, 4)));
        probeFinds(3L, 4 + THRESHOLD);
        blocksPresent(3L, 5, 4 + THRESHOLD);
        blocksPresent(4L, 5, 4 + THRESHOLD);
        when(s3Client.openObjectStream(BlockKeys.blockKey(PREFIX, 3L, 7L))).thenReturn(new InputStream() {
            @Override
            public int read() throws IOException {
                throw new IOException("connection reset");
            }
        });
        poller.poll();
        assertThat(persistence.submitted).containsExactly(5L, 6L);
        assertThat(metrics.requestFailures().get()).isEqualTo(1L);
        verify(s3Client, never()).openObjectStream(matches("blocks/4/.*"));
    }

    @Test
    @DisplayName("A download whose body stalls is abandoned at the deadline and counted as a request failure")
    void stalledDownloadIsAbandonedAtTheDeadline() throws Exception {
        downloadDeadlineMs = 200L;
        final BucketPoller poller = pullingPoller(10, 90_000L);
        poller.handleTssDataUpdate(roster(3L));
        poller.handleStoredBlocksUpdate(stored(range(0, 4)));
        probeFinds(3L, 4 + THRESHOLD);
        blocksPresent(3L, 5, 4 + THRESHOLD);
        final CountDownLatch released = new CountDownLatch(1);
        when(s3Client.openObjectStream(BlockKeys.blockKey(PREFIX, 3L, 7L))).thenReturn(new InputStream() {
            @Override
            public int read() throws IOException {
                try {
                    // blocks until the download is cancelled
                    released.await();
                } catch (final InterruptedException e) {
                    throw new InterruptedIOException("cancelled");
                }
                return -1;
            }
        });
        assertTimeoutPreemptively(Duration.ofSeconds(10), poller::poll);
        assertThat(persistence.submitted).containsExactly(5L, 6L);
        assertThat(metrics.requestFailures().get()).isEqualTo(1L);
    }

    @Test
    @DisplayName("A corrupt source copy is taken from another folder for that block only")
    void corruptSourceCopyFallsBack() throws Exception {
        final BucketPoller poller = sourceNode3WithBlock7InNode4();
        corruptPresent(3L, 7L);
        poller.poll();
        assertFallbackForBlock7Only();
        assertThat(metrics.copiesRejected().get()).isEqualTo(1L);
        // the corrupt copy was read in full too
        assertThat(metrics.copiesDownloaded().get()).isEqualTo(THRESHOLD + 1);
        assertThat(metrics.copyLatencyNs().get()).isPositive();
    }

    @Test
    @DisplayName("A source copy with another block number is taken from another folder for that block only")
    void wrongNumberSourceCopyFallsBack() throws Exception {
        final BucketPoller poller = sourceNode3WithBlock7InNode4();
        blockPresent(3L, 7L, 8L);
        poller.poll();
        assertFallbackForBlock7Only();
        assertThat(metrics.copiesRejected().get()).isEqualTo(1L);
    }

    @Test
    @DisplayName("A missing source copy is taken from another folder for that block only and is not a rejected copy")
    void missingSourceCopyFallsBack() throws Exception {
        final BucketPoller poller = sourceNode3WithBlock7InNode4();
        when(s3Client.openObjectStream(BlockKeys.blockKey(PREFIX, 3L, 7L))).thenReturn(null);
        poller.poll();
        assertFallbackForBlock7Only();
        assertThat(metrics.copiesRejected().get()).isZero();
        // a missing object is not a downloaded copy
        assertThat(metrics.copiesDownloaded().get()).isEqualTo(THRESHOLD);
    }

    @Test
    @DisplayName("Persisting the probe block sets its latency from its consensus timestamp, whoever stored it")
    void probeBlockLatency() throws Exception {
        final Instant consensusTime = Instant.now().minusSeconds(60);
        consensusTimestamp = Timestamp.newBuilder()
                .seconds(consensusTime.getEpochSecond())
                .nanos(consensusTime.getNano())
                .build();
        final BucketPoller poller = pullingPoller(10, 90_000L);
        poller.handleTssDataUpdate(roster(3L));
        poller.handleStoredBlocksUpdate(stored(range(0, 4)));
        probeFinds(3L, 4 + THRESHOLD);
        blocksPresent(3L, 5, 4 + THRESHOLD);
        persistence.answer(4 + THRESHOLD, Answer.PERSIST_BY_PUBLISHER);
        poller.poll();
        assertThat(persistence.submitted).containsExactlyElementsOf(blocks(5, 4 + THRESHOLD));
        assertThat(metrics.probeBlockLatencyMs().get()).isBetween(60_000L, 120_000L);
    }

    @Test
    @DisplayName("A pull that ends before the probe block leaves its latency unchanged")
    void probeBlockLatencyUnchangedByEarlyEnd() throws Exception {
        consensusTimestamp = Timestamp.newBuilder()
                .seconds(Instant.now().minusSeconds(60).getEpochSecond())
                .build();
        final BucketPoller poller = pullingPoller(10, 90_000L);
        poller.handleTssDataUpdate(roster(3L));
        poller.handleStoredBlocksUpdate(stored(range(0, 4)));
        probeFinds(3L, 4 + THRESHOLD);
        blocksPresent(3L, 5, 6);
        blocksPresent(3L, 8, 4 + THRESHOLD);
        poller.poll();
        assertThat(persistence.submitted).containsExactly(5L, 6L);
        assertThat(metrics.probeBlockLatencyMs().get()).isZero();
    }

    @Test
    @DisplayName("No usable copy in any folder: a download failure ends the pull, the blocks before it are submitted")
    void noUsableCopyEndsThePull() throws Exception {
        final BucketPoller poller = pullingPoller(10, 90_000L);
        poller.handleTssDataUpdate(roster(3L, 4L, 5L));
        poller.handleStoredBlocksUpdate(stored(range(0, 4)));
        probeFinds(3L, 4 + THRESHOLD);
        blocksPresent(3L, 5, 4 + THRESHOLD);
        corruptPresent(3L, 7L);
        blockPresent(4L, 7L, 8L);
        pollUntilPulled(poller);
        assertThat(persistence.submitted).containsExactly(5L, 6L);
        assertCounts(2, 0);
        assertThat(metrics.blocksDownloadFailed().get()).isEqualTo(1L);
        assertThat(metrics.blocksNotPresent().get()).isZero();
        assertThat(metrics.copiesRejected().get()).isEqualTo(2L);
        assertThat(metrics.requestFailures().get()).isZero();
        // the source node's folder first, then every other folder, in an order that is random
        for (final long other : List.of(4L, 5L)) {
            final InOrder inOrder = inOrder(s3Client);
            inOrder.verify(s3Client).openObjectStream(BlockKeys.blockKey(PREFIX, 3L, 7L));
            inOrder.verify(s3Client).openObjectStream(BlockKeys.blockKey(PREFIX, other, 7L));
        }
    }

    /// A pull of blocks 5 to 14 from node 3, with a good block 7 in node 4's folder as well.
    private BucketPoller sourceNode3WithBlock7InNode4() throws Exception {
        final BucketPoller poller = pullingPoller(10, 90_000L);
        poller.handleTssDataUpdate(roster(4L, 3L));
        poller.handleStoredBlocksUpdate(stored(range(0, 4)));
        probeFinds(3L, 4 + THRESHOLD);
        blocksPresent(3L, 5, 4 + THRESHOLD);
        blockPresent(4L, 7L, 7L);
        return poller;
    }

    /// Every block is submitted, block 7 from node 4's folder and every other one from node 3's.
    private void assertFallbackForBlock7Only() throws Exception {
        assertThat(persistence.submitted).containsExactlyElementsOf(blocks(5, 4 + THRESHOLD));
        assertCounts(THRESHOLD, 0);
        verify(s3Client).openObjectStream(BlockKeys.blockKey(PREFIX, 4L, 7L));
        verify(s3Client, never()).openObjectStream(BlockKeys.blockKey(PREFIX, 4L, 8L));
        verify(s3Client).openObjectStream(BlockKeys.blockKey(PREFIX, 3L, 8L));
        assertThat(metrics.blocksDownloadFailed().get()).isZero();
        assertThat(metrics.blocksNotPresent().get()).isZero();
    }

    /// Puts an object under the key of a block that is not valid zstd.
    private void corruptPresent(final long nodeId, final long blockNumber) throws Exception {
        when(s3Client.openObjectStream(BlockKeys.blockKey(PREFIX, nodeId, blockNumber)))
                .thenReturn(new ByteArrayInputStream("not zstd".getBytes(StandardCharsets.UTF_8)));
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
            super(config, s3Client, messaging, awaiter, downloader, metrics, 60_000L);
        }

        @Override
        boolean pull(final long last, final long target, final long sourceNode, final List<Long> nodes) {
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
        FAIL_PERSIST,
        NONE
    }

    /// Stands in for the plugin, which forwards the block notifications to the awaiter.
    private final class AwaiterForwarder implements BlockNotificationHandler {
        @Override
        public void handlePersisted(final PersistedNotification notification) {
            awaiter.handlePersisted(notification);
        }

        @Override
        public void handleVerification(final VerificationNotification notification) {
            awaiter.handleVerification(notification);
        }
    }

    /// Stands in for verification and persistence: records each submitted block and answers it
    /// on the same thread, since the test messaging facility is synchronous.
    private final class Persistence implements BlockNotificationHandler {
        private final List<Long> submitted = new CopyOnWriteArrayList<>();
        private final Map<Long, Deque<Answer>> answers = new HashMap<>();
        private final Map<Long, StoredBlocksNotification> storeOnSubmit = new HashMap<>();
        private BucketPoller poller;

        /// Answers the next submissions of a block in the given order, then persists it.
        private void answer(final long blockNumber, final Answer... inOrder) {
            answers.put(blockNumber, new ArrayDeque<>(List.of(inOrder)));
        }

        @Override
        public void handleBackfilled(final BackfilledBlockNotification notification) {
            final long blockNumber = notification.blockNumber();
            submitted.add(blockNumber);
            final StoredBlocksNotification storedUpdate = storeOnSubmit.get(blockNumber);
            if (storedUpdate != null) {
                poller.handleStoredBlocksUpdate(storedUpdate);
            }
            final Deque<Answer> next = answers.get(blockNumber);
            final Answer answer = next == null || next.isEmpty() ? Answer.PERSIST : next.poll();
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
            } else if (answer == Answer.FAIL_PERSIST) {
                messaging.sendBlockPersisted(
                        new PersistedNotification(blockNumber, false, 0, BlockSource.CLOUD_STORAGE));
            }
        }
    }
}
