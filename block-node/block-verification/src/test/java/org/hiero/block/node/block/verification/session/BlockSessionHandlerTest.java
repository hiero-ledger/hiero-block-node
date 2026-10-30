// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.block.node.app.fixtures.TestConfigurationBuilder;
import org.hiero.block.node.app.fixtures.TestUtils;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.blocks.TestBlock;
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.block.verification.BadBlockDumper;
import org.hiero.block.node.block.verification.VerificationConfig;
import org.hiero.block.node.block.verification.VerificationDataProvider;
import org.hiero.block.node.block.verification.VerificationOutbox;
import org.hiero.block.node.block.verification.metrics.MetricsHolder;
import org.hiero.block.node.block.verification.session.BlockVerificationSession.SessionKey;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification.FailureInfo;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification.FailureType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/// Tests for the [BlockSessionHandler] together with the [ActiveSessionsBuffer] it
/// admits sessions to.
/// Sessions run on a [BlockingExecutor] and therefore never progress on their own:
/// a session ends only when it is cancelled, and a cancellation reports
/// synchronously through the [TestBlockMessagingFacility] behind the outbox. Every
/// test runs at the limit of 13, the smallest the configuration accepts.
@DisplayName("Block Session Handler Tests")
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class BlockSessionHandlerTest {
    /// The limit of every test, the smallest the configuration accepts.
    private static final int BUFFER_SIZE = 13;
    /// The executor the sessions are submitted to; it never runs them.
    private BlockingExecutor executor;
    /// Captures the notifications the sessions send.
    private TestBlockMessagingFacility blockMessaging;
    /// The metrics of the handler under test.
    private MetricsHolder metrics;
    /// The buffer of the handler under test, set when the handler is built.
    private ActiveSessionsBuffer activeSessions;
    /// The context the verification data provider is built from.
    private BlockNodeContext context;
    /// The verification data provider handed to the handler under test.
    private VerificationDataProvider verificationDataProvider;
    /// The last verified block handed to the handler under test.
    private AtomicLong lastVerifiedBlock;
    /// The recently verified blocks handed to the handler under test.
    private ConcurrentLinkedDeque<Long> recentlyVerifiedBlocks;

    /// Setup before each test.
    @BeforeEach
    void setUp() {
        blockMessaging = new TestBlockMessagingFacility();
        context = new BlockNodeContext(null, null, null, null, null, null, null, null, null);
        metrics = MetricsHolder.create(TestUtils.createMetrics());
        verificationDataProvider = new VerificationDataProvider(context);
        lastVerifiedBlock = new AtomicLong(-1);
        recentlyVerifiedBlocks = new ConcurrentLinkedDeque<>();
        executor = new BlockingExecutor(new LinkedBlockingQueue<>());
    }

    /// This test aims to assert that when the session handler expects to start a new block on the publisher
    /// source, but the publisher supplies [BlockItems] that do not flag a new block starting, the items are
    /// discarded and no session is started.
    @Test
    @DisplayName(
            "processBlockItems() ignores items, supplied by publisher, when we expect a new block, but receive items that do not flag new block starting")
    void testShouldIgnoreBlockItemsWhenNewBlockIsExpectedButWeReceiveItemsThatDoNotStartABlock() {
        final BlockSessionHandler toTest = newHandlerWithProductionPolicy();
        // First, generate a block.
        final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0);
        // Now, send the block to the handler, which is in a state that it is expecting the start of a new block,
        // but modify the BlockItems record to mark this as not a start of new block, even if we have a full complete
        // block
        toTest.processBlockItems(
                new BlockItems(block.blockUnparsed().blockItems(), block.number(), false, true), BlockSource.PUBLISHER);
        // Assert no session started
        assertThat(executor.wasAnyTaskSubmitted()).isFalse();
        // Now, send the block to the handler, which is in a state that it is expecting the start of a new block,
        // but this time mark the BlockItems record as a start of new block
        toTest.processBlockItems(block.asBlockItems(), BlockSource.PUBLISHER);
        // Assert a session started
        assertThat(executor.wasAnyTaskSubmitted()).isTrue();
    }

    /// Tests for the integration between the handler, its buffer and the production
    /// eviction policy, observed through a recording wrapper.
    @Nested
    @DisplayName("Eviction Policy Integration Tests")
    class EvictionPolicyIntegrationTests {
        /// This test aims to assert that the eviction policy is only consulted when an
        /// admission finds the buffer full: thirteen sessions admitted to a buffer of
        /// thirteen each find room and leave the policy untouched.
        @Test
        @DisplayName("processBlockItems() does not consult the policy while the buffer has room")
        void testDoesNotConsultThePolicyWhileTheBufferIsWithinItsLimit() {
            final RecordingEvictionPolicy policy = recordingProductionPolicy();
            final BlockSessionHandler toTest = newHandler(policy);
            publishRun(toTest, 2, 14);
            assertThat(policy.snapshots()).isEmpty();
            assertThat(gauge()).isEqualTo(13L);
            assertThat(highPriorityKeys())
                    .extracting(SessionKey::blockNumber)
                    .containsExactlyElementsOf(blockNumbers(2, 14));
        }

        /// This test aims to assert that when the admission of the fourteenth session, the
        /// backfilled block 15, finds the buffer of thirteen full, the policy receives
        /// exactly one snapshot carrying the high priority lane, the low priority lane
        /// without the session being admitted, no active high priority session (the
        /// publisher block was complete) and the last verified block. No block is verified
        /// yet, so no session waits for order, and the policy selects the backfilled block
        /// 14, the highest backfilled session and above every publisher block, as the first
        /// backup candidate.
        @Test
        @DisplayName("processBlockItems() hands the policy a snapshot of both lanes")
        void testConsultsThePolicyWithASnapshotOfBothLanesWhenTheBufferIsFull() {
            final RecordingEvictionPolicy policy = recordingProductionPolicy();
            final BlockSessionHandler toTest = newHandler(policy);
            publish(toTest, TestBlockBuilder.generateBlockWithNumber(2));
            backfillRun(toTest, 3, 15);
            assertThat(policy.snapshots()).hasSize(1);
            final ActiveSessionsSnapshot snapshot = policy.snapshots().getFirst();
            assertThat(snapshot.highPrioritySessions()).containsExactly(key(2, 0));
            assertThat(snapshot.lowPrioritySessions())
                    .extracting(SessionKey::blockNumber)
                    .containsExactlyElementsOf(blockNumbers(3, 14));
            assertThat(snapshot.activeHighPrioritySession()).isNull();
            assertThat(snapshot.lastVerifiedBlock()).isEqualTo(-1L);
            assertThat(policy.selections()).containsExactly(key(14, 12));
        }

        /// This test aims to assert that the session the policy selects is removed from its
        /// lane, cancelled, reported once through messaging as a cancellation of a complete
        /// block, counted in the evictions counter of its lane, and that the gauge reflects
        /// the combined count afterwards: with the last verified block 0 and block 1
        /// missing, every session is stuck, and the complete publisher block 20 is the
        /// highest, above the backfilled blocks 2 to 14.
        @Test
        @DisplayName("processBlockItems() evicts what the policy selects")
        void testRemovesCancelsAndReportsTheSelectedSession() {
            lastVerifiedBlock.set(0L);
            final BlockSessionHandler toTest = newHandler(recordingProductionPolicy());
            publish(toTest, TestBlockBuilder.generateBlockWithNumber(20));
            backfillRun(toTest, 2, 14);
            assertThat(highPriorityKeys()).isEmpty();
            assertThat(lowPriorityKeys())
                    .extracting(SessionKey::blockNumber)
                    .containsExactlyElementsOf(blockNumbers(2, 14));
            assertThat(notifications())
                    .hasSize(1)
                    .first()
                    .returns(20L, VerificationNotification::blockNumber)
                    .satisfies(BlockSessionHandlerTest::assertCancelledPublisherBlock);
            assertThat(gauge()).isEqualTo(13L);
            assertThat(evictedHighPriority()).isEqualTo(1L);
            assertThat(evictedLowPriority()).isEqualTo(0L);
        }

        /// This test aims to assert that a session cancelled by supersession in the
        /// same call is cleared under the eviction lock before the policy is consulted,
        /// so clearing it makes room and no eviction takes place: the complete publisher
        /// blocks 10 to 21 and the block 30 the publisher is streaming fill the buffer, the
        /// header of block 31 supersedes 30, and when the admission of 31 finds the buffer
        /// full, 30 is cleared first.
        @Test
        @DisplayName("processBlockItems() clears completed sessions before evicting")
        void testClearsCompletedSessionsBeforeConsultingThePolicy() {
            final RecordingEvictionPolicy policy = recordingProductionPolicy();
            final BlockSessionHandler toTest = newHandler(policy);
            publishRun(toTest, 10, 21);
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(30, 31);
            toTest.processBlockItems(headerOnly(blocks.get(0)), BlockSource.PUBLISHER);
            toTest.processBlockItems(headerOnly(blocks.get(1)), BlockSource.PUBLISHER);
            assertThat(policy.snapshots()).isEmpty();
            assertThat(notifications())
                    .hasSize(1)
                    .first()
                    .returns(
                            FailureInfo.standard(FailureType.CANCELLED_INCOMPLETE),
                            VerificationNotification::failureInfo)
                    .returns(30L, VerificationNotification::blockNumber);
            final List<Long> kept = new ArrayList<>(blockNumbers(10, 21));
            kept.add(31L);
            assertThat(highPriorityKeys()).extracting(SessionKey::blockNumber).containsExactlyElementsOf(kept);
            assertThat(gauge()).isEqualTo(13L);
            assertThat(evictedHighPriority()).isEqualTo(0L);
        }
    }

    /// Tests for the handler managing the lifecycle of its sessions.
    @Nested
    @DisplayName("Session Lifecycle Tests")
    class SessionLifecycleTests {
        /// This test aims to assert that a publisher session cancelled by supersession is
        /// cleared by the admission of the block that superseded it: the handler cancels it
        /// before it admits the next block, so it is complete by then, and the admission
        /// clears it before it adds the new session. Its cancellation is reported once as an
        /// incomplete block, and only the session of block 3 remains, as the active
        /// publisher session.
        @Test
        @DisplayName("processBlockItems() has a superseded session cleared by the admission that follows")
        void testHasASupersededSessionClearedByTheAdmissionThatFollows() {
            final BlockSessionHandler toTest = newHandlerWithProductionPolicy();
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(2, 3);
            toTest.processBlockItems(headerOnly(blocks.get(0)), BlockSource.PUBLISHER);
            toTest.processBlockItems(headerOnly(blocks.get(1)), BlockSource.PUBLISHER);
            assertThat(notifications())
                    .hasSize(1)
                    .first()
                    .returns(
                            FailureInfo.standard(FailureType.CANCELLED_INCOMPLETE),
                            VerificationNotification::failureInfo)
                    .returns(2L, VerificationNotification::blockNumber);
            assertThat(highPriorityKeys()).containsExactly(key(3, 1));
            assertThat(activeSessions.activePublisherSession().sessionKey()).isEqualTo(key(3, 1));
            assertThat(gauge()).isEqualTo(1L);
        }

        /// This test aims to assert that a new publisher block does not cancel the session
        /// of the previous block once that session has received its whole block, even
        /// though it is still being verified: block 2 arrives as a header batch followed
        /// by the batch ending it, then the header of block 3 arrives. No session is
        /// cancelled, nothing is reported, both sessions stay in the buffer, and the
        /// session of block 3 becomes the active publisher session.
        @Test
        @DisplayName("processBlockItems() does not cancel a publisher session that received its whole block")
        void testDoesNotCancelAPublisherSessionThatReceivedItsWholeBlock() {
            final BlockSessionHandler toTest = newHandlerWithProductionPolicy();
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(2, 3);
            toTest.processBlockItems(headerOnly(blocks.get(0)), BlockSource.PUBLISHER);
            toTest.processBlockItems(restOfBlock(blocks.get(0)), BlockSource.PUBLISHER);
            toTest.processBlockItems(headerOnly(blocks.get(1)), BlockSource.PUBLISHER);
            assertThat(notifications()).isEmpty();
            assertThat(highPriorityKeys()).containsExactly(key(2, 0), key(3, 1));
            assertThat(activeSessions.activePublisherSession().sessionKey()).isEqualTo(key(3, 1));
        }

        /// This test aims to assert that a publisher batch that does not start a block and
        /// arrives after the batch that ended the previous block is disregarded: it is not
        /// added to the session of the block that already ended, which keeps exactly the
        /// two batches of its block, and no session is started for it.
        @Test
        @DisplayName("processBlockItems() disregards a publisher batch that arrives after the end of the block")
        void testDisregardsAPublisherBatchThatArrivesAfterTheEndOfTheBlock() {
            final BlockSessionHandler toTest = newHandlerWithProductionPolicy();
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(2);
            final BlockItems header = headerOnly(block);
            final BlockItems rest = restOfBlock(block);
            toTest.processBlockItems(header, BlockSource.PUBLISHER);
            final BlockVerificationSession session = activeSessions.activePublisherSession();
            toTest.processBlockItems(rest, BlockSource.PUBLISHER);
            final BlockItems stray =
                    new BlockItems(block.blockUnparsed().blockItems().subList(1, 2), block.number(), false, false);
            toTest.processBlockItems(stray, BlockSource.PUBLISHER);
            assertThat(session.getBlockItemsDeque()).containsExactly(header, rest);
            assertThat(activeSessions.activePublisherSession()).isNull();
            assertThat(highPriorityKeys()).containsExactly(key(2, 0));
            assertThat(notifications()).isEmpty();
        }
    }

    /// Tests for the handler driving the production policy.
    @Nested
    @DisplayName("Production Policy Tests")
    class ProductionPolicyTests {
        /// This test aims to assert that blocks backfilled in descending order are handled
        /// correctly and never take the buffer over its limit: with the last verified block
        /// 100, the blocks 130 down to 102 wait above the missing block 101, so every
        /// admission into the full buffer evicts the highest stuck session. When block 101
        /// arrives, the held sessions 102 to 114 still wait above it, so the highest, 114,
        /// is evicted before 101 is added. The buffer keeps the thirteen lowest blocks, 101
        /// to 113, which verify as a run; every evicted block is reported once as a
        /// cancelled complete backfilled block, highest first, and the gauge stays at the
        /// limit.
        @Test
        @DisplayName(
                "processBlockItems() keeps the lowest blocks of a descending backfilled delivery and evicts the rest highest first")
        void testKeepsTheLowestBlocksOfADescendingBackfilledDelivery() {
            lastVerifiedBlock.set(100L);
            final BlockSessionHandler toTest = newHandlerWithProductionPolicy();
            for (final TestBlock block :
                    TestBlockBuilder.generateBlocksInRange(101, 130).reversed()) {
                backfill(toTest, block);
            }
            assertThat(lowPriorityKeys())
                    .extracting(SessionKey::blockNumber)
                    .containsExactlyElementsOf(blockNumbers(101, 113));
            assertThat(notifications())
                    .extracting(VerificationNotification::blockNumber)
                    .containsExactlyElementsOf(blockNumbers(114, 130).reversed());
            assertThat(notifications()).allSatisfy(BlockSessionHandlerTest::assertCancelledBackfilledBlock);
            assertThat(gauge()).isEqualTo(13L);
            assertThat(evictedLowPriority()).isEqualTo(17L);
        }

        /// This test aims to assert that the highest stuck session is evicted whatever
        /// its lane: with the last verified block 100 and block 101 missing, the publisher
        /// blocks 102 to 113 and the backfilled blocks 150 and 114 are all stuck, and the
        /// backfilled block 150 is the highest, so it is evicted when the admission of 114
        /// finds the buffer full, while the publisher sessions below it are kept.
        @Test
        @DisplayName(
                "processBlockItems() evicts the highest stuck session, here a backfilled block above the publisher blocks")
        void testEvictsTheHighestStuckSessionWhateverItsLane() {
            lastVerifiedBlock.set(100L);
            final BlockSessionHandler toTest = newHandlerWithProductionPolicy();
            publishRun(toTest, 102, 113);
            backfill(toTest, TestBlockBuilder.generateBlockWithNumber(150));
            backfill(toTest, TestBlockBuilder.generateBlockWithNumber(114));
            assertThat(notifications())
                    .hasSize(1)
                    .first()
                    .returns(150L, VerificationNotification::blockNumber)
                    .satisfies(BlockSessionHandlerTest::assertCancelledBackfilledBlock);
            assertThat(highPriorityKeys())
                    .extracting(SessionKey::blockNumber)
                    .containsExactlyElementsOf(blockNumbers(102, 113));
            assertThat(lowPriorityKeys()).containsExactly(key(114, 13));
        }

        /// This test aims to assert that a far-ahead high priority session is evicted
        /// rather than the backfilled sessions below it: with the last verified block 100
        /// and block 101 missing, the complete publisher block 200 is the highest stuck
        /// session, while the backfilled blocks 102 to 114 are lower and are kept, so the
        /// catch-up they carry can proceed.
        @Test
        @DisplayName(
                "processBlockItems() evicts a far-ahead high priority session rather than the backfilled sessions it waits for")
        void testEvictsAFarAheadHighPrioritySessionRatherThanTheBackfilledSessionsBelowIt() {
            lastVerifiedBlock.set(100L);
            final BlockSessionHandler toTest = newHandlerWithProductionPolicy();
            publish(toTest, TestBlockBuilder.generateBlockWithNumber(200));
            backfillRun(toTest, 102, 114);
            assertThat(notifications())
                    .hasSize(1)
                    .first()
                    .returns(200L, VerificationNotification::blockNumber)
                    .satisfies(BlockSessionHandlerTest::assertCancelledPublisherBlock);
            assertThat(highPriorityKeys()).isEmpty();
            assertThat(lowPriorityKeys())
                    .extracting(SessionKey::blockNumber)
                    .containsExactlyElementsOf(blockNumbers(102, 114));
        }

        /// This test aims to assert that the high priority session still receiving its items
        /// is never evicted, whatever the other lane holds: the publisher streams block 150
        /// above the missing block 101 and the backfilled blocks 102 to 113 fill the buffer,
        /// so when the backfilled block 114 is admitted, the highest stuck session other
        /// than the streaming one, the backfilled 113, makes room for it, and the end of
        /// the publisher block is still accepted afterwards.
        @Test
        @DisplayName("processBlockItems() never evicts the high priority session still receiving items")
        void testNeverEvictsTheHighPrioritySessionStillReceivingItems() {
            lastVerifiedBlock.set(100L);
            final BlockSessionHandler toTest = newHandlerWithProductionPolicy();
            final TestBlock streamed = TestBlockBuilder.generateBlockWithNumber(150);
            toTest.processBlockItems(headerOnly(streamed), BlockSource.PUBLISHER);
            backfillRun(toTest, 102, 114);
            assertThat(notifications())
                    .hasSize(1)
                    .first()
                    .returns(113L, VerificationNotification::blockNumber)
                    .satisfies(BlockSessionHandlerTest::assertCancelledBackfilledBlock);
            final List<Long> keptBackfilled = new ArrayList<>(blockNumbers(102, 112));
            keptBackfilled.add(114L);
            assertThat(highPriorityKeys()).containsExactly(key(150, 0));
            assertThat(lowPriorityKeys()).extracting(SessionKey::blockNumber).containsExactlyElementsOf(keptBackfilled);
            toTest.processBlockItems(restOfBlock(streamed), BlockSource.PUBLISHER);
            assertThat(notifications()).hasSize(1);
            assertThat(highPriorityKeys()).containsExactly(key(150, 0));
            assertThat(lowPriorityKeys()).extracting(SessionKey::blockNumber).containsExactlyElementsOf(keptBackfilled);
            assertThat(gauge()).isEqualTo(13L);
        }

        /// This test aims to assert that in a publisher only burst behind a missing block,
        /// every held session is stuck and the highest of them makes room for the next
        /// block: with the last verified block 100 and block 101 missing, the admission of
        /// block 115 finds the buffer full, and block 114, which has received its whole
        /// block, is evicted before 115 is added, while the lower blocks are kept.
        @Test
        @DisplayName(
                "processBlockItems() evicts the highest stuck session to admit the next block in a publisher only burst")
        void testEvictsTheHighestStuckSessionToAdmitTheNextBlockInAPublisherOnlyBurst() {
            lastVerifiedBlock.set(100L);
            final BlockSessionHandler toTest = newHandlerWithProductionPolicy();
            publishRun(toTest, 102, 115);
            assertThat(notifications())
                    .hasSize(1)
                    .first()
                    .returns(114L, VerificationNotification::blockNumber)
                    .satisfies(BlockSessionHandlerTest::assertCancelledPublisherBlock);
            final List<Long> kept = new ArrayList<>(blockNumbers(102, 113));
            kept.add(115L);
            assertThat(highPriorityKeys()).extracting(SessionKey::blockNumber).containsExactlyElementsOf(kept);
            assertThat(evictedHighPriority()).isEqualTo(1L);
        }

        /// This test aims to assert that held historical backfill, blocks at or below the
        /// last verified block, goes before any publisher block when no session is stuck:
        /// with the last verified block 100, the publisher blocks 101 to 112 form a run with
        /// it and the backfilled block 50 fills the buffer. The admissions of the backfilled
        /// blocks 51 and 52 each evict the held backfilled block below them, 50 and then 51,
        /// as the first backup candidate, while every publisher session is kept.
        @Test
        @DisplayName("processBlockItems() evicts held historical backfill before any publisher block")
        void testEvictsHeldHistoricalBackfillBeforeAnyPublisherBlock() {
            lastVerifiedBlock.set(100L);
            final BlockSessionHandler toTest = newHandlerWithProductionPolicy();
            publishRun(toTest, 101, 112);
            backfillRun(toTest, 50, 52);
            assertThat(notifications())
                    .extracting(VerificationNotification::blockNumber)
                    .containsExactly(50L, 51L);
            assertThat(notifications()).allSatisfy(BlockSessionHandlerTest::assertCancelledBackfilledBlock);
            assertThat(highPriorityKeys())
                    .extracting(SessionKey::blockNumber)
                    .containsExactlyElementsOf(blockNumbers(101, 112));
            assertThat(lowPriorityKeys()).extracting(SessionKey::blockNumber).containsExactly(52L);
            assertThat(gauge()).isEqualTo(13L);
            assertThat(evictedHighPriority()).isZero();
            assertThat(evictedLowPriority()).isEqualTo(2L);
        }

        /// This test aims to assert that a backfilled block admitted to a buffer full of
        /// publisher sessions, with no session stuck and no backfilled session held, gets
        /// its place at the cost of the publisher session at the backup position, since the
        /// policy chooses among the held sessions only: with the last verified block 100,
        /// the publisher blocks 101 to 113 form a run with it and fill the buffer, and the
        /// admission of the historical block 50 evicts the third held session from the top,
        /// 111, which is reported as a cancelled complete publisher block for the publisher
        /// to resend.
        @Test
        @DisplayName(
                "processBlockItems() evicts the publisher session at the backup position to admit a backfilled block when none is held")
        void testEvictsThePublisherSessionAtTheBackupPositionToAdmitABackfilledBlockWhenNoneIsHeld() {
            lastVerifiedBlock.set(100L);
            final BlockSessionHandler toTest = newHandlerWithProductionPolicy();
            publishRun(toTest, 101, 113);
            backfill(toTest, TestBlockBuilder.generateBlockWithNumber(50));
            assertThat(notifications())
                    .hasSize(1)
                    .first()
                    .returns(111L, VerificationNotification::blockNumber)
                    .satisfies(BlockSessionHandlerTest::assertCancelledPublisherBlock);
            final List<Long> keptPublished = new ArrayList<>(blockNumbers(101, 110));
            keptPublished.add(112L);
            keptPublished.add(113L);
            assertThat(highPriorityKeys()).extracting(SessionKey::blockNumber).containsExactlyElementsOf(keptPublished);
            assertThat(lowPriorityKeys()).containsExactly(key(50, 13));
            assertThat(gauge()).isEqualTo(13L);
            assertThat(evictedHighPriority()).isEqualTo(1L);
            assertThat(evictedLowPriority()).isZero();
        }

        /// This test aims to assert that a backfilled run contiguous with the last verified
        /// block is held at the limit without evicting the block being admitted: with the
        /// last verified block 100, the blocks 101 to 113 fill the buffer and none is stuck,
        /// so the admission of 114 evicts the highest held backfilled session, 113. That
        /// leaves every later block stuck above the gap at 113 once it is added, so each
        /// following admission evicts the block admitted before it. The buffer keeps 101 to
        /// 112, which verify as a run, and the last block, 120, which waits for the gap.
        @Test
        @DisplayName(
                "processBlockItems() holds a run contiguous with the last verified block at the limit, never evicting the block being admitted")
        void testHoldsARunContiguousWithTheLastVerifiedBlockAtTheLimit() {
            lastVerifiedBlock.set(100L);
            final BlockSessionHandler toTest = newHandlerWithProductionPolicy();
            backfillRun(toTest, 101, 120);
            assertThat(notifications())
                    .extracting(VerificationNotification::blockNumber)
                    .containsExactlyElementsOf(blockNumbers(113, 119));
            assertThat(notifications()).allSatisfy(BlockSessionHandlerTest::assertCancelledBackfilledBlock);
            final List<Long> kept = new ArrayList<>(blockNumbers(101, 112));
            kept.add(120L);
            assertThat(lowPriorityKeys()).extracting(SessionKey::blockNumber).containsExactlyElementsOf(kept);
            assertThat(gauge()).isEqualTo(13L);
            assertThat(evictedLowPriority()).isEqualTo(7L);
        }

        /// This test aims to assert that the next expected block is admitted to a full
        /// buffer and a stuck session makes room instead: with the last verified block
        /// 100, the publisher blocks 103 to 115 wait above the missing blocks 101 and 102
        /// and fill the buffer; when the backfilled block 101 arrives, every held session
        /// waits above the missing 101, and the highest, block 115, which has received its
        /// whole block and is not the active publisher session any more, is evicted and
        /// reported as a cancelled complete publisher block before 101 is added and kept.
        @Test
        @DisplayName("processBlockItems() admits the next expected block to a full buffer and evicts a stuck session")
        void testAdmitsTheNextExpectedBlockToAFullBufferAndEvictsAStuckSession() {
            lastVerifiedBlock.set(100L);
            final BlockSessionHandler toTest = newHandlerWithProductionPolicy();
            publishRun(toTest, 103, 115);
            backfill(toTest, TestBlockBuilder.generateBlockWithNumber(101));
            assertThat(notifications())
                    .hasSize(1)
                    .first()
                    .returns(115L, VerificationNotification::blockNumber)
                    .satisfies(BlockSessionHandlerTest::assertCancelledPublisherBlock);
            assertThat(highPriorityKeys())
                    .extracting(SessionKey::blockNumber)
                    .containsExactlyElementsOf(blockNumbers(103, 114));
            assertThat(lowPriorityKeys()).containsExactly(key(101, 13));
            assertThat(evictedHighPriority()).isEqualTo(1L);
        }

        /// This test aims to assert that a publisher burst contiguous with the last verified
        /// block is held at the limit: with the last verified block 100, the blocks 101 to
        /// 115 each wait only for the blocks below them. The admission of 114 finds no held
        /// session stuck and evicts the publisher session at the backup position, the third
        /// held session from the top, 111. That opens a gap below 112, 113 and, once it is
        /// added, 114, so the admission of 115 evicts the highest stuck session, 114. Both
        /// are reported as cancelled complete publisher blocks, which the publisher resends.
        @Test
        @DisplayName(
                "processBlockItems() holds a contiguous publisher burst at the limit, starting at the backup position")
        void testHoldsAContiguousPublisherBurstAtTheLimit() {
            lastVerifiedBlock.set(100L);
            final BlockSessionHandler toTest = newHandlerWithProductionPolicy();
            publishRun(toTest, 101, 115);
            assertThat(notifications())
                    .extracting(VerificationNotification::blockNumber)
                    .containsExactly(111L, 114L);
            assertThat(notifications()).allSatisfy(BlockSessionHandlerTest::assertCancelledPublisherBlock);
            final List<Long> kept = new ArrayList<>(blockNumbers(101, 110));
            kept.add(112L);
            kept.add(113L);
            kept.add(115L);
            assertThat(highPriorityKeys()).extracting(SessionKey::blockNumber).containsExactlyElementsOf(kept);
            assertThat(gauge()).isEqualTo(13L);
            assertThat(evictedHighPriority()).isEqualTo(2L);
        }

        /// This test aims to assert that the backfilled block that closes the gap below a full
        /// publisher run always gets its place, so the run can drain: with the last verified
        /// block 100, the publisher blocks 102 to 113 and the block 115 the publisher is
        /// streaming fill the buffer above the missing block 101. When backfill delivers 101,
        /// the policy chooses among the held sessions, all of which but the streaming one wait
        /// above the missing 101, so the highest stuck session, the publisher's 113, is
        /// evicted and reported as a cancelled complete publisher block, and 101 is added: 101
        /// to 112 can then verify as a run, while the streaming 115 is kept.
        @Test
        @DisplayName("processBlockItems() admits the backfilled block that closes the gap below a full publisher run")
        void testAdmitsTheBackfilledBlockThatClosesTheGapBelowAFullPublisherRun() {
            lastVerifiedBlock.set(100L);
            final BlockSessionHandler toTest = newHandlerWithProductionPolicy();
            publishRun(toTest, 102, 113);
            toTest.processBlockItems(headerOnly(TestBlockBuilder.generateBlockWithNumber(115)), BlockSource.PUBLISHER);
            backfill(toTest, TestBlockBuilder.generateBlockWithNumber(101));
            assertThat(notifications())
                    .hasSize(1)
                    .first()
                    .returns(113L, VerificationNotification::blockNumber)
                    .satisfies(BlockSessionHandlerTest::assertCancelledPublisherBlock);
            assertThat(lowPriorityKeys()).containsExactly(key(101, 13));
            final List<Long> kept = new ArrayList<>(blockNumbers(102, 112));
            kept.add(115L);
            assertThat(highPriorityKeys()).extracting(SessionKey::blockNumber).containsExactlyElementsOf(kept);
            assertThat(activeSessions.activePublisherSession().sessionKey()).isEqualTo(key(115, 12));
            assertThat(gauge()).isEqualTo(13L);
            assertThat(evictedHighPriority()).isEqualTo(1L);
            assertThat(evictedLowPriority()).isZero();
        }

        /// This test aims to assert that a backfilled block that arrives stuck far ahead of
        /// a run still gets its place, and goes on the next admission: with the last
        /// verified block 100 and the backfilled run 101 to 113 filling the buffer, no held
        /// session is stuck, so the admission of 500 evicts the highest held session, 113.
        /// 500 then waits above the gap at 113 as the only stuck session, so when backfill
        /// delivers 113 again, 500 is evicted to make room, and the run 101 to 113 is whole
        /// again.
        @Test
        @DisplayName("processBlockItems() admits a backfilled block stuck far ahead of the run and evicts it next")
        void testAdmitsABackfilledBlockStuckFarAheadOfTheRunAndEvictsItNext() {
            lastVerifiedBlock.set(100L);
            final BlockSessionHandler toTest = newHandlerWithProductionPolicy();
            backfillRun(toTest, 101, 113);
            backfill(toTest, TestBlockBuilder.generateBlockWithNumber(500));
            assertThat(notifications())
                    .extracting(VerificationNotification::blockNumber)
                    .containsExactly(113L);
            final List<Long> withTheFarAheadBlock = new ArrayList<>(blockNumbers(101, 112));
            withTheFarAheadBlock.add(500L);
            assertThat(lowPriorityKeys())
                    .extracting(SessionKey::blockNumber)
                    .containsExactlyElementsOf(withTheFarAheadBlock);
            backfill(toTest, TestBlockBuilder.generateBlockWithNumber(113));
            assertThat(notifications())
                    .extracting(VerificationNotification::blockNumber)
                    .containsExactly(113L, 500L);
            assertThat(notifications()).allSatisfy(BlockSessionHandlerTest::assertCancelledBackfilledBlock);
            assertThat(lowPriorityKeys())
                    .extracting(SessionKey::blockNumber)
                    .containsExactlyElementsOf(blockNumbers(101, 113));
            assertThat(evictedLowPriority()).isEqualTo(2L);
        }

        /// This test aims to assert that when sessions of both lanes hold the highest stuck
        /// block, the backfilled session is evicted and the publisher's copy is kept: with
        /// the last verified block 100, block 114 is held by a publisher session and by a
        /// backfilled session, both stuck above the missing 101 when the admission of the
        /// backfilled block 101 finds the buffer full, and the backfilled 114 is evicted
        /// before 101 is added.
        @Test
        @DisplayName("processBlockItems() evicts the backfilled session when both lanes hold the highest stuck block")
        void testEvictsTheBackfilledSessionWhenBothLanesHoldTheHighestStuckBlock() {
            lastVerifiedBlock.set(100L);
            final BlockSessionHandler toTest = newHandlerWithProductionPolicy();
            publishRun(toTest, 103, 114);
            backfill(toTest, TestBlockBuilder.generateBlockWithNumber(114));
            backfill(toTest, TestBlockBuilder.generateBlockWithNumber(101));
            assertThat(notifications())
                    .hasSize(1)
                    .first()
                    .returns(114L, VerificationNotification::blockNumber)
                    .satisfies(BlockSessionHandlerTest::assertCancelledBackfilledBlock);
            assertThat(highPriorityKeys())
                    .extracting(SessionKey::blockNumber)
                    .containsExactlyElementsOf(blockNumbers(103, 114));
            assertThat(lowPriorityKeys()).containsExactly(key(101, 13));
            assertThat(evictedLowPriority()).isEqualTo(1L);
        }
    }

    /// Tests for admissions arriving from both ingress threads at the same time.
    @Nested
    @DisplayName("Concurrent Admission Tests")
    class ConcurrentAdmissionTests {
        /// The number of blocks each ingress thread admits.
        private static final int BLOCKS_PER_THREAD = 300;

        /// This test aims to assert that when both ingress threads admit sessions at the
        /// same time into a full buffer, with sessions that never finish on their own so
        /// that evictions are the only removals, the combined count is exactly the buffer
        /// size once both threads are done: with the last verified block 0, no block is
        /// ever verified and block 1 never arrives, so every session is stuck and every
        /// admission into the full buffer evicts one.
        /// Every admitted block ended either as an active session or as exactly one
        /// cancellation notification, no block was reported twice, and the evictions
        /// counters add up to the number of cancellations.
        @Test
        @DisplayName(
                "processBlockItems() keeps the combined count within the limit under concurrent admissions while every session is stuck")
        void testConcurrentAdmissionsKeepTheCombinedCountWithinTheLimit() throws Exception {
            lastVerifiedBlock.set(0L);
            admitConcurrentlyAndAssertTheBufferIsAtItsLimit(
                    TestBlockBuilder.generateBlocksInRange(1000, 1000 + BLOCKS_PER_THREAD - 1),
                    TestBlockBuilder.generateBlocksInRange(2, BLOCKS_PER_THREAD + 1));
        }

        /// This test aims to assert that when both ingress threads admit blocks in descending
        /// order at the same time, the eviction pass still finds a session to give up on every
        /// admission into the full buffer: with the last verified block 0, no block is ever
        /// verified and block 1 never arrives, so every session is stuck. The combined count is
        /// exactly the buffer size once both threads are
        /// done, every admitted block ended either as an active session or as exactly one
        /// cancellation notification, no block was reported twice, and the evictions counters
        /// add up to the number of cancellations.
        @Test
        @DisplayName(
                "processBlockItems() keeps the combined count within the limit when both threads admit blocks in descending order while every session is stuck")
        void testConcurrentDescendingAdmissionsKeepTheCombinedCountWithinTheLimit() throws Exception {
            lastVerifiedBlock.set(0L);
            admitConcurrentlyAndAssertTheBufferIsAtItsLimit(
                    TestBlockBuilder.generateBlocksInRange(1000, 1000 + BLOCKS_PER_THREAD - 1)
                            .reversed(),
                    TestBlockBuilder.generateBlocksInRange(2, BLOCKS_PER_THREAD + 1)
                            .reversed());
        }

        /// This test aims to assert that when both ingress threads admit sessions at the
        /// same time and no session is stuck, every admission into the full buffer still evicts one
        /// session, whatever the interleaving: with the last verified block 1000, the
        /// publisher blocks from 1001 are admitted in ascending order on one thread and form
        /// a run with the last verified block, and the backfilled blocks are all at or below
        /// it. The combined count is exactly the limit once both threads are done, every
        /// admitted block ended either as an active session or as exactly one cancellation
        /// notification, no block was reported twice, and the evictions counters add up to
        /// the number of cancellations.
        @Test
        @DisplayName(
                "processBlockItems() keeps the combined count within the limit under concurrent admissions when no session is stuck")
        void testConcurrentAdmissionsKeepTheCombinedCountWithinTheLimitWhenNoSessionIsStuck() throws Exception {
            lastVerifiedBlock.set(1000L);
            admitConcurrentlyAndAssertTheBufferIsAtItsLimit(
                    TestBlockBuilder.generateBlocksInRange(1001, 1000 + BLOCKS_PER_THREAD),
                    TestBlockBuilder.generateBlocksInRange(1, BLOCKS_PER_THREAD));
        }

        /// Admits the publisher blocks from one thread and the backfilled blocks from another,
        /// both in the order given, then asserts the invariants of the active sessions buffer.
        private void admitConcurrentlyAndAssertTheBufferIsAtItsLimit(
                final List<TestBlock> publisherBlocks, final List<TestBlock> backfilledBlocks) throws Exception {
            final int bufferSize = BUFFER_SIZE;
            final BlockSessionHandler toTest = newHandlerWithProductionPolicy();
            admitConcurrently(toTest, publisherBlocks, backfilledBlocks);
            final int admitted = publisherBlocks.size() + backfilledBlocks.size();
            assertThat(combinedSize()).isEqualTo(bufferSize);
            assertThat(gauge()).isEqualTo(bufferSize);
            final List<VerificationNotification> notifications = notifications();
            assertThat(notifications).hasSize(admitted - bufferSize);
            assertThat(notifications).allSatisfy(BlockSessionHandlerTest::assertCancelledCompleteBlock);
            assertThat(notifications)
                    .extracting(VerificationNotification::blockNumber)
                    .doesNotHaveDuplicates();
            final Set<Long> accountedFor = new HashSet<>();
            for (final VerificationNotification notification : notifications) {
                accountedFor.add(notification.blockNumber());
            }
            for (final SessionKey key : highPriorityKeys()) {
                accountedFor.add(key.blockNumber());
            }
            for (final SessionKey key : lowPriorityKeys()) {
                accountedFor.add(key.blockNumber());
            }
            final Set<Long> expected = new HashSet<>();
            for (final TestBlock block : publisherBlocks) {
                expected.add(block.number());
            }
            for (final TestBlock block : backfilledBlocks) {
                expected.add(block.number());
            }
            assertThat(accountedFor).isEqualTo(expected);
            assertThat(evictedHighPriority() + evictedLowPriority()).isEqualTo(admitted - bufferSize);
        }

        /// Admits the publisher blocks from one ingress thread and the backfilled blocks from
        /// another, both in the order given and released by a common start signal, and waits
        /// until both are done.
        private void admitConcurrently(
                final BlockSessionHandler toTest,
                final List<TestBlock> publisherBlocks,
                final List<TestBlock> backfilledBlocks)
                throws Exception {
            final CountDownLatch start = new CountDownLatch(1);
            final ExecutorService ingressThreads = Executors.newVirtualThreadPerTaskExecutor();
            try {
                final Future<Void> publisherRun =
                        ingressThreads.submit(new Admissions(toTest, publisherBlocks, BlockSource.PUBLISHER, start));
                final Future<Void> backfillRun =
                        ingressThreads.submit(new Admissions(toTest, backfilledBlocks, BlockSource.BACKFILL, start));
                start.countDown();
                publisherRun.get();
                backfillRun.get();
            } finally {
                ingressThreads.shutdownNow();
            }
        }
    }

    /// Admits every block of a list from one ingress thread, after a common start signal.
    private static final class Admissions implements Callable<Void> {
        /// The handler to admit into.
        private final BlockSessionHandler handler;
        /// The blocks to admit, in order.
        private final List<TestBlock> blocks;
        /// The source the blocks are admitted as.
        private final BlockSource source;
        /// The signal both ingress threads wait for before their first admission.
        private final CountDownLatch start;

        /// Constructor.
        private Admissions(
                final BlockSessionHandler handler,
                final List<TestBlock> blocks,
                final BlockSource source,
                final CountDownLatch start) {
            this.handler = handler;
            this.blocks = blocks;
            this.source = source;
            this.start = start;
        }

        /// {@inheritDoc}
        @Override
        public Void call() throws InterruptedException {
            start.await();
            for (final TestBlock block : blocks) {
                handler.processBlockItems(block.asBlockItems(), source);
            }
            return null;
        }
    }

    /// Tests for the gauge that reflects the live size of the active sessions buffer.
    @Nested
    @DisplayName("Active Sessions Gauge Tests")
    class ActiveSessionsGaugeTests {
        /// This test aims to assert that the active sessions gauge grows together with
        /// the active sessions buffer while new sessions are activated and the buffer
        /// limit is not yet reached.
        @Test
        @DisplayName("gauge grows as new sessions are activated")
        void testGaugeGrowsWithActivatedSessions() {
            final BlockSessionHandler toTest = newHandlerWithProductionPolicy();
            // Create two blocks, well below the limit
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(2, 3);
            // Supply the first block and assert the gauge reflects one active session
            publish(toTest, blocks.get(0));
            assertThat(gauge()).isEqualTo(1L);
            // Supply the second block and assert the gauge reflects two active sessions
            publish(toTest, blocks.get(1));
            assertThat(gauge()).isEqualTo(2L);
        }

        /// This test aims to assert that the active sessions gauge stays at the buffer
        /// limit when the buffer is full and a new session evicts the highest stuck
        /// session other than the active one, mirroring the combined size of both lanes:
        /// with the last verified block 100 and block 101 missing, the admission of the
        /// fourteenth publisher block, 115, evicts 114.
        @Test
        @DisplayName("gauge stays at buffer size when a new session evicts a session")
        void testGaugeStaysAtBufferSizeOnEviction() {
            lastVerifiedBlock.set(100L);
            final BlockSessionHandler toTest = newHandlerWithProductionPolicy();
            publishRun(toTest, 102, 115);
            assertThat(gauge()).isEqualTo(13L).isEqualTo(combinedSize());
            assertThat(highPriorityKeys())
                    .extracting(SessionKey::blockNumber)
                    .doesNotContain(114L)
                    .hasSize(13);
        }

        /// This test aims to assert that the gauge stays at the configured size when
        /// blocks arrive in descending order: with the last verified block 100 and block
        /// 101 missing, every session is stuck, and the admission of the fourteenth block,
        /// 102, evicts the highest stuck session, 115, before 102 is added.
        @Test
        @DisplayName("gauge stays at buffer size when blocks arrive in descending order")
        void testGaugeStaysAtBufferSizeWhenBlocksArriveInDescendingOrder() {
            lastVerifiedBlock.set(100L);
            final BlockSessionHandler toTest = newHandlerWithProductionPolicy();
            for (final TestBlock block :
                    TestBlockBuilder.generateBlocksInRange(102, 115).reversed()) {
                publish(toTest, block);
            }
            assertThat(gauge()).isEqualTo(13L).isEqualTo(combinedSize());
            assertThat(highPriorityKeys())
                    .extracting(SessionKey::blockNumber)
                    .containsExactlyElementsOf(blockNumbers(102, 114));
        }

        /// This test aims to assert that the gauge stays at the limit when no session is
        /// stuck: with the last verified block 100, the publisher blocks 101 to 115 form a
        /// run with it, each admission into the full buffer still evicts one session, and the
        /// gauge reports the limit, matching the combined size of both lanes.
        @Test
        @DisplayName("gauge stays at the limit when no session is stuck")
        void testGaugeStaysAtTheLimitWhenNoSessionIsStuck() {
            lastVerifiedBlock.set(100L);
            final BlockSessionHandler toTest = newHandlerWithProductionPolicy();
            publishRun(toTest, 101, 115);
            assertThat(gauge()).isEqualTo(13L).isEqualTo(combinedSize());
        }
    }

    /// Build a handler with the given policy and the configuration of every test.
    private BlockSessionHandler newHandler(final SessionEvictionPolicy policy) {
        return newHandler(policy, config());
    }

    /// Build a handler with the production policy and the configuration of every test.
    private BlockSessionHandler newHandlerWithProductionPolicy() {
        final VerificationConfig verificationConfig = config();
        return newHandler(new OrderAwareEvictionPolicy(verificationConfig), verificationConfig);
    }

    /// The production policy built from the configuration of every test, wrapped to
    /// record every snapshot it is handed and every key it selects.
    private static RecordingEvictionPolicy recordingProductionPolicy() {
        return RecordingEvictionPolicy.recording(new OrderAwareEvictionPolicy(config()));
    }

    /// Build a handler with the given policy and configuration, and keep its buffer.
    private BlockSessionHandler newHandler(final SessionEvictionPolicy policy, final VerificationConfig config) {
        activeSessions = new ActiveSessionsBuffer(config, policy, lastVerifiedBlock, metrics.sessionHandlerMetrics());
        return new BlockSessionHandler(
                metrics,
                config,
                verificationDataProvider,
                lastVerifiedBlock,
                recentlyVerifiedBlocks,
                executor,
                new BadBlockDumper(config, "test"),
                activeSessions,
                new VerificationOutbox(blockMessaging));
    }

    /// The verification configuration of every test: the defaults, with the limit of
    /// [#BUFFER_SIZE].
    private static VerificationConfig config() {
        return new TestConfigurationBuilder()
                .withConfigDataType(VerificationConfig.class)
                .withValue("verification.activeSessionsBufferSize", String.valueOf(BUFFER_SIZE))
                .getOrCreateConfig()
                .getConfigData(VerificationConfig.class);
    }

    /// Supply a complete block as a single publisher batch.
    private static void publish(final BlockSessionHandler handler, final TestBlock block) {
        handler.processBlockItems(block.asBlockItems(), BlockSource.PUBLISHER);
    }

    /// Supply a complete block as a backfilled batch.
    private static void backfill(final BlockSessionHandler handler, final TestBlock block) {
        handler.processBlockItems(block.asBlockItems(), BlockSource.BACKFILL);
    }

    /// Supply each block from the first block to the last, in ascending order, as a
    /// complete block in a single publisher batch.
    private static void publishRun(final BlockSessionHandler handler, final long firstBlock, final long lastBlock) {
        for (final TestBlock block : TestBlockBuilder.generateBlocksInRange(firstBlock, lastBlock)) {
            publish(handler, block);
        }
    }

    /// Supply each block from the first block to the last, in ascending order, as a
    /// backfilled batch.
    private static void backfillRun(final BlockSessionHandler handler, final long firstBlock, final long lastBlock) {
        for (final TestBlock block : TestBlockBuilder.generateBlocksInRange(firstBlock, lastBlock)) {
            backfill(handler, block);
        }
    }

    /// The block numbers from the first block to the last, ascending.
    private static List<Long> blockNumbers(final long firstBlock, final long lastBlock) {
        final List<Long> blockNumbers = new ArrayList<>();
        for (long blockNumber = firstBlock; blockNumber <= lastBlock; blockNumber++) {
            blockNumbers.add(blockNumber);
        }
        return blockNumbers;
    }

    /// A batch carrying only the header of a block, starting the block without ending it.
    private static BlockItems headerOnly(final TestBlock block) {
        return new BlockItems(List.of(block.getHeaderUnparsed()), block.number(), true, false);
    }

    /// A batch carrying every item after the header of a block, ending the block.
    private static BlockItems restOfBlock(final TestBlock block) {
        return new BlockItems(
                block.blockUnparsed().blockItems().subList(1, block.blockSize()), block.number(), false, true);
    }

    /// A session key.
    private static SessionKey key(final long blockNumber, final long uniqueId) {
        return new SessionKey(blockNumber, uniqueId);
    }

    /// Asserts that a notification reports a cancelled complete block of either source.
    private static void assertCancelledCompleteBlock(final VerificationNotification notification) {
        assertThat(notification)
                .returns(false, VerificationNotification::success)
                .returns(FailureInfo.standard(FailureType.CANCELLED), VerificationNotification::failureInfo)
                .returns(null, VerificationNotification::block)
                .returns(null, VerificationNotification::blockHash);
    }

    /// Asserts that a notification reports a cancelled complete publisher block.
    private static void assertCancelledPublisherBlock(final VerificationNotification notification) {
        assertThat(notification)
                .returns(false, VerificationNotification::success)
                .returns(FailureInfo.standard(FailureType.CANCELLED), VerificationNotification::failureInfo)
                .returns(BlockSource.PUBLISHER, VerificationNotification::source)
                .returns(null, VerificationNotification::block)
                .returns(null, VerificationNotification::blockHash);
    }

    /// Asserts that a notification reports a cancelled complete backfilled block.
    private static void assertCancelledBackfilledBlock(final VerificationNotification notification) {
        assertThat(notification)
                .returns(false, VerificationNotification::success)
                .returns(FailureInfo.standard(FailureType.CANCELLED), VerificationNotification::failureInfo)
                .returns(BlockSource.BACKFILL, VerificationNotification::source)
                .returns(null, VerificationNotification::block)
                .returns(null, VerificationNotification::blockHash);
    }

    /// The notifications sent so far.
    private List<VerificationNotification> notifications() {
        return blockMessaging.getSentVerificationNotifications();
    }

    /// The keys of the high priority lane.
    private NavigableSet<SessionKey> highPriorityKeys() {
        return activeSessions.snapshot().highPrioritySessions();
    }

    /// The keys of the low priority lane.
    private NavigableSet<SessionKey> lowPriorityKeys() {
        return activeSessions.snapshot().lowPrioritySessions();
    }

    /// The combined size of both lanes.
    private long combinedSize() {
        return activeSessions.size();
    }

    /// Reads the current value of the active sessions gauge.
    private long gauge() {
        return metrics.sessionHandlerMetrics().verificationActiveSessions().get();
    }

    /// Reads the evictions counter of the high priority lane.
    private long evictedHighPriority() {
        return metrics.sessionHandlerMetrics()
                .verificationSessionsEvictedHighPriority()
                .get();
    }

    /// Reads the evictions counter of the low priority lane.
    private long evictedLowPriority() {
        return metrics.sessionHandlerMetrics()
                .verificationSessionsEvictedLowPriority()
                .get();
    }
}
