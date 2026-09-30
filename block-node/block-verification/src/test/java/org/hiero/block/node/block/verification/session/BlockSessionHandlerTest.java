// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;
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

/// Tests for the [BlockSessionHandler].
/// Sessions run on a [BlockingExecutor] and therefore never progress on their own:
/// a session ends only when the handler cancels it, and a cancellation reports
/// synchronously through the [TestBlockMessagingFacility] of the context.
@DisplayName("Block Session Handler Tests")
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class BlockSessionHandlerTest {
    /// The executor the sessions are submitted to; it never runs them.
    private BlockingExecutor executor;
    /// Captures the notifications the sessions send.
    private TestBlockMessagingFacility blockMessaging;
    /// The metrics of the handler under test.
    private MetricsHolder metrics;
    /// The high priority lane handed to the handler under test.
    private ConcurrentSkipListMap<SessionKey, BlockVerificationSession> highPrioritySessions;
    /// The low priority lane handed to the handler under test.
    private ConcurrentSkipListMap<SessionKey, BlockVerificationSession> lowPrioritySessions;
    /// The context handed to the handler under test.
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
        context = new BlockNodeContext(
                null, null, null, blockMessaging, null, null, null, null, null, null, null, null, null);
        metrics = MetricsHolder.create(TestUtils.createMetrics());
        verificationDataProvider = new VerificationDataProvider(context);
        lastVerifiedBlock = new AtomicLong(-1);
        recentlyVerifiedBlocks = new ConcurrentLinkedDeque<>();
        highPrioritySessions = new ConcurrentSkipListMap<>();
        lowPrioritySessions = new ConcurrentSkipListMap<>();
        executor = new BlockingExecutor(new LinkedBlockingQueue<>());
    }

    /// This test aims to assert that when the session handler expects to start a new block on the publisher
    /// source, but the publisher supplies [BlockItems] that do not flag a new block starting, the items are
    /// discarded and no session is started.
    @Test
    @DisplayName(
            "processBlockItems() ignores items, supplied by publisher, when we expect a new block, but receive items that do not flag new block starting")
    void testShouldIgnoreBlockItemsWhenNewBlockIsExpectedButWeReceiveItemsThatDoNotStartABlock() {
        final BlockSessionHandler toTest = newHandlerWithProductionPolicy(2);
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

    /// Tests for the integration between the handler and an injected, scripted eviction policy.
    @Nested
    @DisplayName("Eviction Policy Integration Tests")
    class EvictionPolicyIntegrationTests {
        /// This test aims to assert that the eviction policy is only consulted when the
        /// combined count exceeds the limit: two sessions in a buffer of two leave the
        /// policy untouched.
        @Test
        @DisplayName("processBlockItems() does not consult the policy at or below the limit")
        void testDoesNotConsultThePolicyWhileTheBufferIsWithinItsLimit() {
            final RecordingEvictionPolicy policy = RecordingEvictionPolicy.lowestKeyFirst();
            final BlockSessionHandler toTest = newHandler(policy, 2);
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(2, 3);
            publish(toTest, blocks.get(0));
            publish(toTest, blocks.get(1));
            assertThat(policy.snapshots()).isEmpty();
            assertThat(gauge()).isEqualTo(2L);
            assertThat(highPriorityKeys()).containsExactly(key(2, 0), key(3, 1));
        }

        /// This test aims to assert that when the third session exceeds a limit of two
        /// the policy receives exactly one snapshot carrying the high priority lane, the
        /// low priority lane, no active high priority session (the publisher block was
        /// complete) and the last verified block.
        @Test
        @DisplayName("processBlockItems() hands the policy a snapshot of both lanes")
        void testConsultsThePolicyWithASnapshotOfBothLanesWhenTheLimitIsExceeded() {
            final RecordingEvictionPolicy policy = RecordingEvictionPolicy.lowestKeyFirst();
            final BlockSessionHandler toTest = newHandler(policy, 2);
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(2, 4);
            publish(toTest, blocks.get(0));
            backfill(toTest, blocks.get(1));
            backfill(toTest, blocks.get(2));
            assertThat(policy.snapshots()).hasSize(1);
            final ActiveSessionsSnapshot snapshot = policy.snapshots().getFirst();
            assertThat(snapshot.highPrioritySessions()).containsExactly(key(2, 0));
            assertThat(snapshot.lowPrioritySessions()).containsExactly(key(3, 1), key(4, 2));
            assertThat(snapshot.activeHighPrioritySession()).isNull();
            assertThat(snapshot.lastVerifiedBlock()).isEqualTo(-1L);
        }

        /// This test aims to assert that the session selected by the policy is removed
        /// from its lane, cancelled, reported once through messaging as a cancellation
        /// of a complete block, counted in the evictions counter of its lane, and that
        /// the gauge reflects the combined count afterwards.
        @Test
        @DisplayName("processBlockItems() evicts what the policy selects")
        void testRemovesCancelsAndReportsTheSelectedSession() {
            final BlockSessionHandler toTest = newHandler(RecordingEvictionPolicy.lowestKeyFirst(), 2);
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(2, 4);
            publish(toTest, blocks.get(0));
            backfill(toTest, blocks.get(1));
            backfill(toTest, blocks.get(2));
            assertThat(highPriorityKeys()).isEmpty();
            assertThat(lowPriorityKeys()).containsExactly(key(3, 1), key(4, 2));
            assertThat(notifications())
                    .hasSize(1)
                    .first()
                    .returns(false, VerificationNotification::success)
                    .returns(FailureInfo.standard(FailureType.CANCELLED), VerificationNotification::failureInfo)
                    .returns(2L, VerificationNotification::blockNumber)
                    .returns(BlockSource.PUBLISHER, VerificationNotification::source)
                    .returns(null, VerificationNotification::block)
                    .returns(null, VerificationNotification::blockHash);
            assertThat(gauge()).isEqualTo(2L);
            assertThat(evictedHighPriority()).isEqualTo(1L);
            assertThat(evictedLowPriority()).isEqualTo(0L);
        }

        /// This test aims to assert that a policy may select more than one session and
        /// that every selected session is evicted in one pass.
        @Test
        @DisplayName("processBlockItems() evicts every selected session")
        void testEvictsEverySelectedSessionWhenThePolicySelectsSeveral() {
            final RecordingEvictionPolicy policy = RecordingEvictionPolicy.always(key(2, 0), key(3, 1));
            final BlockSessionHandler toTest = newHandler(policy, 2);
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(2, 4);
            backfill(toTest, blocks.get(0));
            backfill(toTest, blocks.get(1));
            backfill(toTest, blocks.get(2));
            assertThat(lowPriorityKeys()).containsExactly(key(4, 2));
            assertThat(notifications())
                    .hasSize(2)
                    .extracting(VerificationNotification::blockNumber)
                    .containsExactly(2L, 3L);
            assertThat(notifications()).allSatisfy(BlockSessionHandlerTest::assertCancelledBackfilledBlock);
            assertThat(gauge()).isEqualTo(1L);
            assertThat(evictedLowPriority()).isEqualTo(2L);
            assertThat(policy.snapshots()).hasSize(1);
        }

        /// This test aims to assert that a selected key that is in no lane evicts
        /// nothing and sends nothing, and that the policy is consulted exactly once per
        /// admission whatever it selects.
        @Test
        @DisplayName("processBlockItems() ignores a selected key that is in no lane")
        void testIgnoresASelectedSessionThatIsNoLongerActive() {
            final RecordingEvictionPolicy policy = RecordingEvictionPolicy.always(key(99, 99));
            final BlockSessionHandler toTest = newHandler(policy, 2);
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(2, 4);
            backfill(toTest, blocks.get(0));
            backfill(toTest, blocks.get(1));
            backfill(toTest, blocks.get(2));
            assertThat(lowPriorityKeys()).containsExactly(key(2, 0), key(3, 1), key(4, 2));
            assertThat(notifications()).isEmpty();
            assertThat(policy.snapshots()).hasSize(1);
            assertThat(gauge()).isEqualTo(3L);
        }

        /// This test aims to assert that when a policy selects the high priority session
        /// still receiving items, the eviction reports an incomplete cancellation and
        /// clears the handler's reference to that session, so that the next snapshot
        /// carries no active high priority session and the remaining items of that block
        /// are discarded.
        @Test
        @DisplayName("processBlockItems() clears the active high priority session when a policy evicts it")
        void testClearsTheActivePublisherReferenceWhenTheActiveSessionIsEvicted() {
            final RecordingEvictionPolicy policy = RecordingEvictionPolicy.always(key(2, 0));
            final BlockSessionHandler toTest = newHandler(policy, 1);
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(2, 4);
            toTest.processBlockItems(headerOnly(blocks.get(0)), BlockSource.PUBLISHER);
            backfill(toTest, blocks.get(1));
            assertThat(notifications())
                    .hasSize(1)
                    .first()
                    .returns(false, VerificationNotification::success)
                    .returns(
                            FailureInfo.standard(FailureType.CANCELLED_INCOMPLETE),
                            VerificationNotification::failureInfo)
                    .returns(2L, VerificationNotification::blockNumber)
                    .returns(BlockSource.PUBLISHER, VerificationNotification::source);
            assertThat(highPriorityKeys()).isEmpty();
            assertThat(evictedHighPriority()).isEqualTo(1L);
            toTest.processBlockItems(restOfBlock(blocks.get(0)), BlockSource.PUBLISHER);
            backfill(toTest, blocks.get(2));
            assertThat(policy.snapshots()).hasSize(2);
            assertThat(policy.snapshots().getLast().activeHighPrioritySession()).isNull();
            assertThat(lowPriorityKeys()).containsExactly(key(3, 1), key(4, 2));
            assertThat(notifications()).hasSize(1);
        }

        /// This test aims to assert that a session cancelled by supersession in the
        /// same call is reaped under the eviction lock before the policy is consulted,
        /// so the count drops back within the limit and no eviction takes place.
        @Test
        @DisplayName("processBlockItems() reaps finished sessions before evicting")
        void testReapsFinishedSessionsBeforeConsultingThePolicy() {
            final RecordingEvictionPolicy policy = RecordingEvictionPolicy.lowestKeyFirst();
            final BlockSessionHandler toTest = newHandler(policy, 1);
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(2, 3);
            toTest.processBlockItems(headerOnly(blocks.get(0)), BlockSource.PUBLISHER);
            toTest.processBlockItems(headerOnly(blocks.get(1)), BlockSource.PUBLISHER);
            assertThat(policy.snapshots()).isEmpty();
            assertThat(notifications())
                    .hasSize(1)
                    .first()
                    .returns(
                            FailureInfo.standard(FailureType.CANCELLED_INCOMPLETE),
                            VerificationNotification::failureInfo)
                    .returns(2L, VerificationNotification::blockNumber);
            assertThat(highPriorityKeys()).containsExactly(key(3, 1));
            assertThat(gauge()).isEqualTo(1L);
            assertThat(evictedHighPriority()).isEqualTo(0L);
        }
    }

    /// Tests for the handler driving the production policy.
    @Nested
    @DisplayName("Production Policy Tests")
    class ProductionPolicyTests {
        /// This test aims to assert that backfilled blocks arriving in descending order
        /// into a full buffer evict the highest session on every admission, keep the
        /// combined count at the limit, and report each evicted block as a cancelled
        /// complete block; the former rule evicted nothing in this case and let the
        /// buffer grow.
        @Test
        @DisplayName("processBlockItems() evicts the highest session on every admission of a descending delivery")
        void testEvictsTheHighestSessionOnEveryAdmissionOfADescendingBackfillDelivery() {
            final BlockSessionHandler toTest = newHandlerWithProductionPolicy(3);
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(6, 10);
            backfill(toTest, blocks.get(4));
            backfill(toTest, blocks.get(3));
            backfill(toTest, blocks.get(2));
            backfill(toTest, blocks.get(1));
            assertThat(lowPriorityKeys()).containsExactly(key(7, 3), key(8, 2), key(9, 1));
            assertThat(notifications())
                    .hasSize(1)
                    .first()
                    .returns(10L, VerificationNotification::blockNumber)
                    .satisfies(BlockSessionHandlerTest::assertCancelledBackfilledBlock);
            assertThat(gauge()).isEqualTo(3L);
            backfill(toTest, blocks.get(0));
            assertThat(lowPriorityKeys()).containsExactly(key(6, 4), key(7, 3), key(8, 2));
            assertThat(notifications())
                    .hasSize(2)
                    .last()
                    .returns(9L, VerificationNotification::blockNumber)
                    .satisfies(BlockSessionHandlerTest::assertCancelledBackfilledBlock);
            assertThat(gauge()).isEqualTo(3L);
            assertThat(evictedLowPriority()).isEqualTo(2L);
        }

        /// This test aims to assert that when the buffer overflows and the low priority
        /// lane holds a session nobody waits for, that session is evicted and the
        /// high priority lane is untouched.
        @Test
        @DisplayName("processBlockItems() gives up a low priority session before a high priority session")
        void testEvictsLowPrioritySessionsBeforeHighPrioritySessions() {
            final BlockSessionHandler toTest = newHandlerWithProductionPolicy(2);
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(2, 5);
            publish(toTest, blocks.get(1));
            backfill(toTest, blocks.get(0));
            backfill(toTest, blocks.get(3));
            assertThat(notifications())
                    .hasSize(1)
                    .first()
                    .returns(5L, VerificationNotification::blockNumber)
                    .satisfies(BlockSessionHandlerTest::assertCancelledBackfilledBlock);
            assertThat(highPriorityKeys()).containsExactly(key(3, 0));
            assertThat(lowPriorityKeys()).containsExactly(key(2, 1));
        }

        /// This test aims to assert that when every low priority session is needed by a
        /// complete high priority session above them, the high priority session is evicted and
        /// the backfilled sessions are kept, so the catch-up they carry can proceed.
        @Test
        @DisplayName(
                "processBlockItems() evicts a far-ahead high priority session rather than the backfilled sessions it waits for")
        void testEvictsAFarAheadHighPrioritySessionBeforeNeededLowPrioritySessions() {
            final BlockSessionHandler toTest = newHandlerWithProductionPolicy(2);
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(2, 10);
            publish(toTest, blocks.get(8));
            backfill(toTest, blocks.get(0));
            backfill(toTest, blocks.get(1));
            assertThat(notifications())
                    .hasSize(1)
                    .first()
                    .returns(false, VerificationNotification::success)
                    .returns(FailureInfo.standard(FailureType.CANCELLED), VerificationNotification::failureInfo)
                    .returns(10L, VerificationNotification::blockNumber)
                    .returns(BlockSource.PUBLISHER, VerificationNotification::source);
            assertThat(highPriorityKeys()).isEmpty();
            assertThat(lowPriorityKeys()).containsExactly(key(2, 1), key(3, 2));
        }

        /// This test aims to assert that the high priority session still receiving its items
        /// is never evicted, whatever the other lane holds: the backfilled newcomer is
        /// evicted instead, and the end of the publisher block is still accepted
        /// afterwards.
        @Test
        @DisplayName("processBlockItems() never evicts the high priority session still receiving items")
        void testNeverEvictsTheHighPrioritySessionStillReceivingItems() {
            final BlockSessionHandler toTest = newHandlerWithProductionPolicy(2);
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(7, 9);
            toTest.processBlockItems(headerOnly(blocks.get(0)), BlockSource.PUBLISHER);
            backfill(toTest, blocks.get(1));
            backfill(toTest, blocks.get(2));
            assertThat(notifications())
                    .hasSize(1)
                    .first()
                    .returns(9L, VerificationNotification::blockNumber)
                    .satisfies(BlockSessionHandlerTest::assertCancelledBackfilledBlock);
            assertThat(highPriorityKeys()).containsExactly(key(7, 0));
            assertThat(lowPriorityKeys()).containsExactly(key(8, 1));
            toTest.processBlockItems(restOfBlock(blocks.get(0)), BlockSource.PUBLISHER);
            assertThat(notifications()).hasSize(1);
            assertThat(highPriorityKeys()).containsExactly(key(7, 0));
            assertThat(lowPriorityKeys()).containsExactly(key(8, 1));
            assertThat(gauge()).isEqualTo(2L);
        }

        /// This test aims to assert that in a publisher only burst the highest complete
        /// session other than the one just started is evicted, and the lowest, which is
        /// the next block to be released, is kept.
        @Test
        @DisplayName("processBlockItems() evicts the highest complete high priority session in a burst")
        void testEvictsTheHighestCompleteHighPrioritySessionInAPublisherOnlyBurst() {
            final BlockSessionHandler toTest = newHandlerWithProductionPolicy(2);
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(2, 4);
            publish(toTest, blocks.get(0));
            publish(toTest, blocks.get(1));
            publish(toTest, blocks.get(2));
            assertThat(notifications())
                    .hasSize(1)
                    .first()
                    .returns(false, VerificationNotification::success)
                    .returns(FailureInfo.standard(FailureType.CANCELLED), VerificationNotification::failureInfo)
                    .returns(3L, VerificationNotification::blockNumber)
                    .returns(BlockSource.PUBLISHER, VerificationNotification::source);
            assertThat(highPriorityKeys()).containsExactly(key(2, 0), key(4, 2));
            assertThat(evictedHighPriority()).isEqualTo(1L);
        }
    }

    /// Tests for admissions arriving from both ingress threads at the same time.
    @Nested
    @DisplayName("Concurrent Admission Tests")
    class ConcurrentAdmissionTests {
        /// The buffer size used by the concurrent admission tests.
        private static final int BUFFER_SIZE = 5;
        /// The number of blocks each ingress thread admits.
        private static final int BLOCKS_PER_THREAD = 300;

        /// This test aims to assert that when both ingress threads admit sessions at the
        /// same time into a small buffer, with sessions that never finish on their own so
        /// that evictions are the only removals, the combined count is exactly the buffer
        /// size once both threads are done, every admitted block ended either as an active
        /// session or as exactly one cancellation notification, no block was reported
        /// twice, and the evictions counters add up to the number of cancellations.
        @Test
        @DisplayName("processBlockItems() keeps the combined count within the limit under concurrent admissions")
        void testConcurrentAdmissionsKeepTheCombinedCountWithinTheLimit() throws Exception {
            admitConcurrentlyAndAssertTheBufferIsAtItsLimit(
                    TestBlockBuilder.generateBlocksInRange(1000, 1000 + BLOCKS_PER_THREAD - 1),
                    TestBlockBuilder.generateBlocksInRange(1, BLOCKS_PER_THREAD));
        }

        /// This test aims to assert that when both ingress threads admit blocks in descending
        /// order at the same time, the order in which the former lowest-block rule evicted
        /// nothing and let the buffer grow, the eviction pass still finds a session to give up
        /// on every overflowing admission: the combined count is exactly the buffer size once
        /// both threads are done, every admitted block ended either as an active session or as
        /// exactly one cancellation notification, no block was reported twice, and the
        /// evictions counters add up to the number of cancellations.
        @Test
        @DisplayName(
                "processBlockItems() keeps the combined count within the limit when both threads admit blocks in descending order")
        void testConcurrentDescendingAdmissionsKeepTheCombinedCountWithinTheLimit() throws Exception {
            admitConcurrentlyAndAssertTheBufferIsAtItsLimit(
                    TestBlockBuilder.generateBlocksInRange(1000, 1000 + BLOCKS_PER_THREAD - 1)
                            .reversed(),
                    TestBlockBuilder.generateBlocksInRange(1, BLOCKS_PER_THREAD).reversed());
        }

        /// Admits the publisher blocks from one thread and the backfilled blocks from another,
        /// both in the order given, then asserts the invariants of the active sessions buffer.
        private void admitConcurrentlyAndAssertTheBufferIsAtItsLimit(
                final List<TestBlock> publisherBlocks, final List<TestBlock> backfilledBlocks) throws Exception {
            final int bufferSize = BUFFER_SIZE;
            final BlockSessionHandler toTest = newHandlerWithProductionPolicy(bufferSize);
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
            final BlockSessionHandler toTest = newHandlerWithProductionPolicy(2);
            // Create two blocks, matching the buffer size configured in setup
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(2, 3);
            // Supply the first block and assert the gauge reflects one active session
            publish(toTest, blocks.get(0));
            assertThat(gauge()).isEqualTo(1L);
            // Supply the second block and assert the gauge reflects two active sessions
            publish(toTest, blocks.get(1));
            assertThat(gauge()).isEqualTo(2L);
        }

        /// This test aims to assert that the active sessions gauge stays at the buffer
        /// limit when the buffer is full and a new session evicts the highest complete
        /// session, mirroring the combined size of both lanes.
        @Test
        @DisplayName("gauge stays at buffer size when a new session evicts a session")
        void testGaugeStaysAtBufferSizeOnEviction() {
            final BlockSessionHandler toTest = newHandlerWithProductionPolicy(2);
            // Create three blocks, one more than the buffer size configured in setup
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(2, 4);
            // Supply blocks in order so the third activation evicts the highest complete session
            publish(toTest, blocks.get(0));
            publish(toTest, blocks.get(1));
            publish(toTest, blocks.get(2));
            // Assert the gauge matches the buffer, capped at the configured size
            assertThat(gauge()).isEqualTo(2L).isEqualTo(combinedSize());
            assertThat(highPriorityKeys()).containsExactly(key(2, 0), key(4, 2));
        }

        /// This test aims to assert that the gauge stays at the configured size when
        /// blocks arrive in descending order, because the highest session is evicted on
        /// the overflowing admission; the former rule let the buffer and the gauge grow
        /// beyond the limit in this case.
        @Test
        @DisplayName("gauge stays at buffer size when blocks arrive in descending order")
        void testGaugeStaysAtBufferSizeWhenBlocksArriveInDescendingOrder() {
            final BlockSessionHandler toTest = newHandlerWithProductionPolicy(2);
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(2, 4);
            publish(toTest, blocks.get(2));
            publish(toTest, blocks.get(1));
            publish(toTest, blocks.get(0));
            assertThat(gauge()).isEqualTo(2L).isEqualTo(combinedSize());
            assertThat(highPriorityKeys()).containsExactly(key(2, 2), key(3, 1));
        }
    }

    /// Build a handler with the given policy and buffer size.
    private BlockSessionHandler newHandler(final SessionEvictionPolicy policy, final int bufferSize) {
        return newHandler(policy, configWithBufferSize(bufferSize));
    }

    /// Build a handler with the production policy and the given buffer size.
    private BlockSessionHandler newHandlerWithProductionPolicy(final int bufferSize) {
        final VerificationConfig verificationConfig = configWithBufferSize(bufferSize);
        return newHandler(new OrderAwareEvictionPolicy(verificationConfig), verificationConfig);
    }

    /// Build a handler with the given policy and configuration.
    private BlockSessionHandler newHandler(final SessionEvictionPolicy policy, final VerificationConfig config) {
        return new BlockSessionHandler(
                context,
                metrics,
                config,
                verificationDataProvider,
                lastVerifiedBlock,
                recentlyVerifiedBlocks,
                highPrioritySessions,
                lowPrioritySessions,
                policy,
                executor,
                new BadBlockDumper(config, "test"));
    }

    /// Build a verification configuration with the given buffer size.
    private static VerificationConfig configWithBufferSize(final int bufferSize) {
        return new TestConfigurationBuilder()
                .withConfigDataType(VerificationConfig.class)
                .withValue("verification.activeSessionsBufferSize", String.valueOf(bufferSize))
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
        return highPrioritySessions.keySet();
    }

    /// The keys of the low priority lane.
    private NavigableSet<SessionKey> lowPriorityKeys() {
        return lowPrioritySessions.keySet();
    }

    /// The combined size of both lanes.
    private long combinedSize() {
        return highPrioritySessions.size() + lowPrioritySessions.size();
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
