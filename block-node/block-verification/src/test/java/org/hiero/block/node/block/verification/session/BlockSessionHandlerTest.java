// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.node.app.fixtures.TestConfigurationBuilder;
import org.hiero.block.node.app.fixtures.TestUtils;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.blocks.TestBlock;
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
import org.hiero.block.node.block.verification.BadBlockDumper;
import org.hiero.block.node.block.verification.VerificationConfig;
import org.hiero.block.node.block.verification.VerificationDataProvider;
import org.hiero.block.node.block.verification.metrics.MetricsHolder;
import org.hiero.block.node.block.verification.session.BlockVerificationSession.SessionKey;
import org.hiero.block.node.block.verification.session.eviction.EvictionPolicy;
import org.hiero.block.node.block.verification.session.eviction.EvictionSnapshot;
import org.hiero.block.node.block.verification.session.eviction.GapAwareEvictionPolicy;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/// Tests for the [BlockSessionHandler].
///
/// Sessions run on a [BlockingExecutor] that never executes them, so no
/// session ever completes on its own and every assertion on the active
/// sessions buffer is deterministic. The last verified block is fixed at 1,
/// so block 2 is the next expected block (never stuck) and blocks 3 and above
/// wait for order (stuck) unless configured otherwise.
@DisplayName("Block Session Handler Tests")
class BlockSessionHandlerTest {
    /// The buffer limit configured for every test.
    private static final int BUFFER_LIMIT = 2;
    private BlockingExecutor executor;
    private ConcurrentSkipListMap<SessionKey, BlockVerificationSession> activeSessions;
    private MetricsHolder metrics;
    private AtomicLong lastVerifiedBlock;
    private VerificationConfig verificationConfig;
    private VerificationDataProvider verificationDataProvider;
    private BadBlockDumper badBlockDumper;
    private BlockNodeContext context;
    private BlockSessionHandler toTest;

    /// Setup before each test.
    @BeforeEach
    void setUp() {
        context = new BlockNodeContext(null, null, null, null, null, null, null, null, null, null, null, null, null);
        metrics = MetricsHolder.create(TestUtils.createMetrics());
        final TestConfigurationBuilder configBuilder = new TestConfigurationBuilder();
        verificationConfig = configBuilder
                .withConfigDataType(VerificationConfig.class)
                .withValue("verification.activeSessionsBufferSize", Integer.toString(BUFFER_LIMIT))
                .getOrCreateConfig()
                .getConfigData(VerificationConfig.class);
        verificationDataProvider = new VerificationDataProvider(context);
        lastVerifiedBlock = new AtomicLong(1);
        activeSessions = new ConcurrentSkipListMap<>();
        badBlockDumper = new BadBlockDumper(verificationConfig, "test");
        executor = new BlockingExecutor(new LinkedBlockingQueue<>());
        toTest = createHandler(new GapAwareEvictionPolicy());
    }

    /// Create a handler wired to the shared test state with the given policy.
    private BlockSessionHandler createHandler(final EvictionPolicy evictionPolicy) {
        return new BlockSessionHandler(
                context,
                metrics,
                verificationConfig,
                verificationDataProvider,
                lastVerifiedBlock,
                new ConcurrentLinkedDeque<>(),
                activeSessions,
                executor,
                badBlockDumper,
                evictionPolicy);
    }

    /// Reads the current value of the active sessions gauge.
    private long currentGaugeValue() {
        return metrics.sessionHandlerMetrics().verificationActiveSessions().get();
    }

    /// Reads the current value of the evicted sessions counter for a priority.
    private long evictedCount(final SessionPriority priority) {
        return metrics.sessionHandlerMetrics()
                .verificationSessionsEvicted(priority)
                .get();
    }

    /// This test aims to assert that when session handler expects to start a new block on publisher source,
    /// but publisher supplies [BlockItems] that do not flag a new block starting, the items will be discarded.
    @Test
    @DisplayName(
            "processLiveItems() ignores items, supplied by publisher, when we expect a new block, but receive items that do not flag new block starting")
    void testShouldIgnoreBlockItemsWhenNewBlockIsExpectedButWeReceiveItemsThatDoNotStartABlock() {
        // First, generate a block.
        final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0);
        // Now, send the block to the handler, which is in a state that it is expecting the start of a new block,
        // but modify the BlockItems record to mark this as not a start of new block, even if we have a full complete
        // block
        toTest.processLiveItems(new BlockItems(block.blockUnparsed().blockItems(), block.number(), false, true));
        // Assert no session started
        assertThat(executor.wasAnyTaskSubmitted()).isFalse();
        // Now, send the block to the handler, which is in a state that it is expecting the start of a new block,
        // but this time mark the BlockItems record as a start of new block
        toTest.processLiveItems(block.asBlockItems());
        // Assert a session started
        assertThat(executor.wasAnyTaskSubmitted()).isTrue();
    }

    /// Tests for the default eviction behaviour through the handler.
    @Nested
    @DisplayName("Default Eviction Tests")
    class DefaultEvictionTests {
        /// This test aims to assert that when the buffer is full and blocks arrive in order, the
        /// session that is evicted is the highest block that is not the one just activated, so the
        /// next expected block (the head of the chain) is kept and releases everything the moment
        /// it completes.
        @Test
        @DisplayName("in order arrival evicts the highest stuck session that is not the current one")
        void testInOrderArrivalEvictsHighestNonCurrent() {
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(2, 4);
            toTest.processLiveItems(blocks.get(0).asBlockItems());
            toTest.processLiveItems(blocks.get(1).asBlockItems());
            toTest.processLiveItems(blocks.get(2).asBlockItems());
            assertThat(activeSessions)
                    .hasSize(BUFFER_LIMIT)
                    .containsKeys(new SessionKey(2, 0), new SessionKey(4, 2))
                    .doesNotContainKey(new SessionKey(3, 1));
            assertThat(evictedCount(SessionPriority.HIGH)).isEqualTo(1L);
            assertThat(evictedCount(SessionPriority.LOW)).isZero();
        }

        /// This test aims to assert that when blocks arrive in reverse order the buffer never grows
        /// past its limit: the just activated lowest block is kept and the highest stuck session is
        /// evicted instead. This closes the unbounded growth the previous lowest first rule allowed.
        @Test
        @DisplayName("reverse order arrival keeps the buffer at its limit by evicting the highest session")
        void testReverseOrderArrivalStaysWithinLimit() {
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(2, 4);
            toTest.processLiveItems(blocks.get(2).asBlockItems());
            toTest.processLiveItems(blocks.get(1).asBlockItems());
            toTest.processLiveItems(blocks.get(0).asBlockItems());
            assertThat(activeSessions)
                    .hasSize(BUFFER_LIMIT)
                    .containsKeys(new SessionKey(3, 1), new SessionKey(2, 2))
                    .doesNotContainKey(new SessionKey(4, 0));
        }

        /// This test aims to assert that a whole block session (low priority) waiting for order is
        /// evicted before any publisher session when both are stuck and the low priority block fills
        /// no gap another session waits for.
        @Test
        @DisplayName("stuck whole block session is evicted before publisher sessions")
        void testWholeBlockSessionEvictedBeforePublisherSessions() {
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(3, 4);
            final TestBlock farAhead = TestBlockBuilder.generateBlockWithNumber(10);
            toTest.processWholeBlock(farAhead.asBlockItems(), BlockSource.BACKFILL);
            toTest.processLiveItems(blocks.get(0).asBlockItems());
            toTest.processLiveItems(blocks.get(1).asBlockItems());
            assertThat(activeSessions)
                    .hasSize(BUFFER_LIMIT)
                    .containsKeys(new SessionKey(3, 1), new SessionKey(4, 2))
                    .doesNotContainKey(new SessionKey(10, 0));
            assertThat(evictedCount(SessionPriority.LOW)).isEqualTo(1L);
            assertThat(evictedCount(SessionPriority.HIGH)).isZero();
        }

        /// This test aims to assert that a publisher session that has not yet received the end of its
        /// block is protected from eviction triggered by whole block activations: cancelling it would
        /// report an incomplete cancellation the publisher does not act on. The whole block session
        /// that fills the gap toward the newest whole block is evicted instead.
        @Test
        @DisplayName("incomplete publisher session is protected from whole block activations")
        void testIncompletePublisherSessionProtected() {
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(2, 4);
            final TestBlock headerOnly = blocks.get(0);
            toTest.processLiveItems(
                    new BlockItems(List.of(headerOnly.getHeaderUnparsed()), headerOnly.number(), true, false));
            toTest.processWholeBlock(blocks.get(1).asBlockItems(), BlockSource.BACKFILL);
            toTest.processWholeBlock(blocks.get(2).asBlockItems(), BlockSource.BACKFILL);
            assertThat(activeSessions)
                    .hasSize(BUFFER_LIMIT)
                    .containsKeys(new SessionKey(2, 0), new SessionKey(4, 2))
                    .doesNotContainKey(new SessionKey(3, 1));
            assertThat(activeSessions.get(new SessionKey(2, 0)).isEndOfBlockReceived())
                    .isFalse();
        }

        /// This test aims to assert that sessions which will complete on their own are never evicted
        /// and the buffer is allowed to transiently overshoot its limit: whole blocks below the next
        /// expected block are not stuck, so with only such sessions present nothing is cancelled.
        @Test
        @DisplayName("buffer overshoots transiently when only non stuck sessions are present")
        void testOvershootWithNonStuckSessions() {
            lastVerifiedBlock.set(100);
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(5, 7);
            for (final TestBlock block : blocks) {
                toTest.processWholeBlock(block.asBlockItems(), BlockSource.BACKFILL);
            }
            assertThat(activeSessions).hasSize(3);
            assertThat(evictedCount(SessionPriority.LOW)).isZero();
            assertThat(currentGaugeValue()).isEqualTo(3L);
        }
    }

    /// Tests for the priority recorded on started sessions.
    @Nested
    @DisplayName("Session Priority Tests")
    class SessionPriorityTests {
        /// This test aims to assert that sessions started from live publisher items are high priority
        /// and carry the publisher source.
        @Test
        @DisplayName("live items start high priority publisher sessions")
        void testLiveItemsStartHighPrioritySessions() {
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(2);
            toTest.processLiveItems(block.asBlockItems());
            final BlockVerificationSession session = activeSessions.get(new SessionKey(2, 0));
            assertThat(session)
                    .returns(SessionPriority.HIGH, BlockVerificationSession::priority)
                    .returns(BlockSource.PUBLISHER, BlockVerificationSession::blockSource)
                    .returns(true, BlockVerificationSession::isEndOfBlockReceived);
        }

        /// This test aims to assert that sessions started from whole blocks are low priority and carry
        /// the source they were delivered with.
        @Test
        @DisplayName("whole blocks start low priority sessions with their source")
        void testWholeBlocksStartLowPrioritySessions() {
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(2);
            toTest.processWholeBlock(block.asBlockItems(), BlockSource.BACKFILL);
            final BlockVerificationSession session = activeSessions.get(new SessionKey(2, 0));
            assertThat(session)
                    .returns(SessionPriority.LOW, BlockVerificationSession::priority)
                    .returns(BlockSource.BACKFILL, BlockVerificationSession::blockSource)
                    .returns(true, BlockVerificationSession::isEndOfBlockReceived);
        }
    }

    /// Tests for the contract between the handler and a policy, using a recording test policy.
    @Nested
    @DisplayName("Eviction Policy Contract Tests")
    class EvictionPolicyContractTests {
        /// This test aims to assert that the policy is not consulted while the buffer is at or below
        /// its limit, so no eviction can ever happen prematurely.
        @Test
        @DisplayName("policy is not consulted while the buffer is within its limit")
        void testPolicyNotConsultedWithinLimit() {
            final AtomicReference<EvictionSnapshot> recorded = new AtomicReference<>();
            toTest = createHandler(snapshot -> {
                recorded.set(snapshot);
                return List.of();
            });
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(3, 4);
            toTest.processLiveItems(blocks.get(0).asBlockItems());
            toTest.processLiveItems(blocks.get(1).asBlockItems());
            assertThat(recorded.get()).isNull();
        }

        /// This test aims to assert that the snapshot handed to the policy describes the buffer
        /// faithfully: every active session with its priority and source, the last verified block,
        /// the limit, the ordering settings, and the just activated session as protected.
        @Test
        @DisplayName("policy receives a faithful snapshot with the current session protected")
        void testPolicyReceivesFaithfulSnapshot() {
            final AtomicReference<EvictionSnapshot> recorded = new AtomicReference<>();
            toTest = createHandler(snapshot -> {
                recorded.set(snapshot);
                return List.of();
            });
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(3, 5);
            toTest.processLiveItems(blocks.get(0).asBlockItems());
            toTest.processWholeBlock(blocks.get(1).asBlockItems(), BlockSource.BACKFILL);
            toTest.processLiveItems(blocks.get(2).asBlockItems());
            final EvictionSnapshot snapshot = recorded.get();
            assertThat(snapshot).isNotNull();
            assertThat(snapshot.lastVerifiedBlock()).isEqualTo(1L);
            assertThat(snapshot.limit()).isEqualTo(BUFFER_LIMIT);
            assertThat(snapshot.firstOrderedBlock()).isEqualTo(verificationConfig.firstOrderedBlock());
            assertThat(snapshot.allSourcesRequireOrdering()).isEqualTo(verificationConfig.allSourcesRequireOrdering());
            assertThat(snapshot.protectedKeys()).containsExactly(new SessionKey(5, 2));
            assertThat(snapshot.sessions())
                    .extracting(s -> s.key(), s -> s.priority(), s -> s.source(), s -> s.endOfBlockReceived())
                    .containsExactly(
                            tuple(new SessionKey(3, 0), SessionPriority.HIGH, BlockSource.PUBLISHER, true),
                            tuple(new SessionKey(4, 1), SessionPriority.LOW, BlockSource.BACKFILL, true),
                            tuple(new SessionKey(5, 2), SessionPriority.HIGH, BlockSource.PUBLISHER, true));
        }

        /// This test aims to assert that the handler evicts exactly what the policy selects, in order,
        /// and counts each eviction under the priority of the evicted session. A naive policy that
        /// always selects the lowest key is used, so the outcome differs visibly from the default.
        @Test
        @DisplayName("handler evicts exactly what the policy selects")
        void testHandlerEvictsPolicySelection() {
            toTest = createHandler(
                    snapshot -> List.of(snapshot.sessions().getFirst().key()));
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(2, 4);
            toTest.processWholeBlock(blocks.get(0).asBlockItems(), BlockSource.BACKFILL);
            toTest.processLiveItems(blocks.get(1).asBlockItems());
            toTest.processLiveItems(blocks.get(2).asBlockItems());
            assertThat(activeSessions)
                    .hasSize(BUFFER_LIMIT)
                    .containsKeys(new SessionKey(3, 1), new SessionKey(4, 2))
                    .doesNotContainKey(new SessionKey(2, 0));
            assertThat(evictedCount(SessionPriority.LOW)).isEqualTo(1L);
            assertThat(evictedCount(SessionPriority.HIGH)).isZero();
        }

        /// This test aims to assert that a victim the policy selects which is no longer in the buffer
        /// is skipped without error and without counting an eviction, since the handler always
        /// re-checks that a removal actually removed something before cancelling.
        @Test
        @DisplayName("victims no longer in the buffer are skipped")
        void testUnknownVictimSkipped() {
            toTest = createHandler(snapshot -> List.of(new SessionKey(999, 999)));
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(2, 4);
            for (final TestBlock block : blocks) {
                toTest.processLiveItems(block.asBlockItems());
            }
            assertThat(activeSessions).hasSize(3);
            assertThat(evictedCount(SessionPriority.HIGH)).isZero();
            assertThat(evictedCount(SessionPriority.LOW)).isZero();
        }

        /// This test aims to assert that evicting the publisher session currently receiving items
        /// clears the handler's reference to it, so that subsequent items for that block are
        /// disregarded instead of being offered to a cancelled session.
        @Test
        @DisplayName("evicting the active publisher session clears the reference to it")
        void testEvictingActivePublisherSessionClearsReference() {
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(2, 4);
            final TestBlock incomplete = blocks.get(0);
            toTest = createHandler(snapshot -> List.of(new SessionKey(incomplete.number(), 0)));
            toTest.processLiveItems(
                    new BlockItems(List.of(incomplete.getHeaderUnparsed()), incomplete.number(), true, false));
            toTest.processWholeBlock(blocks.get(1).asBlockItems(), BlockSource.BACKFILL);
            toTest.processWholeBlock(blocks.get(2).asBlockItems(), BlockSource.BACKFILL);
            assertThat(activeSessions).doesNotContainKey(new SessionKey(2, 0));
            // the ending batch of block 2 must now be disregarded: no new session and no re-activation
            final List<BlockItemUnparsed> items = incomplete.blockUnparsed().blockItems();
            toTest.processLiveItems(new BlockItems(items.subList(1, items.size()), incomplete.number(), false, true));
            assertThat(activeSessions).doesNotContainKey(new SessionKey(2, 0)).hasSize(BUFFER_LIMIT);
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
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(2, 3);
            toTest.processLiveItems(blocks.get(0).asBlockItems());
            assertThat(currentGaugeValue()).isEqualTo(1L);
            toTest.processLiveItems(blocks.get(1).asBlockItems());
            assertThat(currentGaugeValue()).isEqualTo(2L);
        }

        /// This test aims to assert that the active sessions gauge stays at the buffer
        /// limit when the buffer is full and a new session evicts another session,
        /// mirroring the actual size of the active sessions buffer.
        @Test
        @DisplayName("gauge stays at buffer size when a new session evicts another session")
        void testGaugeStaysAtBufferSizeOnEviction() {
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(2, 4);
            toTest.processLiveItems(blocks.get(0).asBlockItems());
            toTest.processLiveItems(blocks.get(1).asBlockItems());
            toTest.processLiveItems(blocks.get(2).asBlockItems());
            assertThat(currentGaugeValue()).isEqualTo(BUFFER_LIMIT).isEqualTo(activeSessions.size());
        }

        /// This test aims to assert that the active sessions gauge stays at the buffer
        /// limit when blocks arrive in reverse order, since the just activated lowest
        /// block no longer prevents an eviction.
        @Test
        @DisplayName("gauge stays at buffer size when blocks arrive in reverse order")
        void testGaugeStaysAtBufferSizeInReverseOrder() {
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(2, 4);
            toTest.processLiveItems(blocks.get(2).asBlockItems());
            toTest.processLiveItems(blocks.get(1).asBlockItems());
            toTest.processLiveItems(blocks.get(0).asBlockItems());
            assertThat(currentGaugeValue()).isEqualTo(BUFFER_LIMIT).isEqualTo(activeSessions.size());
        }
    }
}
