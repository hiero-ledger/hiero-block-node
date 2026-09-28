// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.tuple;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.hiero.block.internal.BlockItemUnparsed;
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
import org.hiero.block.node.block.verification.session.eviction.EvictionPolicy;
import org.hiero.block.node.block.verification.session.eviction.EvictionSnapshot;
import org.hiero.block.node.block.verification.session.eviction.GapAwareEvictionPolicy;
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

/// Tests for the [BlockSessionHandler].
///
/// Sessions run on a [BlockingExecutor] that never executes them on its own,
/// so no session completes unless a test runs the queued sessions on the
/// test thread, and every assertion on the active sessions buffer is
/// deterministic. No verification data is available, so a session that runs
/// finishes with a failure for its block, which is enough to observe that it
/// consumed its items and left the buffer. The last verified block is fixed
/// at 1, so block 2 is the next expected block (never waiting) and blocks 3
/// and above wait for order unless configured otherwise.
@DisplayName("Block Session Handler Tests")
class BlockSessionHandlerTest {
    /// The buffer limit configured for every test.
    private static final int BUFFER_LIMIT = 2;
    private BlockingExecutor executor;
    private TestBlockMessagingFacility blockMessaging;
    private MetricsHolder metrics;
    private AtomicLong lastVerifiedBlock;
    private VerificationConfig verificationConfig;
    private VerificationDataProvider verificationDataProvider;
    private BadBlockDumper badBlockDumper;
    private BlockNodeContext context;
    private ActiveSessionsBuffer buffer;
    private BlockSessionHandler toTest;

    /// Setup before each test.
    @BeforeEach
    void setUp() {
        blockMessaging = new TestBlockMessagingFacility();
        context = new BlockNodeContext(
                null, null, null, blockMessaging, null, null, null, null, null, null, null, null, null);
        metrics = MetricsHolder.create(TestUtils.createMetrics());
        verificationConfig = new TestConfigurationBuilder()
                .withConfigDataType(VerificationConfig.class)
                .withValue("verification.activeSessionsBufferSize", Integer.toString(BUFFER_LIMIT))
                .getOrCreateConfig()
                .getConfigData(VerificationConfig.class);
        verificationDataProvider = new VerificationDataProvider(context);
        lastVerifiedBlock = new AtomicLong(1);
        badBlockDumper = new BadBlockDumper(verificationConfig, "test");
        executor = new BlockingExecutor(new LinkedBlockingQueue<>());
        toTest = createHandler(new GapAwareEvictionPolicy());
    }

    /// Create a handler, and the buffer it activates sessions in, with the given policy.
    private BlockSessionHandler createHandler(final EvictionPolicy evictionPolicy) {
        buffer = new ActiveSessionsBuffer(
                verificationConfig, lastVerifiedBlock, evictionPolicy, metrics.sessionHandlerMetrics());
        return new BlockSessionHandler(
                context,
                metrics,
                verificationConfig,
                verificationDataProvider,
                lastVerifiedBlock,
                new ConcurrentLinkedDeque<>(),
                executor,
                badBlockDumper,
                buffer);
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

    /// Reads the current value of the blocks received counter.
    private long blocksReceived() {
        return metrics.sessionHandlerMetrics().verificationBlocksReceived().get();
    }

    /// A batch holding only the header of the block, starting but not ending it.
    private static BlockItems headerOnly(final TestBlock block) {
        return new BlockItems(List.of(block.getHeaderUnparsed()), block.number(), true, false);
    }

    /// A batch holding everything after the header of the block, ending but not starting it.
    private static BlockItems remainderEnding(final TestBlock block) {
        final List<BlockItemUnparsed> items = block.blockUnparsed().blockItems();
        return new BlockItems(items.subList(1, items.size()), block.number(), false, true);
    }

    /// A batch starting the block whose first item is not a header, so the
    /// session fails as soon as it runs.
    private static BlockItems startingWithoutHeader(final TestBlock block) {
        final List<BlockItemUnparsed> items = block.blockUnparsed().blockItems();
        return new BlockItems(items.subList(1, items.size()), block.number(), true, false);
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

    /// Tests for the whole block entry point.
    @Nested
    @DisplayName("Whole Block Flow Tests")
    class WholeBlockFlowTests {
        /// This test aims to assert that a whole block starts one session that
        /// is activated in the buffer, and that the session received its
        /// batch: once it runs it produces a result for its block and leaves
        /// the buffer on its own, without any cancellation.
        @Test
        @DisplayName("a whole block starts one session that runs to a result and leaves the buffer")
        void testWholeBlockStartsSessionThatRunsToResultAndLeaves() {
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(2);
            toTest.processWholeBlock(block.asBlockItems(), BlockSource.BACKFILL);
            assertThat(buffer.contains(new SessionKey(2, 0))).isTrue();
            assertThat(blocksReceived()).isEqualTo(1L);
            executor.executeSerially();
            assertThat(buffer.contains(new SessionKey(2, 0))).isFalse();
            assertThat(currentGaugeValue()).isZero();
            assertThat(blockMessaging.getSentVerificationNotifications())
                    .hasSize(1)
                    .first()
                    .returns(false, VerificationNotification::success)
                    .returns(2L, VerificationNotification::blockNumber)
                    .returns(BlockSource.BACKFILL, VerificationNotification::source);
            assertThat(evictedCount(SessionPriority.LOW)).isZero();
        }

        /// This test aims to assert that the whole block entry point refuses a
        /// batch that does not both start and end the block, since such a
        /// session could never receive the rest of its block.
        @Test
        @DisplayName("a whole block must be a single batch that starts and ends the block")
        void testWholeBlockRejectsPartialBatches() {
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(2);
            assertThatIllegalArgumentException()
                    .isThrownBy(() -> toTest.processWholeBlock(headerOnly(block), BlockSource.BACKFILL));
            assertThatIllegalArgumentException()
                    .isThrownBy(() -> toTest.processWholeBlock(remainderEnding(block), BlockSource.BACKFILL));
            assertThat(executor.wasAnyTaskSubmitted()).isFalse();
            assertThat(buffer.size()).isZero();
        }

        /// This test aims to assert that when whole blocks waiting for an
        /// earlier block push the buffer over its limit, the one at the top of
        /// the waiting range is evicted, even though it is the newest, and
        /// reports a cancellation with the backfill source.
        @Test
        @DisplayName("whole blocks over the limit evict the one at the top of the waiting range")
        void testWholeBlocksOverLimitEvictTop() {
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(10, 12);
            for (final TestBlock block : blocks) {
                toTest.processWholeBlock(block.asBlockItems(), BlockSource.BACKFILL);
            }
            assertThat(buffer.size()).isEqualTo(BUFFER_LIMIT);
            assertThat(buffer.contains(new SessionKey(10, 0))).isTrue();
            assertThat(buffer.contains(new SessionKey(11, 1))).isTrue();
            assertThat(buffer.contains(new SessionKey(12, 2))).isFalse();
            assertThat(evictedCount(SessionPriority.LOW)).isEqualTo(1L);
            assertThat(blockMessaging.getSentVerificationNotifications())
                    .hasSize(1)
                    .first()
                    .returns(FailureInfo.standard(FailureType.CANCELLED), VerificationNotification::failureInfo)
                    .returns(12L, VerificationNotification::blockNumber)
                    .returns(BlockSource.BACKFILL, VerificationNotification::source);
        }
    }

    /// Tests for the live items entry point.
    @Nested
    @DisplayName("Live Flow Tests")
    class LiveFlowTests {
        /// This test aims to assert that a live block delivered in a single
        /// batch starts one session that is activated in the buffer, received
        /// its batch, and once it runs produces a result for its block and
        /// leaves the buffer on its own.
        @Test
        @DisplayName("a single batch live block starts one session that runs to a result and leaves the buffer")
        void testLiveSingleBatchStartsSessionThatRunsToResultAndLeaves() {
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(2);
            toTest.processLiveItems(block.asBlockItems());
            assertThat(buffer.contains(new SessionKey(2, 0))).isTrue();
            executor.executeSerially();
            assertThat(buffer.contains(new SessionKey(2, 0))).isFalse();
            assertThat(currentGaugeValue()).isZero();
            assertThat(blockMessaging.getSentVerificationNotifications())
                    .hasSize(1)
                    .first()
                    .returns(false, VerificationNotification::success)
                    .returns(2L, VerificationNotification::blockNumber)
                    .returns(BlockSource.PUBLISHER, VerificationNotification::source);
        }

        /// This test aims to assert that the batches of a live block delivered
        /// in several batches are all routed to the same session: the session
        /// only produces a result once the ending batch was delivered to it,
        /// and no further session is started for the block.
        @Test
        @DisplayName("a multi batch live block is routed to one session until its end")
        void testLiveMultiBatchIsRoutedToOneSessionUntilEnd() {
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(2);
            toTest.processLiveItems(headerOnly(block));
            toTest.processLiveItems(remainderEnding(block));
            assertThat(blocksReceived()).isEqualTo(1L);
            assertThat(buffer.size()).isEqualTo(1);
            executor.executeSerially();
            assertThat(buffer.size()).isZero();
            assertThat(blockMessaging.getSentVerificationNotifications())
                    .hasSize(1)
                    .first()
                    .returns(2L, VerificationNotification::blockNumber)
                    .returns(BlockSource.PUBLISHER, VerificationNotification::source);
        }

        /// This test aims to assert that once a live block has ended, further
        /// batches that do not start a block are disregarded: no session is
        /// started for them and nothing is delivered.
        @Test
        @DisplayName("batches after the end of a live block are disregarded")
        void testBatchesAfterEndOfLiveBlockAreDisregarded() {
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(2);
            toTest.processLiveItems(block.asBlockItems());
            toTest.processLiveItems(remainderEnding(block));
            assertThat(blocksReceived()).isEqualTo(1L);
            assertThat(buffer.size()).isEqualTo(1);
        }

        /// This test aims to assert that when the session receiving a live
        /// block produces its result before the block ended (here it fails at
        /// once because its first item is not a header), the handler drops its
        /// reference to it: the remaining batches of the block are disregarded
        /// instead of being delivered to a finished session, and no new session
        /// is started for them.
        @Test
        @DisplayName("a finished live session no longer receives batches")
        void testFinishedLiveSessionReferenceIsDropped() {
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(2);
            toTest.processLiveItems(startingWithoutHeader(block));
            assertThat(buffer.contains(new SessionKey(2, 0))).isTrue();
            executor.executeSerially();
            assertThat(buffer.contains(new SessionKey(2, 0))).isFalse();
            assertThat(blockMessaging.getSentVerificationNotifications())
                    .hasSize(1)
                    .first()
                    .returns(false, VerificationNotification::success)
                    .returns(2L, VerificationNotification::blockNumber);
            toTest.processLiveItems(remainderEnding(block));
            assertThat(blocksReceived()).isEqualTo(1L);
            assertThat(buffer.size()).isZero();
        }
    }

    /// Tests for a live block starting before the previous one ended.
    @Nested
    @DisplayName("Supersession Tests")
    class SupersessionTests {
        /// This test aims to assert that when a new live block starts before
        /// the previous one ended, the previous session is cancelled and has
        /// left the buffer before the new session is activated: it reports an
        /// incomplete cancellation, it is not counted as an eviction, and the
        /// buffer holds only the new session.
        @Test
        @DisplayName("a new live block cancels the incomplete previous session and removes it before activation")
        void testNewStartCancelsIncompleteSessionAndRemovesItBeforeActivation() {
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(2, 3);
            toTest.processLiveItems(headerOnly(blocks.get(0)));
            assertThat(buffer.contains(new SessionKey(2, 0))).isTrue();
            toTest.processLiveItems(blocks.get(1).asBlockItems());
            assertThat(buffer.contains(new SessionKey(2, 0))).isFalse();
            assertThat(buffer.contains(new SessionKey(3, 1))).isTrue();
            assertThat(buffer.size()).isEqualTo(1);
            assertThat(currentGaugeValue()).isEqualTo(1L);
            assertThat(evictedCount(SessionPriority.HIGH)).isZero();
            assertThat(blockMessaging.getSentVerificationNotifications())
                    .hasSize(1)
                    .first()
                    .returns(
                            FailureInfo.standard(FailureType.CANCELLED_INCOMPLETE),
                            VerificationNotification::failureInfo)
                    .returns(2L, VerificationNotification::blockNumber)
                    .returns(BlockSource.PUBLISHER, VerificationNotification::source);
        }

        /// This test aims to assert that a supersession never triggers an
        /// eviction round: with the buffer exactly at its limit, the new live
        /// block replaces the superseded session and the policy is not
        /// consulted, since the superseded session left before the activation.
        @Test
        @DisplayName("supersession replaces the superseded session without consulting the policy")
        void testSupersessionDoesNotTriggerEviction() {
            final AtomicReference<EvictionSnapshot> recorded = new AtomicReference<>();
            toTest = createHandler(snapshot -> {
                recorded.set(snapshot);
                return List.of();
            });
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(3, 4);
            final TestBlock farAhead = TestBlockBuilder.generateBlockWithNumber(10);
            toTest.processWholeBlock(farAhead.asBlockItems(), BlockSource.BACKFILL);
            toTest.processLiveItems(headerOnly(blocks.get(0)));
            toTest.processLiveItems(blocks.get(1).asBlockItems());
            assertThat(recorded.get()).isNull();
            assertThat(buffer.size()).isEqualTo(BUFFER_LIMIT);
            assertThat(buffer.contains(new SessionKey(10, 0))).isTrue();
            assertThat(buffer.contains(new SessionKey(3, 1))).isFalse();
            assertThat(buffer.contains(new SessionKey(4, 2))).isTrue();
        }

        /// This test aims to assert that a new live block starting after the
        /// previous session already produced its result does not cancel that
        /// session again: exactly one result is reported for the previous block.
        @Test
        @DisplayName("a new live block does not cancel a session that already finished")
        void testNewStartAfterFinishedSessionDoesNotCancelTwice() {
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(2, 3);
            toTest.processLiveItems(startingWithoutHeader(blocks.get(0)));
            executor.executeSerially();
            toTest.processLiveItems(blocks.get(1).asBlockItems());
            assertThat(blockMessaging.getSentVerificationNotifications())
                    .hasSize(1)
                    .first()
                    .returns(2L, VerificationNotification::blockNumber);
            assertThat(buffer.contains(new SessionKey(3, 1))).isTrue();
            assertThat(buffer.size()).isEqualTo(1);
        }
    }

    /// Tests for the priority recorded on started sessions, observed through the policy snapshot.
    @Nested
    @DisplayName("Session Priority Tests")
    class SessionPriorityTests {
        /// This test aims to assert that sessions started from live publisher items are high priority
        /// and carry the publisher source.
        @Test
        @DisplayName("live items start high priority publisher sessions")
        void testLiveItemsStartHighPrioritySessions() {
            final AtomicReference<EvictionSnapshot> recorded = new AtomicReference<>();
            toTest = createHandler(snapshot -> {
                recorded.set(snapshot);
                return List.of();
            });
            for (final TestBlock block : TestBlockBuilder.generateBlocksInRange(2, 4)) {
                toTest.processLiveItems(block.asBlockItems());
            }
            assertThat(recorded.get().sessions())
                    .extracting(s -> s.priority(), s -> s.source())
                    .containsOnly(tuple(SessionPriority.HIGH, BlockSource.PUBLISHER));
        }

        /// This test aims to assert that sessions started from whole blocks are low priority and carry
        /// the source they were delivered with.
        @Test
        @DisplayName("whole blocks start low priority sessions with their source")
        void testWholeBlocksStartLowPrioritySessions() {
            final AtomicReference<EvictionSnapshot> recorded = new AtomicReference<>();
            toTest = createHandler(snapshot -> {
                recorded.set(snapshot);
                return List.of();
            });
            for (final TestBlock block : TestBlockBuilder.generateBlocksInRange(2, 4)) {
                toTest.processWholeBlock(block.asBlockItems(), BlockSource.BACKFILL);
            }
            assertThat(recorded.get().sessions())
                    .extracting(s -> s.priority(), s -> s.source())
                    .containsOnly(tuple(SessionPriority.LOW, BlockSource.BACKFILL));
        }
    }

    /// Tests for the default eviction behaviour through the handler.
    @Nested
    @DisplayName("Default Eviction Tests")
    class DefaultEvictionTests {
        /// This test aims to assert that when the buffer is full and complete
        /// live blocks arrive in order, the newest block is the top of the
        /// waiting range and is evicted on its own activation, so the next
        /// expected block (the head of the chain) and the chain above it are
        /// kept and release the moment the head completes.
        @Test
        @DisplayName("in order arrival evicts the newest waiting session")
        void testInOrderArrivalEvictsNewestWaitingSession() {
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(2, 4);
            for (final TestBlock block : blocks) {
                toTest.processLiveItems(block.asBlockItems());
            }
            assertThat(buffer.size()).isEqualTo(BUFFER_LIMIT);
            assertThat(buffer.contains(new SessionKey(2, 0))).isTrue();
            assertThat(buffer.contains(new SessionKey(3, 1))).isTrue();
            assertThat(buffer.contains(new SessionKey(4, 2))).isFalse();
            assertThat(evictedCount(SessionPriority.HIGH)).isEqualTo(1L);
            assertThat(evictedCount(SessionPriority.LOW)).isZero();
            assertThat(blockMessaging.getSentVerificationNotifications())
                    .hasSize(1)
                    .first()
                    .returns(FailureInfo.standard(FailureType.CANCELLED), VerificationNotification::failureInfo)
                    .returns(4L, VerificationNotification::blockNumber);
        }

        /// This test aims to assert that when blocks arrive in reverse order the buffer never grows
        /// past its limit: the newest, lowest block is kept and the highest waiting session is
        /// evicted instead.
        @Test
        @DisplayName("reverse order arrival keeps the buffer at its limit by evicting the highest session")
        void testReverseOrderArrivalStaysWithinLimit() {
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(2, 4);
            toTest.processLiveItems(blocks.get(2).asBlockItems());
            toTest.processLiveItems(blocks.get(1).asBlockItems());
            toTest.processLiveItems(blocks.get(0).asBlockItems());
            assertThat(buffer.size()).isEqualTo(BUFFER_LIMIT);
            assertThat(buffer.contains(new SessionKey(3, 1))).isTrue();
            assertThat(buffer.contains(new SessionKey(2, 2))).isTrue();
            assertThat(buffer.contains(new SessionKey(4, 0))).isFalse();
        }

        /// This test aims to assert that a whole block session (low priority) at the top of the
        /// waiting range is evicted before any publisher session.
        @Test
        @DisplayName("whole block session at the top of the waiting range is evicted before publisher sessions")
        void testWholeBlockAtTopEvictedBeforePublisherSessions() {
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(3, 4);
            final TestBlock farAhead = TestBlockBuilder.generateBlockWithNumber(10);
            toTest.processWholeBlock(farAhead.asBlockItems(), BlockSource.BACKFILL);
            toTest.processLiveItems(blocks.get(0).asBlockItems());
            toTest.processLiveItems(blocks.get(1).asBlockItems());
            assertThat(buffer.size()).isEqualTo(BUFFER_LIMIT);
            assertThat(buffer.contains(new SessionKey(3, 1))).isTrue();
            assertThat(buffer.contains(new SessionKey(4, 2))).isTrue();
            assertThat(buffer.contains(new SessionKey(10, 0))).isFalse();
            assertThat(evictedCount(SessionPriority.LOW)).isEqualTo(1L);
            assertThat(evictedCount(SessionPriority.HIGH)).isZero();
        }

        /// This test aims to assert that a publisher session that has not yet received the end of its
        /// block is protected from eviction triggered by whole block activations, even when it is the
        /// top of the waiting range: cancelling it would report an incomplete cancellation the publisher
        /// does not act on. The highest whole block session below it is evicted instead.
        @Test
        @DisplayName("incomplete publisher session at the top is protected from whole block activations")
        void testIncompletePublisherSessionProtected() {
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(3, 5);
            toTest.processLiveItems(headerOnly(blocks.get(2)));
            toTest.processWholeBlock(blocks.get(0).asBlockItems(), BlockSource.BACKFILL);
            toTest.processWholeBlock(blocks.get(1).asBlockItems(), BlockSource.BACKFILL);
            assertThat(buffer.size()).isEqualTo(BUFFER_LIMIT);
            assertThat(buffer.contains(new SessionKey(5, 0))).isTrue();
            assertThat(buffer.contains(new SessionKey(3, 1))).isTrue();
            assertThat(buffer.contains(new SessionKey(4, 2))).isFalse();
            assertThat(evictedCount(SessionPriority.LOW)).isEqualTo(1L);
            assertThat(evictedCount(SessionPriority.HIGH)).isZero();
            assertThat(blockMessaging.getSentVerificationNotifications())
                    .hasSize(1)
                    .first()
                    .returns(FailureInfo.standard(FailureType.CANCELLED), VerificationNotification::failureInfo)
                    .returns(4L, VerificationNotification::blockNumber)
                    .returns(BlockSource.BACKFILL, VerificationNotification::source);
        }

        /// This test aims to assert that sessions which will complete on their own are never evicted
        /// and the buffer is allowed to transiently overshoot its limit: whole blocks below the next
        /// expected block are not waiting, so with only such sessions present nothing is cancelled.
        @Test
        @DisplayName("buffer overshoots transiently when only non waiting sessions are present")
        void testOvershootWithNonWaitingSessions() {
            lastVerifiedBlock.set(100);
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(5, 7);
            for (final TestBlock block : blocks) {
                toTest.processWholeBlock(block.asBlockItems(), BlockSource.BACKFILL);
            }
            assertThat(buffer.size()).isEqualTo(3);
            assertThat(evictedCount(SessionPriority.LOW)).isZero();
            assertThat(currentGaugeValue()).isEqualTo(3L);
            assertThat(blockMessaging.getSentVerificationNotifications()).isEmpty();
        }
    }

    /// Tests for the contract between the handler, the buffer and a policy, using a recording test policy.
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
        /// the limit, the ordering settings, and no protected key when every session received its
        /// complete block, the just activated one included.
        @Test
        @DisplayName("policy receives a faithful snapshot without protecting complete sessions")
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
            assertThat(snapshot.protectedKeys()).isEmpty();
            assertThat(snapshot.sessions())
                    .extracting(s -> s.key(), s -> s.priority(), s -> s.source())
                    .containsExactly(
                            tuple(new SessionKey(3, 0), SessionPriority.HIGH, BlockSource.PUBLISHER),
                            tuple(new SessionKey(4, 1), SessionPriority.LOW, BlockSource.BACKFILL),
                            tuple(new SessionKey(5, 2), SessionPriority.HIGH, BlockSource.PUBLISHER));
        }

        /// This test aims to assert that the snapshot marks the publisher session still receiving
        /// its block as protected, and nothing else.
        @Test
        @DisplayName("policy receives the incomplete publisher session as protected")
        void testPolicyReceivesIncompletePublisherSessionAsProtected() {
            final AtomicReference<EvictionSnapshot> recorded = new AtomicReference<>();
            toTest = createHandler(snapshot -> {
                recorded.set(snapshot);
                return List.of();
            });
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(3, 5);
            toTest.processLiveItems(headerOnly(blocks.get(0)));
            toTest.processWholeBlock(blocks.get(1).asBlockItems(), BlockSource.BACKFILL);
            toTest.processWholeBlock(blocks.get(2).asBlockItems(), BlockSource.BACKFILL);
            assertThat(recorded.get().protectedKeys()).containsExactly(new SessionKey(3, 0));
        }

        /// This test aims to assert that the buffer evicts exactly what the policy selects, in order,
        /// and counts each eviction under the priority of the evicted session. A naive policy that
        /// always selects the lowest key is used, so the outcome differs visibly from the default.
        @Test
        @DisplayName("buffer evicts exactly what the policy selects")
        void testBufferEvictsPolicySelection() {
            toTest = createHandler(
                    snapshot -> List.of(snapshot.sessions().getFirst().key()));
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(2, 4);
            toTest.processWholeBlock(blocks.get(0).asBlockItems(), BlockSource.BACKFILL);
            toTest.processLiveItems(blocks.get(1).asBlockItems());
            toTest.processLiveItems(blocks.get(2).asBlockItems());
            assertThat(buffer.size()).isEqualTo(BUFFER_LIMIT);
            assertThat(buffer.contains(new SessionKey(3, 1))).isTrue();
            assertThat(buffer.contains(new SessionKey(4, 2))).isTrue();
            assertThat(buffer.contains(new SessionKey(2, 0))).isFalse();
            assertThat(evictedCount(SessionPriority.LOW)).isEqualTo(1L);
            assertThat(evictedCount(SessionPriority.HIGH)).isZero();
        }

        /// This test aims to assert that a victim the policy selects which is not in the buffer
        /// is skipped without error and without counting an eviction.
        @Test
        @DisplayName("victims not in the buffer are skipped")
        void testUnknownVictimSkipped() {
            toTest = createHandler(snapshot -> List.of(new SessionKey(999, 999)));
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(2, 4);
            for (final TestBlock block : blocks) {
                toTest.processLiveItems(block.asBlockItems());
            }
            assertThat(buffer.size()).isEqualTo(3);
            assertThat(evictedCount(SessionPriority.HIGH)).isZero();
            assertThat(evictedCount(SessionPriority.LOW)).isZero();
        }

        /// This test aims to assert that the publisher session still receiving its block survives
        /// even a policy that selects it: it is kept, its ending batch is still delivered, and once
        /// it runs it produces a result for its block rather than a cancellation.
        @Test
        @DisplayName("the incomplete publisher session survives a policy selecting it")
        void testProtectedPublisherSessionSurvivesNaivePolicy() {
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(2, 4);
            final TestBlock incomplete = blocks.get(0);
            toTest = createHandler(snapshot -> List.of(new SessionKey(incomplete.number(), 0)));
            toTest.processLiveItems(headerOnly(incomplete));
            toTest.processWholeBlock(blocks.get(1).asBlockItems(), BlockSource.BACKFILL);
            toTest.processWholeBlock(blocks.get(2).asBlockItems(), BlockSource.BACKFILL);
            assertThat(buffer.contains(new SessionKey(2, 0))).isTrue();
            assertThat(buffer.size()).isEqualTo(3);
            assertThat(evictedCount(SessionPriority.HIGH)).isZero();
            toTest.processLiveItems(remainderEnding(incomplete));
            executor.executeSerially();
            assertThat(buffer.size()).isZero();
            assertThat(blockMessaging.getSentVerificationNotifications())
                    .filteredOn(notification -> notification.blockNumber() == 2L)
                    .hasSize(1)
                    .first()
                    .returns(false, VerificationNotification::success)
                    .extracting(notification -> notification.failureInfo().failureType())
                    .isNotIn(FailureType.CANCELLED, FailureType.CANCELLED_INCOMPLETE);
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
            for (final TestBlock block : blocks) {
                toTest.processLiveItems(block.asBlockItems());
            }
            assertThat(currentGaugeValue()).isEqualTo(BUFFER_LIMIT).isEqualTo(buffer.size());
        }

        /// This test aims to assert that the active sessions gauge stays at the buffer
        /// limit when blocks arrive in reverse order, since the newest lowest block is
        /// kept and the highest one is evicted.
        @Test
        @DisplayName("gauge stays at buffer size when blocks arrive in reverse order")
        void testGaugeStaysAtBufferSizeInReverseOrder() {
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(2, 4);
            toTest.processLiveItems(blocks.get(2).asBlockItems());
            toTest.processLiveItems(blocks.get(1).asBlockItems());
            toTest.processLiveItems(blocks.get(0).asBlockItems());
            assertThat(currentGaugeValue()).isEqualTo(BUFFER_LIMIT).isEqualTo(buffer.size());
        }

        /// This test aims to assert that the gauge drops as soon as a session produces its result
        /// and leaves the buffer on its own, without waiting for new input.
        @Test
        @DisplayName("gauge drops when a session finishes")
        void testGaugeDropsWhenSessionFinishes() {
            toTest.processLiveItems(TestBlockBuilder.generateBlockWithNumber(2).asBlockItems());
            assertThat(currentGaugeValue()).isEqualTo(1L);
            executor.executeSerially();
            assertThat(currentGaugeValue()).isZero();
        }

        /// This test aims to assert that the gauge reflects a supersession at once: the superseded
        /// session leaves and the new one enters, so the gauge stays at one.
        @Test
        @DisplayName("gauge reflects a supersession at once")
        void testGaugeReflectsSupersession() {
            final List<TestBlock> blocks = TestBlockBuilder.generateBlocksInRange(2, 3);
            toTest.processLiveItems(headerOnly(blocks.get(0)));
            assertThat(currentGaugeValue()).isEqualTo(1L);
            toTest.processLiveItems(blocks.get(1).asBlockItems());
            assertThat(currentGaugeValue()).isEqualTo(1L).isEqualTo(buffer.size());
        }
    }
}
