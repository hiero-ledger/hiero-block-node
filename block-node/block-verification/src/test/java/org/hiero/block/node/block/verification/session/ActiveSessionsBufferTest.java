// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.block.node.app.fixtures.TestConfigurationBuilder;
import org.hiero.block.node.app.fixtures.TestUtils;
import org.hiero.block.node.block.verification.VerificationConfig;
import org.hiero.block.node.block.verification.metrics.MetricsHolder;
import org.hiero.block.node.block.verification.session.BlockVerificationSession.SessionKey;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/// Tests for the [ActiveSessionsBuffer].
/// The buffer is exercised with stand-in sessions that run nothing and never touch
/// the buffer: a test decides when a stand-in is complete, when it has received the
/// end of its block, and when the buffer is cleared. The buffer applies the
/// production eviction policy, observed through a recording wrapper. Nothing is ever
/// verified, so the last verified block stays where a test puts it, -1 unless a test
/// sets it. While it is -1, no session waits for order, as before the first success
/// in production, so no session is stuck and the policy selects backup candidates.
@DisplayName("Active Sessions Buffer Tests")
class ActiveSessionsBufferTest {
    /// The buffer limit of every test, the smallest the configuration accepts.
    private static final int BUFFER_SIZE = 13;
    /// The metrics the buffer under test records.
    private MetricsHolder metrics;
    /// The last verified block handed to the buffer under test.
    private AtomicLong lastVerifiedBlock;
    /// The policy of the buffer under test: the production policy, wrapped to record
    /// every snapshot it is handed and every key it selects.
    private RecordingEvictionPolicy policy;
    /// The instance under test.
    private ActiveSessionsBuffer toTest;
    /// The source of unique ids for stand-in sessions.
    private long nextUniqueId;

    /// Setup before each test.
    @BeforeEach
    void setUp() {
        metrics = MetricsHolder.create(TestUtils.createMetrics());
        lastVerifiedBlock = new AtomicLong(-1);
        policy = RecordingEvictionPolicy.recording(productionPolicy());
        nextUniqueId = 0;
        toTest = newBuffer();
    }

    /// Tests for admitting sessions to their lanes.
    @Nested
    @DisplayName("Admission Tests")
    class AdmissionTests {
        /// This test aims to assert that a publisher session is admitted to the high
        /// priority lane and becomes the active publisher session, and that the gauge
        /// follows.
        @Test
        @DisplayName("admit() puts a publisher session in the high priority lane as the active publisher session")
        void testAdmitsAPublisherSessionToTheHighPriorityLaneAsTheActivePublisherSession() {
            final StandInSession session = publisherSession(5);
            toTest.admit(session);
            assertThat(toTest.snapshot().highPrioritySessions()).containsExactly(session.sessionKey());
            assertThat(toTest.snapshot().lowPrioritySessions()).isEmpty();
            assertThat(toTest.activePublisherSession()).isSameAs(session);
            assertThat(gauge()).isEqualTo(1L);
        }

        /// This test aims to assert that a session of any source other than the
        /// publisher is admitted to the low priority lane and never becomes the active
        /// publisher session. Every such source is exercised.
        @ParameterizedTest
        @EnumSource(
                value = BlockSource.class,
                names = {"PUBLISHER"},
                mode = EnumSource.Mode.EXCLUDE)
        @DisplayName("admit() puts a session of any other source in the low priority lane")
        void testAdmitsASessionOfAnyOtherSourceToTheLowPriorityLane(final BlockSource source) {
            final StandInSession session = newSession(5, source);
            toTest.admit(session);
            assertThat(toTest.snapshot().lowPrioritySessions()).containsExactly(session.sessionKey());
            assertThat(toTest.snapshot().highPrioritySessions()).isEmpty();
            assertThat(toTest.activePublisherSession()).isNull();
        }

        /// This test aims to assert that admitting a publisher session makes it the active
        /// publisher session in place of the previous one, and that the buffer leaves the
        /// previous one alone: cancelling it is up to the session handler, so it is not
        /// cancelled and stays in its lane.
        @Test
        @DisplayName("admit() makes a new publisher session the active one without cancelling the previous")
        void testANewPublisherSessionBecomesTheActiveOneWithoutCancellingThePrevious() {
            final StandInSession first = publisherSession(5);
            final StandInSession second = publisherSession(6);
            toTest.admit(first);
            toTest.admit(second);
            assertThat(toTest.activePublisherSession()).isSameAs(second);
            assertThat(first.cancelCalls()).isZero();
            assertThat(toTest.snapshot().highPrioritySessions())
                    .containsExactly(first.sessionKey(), second.sessionKey());
        }

        /// This test aims to assert that an admission into a full buffer makes room before
        /// it adds its session, so the policy never sees the session being admitted and
        /// never evicts it: the backfilled sessions 1 to 13 fill the buffer, and when the
        /// publisher session 20 is admitted, the snapshot handed to the policy holds only
        /// the backfilled sessions, no active publisher session yet. No block is verified,
        /// so no session waits for order, and the policy selects the highest backfilled
        /// session, 13, as the first backup candidate, although 20 is higher. 20 is then
        /// added as the active publisher session.
        @Test
        @DisplayName("admit() makes room before adding a session, so the policy never sees the session being admitted")
        void testMakesRoomBeforeAddingASessionSoThePolicyNeverSeesTheSessionBeingAdmitted() {
            final List<StandInSession> backfilled = admitRun(1, 13, BlockSource.BACKFILL);
            final StandInSession published = publisherSession(20);
            toTest.admit(published);
            assertThat(policy.snapshots()).hasSize(1);
            final ActiveSessionsSnapshot snapshot = policy.snapshots().getFirst();
            assertThat(snapshot.highPrioritySessions()).isEmpty();
            assertThat(snapshot.lowPrioritySessions())
                    .extracting(SessionKey::blockNumber)
                    .containsExactlyElementsOf(blocks(1, 13));
            assertThat(snapshot.activeHighPrioritySession()).isNull();
            assertThat(policy.selections()).containsExactly(backfilled.getLast().sessionKey());
            assertThat(published.cancelCalls()).isZero();
            assertThat(toTest.snapshot().highPrioritySessions()).containsExactly(published.sessionKey());
            assertThat(toTest.activePublisherSession()).isSameAs(published);
            assertThat(gauge()).isEqualTo(13L);
        }
    }

    /// Tests for the active publisher session.
    @Nested
    @DisplayName("Active Publisher Session Tests")
    class ActivePublisherSessionTests {
        /// This test aims to assert that the latest publisher session stops being active
        /// once it has received the end of its block, while it stays in its lane, and
        /// that the policy may select it from then on.
        @Test
        @DisplayName("activePublisherSession() is empty once the session received the end of its block")
        void testPublisherSessionIsNotActiveOnceItReceivedTheEndOfItsBlock() {
            final StandInSession session = publisherSession(5);
            toTest.admit(session);
            session.markEndOfBlockReceived();
            assertThat(toTest.activePublisherSession()).isNull();
            assertThat(toTest.snapshot().activeHighPrioritySession()).isNull();
            assertThat(toTest.snapshot().highPrioritySessions()).containsExactly(session.sessionKey());
        }

        /// This test aims to assert that a publisher session that completed on its own,
        /// for example because its block failed verification, stays the active publisher
        /// session until the next admission clears it: once the backfilled block 3 is
        /// admitted, the complete session has left its lane and there is no active
        /// publisher session any more.
        @Test
        @DisplayName("activePublisherSession() is empty once the next admission clears the session")
        void testPublisherSessionIsNotActiveOnceTheNextAdmissionClearsIt() {
            final StandInSession session = publisherSession(5);
            toTest.admit(session);
            session.completeOnItsOwn();
            assertThat(toTest.activePublisherSession()).isSameAs(session);
            toTest.admit(newSession(3, BlockSource.BACKFILL));
            assertThat(toTest.activePublisherSession()).isNull();
            assertThat(toTest.snapshot().highPrioritySessions()).isEmpty();
        }
    }

    /// Tests for clearing the sessions that are complete.
    @Nested
    @DisplayName("Clearing Tests")
    class ClearingTests {
        /// This test aims to assert that a complete session stays in the buffer until the
        /// next admission clears it: the buffer never removes a session on its own.
        @Test
        @DisplayName("a complete session stays in the buffer until the next admission")
        void testACompleteSessionStaysUntilTheNextAdmission() {
            final StandInSession session = newSession(3, BlockSource.BACKFILL);
            toTest.admit(session);
            session.completeOnItsOwn();
            assertThat(toTest.snapshot().lowPrioritySessions()).containsExactly(session.sessionKey());
            assertThat(gauge()).isEqualTo(1L);
        }

        /// This test aims to assert that an admission first clears every complete session,
        /// in either lane, keeps the sessions that are still running, then adds its own
        /// session, and that the gauge follows: the publisher session 5 and the backfilled
        /// session 3 complete, and the admission of the backfilled session 4 clears both,
        /// keeping the running publisher session 6.
        @Test
        @DisplayName("admit() clears the complete sessions of both lanes first")
        void testAdmissionClearsTheCompleteSessionsOfBothLanesFirst() {
            final StandInSession published = publisherSession(5);
            final StandInSession running = publisherSession(6);
            final StandInSession backfilled = newSession(3, BlockSource.BACKFILL);
            toTest.admit(published);
            toTest.admit(running);
            toTest.admit(backfilled);
            published.completeOnItsOwn();
            backfilled.completeOnItsOwn();
            final StandInSession admitted = newSession(4, BlockSource.BACKFILL);
            toTest.admit(admitted);
            assertThat(toTest.snapshot().highPrioritySessions()).containsExactly(running.sessionKey());
            assertThat(toTest.snapshot().lowPrioritySessions()).containsExactly(admitted.sessionKey());
            assertThat(gauge()).isEqualTo(2L);
        }

        /// This test aims to assert that a session that completed before it was admitted
        /// is added like any other and cleared by the next admission, so the order of
        /// completing and admitting can never leave a session in the buffer for good.
        @Test
        @DisplayName("admit() clears a session that completed before it was admitted on the next admission")
        void testAdmissionClearsASessionThatCompletedBeforeItWasAdmitted() {
            final StandInSession session = newSession(3, BlockSource.BACKFILL);
            session.completeOnItsOwn();
            toTest.admit(session);
            assertThat(toTest.size()).isEqualTo(1);
            final StandInSession next = newSession(4, BlockSource.BACKFILL);
            toTest.admit(next);
            assertThat(toTest.snapshot().lowPrioritySessions()).containsExactly(next.sessionKey());
            assertThat(gauge()).isEqualTo(1L);
        }
    }

    /// Tests for the eviction of the sessions the policy selects.
    @Nested
    @DisplayName("Eviction Tests")
    class EvictionTests {
        /// This test aims to assert that the session the policy selects is removed from its
        /// lane, cancelled once and counted in the evictions counter of its lane when its
        /// cancellation stopped it, and that the session being admitted is added after it:
        /// the backfilled sessions 2 to 14 fill the buffer, no block is verified, so no
        /// session waits for order, and the admission of 15 evicts the highest backfilled
        /// session, 14, as the first backup candidate, then adds 15, which is never
        /// cancelled although it is higher.
        @Test
        @DisplayName("admit() evicts, cancels and counts what the policy selects, then adds the new session")
        void testEvictsCancelsAndCountsTheSelectedSession() {
            final List<StandInSession> admitted = admitRun(2, 14, BlockSource.BACKFILL);
            final StandInSession newcomer = newSession(15, BlockSource.BACKFILL);
            toTest.admit(newcomer);
            final StandInSession highestHeld = admitted.getLast();
            assertThat(policy.selections()).containsExactly(highestHeld.sessionKey());
            assertThat(highestHeld.cancelCalls()).isEqualTo(1);
            assertThat(newcomer.cancelCalls()).isZero();
            final List<Long> kept = new ArrayList<>(blocks(2, 13));
            kept.add(15L);
            assertThat(toTest.snapshot().lowPrioritySessions())
                    .extracting(SessionKey::blockNumber)
                    .containsExactlyElementsOf(kept);
            assertThat(evictedLowPriority()).isEqualTo(1L);
            assertThat(gauge()).isEqualTo(13L);
        }

        /// This test aims to assert that complete sessions are cleared before the policy
        /// is consulted: the session for block 2 of the full buffer completes, and when the
        /// admission of 15 finds the buffer full, clearing it first makes room, so nothing
        /// is evicted and the policy is not consulted.
        @Test
        @DisplayName("admit() clears complete sessions before consulting the policy")
        void testClearsCompleteSessionsBeforeConsultingThePolicy() {
            final List<StandInSession> admitted = admitRun(2, 14, BlockSource.BACKFILL);
            admitted.getFirst().completeOnItsOwn();
            toTest.admit(newSession(15, BlockSource.BACKFILL));
            assertThat(policy.snapshots()).isEmpty();
            assertThat(admitted)
                    .allSatisfy(session -> assertThat(session.cancelCalls()).isZero());
            assertThat(toTest.snapshot().lowPrioritySessions())
                    .extracting(SessionKey::blockNumber)
                    .containsExactlyElementsOf(blocks(3, 15));
            assertThat(evictedLowPriority()).isZero();
        }

        /// This test aims to assert that a selected session whose result was already
        /// produced when it was cancelled (its cancellation returns false while its result
        /// handling is still running) is removed but not counted as evicted: the session
        /// for block 20 has produced its result, and when the admission of 14 finds the
        /// buffer full, the policy selects 20, the highest backfilled session.
        @Test
        @DisplayName("admit() does not count a selected session that had already produced its result")
        void testDoesNotCountASelectedSessionThatHadAlreadyProducedItsResult() {
            final StandInSession highest = newSession(20, BlockSource.BACKFILL);
            toTest.admit(highest);
            highest.produceResultWhileItIsStillBeingHandled();
            admitRun(2, 14, BlockSource.BACKFILL);
            assertThat(policy.selections()).containsExactly(highest.sessionKey());
            assertThat(highest.cancelCalls()).isEqualTo(1);
            assertThat(toTest.snapshot().lowPrioritySessions())
                    .extracting(SessionKey::blockNumber)
                    .containsExactlyElementsOf(blocks(2, 14));
            assertThat(evictedLowPriority()).isZero();
        }

        /// This test aims to assert that every admission into a full buffer evicts one of
        /// the sessions already held, even when no session is stuck, so the count never
        /// exceeds the limit: with the last verified block 1, the backfilled sessions 2 to
        /// 14 form a run with it and fill the buffer. The admission of 15 finds no session
        /// stuck and evicts the highest held backfilled session, 14, which leaves 15 stuck
        /// above the gap at 14 once it is added, so the admission of 16 evicts 15. Each is
        /// cancelled and counted, no session is evicted by its own admission, and the gauge
        /// reports the limit.
        @Test
        @DisplayName(
                "admit() evicts one held session on every admission into a full buffer, even when no session is stuck")
        void testEvictsOneHeldSessionOnEveryAdmissionIntoAFullBufferEvenWhenNoSessionIsStuck() {
            lastVerifiedBlock.set(1L);
            final List<StandInSession> admitted = admitRun(2, 16, BlockSource.BACKFILL);
            assertThat(policy.snapshots()).hasSize(2);
            assertThat(policy.selections())
                    .containsExactly(
                            admitted.get(12).sessionKey(), admitted.get(13).sessionKey());
            assertThat(admitted.get(12).cancelCalls()).isEqualTo(1);
            assertThat(admitted.get(13).cancelCalls()).isEqualTo(1);
            assertThat(admitted.get(14).cancelCalls()).isZero();
            final List<Long> kept = new ArrayList<>(blocks(2, 13));
            kept.add(16L);
            assertThat(toTest.snapshot().lowPrioritySessions())
                    .extracting(SessionKey::blockNumber)
                    .containsExactlyElementsOf(kept);
            assertThat(evictedLowPriority()).isEqualTo(2L);
            assertThat(gauge()).isEqualTo(13L);
        }

        /// This test aims to assert that when the lanes hold publisher sessions only and
        /// none is stuck, the publisher session at the backup position is evicted and
        /// counted in the high priority lane, never the session being admitted: with the
        /// last verified block 100, the complete publisher blocks 101 to 113 fill the
        /// buffer and the publisher starts block 114, which is added once room is made, so
        /// the third held session from the top, 111, is evicted.
        @Test
        @DisplayName(
                "admit() evicts the publisher session at the backup position when the lanes hold publisher sessions only")
        void testEvictsThePublisherSessionAtTheBackupPositionWhenTheLanesHoldPublisherSessionsOnly() {
            lastVerifiedBlock.set(100L);
            final List<StandInSession> admitted = admitPublisherRun(101, 113);
            final StandInSession streaming = publisherSession(114);
            toTest.admit(streaming);
            final StandInSession atBackupPosition = admitted.get(10);
            assertThat(policy.selections()).containsExactly(atBackupPosition.sessionKey());
            assertThat(atBackupPosition.cancelCalls()).isEqualTo(1);
            assertThat(streaming.cancelCalls()).isZero();
            assertThat(toTest.snapshot().highPrioritySessions())
                    .extracting(SessionKey::blockNumber)
                    .doesNotContain(111L)
                    .hasSize(13)
                    .contains(114L);
            assertThat(toTest.activePublisherSession()).isSameAs(streaming);
            assertThat(evictedHighPriority()).isEqualTo(1L);
            assertThat(gauge()).isEqualTo(13L);
        }
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

    /// The production eviction policy, built from the configuration of every test.
    private static OrderAwareEvictionPolicy productionPolicy() {
        return new OrderAwareEvictionPolicy(config());
    }

    /// Build a buffer with the recording policy and the configuration of every test.
    private ActiveSessionsBuffer newBuffer() {
        return new ActiveSessionsBuffer(config(), policy, lastVerifiedBlock, metrics.sessionHandlerMetrics());
    }

    /// Admit a stand-in session of the given source for each block from the first block
    /// to the last, in ascending order.
    ///
    /// @return the admitted sessions, in order
    private List<StandInSession> admitRun(final long firstBlock, final long lastBlock, final BlockSource source) {
        final List<StandInSession> admitted = new ArrayList<>();
        for (long blockNumber = firstBlock; blockNumber <= lastBlock; blockNumber++) {
            final StandInSession session = newSession(blockNumber, source);
            toTest.admit(session);
            admitted.add(session);
        }
        return admitted;
    }

    /// Admit a stand-in publisher session for each block from the first block to the
    /// last, in ascending order, each receiving the end of its block once admitted, as
    /// a block that comes in one batch does.
    ///
    /// @return the admitted sessions, in order
    private List<StandInSession> admitPublisherRun(final long firstBlock, final long lastBlock) {
        final List<StandInSession> admitted = new ArrayList<>();
        for (long blockNumber = firstBlock; blockNumber <= lastBlock; blockNumber++) {
            final StandInSession session = publisherSession(blockNumber);
            toTest.admit(session);
            session.markEndOfBlockReceived();
            admitted.add(session);
        }
        return admitted;
    }

    /// The block numbers from the first block to the last, ascending.
    private static List<Long> blocks(final long firstBlock, final long lastBlock) {
        final List<Long> blocks = new ArrayList<>();
        for (long blockNumber = firstBlock; blockNumber <= lastBlock; blockNumber++) {
            blocks.add(blockNumber);
        }
        return blocks;
    }

    /// A stand-in session for a publisher block.
    private StandInSession publisherSession(final long blockNumber) {
        return newSession(blockNumber, BlockSource.PUBLISHER);
    }

    /// A stand-in session for a block of the given source.
    private StandInSession newSession(final long blockNumber, final BlockSource source) {
        return new StandInSession(new SessionKey(blockNumber, nextUniqueId++), source);
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

    /// A session that runs nothing and never touches the buffer. Cancelling it while it
    /// is running makes it complete, as the result handling of a real session does on
    /// the cancelling thread; a test can also make it complete on its own, or produce
    /// its result while its result handling is still running.
    private static final class StandInSession implements BlockVerificationSession {
        /// The key of the session.
        private final SessionKey sessionKey;
        /// The source of the session's block.
        private final BlockSource blockSource;
        /// Raised once the session has produced its result; a cancellation after that stops nothing.
        private final AtomicBoolean resultProduced;
        /// Raised once the session's result has been handled.
        private final AtomicBoolean complete;
        /// Raised once the end of the session's block has been marked.
        private final AtomicBoolean endOfBlockReceived;
        /// The number of calls to [#cancel()].
        private final AtomicInteger cancelCalls;

        /// Constructor.
        private StandInSession(final SessionKey sessionKey, final BlockSource blockSource) {
            this.sessionKey = sessionKey;
            this.blockSource = blockSource;
            this.resultProduced = new AtomicBoolean(false);
            this.complete = new AtomicBoolean(false);
            this.endOfBlockReceived = new AtomicBoolean(false);
            this.cancelCalls = new AtomicInteger(0);
        }

        /// {@inheritDoc}
        @Override
        public SessionKey sessionKey() {
            return sessionKey;
        }

        /// {@inheritDoc}
        @Override
        public BlockSource blockSource() {
            return blockSource;
        }

        /// {@inheritDoc}
        @Override
        public void start() {
            // nothing runs
        }

        /// {@inheritDoc}
        /// ---
        /// Stops a running stand-in, which makes it complete.
        @Override
        public boolean cancel() {
            cancelCalls.incrementAndGet();
            final boolean stopped = resultProduced.compareAndSet(false, true);
            if (stopped) {
                complete.set(true);
            }
            return stopped;
        }

        /// {@inheritDoc}
        @Override
        public void markEndOfBlockReceived() {
            endOfBlockReceived.set(true);
        }

        /// {@inheritDoc}
        @Override
        public boolean isEndOfBlockReceived() {
            return endOfBlockReceived.get();
        }

        /// {@inheritDoc}
        @Override
        public boolean isComplete() {
            return complete.get();
        }

        /// {@inheritDoc}
        @Override
        public ConcurrentLinkedDeque<BlockItems> getBlockItemsDeque() {
            return new ConcurrentLinkedDeque<>();
        }

        /// Produce a result and handle it, as a session whose verification ended on its
        /// own does.
        private void completeOnItsOwn() {
            resultProduced.set(true);
            complete.set(true);
        }

        /// Produce a result while its result handling is still running: a cancellation
        /// stops nothing any more, and the session is not complete yet.
        private void produceResultWhileItIsStillBeingHandled() {
            resultProduced.set(true);
        }

        /// The number of calls to [#cancel()] so far.
        private int cancelCalls() {
            return cancelCalls.get();
        }
    }
}
