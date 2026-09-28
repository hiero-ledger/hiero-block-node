// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import org.hiero.block.node.app.fixtures.TestConfigurationBuilder;
import org.hiero.block.node.app.fixtures.TestUtils;
import org.hiero.block.node.block.verification.VerificationConfig;
import org.hiero.block.node.block.verification.metrics.SessionHandlerMetrics;
import org.hiero.block.node.block.verification.session.BlockVerificationSession.SessionKey;
import org.hiero.block.node.block.verification.session.eviction.EvictionPolicy;
import org.hiero.block.node.block.verification.session.eviction.EvictionSnapshot;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/// Tests for the [ActiveSessionsBuffer].
///
/// The buffer is exercised with hand written fake sessions that never run
/// anything, so every test states exactly which sessions are in the buffer,
/// what the policy selects, and asserts the buffer's reaction: what is
/// removed, what is cancelled, what is counted. The last verified block is
/// fixed at 100, the limit at two.
@DisplayName("Active Sessions Buffer Tests")
class ActiveSessionsBufferTest {
    /// The buffer limit configured for every test.
    private static final int LIMIT = 2;
    /// The last verified block shared with the buffer.
    private AtomicLong lastVerifiedBlock;
    /// The metrics the buffer records into.
    private SessionHandlerMetrics metrics;
    /// The configuration for verification with the test limit.
    private VerificationConfig verificationConfig;

    /// Setup before each test.
    @BeforeEach
    void setUp() {
        lastVerifiedBlock = new AtomicLong(100);
        metrics = SessionHandlerMetrics.create(TestUtils.createMetrics());
        verificationConfig = new TestConfigurationBuilder()
                .withConfigDataType(VerificationConfig.class)
                .withValue("verification.activeSessionsBufferSize", Integer.toString(LIMIT))
                .getOrCreateConfig()
                .getConfigData(VerificationConfig.class);
    }

    /// Create a buffer with the given policy.
    private ActiveSessionsBuffer buffer(final EvictionPolicy policy) {
        return new ActiveSessionsBuffer(verificationConfig, lastVerifiedBlock, policy, metrics);
    }

    /// Reads the current value of the active sessions gauge.
    private long gauge() {
        return metrics.verificationActiveSessions().get();
    }

    /// Reads the current value of the evicted sessions counter for a priority.
    private long evicted(final SessionPriority priority) {
        return metrics.verificationSessionsEvicted(priority).get();
    }

    /// A policy that records every snapshot it receives and returns a fixed selection.
    private static final class RecordingPolicy implements EvictionPolicy {
        /// The snapshots received, in order.
        private final List<EvictionSnapshot> snapshots = new ArrayList<>();
        /// The keys to return on every call.
        private final List<SessionKey> selection;

        private RecordingPolicy(final SessionKey... selection) {
            this.selection = List.of(selection);
        }

        @Override
        public List<SessionKey> selectVictims(final EvictionSnapshot snapshot) {
            snapshots.add(snapshot);
            return selection;
        }
    }

    /// A session that never runs. Its cancellation result is scripted, and the
    /// order in which fake sessions are cancelled is recorded through a shared
    /// sequence so tests can assert the eviction order.
    private static final class FakeSession implements BlockVerificationSession {
        /// The shared sequence of cancellations across all fake sessions.
        private static final AtomicInteger CANCEL_SEQUENCE = new AtomicInteger();
        private final SessionKey key;
        private final SessionPriority priority;
        private final BlockSource source;
        private final boolean cancelResult;
        private final ConcurrentLinkedDeque<BlockItems> deque = new ConcurrentLinkedDeque<>();
        private boolean endOfBlockReceived;
        private volatile boolean finished;
        private int cancelCount;
        private int cancelOrder = -1;

        private FakeSession(
                final SessionKey key,
                final SessionPriority priority,
                final BlockSource source,
                final boolean endOfBlockReceived,
                final boolean cancelResult) {
            this.key = key;
            this.priority = priority;
            this.source = source;
            this.endOfBlockReceived = endOfBlockReceived;
            this.cancelResult = cancelResult;
        }

        /// A complete high priority publisher session that cancels successfully.
        private static FakeSession high(final long blockNumber, final long uniqueId) {
            return new FakeSession(
                    new SessionKey(blockNumber, uniqueId), SessionPriority.HIGH, BlockSource.PUBLISHER, true, true);
        }

        /// A complete low priority backfill session that cancels successfully.
        private static FakeSession low(final long blockNumber, final long uniqueId) {
            return new FakeSession(
                    new SessionKey(blockNumber, uniqueId), SessionPriority.LOW, BlockSource.BACKFILL, true, true);
        }

        /// A high priority publisher session still receiving its block.
        private static FakeSession incompleteHigh(final long blockNumber, final long uniqueId) {
            return new FakeSession(
                    new SessionKey(blockNumber, uniqueId), SessionPriority.HIGH, BlockSource.PUBLISHER, false, true);
        }

        /// A low priority backfill session that has not received its end of block.
        private static FakeSession incompleteLow(final long blockNumber, final long uniqueId) {
            return new FakeSession(
                    new SessionKey(blockNumber, uniqueId), SessionPriority.LOW, BlockSource.BACKFILL, false, true);
        }

        /// A complete high priority session whose cancellation reports that it already produced its result.
        private static FakeSession highCompletingConcurrently(final long blockNumber, final long uniqueId) {
            return new FakeSession(
                    new SessionKey(blockNumber, uniqueId), SessionPriority.HIGH, BlockSource.PUBLISHER, true, false);
        }

        /// Mark the session as having produced its result.
        private FakeSession finish() {
            finished = true;
            return this;
        }

        @Override
        public SessionKey sessionKey() {
            return key;
        }

        @Override
        public SessionPriority priority() {
            return priority;
        }

        @Override
        public BlockSource blockSource() {
            return source;
        }

        @Override
        public boolean isEndOfBlockReceived() {
            return endOfBlockReceived;
        }

        @Override
        public boolean isFinished() {
            return finished;
        }

        @Override
        public void start() {
            // never runs
        }

        @Override
        public boolean cancel() {
            cancelCount++;
            cancelOrder = CANCEL_SEQUENCE.getAndIncrement();
            final boolean result = !finished && cancelResult;
            finished = true;
            return result;
        }

        @Override
        public void markEndOfBlockReceived() {
            endOfBlockReceived = true;
        }

        @Override
        public ConcurrentLinkedDeque<BlockItems> getBlockItemsDeque() {
            return deque;
        }
    }

    /// Tests for activating sessions within and over the limit.
    @Nested
    @DisplayName("Activation Tests")
    class ActivationTests {
        /// This test aims to assert that sessions activated while the buffer is
        /// at or below its limit are simply added, the policy is never
        /// consulted and the gauge follows the size.
        @Test
        @DisplayName("activation within the limit adds the session without consulting the policy")
        void testActivateWithinLimitDoesNotConsultPolicy() {
            final RecordingPolicy policy = new RecordingPolicy();
            final ActiveSessionsBuffer toTest = buffer(policy);
            final FakeSession first = FakeSession.high(102, 0);
            final FakeSession second = FakeSession.low(103, 1);
            toTest.activate(first);
            toTest.activate(second);
            assertThat(toTest.size()).isEqualTo(LIMIT);
            assertThat(toTest.contains(first.sessionKey())).isTrue();
            assertThat(toTest.contains(second.sessionKey())).isTrue();
            assertThat(policy.snapshots).isEmpty();
            assertThat(gauge()).isEqualTo(LIMIT);
        }

        /// This test aims to assert that an activation pushing the buffer over
        /// its limit consults the policy exactly once with a snapshot that
        /// describes the buffer faithfully: every session with its key,
        /// priority and source in key order, the last verified block, the
        /// ordering settings, the limit, and no protected key when every
        /// session has received its complete block.
        @Test
        @DisplayName("activation over the limit consults the policy once with a faithful snapshot")
        void testActivateOverLimitConsultsPolicyOnceWithFaithfulSnapshot() {
            final RecordingPolicy policy = new RecordingPolicy();
            final ActiveSessionsBuffer toTest = buffer(policy);
            toTest.activate(FakeSession.high(102, 0));
            toTest.activate(FakeSession.low(103, 1));
            toTest.activate(FakeSession.high(104, 2));
            assertThat(policy.snapshots).hasSize(1);
            final EvictionSnapshot snapshot = policy.snapshots.getFirst();
            assertThat(snapshot.lastVerifiedBlock()).isEqualTo(100L);
            assertThat(snapshot.limit()).isEqualTo(LIMIT);
            assertThat(snapshot.firstOrderedBlock()).isEqualTo(verificationConfig.firstOrderedBlock());
            assertThat(snapshot.allSourcesRequireOrdering()).isEqualTo(verificationConfig.allSourcesRequireOrdering());
            assertThat(snapshot.protectedKeys()).isEmpty();
            assertThat(snapshot.sessions())
                    .extracting(s -> s.key(), s -> s.priority(), s -> s.source())
                    .containsExactly(
                            tuple(new SessionKey(102, 0), SessionPriority.HIGH, BlockSource.PUBLISHER),
                            tuple(new SessionKey(103, 1), SessionPriority.LOW, BlockSource.BACKFILL),
                            tuple(new SessionKey(104, 2), SessionPriority.HIGH, BlockSource.PUBLISHER));
        }

        /// This test aims to assert that a session which produced its result
        /// before it became visible to the buffer is removed on activation:
        /// its own removal ran before it was added and found nothing, so the
        /// buffer must not keep it. The buffer stays within its limit and the
        /// policy is not consulted.
        @Test
        @DisplayName("a session finished before activation is removed at once")
        void testFinishedSessionIsRemovedOnActivation() {
            final RecordingPolicy policy = new RecordingPolicy();
            final ActiveSessionsBuffer toTest = buffer(policy);
            toTest.activate(FakeSession.high(102, 0));
            toTest.activate(FakeSession.high(103, 1));
            final FakeSession finished = FakeSession.high(104, 2).finish();
            toTest.activate(finished);
            assertThat(toTest.contains(finished.sessionKey())).isFalse();
            assertThat(toTest.size()).isEqualTo(LIMIT);
            assertThat(policy.snapshots).isEmpty();
            assertThat(gauge()).isEqualTo(LIMIT);
        }

        /// This test aims to assert that sessions which already produced their
        /// result are left out of the snapshot handed to the policy, since
        /// they are leaving on their own and must not be counted or selected.
        @Test
        @DisplayName("finished sessions are left out of the snapshot")
        void testSessionsThatFinishedAreLeftOutOfTheSnapshot() {
            final RecordingPolicy policy = new RecordingPolicy();
            final ActiveSessionsBuffer toTest = buffer(policy);
            final FakeSession first = FakeSession.high(102, 0);
            toTest.activate(first);
            toTest.activate(FakeSession.high(103, 1));
            first.finish();
            toTest.activate(FakeSession.high(104, 2));
            assertThat(policy.snapshots).hasSize(1);
            assertThat(policy.snapshots.getFirst().sessions())
                    .extracting(s -> s.key())
                    .containsExactly(new SessionKey(103, 1), new SessionKey(104, 2));
        }

        /// This test aims to assert that a null session is rejected up front.
        @Test
        @DisplayName("activation rejects a null session")
        void testActivateRejectsNull() {
            final ActiveSessionsBuffer toTest = buffer(new RecordingPolicy());
            assertThatNullPointerException().isThrownBy(() -> toTest.activate(null));
        }
    }

    /// Tests for the protection of sessions still receiving their block.
    @Nested
    @DisplayName("Protection Tests")
    class ProtectionTests {
        /// This test aims to assert that the protection predicate holds for a
        /// high priority session that has not received its end of block, and
        /// for nothing else: a complete high priority session and low priority
        /// sessions, complete or not, are never protected.
        @Test
        @DisplayName("only a high priority session still receiving its block is protected")
        void testOnlyIncompleteHighPrioritySessionIsProtected() {
            assertThat(ActiveSessionsBuffer.isProtected(FakeSession.incompleteHigh(102, 0)))
                    .isTrue();
            assertThat(ActiveSessionsBuffer.isProtected(FakeSession.high(102, 1)))
                    .isFalse();
            assertThat(ActiveSessionsBuffer.isProtected(FakeSession.incompleteLow(102, 2)))
                    .isFalse();
            assertThat(ActiveSessionsBuffer.isProtected(FakeSession.low(102, 3)))
                    .isFalse();
        }

        /// This test aims to assert that the protected keys handed to the
        /// policy contain exactly the high priority sessions still receiving
        /// their block, while low priority sessions without their end of block
        /// are not protected.
        @Test
        @DisplayName("protected keys hold exactly the incomplete high priority sessions")
        void testProtectedKeysHoldIncompleteHighPrioritySessions() {
            final RecordingPolicy policy = new RecordingPolicy();
            final ActiveSessionsBuffer toTest = buffer(policy);
            toTest.activate(FakeSession.incompleteHigh(102, 0));
            toTest.activate(FakeSession.incompleteLow(103, 1));
            toTest.activate(FakeSession.high(104, 2));
            assertThat(policy.snapshots).hasSize(1);
            assertThat(policy.snapshots.getFirst().protectedKeys()).containsExactly(new SessionKey(102, 0));
        }

        /// This test aims to assert that a protected session selected by a
        /// policy in breach of its contract is kept: it is not removed, not
        /// cancelled and not counted, and the buffer overshoots instead.
        @Test
        @DisplayName("a protected victim is kept without being cancelled")
        void testProtectedVictimIsSkippedWithoutCancel() {
            final FakeSession incomplete = FakeSession.incompleteHigh(102, 0);
            final ActiveSessionsBuffer toTest = buffer(new RecordingPolicy(incomplete.sessionKey()));
            toTest.activate(incomplete);
            toTest.activate(FakeSession.high(103, 1));
            toTest.activate(FakeSession.high(104, 2));
            assertThat(toTest.contains(incomplete.sessionKey())).isTrue();
            assertThat(incomplete.cancelCount).isZero();
            assertThat(toTest.size()).isEqualTo(3);
            assertThat(evicted(SessionPriority.HIGH)).isZero();
            assertThat(evicted(SessionPriority.LOW)).isZero();
        }
    }

    /// Tests for the removal, cancellation and accounting of the selected victims.
    @Nested
    @DisplayName("Eviction Round Tests")
    class EvictionRoundTests {
        /// This test aims to assert that a selected victim is removed from the
        /// buffer, cancelled exactly once and counted under its priority, and
        /// that the gauge reflects the size after the round.
        @Test
        @DisplayName("a victim is removed, cancelled and counted under its priority")
        void testVictimIsRemovedCancelledAndCounted() {
            final FakeSession victim = FakeSession.low(102, 0);
            final ActiveSessionsBuffer toTest = buffer(new RecordingPolicy(victim.sessionKey()));
            toTest.activate(victim);
            toTest.activate(FakeSession.high(103, 1));
            toTest.activate(FakeSession.high(104, 2));
            assertThat(toTest.contains(victim.sessionKey())).isFalse();
            assertThat(victim.cancelCount).isEqualTo(1);
            assertThat(evicted(SessionPriority.LOW)).isEqualTo(1L);
            assertThat(evicted(SessionPriority.HIGH)).isZero();
            assertThat(toTest.size()).isEqualTo(LIMIT);
            assertThat(gauge()).isEqualTo(LIMIT);
        }

        /// This test aims to assert that a victim which is no longer in the
        /// buffer is skipped without error and without counting an eviction.
        @Test
        @DisplayName("a victim no longer in the buffer is skipped")
        void testVictimGoneIsSkipped() {
            final ActiveSessionsBuffer toTest = buffer(new RecordingPolicy(new SessionKey(999, 999)));
            final FakeSession first = FakeSession.high(102, 0);
            toTest.activate(first);
            toTest.activate(FakeSession.high(103, 1));
            toTest.activate(FakeSession.high(104, 2));
            assertThat(toTest.size()).isEqualTo(3);
            assertThat(first.cancelCount).isZero();
            assertThat(evicted(SessionPriority.HIGH)).isZero();
        }

        /// This test aims to assert that a victim which already produced its
        /// result by the time the round looks at it is left alone: it removes
        /// itself, so the round neither removes nor cancels it.
        @Test
        @DisplayName("a victim that already finished is left to remove itself")
        void testVictimAlreadyFinishedIsLeftToRemoveItself() {
            final FakeSession victim = FakeSession.high(102, 0);
            final ActiveSessionsBuffer toTest = buffer(new RecordingPolicy(victim.sessionKey()));
            toTest.activate(victim);
            toTest.activate(FakeSession.high(103, 1));
            victim.finish();
            toTest.activate(FakeSession.high(104, 2));
            assertThat(toTest.contains(victim.sessionKey())).isTrue();
            assertThat(victim.cancelCount).isZero();
            assertThat(evicted(SessionPriority.HIGH)).isZero();
        }

        /// This test aims to assert that a removed victim whose cancellation
        /// reports that it produced its result in the meantime is not counted
        /// as evicted: its result is handled normally and the slot is free
        /// either way.
        @Test
        @DisplayName("a cancellation reporting a concurrent completion is not counted")
        void testCancelReportingConcurrentCompletionIsNotCounted() {
            final FakeSession victim = FakeSession.highCompletingConcurrently(102, 0);
            final ActiveSessionsBuffer toTest = buffer(new RecordingPolicy(victim.sessionKey()));
            toTest.activate(victim);
            toTest.activate(FakeSession.high(103, 1));
            toTest.activate(FakeSession.high(104, 2));
            assertThat(toTest.contains(victim.sessionKey())).isFalse();
            assertThat(victim.cancelCount).isEqualTo(1);
            assertThat(evicted(SessionPriority.HIGH)).isZero();
            assertThat(toTest.size()).isEqualTo(LIMIT);
        }

        /// This test aims to assert that when the policy selects several
        /// victims they are cancelled in the order selected and each is
        /// counted under its own priority.
        @Test
        @DisplayName("several victims are evicted in policy order and counted per priority")
        void testMultipleVictimsEvictedInPolicyOrder() {
            final FakeSession low = FakeSession.low(102, 0);
            final FakeSession high = FakeSession.high(103, 1);
            final AtomicInteger calls = new AtomicInteger();
            // the first round selects nothing, so the buffer is over its limit by two on the next activation
            final EvictionPolicy policy =
                    snapshot -> calls.getAndIncrement() == 0 ? List.of() : List.of(high.sessionKey(), low.sessionKey());
            final ActiveSessionsBuffer toTest = buffer(policy);
            toTest.activate(low);
            toTest.activate(high);
            toTest.activate(FakeSession.high(104, 2));
            toTest.activate(FakeSession.high(105, 3));
            assertThat(toTest.contains(low.sessionKey())).isFalse();
            assertThat(toTest.contains(high.sessionKey())).isFalse();
            assertThat(high.cancelOrder).isLessThan(low.cancelOrder);
            assertThat(evicted(SessionPriority.HIGH)).isEqualTo(1L);
            assertThat(evicted(SessionPriority.LOW)).isEqualTo(1L);
            assertThat(toTest.size()).isEqualTo(LIMIT);
        }

        /// This test aims to assert that a failing policy never fails the
        /// activation: the new session is in the buffer, nothing is cancelled
        /// or counted, and the buffer overshoots until the next round.
        @Test
        @DisplayName("a failing policy is contained and the activation succeeds")
        void testPolicyFailureIsContainedAndActivationSucceeds() {
            final ActiveSessionsBuffer toTest = buffer(snapshot -> {
                throw new IllegalStateException("policy failure");
            });
            final FakeSession first = FakeSession.high(102, 0);
            final FakeSession last = FakeSession.high(104, 2);
            toTest.activate(first);
            toTest.activate(FakeSession.high(103, 1));
            toTest.activate(last);
            assertThat(toTest.contains(last.sessionKey())).isTrue();
            assertThat(toTest.size()).isEqualTo(3);
            assertThat(first.cancelCount).isZero();
            assertThat(evicted(SessionPriority.HIGH)).isZero();
            assertThat(gauge()).isEqualTo(3L);
        }

        /// This test aims to assert that the round stops as soon as the buffer
        /// is back within its limit, so a policy returning more victims than
        /// needed only costs as many sessions as necessary.
        @Test
        @DisplayName("the round stops once the buffer is within its limit")
        void testRoundStopsWhenBufferIsBackWithinLimit() {
            final FakeSession first = FakeSession.high(102, 0);
            final FakeSession second = FakeSession.high(103, 1);
            final ActiveSessionsBuffer toTest = buffer(new RecordingPolicy(first.sessionKey(), second.sessionKey()));
            toTest.activate(first);
            toTest.activate(second);
            toTest.activate(FakeSession.high(104, 2));
            assertThat(toTest.contains(first.sessionKey())).isFalse();
            assertThat(toTest.contains(second.sessionKey())).isTrue();
            assertThat(second.cancelCount).isZero();
            assertThat(evicted(SessionPriority.HIGH)).isEqualTo(1L);
        }

        /// This test aims to assert that exactly one round runs per activation
        /// over the limit: a round that evicts nothing does not repeat, and the
        /// next activation runs the next round.
        @Test
        @DisplayName("one round runs per activation over the limit")
        void testRoundRunsOncePerOverLimitActivation() {
            final RecordingPolicy policy = new RecordingPolicy();
            final ActiveSessionsBuffer toTest = buffer(policy);
            toTest.activate(FakeSession.high(102, 0));
            toTest.activate(FakeSession.high(103, 1));
            toTest.activate(FakeSession.high(104, 2));
            assertThat(policy.snapshots).hasSize(1);
            toTest.activate(FakeSession.high(105, 3));
            assertThat(policy.snapshots).hasSize(2);
            assertThat(toTest.size()).isEqualTo(4);
        }
    }

    /// Tests for sessions removing themselves.
    @Nested
    @DisplayName("Removal Tests")
    class RemovalTests {
        /// This test aims to assert that removing a session drops it from the
        /// buffer and updates the gauge, without any cancellation.
        @Test
        @DisplayName("removal drops the session and updates the gauge")
        void testRemoveDropsSessionAndUpdatesGauge() {
            final ActiveSessionsBuffer toTest = buffer(new RecordingPolicy());
            final FakeSession session = FakeSession.high(102, 0);
            toTest.activate(session);
            assertThat(gauge()).isEqualTo(1L);
            toTest.remove(session.sessionKey());
            assertThat(toTest.contains(session.sessionKey())).isFalse();
            assertThat(toTest.size()).isZero();
            assertThat(gauge()).isZero();
            assertThat(session.cancelCount).isZero();
        }

        /// This test aims to assert that removing a key that is not in the
        /// buffer, such as the key of an already evicted session, is a no-op.
        @Test
        @DisplayName("removing an unknown key is a no-op")
        void testRemoveUnknownKeyIsNoOp() {
            final ActiveSessionsBuffer toTest = buffer(new RecordingPolicy());
            toTest.activate(FakeSession.high(102, 0));
            toTest.remove(new SessionKey(999, 999));
            assertThat(toTest.size()).isEqualTo(1);
            assertThat(gauge()).isEqualTo(1L);
        }

        /// This test aims to assert that null keys are rejected up front by
        /// both removal and membership checks.
        @Test
        @DisplayName("removal and membership checks reject a null key")
        void testRemoveAndContainsRejectNull() {
            final ActiveSessionsBuffer toTest = buffer(new RecordingPolicy());
            assertThatNullPointerException().isThrownBy(() -> toTest.remove(null));
            assertThatNullPointerException().isThrownBy(() -> toTest.contains(null));
        }
    }

    /// Tests for the metrics recorded by the buffer.
    @Nested
    @DisplayName("Metrics Tests")
    class MetricsTests {
        /// This test aims to assert that the gauge follows the size of the
        /// buffer through activations, removals and eviction rounds.
        @Test
        @DisplayName("gauge follows the size through activate, remove and evict")
        void testGaugeFollowsSize() {
            final FakeSession first = FakeSession.high(102, 0);
            final ActiveSessionsBuffer toTest = buffer(new RecordingPolicy(first.sessionKey()));
            toTest.activate(first);
            assertThat(gauge()).isEqualTo(1L);
            final FakeSession second = FakeSession.high(103, 1);
            toTest.activate(second);
            assertThat(gauge()).isEqualTo(2L);
            toTest.remove(second.sessionKey());
            assertThat(gauge()).isEqualTo(1L);
            toTest.activate(FakeSession.high(104, 2));
            toTest.activate(FakeSession.high(105, 3));
            assertThat(gauge()).isEqualTo(LIMIT).isEqualTo(toTest.size());
        }

        /// This test aims to assert that the evicted counter is labelled by
        /// the priority of the evicted session, one series per priority.
        @Test
        @DisplayName("evicted counter is labelled by priority")
        void testEvictedCounterIsLabelledByPriority() {
            final FakeSession low = FakeSession.low(102, 0);
            final FakeSession high = FakeSession.high(103, 1);
            final AtomicInteger calls = new AtomicInteger();
            final EvictionPolicy policy =
                    snapshot -> calls.getAndIncrement() == 0 ? List.of(low.sessionKey()) : List.of(high.sessionKey());
            final ActiveSessionsBuffer toTest = buffer(policy);
            toTest.activate(low);
            toTest.activate(high);
            toTest.activate(FakeSession.high(104, 2));
            toTest.activate(FakeSession.high(105, 3));
            assertThat(evicted(SessionPriority.LOW)).isEqualTo(1L);
            assertThat(evicted(SessionPriority.HIGH)).isEqualTo(1L);
        }
    }

    /// Tests for the serialization of activations from two threads.
    @Nested
    @DisplayName("Mutual Exclusion Tests")
    class MutualExclusionTests {
        /// This test aims to assert that activations are serialized: while one
        /// thread runs an eviction round (the policy is held on a latch), a
        /// second thread activating a session waits for the lock instead of
        /// running a concurrent round, and completes its own activation, with
        /// its own round, once the first thread is done. The waiting thread is
        /// observed through its thread state, no timing is assumed.
        @Test
        @DisplayName("an activation waits while another activation runs the eviction round")
        void testActivationWaitsWhileAnotherActivationRunsTheRound() throws InterruptedException {
            final CountDownLatch policyEntered = new CountDownLatch(1);
            final CountDownLatch releasePolicy = new CountDownLatch(1);
            final AtomicInteger policyCalls = new AtomicInteger();
            final EvictionPolicy policy = snapshot -> {
                policyCalls.incrementAndGet();
                policyEntered.countDown();
                try {
                    releasePolicy.await();
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return List.of();
            };
            verificationConfig = new TestConfigurationBuilder()
                    .withConfigDataType(VerificationConfig.class)
                    .withValue("verification.activeSessionsBufferSize", "1")
                    .getOrCreateConfig()
                    .getConfigData(VerificationConfig.class);
            final ActiveSessionsBuffer toTest = buffer(policy);
            final FakeSession first = FakeSession.high(102, 0);
            final FakeSession second = FakeSession.high(103, 1);
            final FakeSession third = FakeSession.high(104, 2);
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            toTest.activate(first);
            final Thread inRound = new Thread(() -> toTest.activate(second), "activation-in-round");
            final Thread waiting = new Thread(() -> toTest.activate(third), "activation-waiting");
            inRound.setUncaughtExceptionHandler((thread, throwable) -> failure.set(throwable));
            waiting.setUncaughtExceptionHandler((thread, throwable) -> failure.set(throwable));
            inRound.start();
            assertThat(policyEntered.await(10, TimeUnit.SECONDS)).isTrue();
            waiting.start();
            awaitState(waiting, Thread.State.WAITING);
            assertThat(waiting.getState()).isEqualTo(Thread.State.WAITING);
            assertThat(toTest.contains(second.sessionKey())).isTrue();
            assertThat(toTest.contains(third.sessionKey())).isFalse();
            assertThat(policyCalls.get()).isEqualTo(1);
            releasePolicy.countDown();
            inRound.join(TimeUnit.SECONDS.toMillis(10));
            waiting.join(TimeUnit.SECONDS.toMillis(10));
            assertThat(failure.get()).isNull();
            assertThat(toTest.contains(third.sessionKey())).isTrue();
            assertThat(toTest.size()).isEqualTo(3);
            assertThat(policyCalls.get()).isEqualTo(2);
        }

        /// Wait, bounded, until the thread reaches the expected state.
        private void awaitState(final Thread thread, final Thread.State expected) {
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (thread.getState() != expected && System.nanoTime() < deadline) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
            }
        }
    }
}
