// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import static org.assertj.core.api.Assertions.assertThat;

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
/// end of its block, and when the buffer is cleared.
@DisplayName("Active Sessions Buffer Tests")
class ActiveSessionsBufferTest {
    /// The buffer limit used unless a test needs another.
    private static final int BUFFER_SIZE = 2;
    /// The metrics the buffer under test records.
    private MetricsHolder metrics;
    /// The last verified block handed to the buffer under test.
    private AtomicLong lastVerifiedBlock;
    /// The policy of the buffer under test, recording every snapshot it receives.
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
        policy = RecordingEvictionPolicy.lowestKeyFirst();
        nextUniqueId = 0;
        toTest = newBuffer(BUFFER_SIZE);
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

        /// This test aims to assert that a publisher session is protected during its own
        /// admission: it is admitted before it receives any batch, so when it takes the
        /// count over the limit, the snapshot handed to the policy names it as the active
        /// publisher session and the policy never selects it. This holds for a block
        /// that comes in one batch as well.
        @Test
        @DisplayName("admit() protects a newly admitted publisher session during its own admission")
        void testProtectsANewlyAdmittedPublisherSessionDuringItsOwnAdmission() {
            toTest = newBuffer(1);
            final StandInSession backfilled = newSession(7, BlockSource.BACKFILL);
            final StandInSession published = publisherSession(9);
            toTest.admit(backfilled);
            toTest.admit(published);
            assertThat(policy.snapshots()).hasSize(1);
            assertThat(policy.snapshots().getFirst().activeHighPrioritySession())
                    .isEqualTo(published.sessionKey());
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
        /// for example because its block failed verification, stops being active once it
        /// is cleared, so the publisher's remaining batches for its block are disregarded.
        @Test
        @DisplayName("activePublisherSession() is empty once the session is cleared")
        void testPublisherSessionIsNotActiveOnceItIsCleared() {
            final StandInSession session = publisherSession(5);
            toTest.admit(session);
            session.completeOnItsOwn();
            toTest.clearCompletedSessions();
            assertThat(toTest.activePublisherSession()).isNull();
        }
    }

    /// Tests for clearing the sessions that are complete.
    @Nested
    @DisplayName("Clearing Tests")
    class ClearingTests {
        /// This test aims to assert that a complete session stays in the buffer until the
        /// session handler clears the buffer: the buffer never removes a session on its
        /// own.
        @Test
        @DisplayName("a complete session stays in the buffer until it is cleared")
        void testACompleteSessionStaysUntilItIsCleared() {
            final StandInSession session = newSession(3, BlockSource.BACKFILL);
            toTest.admit(session);
            session.completeOnItsOwn();
            assertThat(toTest.snapshot().lowPrioritySessions()).containsExactly(session.sessionKey());
            assertThat(gauge()).isEqualTo(1L);
        }

        /// This test aims to assert that clearing removes every complete session, in
        /// either lane, keeps the sessions that are still running, and that the gauge
        /// follows.
        @Test
        @DisplayName("clearCompletedSessions() removes the complete sessions of both lanes")
        void testClearingRemovesTheCompleteSessionsOfBothLanes() {
            toTest = newBuffer(5);
            final StandInSession published = publisherSession(5);
            final StandInSession running = publisherSession(6);
            final StandInSession backfilled = newSession(3, BlockSource.BACKFILL);
            toTest.admit(published);
            toTest.admit(running);
            toTest.admit(backfilled);
            published.completeOnItsOwn();
            backfilled.completeOnItsOwn();
            toTest.clearCompletedSessions();
            assertThat(toTest.snapshot().highPrioritySessions()).containsExactly(running.sessionKey());
            assertThat(toTest.snapshot().lowPrioritySessions()).isEmpty();
            assertThat(gauge()).isEqualTo(1L);
        }

        /// This test aims to assert that a session that completed before it was admitted
        /// is cleared like any other at the next clearing, so the order of completing and
        /// admitting can never leave a session in the buffer.
        @Test
        @DisplayName("clearCompletedSessions() removes a session that completed before it was admitted")
        void testClearingRemovesASessionThatCompletedBeforeItWasAdmitted() {
            final StandInSession session = newSession(3, BlockSource.BACKFILL);
            session.completeOnItsOwn();
            toTest.admit(session);
            toTest.clearCompletedSessions();
            assertThat(toTest.size()).isZero();
            assertThat(gauge()).isZero();
        }
    }

    /// Tests for the eviction of the sessions the policy selects.
    @Nested
    @DisplayName("Eviction Tests")
    class EvictionTests {
        /// This test aims to assert that a session the policy selects is removed from its
        /// lane, cancelled once and counted in the evictions counter of its lane when its
        /// cancellation stopped it.
        @Test
        @DisplayName("admit() evicts, cancels and counts what the policy selects")
        void testEvictsCancelsAndCountsTheSelectedSession() {
            final StandInSession lowest = newSession(2, BlockSource.BACKFILL);
            toTest.admit(lowest);
            toTest.admit(newSession(3, BlockSource.BACKFILL));
            toTest.admit(newSession(4, BlockSource.BACKFILL));
            assertThat(lowest.cancelCalls()).isEqualTo(1);
            assertThat(toTest.snapshot().lowPrioritySessions())
                    .extracting(SessionKey::blockNumber)
                    .containsExactly(3L, 4L);
            assertThat(evictedLowPriority()).isEqualTo(1L);
            assertThat(gauge()).isEqualTo(2L);
        }

        /// This test aims to assert that complete sessions are cleared before the policy
        /// is consulted: when clearing brings the count back within the limit, nothing is
        /// evicted and the policy is not consulted.
        @Test
        @DisplayName("admit() clears complete sessions before consulting the policy")
        void testClearsCompleteSessionsBeforeConsultingThePolicy() {
            final StandInSession completed = newSession(2, BlockSource.BACKFILL);
            final StandInSession running = newSession(3, BlockSource.BACKFILL);
            toTest.admit(completed);
            toTest.admit(running);
            completed.completeOnItsOwn();
            toTest.admit(newSession(4, BlockSource.BACKFILL));
            assertThat(policy.snapshots()).isEmpty();
            assertThat(running.cancelCalls()).isZero();
            assertThat(toTest.snapshot().lowPrioritySessions())
                    .extracting(SessionKey::blockNumber)
                    .containsExactly(3L, 4L);
            assertThat(evictedLowPriority()).isZero();
        }

        /// This test aims to assert that a selected session whose result was already
        /// produced when it was cancelled (its cancellation returns false while its result
        /// handling is still running) is removed but not counted as evicted.
        @Test
        @DisplayName("admit() does not count a selected session that had already produced its result")
        void testDoesNotCountASelectedSessionThatHadAlreadyProducedItsResult() {
            final StandInSession lowest = newSession(2, BlockSource.BACKFILL);
            toTest.admit(lowest);
            lowest.produceResultWhileItIsStillBeingHandled();
            toTest.admit(newSession(3, BlockSource.BACKFILL));
            toTest.admit(newSession(4, BlockSource.BACKFILL));
            assertThat(lowest.cancelCalls()).isEqualTo(1);
            assertThat(toTest.snapshot().lowPrioritySessions())
                    .extracting(SessionKey::blockNumber)
                    .containsExactly(3L, 4L);
            assertThat(evictedLowPriority()).isZero();
        }
    }

    /// Build a buffer with the recording policy and the given limit.
    private ActiveSessionsBuffer newBuffer(final int bufferSize) {
        final VerificationConfig config = new TestConfigurationBuilder()
                .withConfigDataType(VerificationConfig.class)
                .withValue("verification.activeSessionsBufferSize", String.valueOf(bufferSize))
                .getOrCreateConfig()
                .getConfigData(VerificationConfig.class);
        return new ActiveSessionsBuffer(config, policy, lastVerifiedBlock, metrics.sessionHandlerMetrics());
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
