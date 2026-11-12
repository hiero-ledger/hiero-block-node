// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.pbj.runtime.ParseException;
import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.hiero.block.api.RangedAddressBookHistory;
import org.hiero.block.api.RangedNodeAddressBook;
import org.hiero.block.node.app.fixtures.TestConfigurationBuilder;
import org.hiero.block.node.app.fixtures.TestUtils;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.blocks.ResourceTestBlockBuilder;
import org.hiero.block.node.app.fixtures.blocks.ResourceTestBlockBuilder.WRB;
import org.hiero.block.node.app.fixtures.blocks.ResourceTestWRBBlock;
import org.hiero.block.node.app.fixtures.blocks.TestBlock;
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
import org.hiero.block.node.app.fixtures.plugintest.TestApplicationStateFacility;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.block.verification.BadBlockDumper;
import org.hiero.block.node.block.verification.VerificationConfig;
import org.hiero.block.node.block.verification.VerificationDataProvider;
import org.hiero.block.node.block.verification.VerificationOutbox;
import org.hiero.block.node.block.verification.metrics.MetricsHolder;
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

/// Tests for the [CompletableVerificationSession], with its real stages and its real
/// result handling stage. The sessions run on an executor that runs their stages only
/// when a test says so.
@DisplayName("Completable Verification Session Tests")
@Timeout(value = 30, unit = SECONDS)
class CompletableVerificationSessionTest {
    /// The block the session under test verifies.
    private static final long BLOCK_NUMBER = 5L;
    /// How long a test waits for stages running in the background to stop.
    private static final Duration STAGES_TIMEOUT = Duration.ofSeconds(10);
    /// How often a test checks whether a session has verified its block.
    private static final Duration VERIFIED_BLOCK_POLL_INTERVAL = Duration.ofMillis(1);
    /// The rounds of the test that cancels sessions from two threads at the same time.
    private static final int SIMULTANEOUS_CANCELLATION_ROUNDS = 1_000;
    /// The rounds of the test that releases sessions waiting for their turn as they are cancelled.
    private static final int RELEASE_RACE_ROUNDS = 110;
    /// How much longer each round of that test waits before cancelling than the round before,
    /// over [#RELEASE_RACE_DELAY_STEPS] rounds, after which the delay starts again from none.
    private static final Duration RELEASE_RACE_DELAY_STEP = Duration.of(100, ChronoUnit.MICROS);
    /// The number of different delays that test cancels after, from none up.
    private static final int RELEASE_RACE_DELAY_STEPS = 11;
    /// The executor the sessions' stages are submitted to; it runs them only on demand.
    private BlockingExecutor executor;
    /// Captures the notifications the sessions send.
    private TestBlockMessagingFacility blockMessaging;
    /// The configuration for verification, the defaults.
    private VerificationConfig verificationConfig;
    /// The verification metrics, shared by the sessions of a test.
    private MetricsHolder metricsHolder;
    /// The block the session under test verifies, which carries a TSS signature.
    private TestBlock block;
    /// The instance under test, a publisher session for [#BLOCK_NUMBER].
    private CompletableVerificationSession toTest;

    /// Setup before each test.
    @BeforeEach
    void setUp() {
        executor = new BlockingExecutor(new LinkedBlockingQueue<>());
        blockMessaging = new TestBlockMessagingFacility();
        verificationConfig = new TestConfigurationBuilder()
                .withConfigDataType(VerificationConfig.class)
                .getOrCreateConfig()
                .getConfigData(VerificationConfig.class);
        metricsHolder = MetricsHolder.create(TestUtils.createMetrics());
        block = TestBlockBuilder.generateBlockWithNumber(BLOCK_NUMBER);
        toTest = newSession(0L, BLOCK_NUMBER, new TestApplicationStateFacility(), BLOCK_NUMBER - 1);
    }

    /// Tests for when a session is complete: once its result handling stage has handled
    /// its result and sent its notification.
    @Nested
    @DisplayName("Completeness Tests")
    class CompletenessTests {
        /// This test aims to assert that a started session is not complete while its
        /// result has not been handled: its stages have not run yet, and no notification
        /// has been sent.
        @Test
        @DisplayName("isComplete() is false while the session's result has not been handled")
        void testIsNotCompleteWhileTheResultHasNotBeenHandled() {
            toTest.start();
            toTest.getBlockItemsDeque().offer(block.asBlockItems());
            assertThat(toTest.isComplete()).isFalse();
            assertThat(blockMessaging.getSentVerificationNotifications()).isEmpty();
        }

        /// This test aims to assert that cancelling a running session handles its result
        /// on the cancelling thread, so the session is complete as soon as `cancel()`
        /// returns: the publisher session for block 5 never received the end of its
        /// block, the cancellation returns true, and exactly one `CANCELLED_INCOMPLETE`
        /// notification has been sent by then.
        @Test
        @DisplayName("isComplete() is true as soon as cancelling the session returns")
        void testIsCompleteAsSoonAsCancellingReturns() {
            toTest.start();
            assertThat(toTest.cancel()).isTrue();
            assertThat(toTest.isComplete()).isTrue();
            assertThat(blockMessaging.getSentVerificationNotifications())
                    .singleElement()
                    .returns(
                            FailureInfo.standard(FailureType.CANCELLED_INCOMPLETE),
                            VerificationNotification::failureInfo);
        }

        /// This test aims to assert that a session whose result is handled on its own, on
        /// the thread that runs its stages, is complete once that has happened: the whole
        /// block 5 is supplied, and the executor runs the stages, which report the block's
        /// result in one notification.
        @Test
        @DisplayName("isComplete() is true once the session's result is handled on its own")
        void testIsCompleteOnceTheResultIsHandledOnItsOwn() {
            toTest.start();
            toTest.markEndOfBlockReceived();
            toTest.getBlockItemsDeque().offer(block.asBlockItems());
            executor.executeSerially();
            assertThat(toTest.isComplete()).isTrue();
            assertThat(blockMessaging.getSentVerificationNotifications()).hasSize(1);
        }
    }

    /// Tests for cancelling a session. Exactly one call returns true for a session stopped
    /// by cancellation, and none for a session that produced its result on its own, so a
    /// caller counting the calls that return true counts each cancelled session once.
    @Nested
    @DisplayName("Cancellation Tests")
    class CancellationTests {
        /// This test aims to assert that the first cancellation of a running session
        /// returns true and stops the session's stages. The publisher session for block 5
        /// has received only the first part of its block, and its stages run in the
        /// background: the hashing stage waits for the rest of the block, which never
        /// comes, and stops only once it sees the session cancelled. Cancelling the
        /// session returns true and sends one `CANCELLED_INCOMPLETE` notification, and the
        /// stages stop.
        @Test
        @DisplayName("cancel() returns true for the first cancellation of a running session, which stops it")
        void testTheFirstCancellationOfARunningSessionStopsIt() {
            toTest.start();
            toTest.getBlockItemsDeque().offer(firstPartOf(block));
            final CompletableFuture<Void> stages = runStagesInTheBackground();
            assertThat(toTest.cancel()).isTrue();
            assertThat(stages).succeedsWithin(STAGES_TIMEOUT);
            assertThat(blockMessaging.getSentVerificationNotifications())
                    .singleElement()
                    .returns(
                            FailureInfo.standard(FailureType.CANCELLED_INCOMPLETE),
                            VerificationNotification::failureInfo);
        }

        /// This test aims to assert that cancelling a session that waits for its turn, as
        /// the eviction of a stuck session does, returns true and stops the session's
        /// stages. Block 2 of a real chain of wrapped record blocks is supplied in full,
        /// with the RSA address book of the chain, while the last verified block is 0. The
        /// stages run in the background, and once the block has verified, as the
        /// verification time metric shows, the ordering stage waits for block 1, which
        /// never comes, and stops only once it sees the session cancelled. Cancelling the
        /// session returns true and sends one `CANCELLED` notification, as the end of the
        /// block had been received, and the stages stop.
        @Test
        @DisplayName("cancel() returns true for a session waiting for its turn, which stops it")
        void testCancellingASessionWaitingForItsTurnStopsIt() throws IOException, ParseException {
            final ResourceTestWRBBlock block2 = ResourceTestBlockBuilder.load(WRB.SOLO_4N_BLOCK_2);
            final CompletableVerificationSession session =
                    newSession(1L, block2.number(), applicationStateWith(block2.nodeAddressBook()), 0L);
            final long verificationTimeBefore = recordedVerificationTime();
            session.start();
            session.markEndOfBlockReceived();
            session.getBlockItemsDeque().offer(block2.asBlockItems());
            final CompletableFuture<Void> stages = runStagesInTheBackground();
            awaitAVerifiedBlockAfter(verificationTimeBefore);
            assertThat(session.cancel()).isTrue();
            assertThat(stages).succeedsWithin(STAGES_TIMEOUT);
            assertThat(blockMessaging.getSentVerificationNotifications())
                    .singleElement()
                    .returns(FailureInfo.standard(FailureType.CANCELLED), VerificationNotification::failureInfo);
        }

        /// This test aims to assert that cancelling a session that finished on its own just
        /// before returns false, and that its stages end on their own, as nothing in them is
        /// left waiting. Block 2 of a real chain of wrapped record blocks is supplied in full,
        /// with the RSA address book of the chain, while the last verified block is 0, and the
        /// stages run in the background. Once the block has verified, block 1 is marked as
        /// verified, so the session's turn comes and it reports its success in one
        /// notification. Cancelling the session after that returns false, the stages end, and
        /// nothing more is sent.
        @Test
        @DisplayName("cancel() returns false for a session released just before, whose stages end on their own")
        void testCancellingASessionReleasedJustBeforeReturnsFalse() throws IOException, ParseException {
            final ResourceTestWRBBlock block2 = ResourceTestBlockBuilder.load(WRB.SOLO_4N_BLOCK_2);
            final AtomicLong lastVerifiedBlock = new AtomicLong(0L);
            final CompletableVerificationSession session =
                    newSession(1L, block2.number(), applicationStateWith(block2.nodeAddressBook()), lastVerifiedBlock);
            final long verificationTimeBefore = recordedVerificationTime();
            session.start();
            session.markEndOfBlockReceived();
            session.getBlockItemsDeque().offer(block2.asBlockItems());
            final CompletableFuture<Void> stages = runStagesInTheBackground();
            awaitAVerifiedBlockAfter(verificationTimeBefore);
            lastVerifiedBlock.set(block2.number() - 1);
            assertThat(blockMessaging.getSentVerificationNotifications(1))
                    .singleElement()
                    .returns(true, VerificationNotification::success);
            assertThat(session.cancel()).isFalse();
            assertThat(stages).succeedsWithin(STAGES_TIMEOUT);
            assertThat(blockMessaging.getSentVerificationNotifications()).hasSize(1);
        }

        /// This test aims to assert that when a session waiting for its turn is released at
        /// the moment it is cancelled, exactly one of the two decides the outcome, and the
        /// stages end either way. In each round a fresh session verifies block 2 of a real
        /// chain of wrapped record blocks, with the RSA address book of the chain, and waits
        /// for block 1, the last verified block being 0. Two threads then act together: one
        /// marks block 1 as verified, which releases the session, and the other cancels the
        /// session after a delay that grows from round to round, from none to one
        /// millisecond, to vary which of the two comes first. In every round the stages end,
        /// exactly one notification is sent, the session is complete, and `cancel()` returns
        /// true when that notification is the cancellation and false when it is the success.
        @Test
        @DisplayName("cancel() and a release at the same moment: one decides the outcome, the stages end either way")
        void testASessionReleasedAsItIsCancelledEndsEitherWay()
                throws IOException, ParseException, InterruptedException, ExecutionException {
            final ResourceTestWRBBlock block2 = ResourceTestBlockBuilder.load(WRB.SOLO_4N_BLOCK_2);
            final TestApplicationStateFacility applicationState = applicationStateWith(block2.nodeAddressBook());
            try (final ExecutorService racers = Executors.newFixedThreadPool(2)) {
                for (int round = 0; round < RELEASE_RACE_ROUNDS; round++) {
                    final AtomicLong lastVerifiedBlock = new AtomicLong(0L);
                    final CompletableVerificationSession session =
                            newSession(round, block2.number(), applicationState, lastVerifiedBlock);
                    final long verificationTimeBefore = recordedVerificationTime();
                    final int notificationsBefore =
                            blockMessaging.getSentVerificationNotifications().size();
                    session.start();
                    session.markEndOfBlockReceived();
                    session.getBlockItemsDeque().offer(block2.asBlockItems());
                    final CompletableFuture<Void> stages = runStagesInTheBackground();
                    awaitAVerifiedBlockAfter(verificationTimeBefore);
                    final Duration cancellationDelay =
                            RELEASE_RACE_DELAY_STEP.multipliedBy(round % RELEASE_RACE_DELAY_STEPS);
                    final CyclicBarrier bothReady = new CyclicBarrier(2);
                    final Future<Long> release = racers.submit(
                            () -> markVerifiedWhenBothReady(lastVerifiedBlock, block2.number() - 1, bothReady));
                    final Future<Boolean> cancellation =
                            racers.submit(() -> cancelWhenBothReady(session, bothReady, cancellationDelay));
                    release.get();
                    final boolean stopped = cancellation.get();
                    assertThat(stages).succeedsWithin(STAGES_TIMEOUT);
                    assertThat(session.isComplete()).isTrue();
                    final List<VerificationNotification> sent = blockMessaging.getSentVerificationNotifications();
                    assertThat(sent).hasSize(notificationsBefore + 1);
                    if (stopped) {
                        assertThat(sent.getLast())
                                .returns(
                                        FailureInfo.standard(FailureType.CANCELLED),
                                        VerificationNotification::failureInfo);
                    } else {
                        assertThat(sent.getLast()).returns(true, VerificationNotification::success);
                    }
                }
            }
        }

        /// This test aims to assert that cancelling a session a second time returns false
        /// and sends nothing more. The stage chain of a cancelled session answers every
        /// later cancellation as if that one had stopped it, so the session must tell the
        /// calls apart itself: the first cancellation of the publisher session for block 5
        /// returns true and sends one `CANCELLED_INCOMPLETE` notification, the second one
        /// returns false.
        @Test
        @DisplayName("cancel() returns false for a session that was already cancelled")
        void testASecondCancellationReturnsFalse() {
            toTest.start();
            assertThat(toTest.cancel()).isTrue();
            assertThat(toTest.cancel()).isFalse();
            assertThat(blockMessaging.getSentVerificationNotifications())
                    .singleElement()
                    .returns(
                            FailureInfo.standard(FailureType.CANCELLED_INCOMPLETE),
                            VerificationNotification::failureInfo);
        }

        /// This test aims to assert that cancelling a session that succeeded on its own
        /// returns false and sends nothing more. Block 1 of a real chain of wrapped record
        /// blocks is supplied in full, with the RSA address book of the chain, while the
        /// last verified block is 0: the block verifies and is next in line, so the session
        /// reports the success in one notification, and the cancellation that follows finds
        /// the session done.
        @Test
        @DisplayName("cancel() returns false for a session that succeeded on its own")
        void testCancellingASessionThatSucceededReturnsFalse() throws IOException, ParseException {
            final ResourceTestWRBBlock block1 = ResourceTestBlockBuilder.load(WRB.SOLO_4N_BLOCK_1);
            final CompletableVerificationSession session =
                    newSession(1L, block1.number(), applicationStateWith(block1.nodeAddressBook()), 0L);
            session.start();
            session.markEndOfBlockReceived();
            session.getBlockItemsDeque().offer(block1.asBlockItems());
            executor.executeSerially();
            assertThat(session.cancel()).isFalse();
            assertThat(blockMessaging.getSentVerificationNotifications())
                    .singleElement()
                    .returns(true, VerificationNotification::success)
                    .returns(block1.number(), VerificationNotification::blockNumber);
        }

        /// This test aims to assert that cancelling a session that failed on its own
        /// returns false and sends nothing more. Block 1 of a real chain of wrapped record
        /// blocks is supplied in full while the last verified block is 0, as in the success
        /// case, but no RSA address book covers the block: the session reports a
        /// `MISSING_VERIFICATION_DATA` failure in one notification, and the cancellation
        /// that follows finds the session done.
        @Test
        @DisplayName("cancel() returns false for a session that failed on its own")
        void testCancellingASessionThatFailedReturnsFalse() throws IOException, ParseException {
            final ResourceTestWRBBlock block1 = ResourceTestBlockBuilder.load(WRB.SOLO_4N_BLOCK_1);
            final CompletableVerificationSession session =
                    newSession(1L, block1.number(), new TestApplicationStateFacility(), 0L);
            session.start();
            session.markEndOfBlockReceived();
            session.getBlockItemsDeque().offer(block1.asBlockItems());
            executor.executeSerially();
            assertThat(session.cancel()).isFalse();
            assertThat(blockMessaging.getSentVerificationNotifications())
                    .singleElement()
                    .returns(
                            FailureInfo.standard(FailureType.MISSING_VERIFICATION_DATA),
                            VerificationNotification::failureInfo);
        }

        /// This test aims to assert that when two threads cancel a running session at the
        /// same time, exactly one of them gets true and exactly one notification is sent.
        /// In each round a fresh publisher session for block 5 is started and both threads
        /// cancel it once they are both ready. After each round the session's stages are
        /// run, and they stop at once, whichever thread got true.
        @Test
        @DisplayName("cancel() returns true to exactly one of two threads cancelling at the same time")
        void testExactlyOneOfTwoSimultaneousCancellationsReturnsTrue() throws InterruptedException, ExecutionException {
            final TestApplicationStateFacility applicationState = new TestApplicationStateFacility();
            try (final ExecutorService cancellers = Executors.newFixedThreadPool(2)) {
                for (int round = 0; round < SIMULTANEOUS_CANCELLATION_ROUNDS; round++) {
                    final CompletableVerificationSession session =
                            newSession(round, BLOCK_NUMBER, applicationState, BLOCK_NUMBER - 1);
                    session.start();
                    final CyclicBarrier bothReady = new CyclicBarrier(2);
                    final Future<Boolean> first =
                            cancellers.submit(() -> cancelWhenBothReady(session, bothReady, Duration.ZERO));
                    final Future<Boolean> second =
                            cancellers.submit(() -> cancelWhenBothReady(session, bothReady, Duration.ZERO));
                    assertThat(List.of(first.get(), second.get())).containsExactlyInAnyOrder(true, false);
                    assertThat(blockMessaging.getSentVerificationNotifications())
                            .hasSize(round + 1);
                    executor.executeSerially();
                }
            }
        }

        /// This test aims to assert that a session cannot be cancelled before it was
        /// started: `cancel()` throws an `IllegalStateException`, and the session stays
        /// incomplete.
        @Test
        @DisplayName("cancel() throws for a session that was never started")
        void testCancelThrowsForASessionThatWasNeverStarted() {
            assertThatIllegalStateException().isThrownBy(toTest::cancel);
            assertThat(toTest.isComplete()).isFalse();
        }
    }

    /// Create a publisher session for the given block, as the other `newSession` does, with
    /// a last verified block of its own that starts at the given block.
    private CompletableVerificationSession newSession(
            final long uniqueId,
            final long blockNumber,
            final TestApplicationStateFacility applicationState,
            final long lastVerifiedBlock) {
        return newSession(uniqueId, blockNumber, applicationState, new AtomicLong(lastVerifiedBlock));
    }

    /// Create a publisher session for the given block on the test's executor, verified
    /// with the RSA address books of the given application state, following the given
    /// last verified block, and sending its notification through the test's messaging.
    private CompletableVerificationSession newSession(
            final long uniqueId,
            final long blockNumber,
            final TestApplicationStateFacility applicationState,
            final AtomicLong lastVerifiedBlock) {
        return new CompletableVerificationSession(
                uniqueId,
                blockNumber,
                metricsHolder,
                BlockSource.PUBLISHER,
                new VerificationDataProvider(
                        new BlockNodeContext(null, null, null, null, null, applicationState, null, null, null)),
                lastVerifiedBlock,
                new ConcurrentLinkedDeque<>(),
                executor,
                new VerificationOutbox(blockMessaging),
                verificationConfig,
                new BadBlockDumper(verificationConfig, "test"));
    }

    /// Run the stages submitted to the executor on a thread of their own, without waiting
    /// for them; the returned future completes once they have stopped.
    private CompletableFuture<Void> runStagesInTheBackground() {
        return executor.executeAsync(STAGES_TIMEOUT.toMillis(), false, false, false)
                .getFirst();
    }

    /// The verification time recorded so far in the test's metrics, which grows each time a
    /// session verifies its block.
    private long recordedVerificationTime() {
        return metricsHolder.proofVerificationMetrics().verificationBlockTime().get();
    }

    /// Wait until a session verifies its block after the given verification time was
    /// recorded, as the verification time metric shows: the verifier records the time just
    /// before it hands its result to the ordering stage.
    private void awaitAVerifiedBlockAfter(final long verificationTimeBefore) {
        while (recordedVerificationTime() == verificationTimeBefore) {
            LockSupport.parkNanos(VERIFIED_BLOCK_POLL_INTERVAL.toNanos());
        }
    }

    /// The batch that starts the given block, holding only its header.
    private static BlockItems firstPartOf(final TestBlock block) {
        return new BlockItems(block.blockUnparsed().blockItems().subList(0, 1), block.number(), true, false);
    }

    /// An application state whose RSA address book history holds the given address book
    /// for every block from 0 on.
    private static TestApplicationStateFacility applicationStateWith(final NodeAddressBook addressBook) {
        final TestApplicationStateFacility applicationState = new TestApplicationStateFacility();
        applicationState.setAddressBookHistory(RangedAddressBookHistory.newBuilder()
                .addressBooks(RangedNodeAddressBook.newBuilder()
                        .addressBook(addressBook)
                        .startBlock(0L)
                        .endBlock(-1L)
                        .build())
                .build());
        return applicationState;
    }

    /// Wait until the other thread is ready as well, then wait the given delay and cancel
    /// the session.
    private static boolean cancelWhenBothReady(
            final BlockVerificationSession session, final CyclicBarrier bothReady, final Duration delay)
            throws InterruptedException, BrokenBarrierException {
        bothReady.await();
        LockSupport.parkNanos(delay.toNanos());
        return session.cancel();
    }

    /// Wait until the other thread is ready as well, then mark the given block as the last
    /// verified one, as the session that verifies it does.
    ///
    /// @return the last verified block before it
    private static long markVerifiedWhenBothReady(
            final AtomicLong lastVerifiedBlock, final long blockNumber, final CyclicBarrier bothReady)
            throws InterruptedException, BrokenBarrierException {
        bothReady.await();
        return lastVerifiedBlock.getAndSet(blockNumber);
    }
}
