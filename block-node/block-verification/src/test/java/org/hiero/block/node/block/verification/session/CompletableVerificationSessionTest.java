// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.LinkedBlockingQueue;
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
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification.FailureInfo;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification.FailureType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/// Tests for the [CompletableVerificationSession].
///
/// Sessions run on a [BlockingExecutor] that never executes them on its own,
/// so a session only runs when a test executes the queued work on the test
/// thread. No verification data is available, so a session that runs to a
/// result reports a failure for its block. The finished callback records the
/// keys it receives together with the number of notifications sent at that
/// moment, so tests can assert that result handling precedes the callback.
@DisplayName("Completable Verification Session Tests")
class CompletableVerificationSessionTest {
    /// The number of the block every session verifies.
    private static final long BLOCK_NUMBER = 10L;
    private BlockingExecutor executor;
    private TestBlockMessagingFacility blockMessaging;
    private BlockNodeContext context;
    private MetricsHolder metrics;
    private VerificationConfig verificationConfig;
    private VerificationDataProvider verificationDataProvider;
    private BadBlockDumper badBlockDumper;
    private ConcurrentLinkedDeque<Long> recentlyVerifiedBlocks;
    /// The keys handed to the finished callback, in order.
    private List<SessionKey> finishedKeys;
    /// The number of notifications sent at the moment of each finished callback.
    private List<Integer> notificationsAtFinish;

    /// Setup before each test.
    @BeforeEach
    void setUp() {
        executor = new BlockingExecutor(new LinkedBlockingQueue<>());
        blockMessaging = new TestBlockMessagingFacility();
        context = new BlockNodeContext(
                null, null, null, blockMessaging, null, null, null, null, null, null, null, null, null);
        metrics = MetricsHolder.create(TestUtils.createMetrics());
        verificationConfig = new TestConfigurationBuilder()
                .withConfigDataType(VerificationConfig.class)
                .getOrCreateConfig()
                .getConfigData(VerificationConfig.class);
        verificationDataProvider = new VerificationDataProvider(context);
        badBlockDumper = new BadBlockDumper(verificationConfig, "test");
        recentlyVerifiedBlocks = new ConcurrentLinkedDeque<>();
        finishedKeys = new ArrayList<>();
        notificationsAtFinish = new ArrayList<>();
    }

    /// Create a not yet started session for the test block.
    private CompletableVerificationSession session() {
        return new CompletableVerificationSession(
                0L,
                BLOCK_NUMBER,
                metrics,
                BlockSource.PUBLISHER,
                SessionPriority.HIGH,
                verificationDataProvider,
                new AtomicLong(BLOCK_NUMBER - 1),
                recentlyVerifiedBlocks,
                executor,
                context,
                verificationConfig,
                key -> {
                    finishedKeys.add(key);
                    notificationsAtFinish.add(
                            blockMessaging.getSentVerificationNotifications().size());
                },
                badBlockDumper);
    }

    /// The complete test block as a single batch.
    private static TestBlock block() {
        return TestBlockBuilder.generateBlockWithNumber(BLOCK_NUMBER);
    }

    /// Tests for the finished callback and its ordering against result handling.
    @Nested
    @DisplayName("Finished Callback Tests")
    class FinishedCallbackTests {
        /// This test aims to assert that a session which runs to a result on
        /// its own reports the result first and invokes the finished callback
        /// exactly once afterwards, with its own key, and is then finished.
        @Test
        @DisplayName("running to a result reports it and then invokes the callback once")
        void testCompletionInvokesCallbackAfterResultHandling() {
            final CompletableVerificationSession toTest = session();
            toTest.start();
            toTest.markEndOfBlockReceived();
            toTest.getBlockItemsDeque().offer(block().asBlockItems());
            assertThat(toTest.isFinished()).isFalse();
            executor.executeSerially();
            assertThat(toTest.isFinished()).isTrue();
            assertThat(blockMessaging.getSentVerificationNotifications())
                    .hasSize(1)
                    .first()
                    .returns(false, VerificationNotification::success)
                    .returns(BLOCK_NUMBER, VerificationNotification::blockNumber);
            assertThat(finishedKeys).containsExactly(new SessionKey(BLOCK_NUMBER, 0L));
            assertThat(notificationsAtFinish).containsExactly(1);
        }

        /// This test aims to assert that cancelling a running session reports
        /// the cancellation first, on the cancelling thread, and then invokes
        /// the finished callback exactly once; the cancellation is reported as
        /// incomplete when the end of the block was never received.
        @Test
        @DisplayName("cancelling reports the cancellation and then invokes the callback once")
        void testCancellationInvokesCallbackAfterResultHandling() {
            final CompletableVerificationSession toTest = session();
            toTest.start();
            assertThat(toTest.cancel()).isTrue();
            assertThat(toTest.isFinished()).isTrue();
            assertThat(blockMessaging.getSentVerificationNotifications())
                    .hasSize(1)
                    .first()
                    .returns(
                            FailureInfo.standard(FailureType.CANCELLED_INCOMPLETE),
                            VerificationNotification::failureInfo)
                    .returns(BLOCK_NUMBER, VerificationNotification::blockNumber);
            assertThat(finishedKeys).containsExactly(new SessionKey(BLOCK_NUMBER, 0L));
            assertThat(notificationsAtFinish).containsExactly(1);
        }

        /// This test aims to assert that the cancellation of a session that
        /// received its complete block is reported as a plain cancellation.
        @Test
        @DisplayName("cancelling a session with its complete block reports a plain cancellation")
        void testCancellationOfCompleteBlockIsReportedAsCancelled() {
            final CompletableVerificationSession toTest = session();
            toTest.start();
            toTest.markEndOfBlockReceived();
            assertThat(toTest.cancel()).isTrue();
            assertThat(blockMessaging.getSentVerificationNotifications())
                    .hasSize(1)
                    .first()
                    .returns(FailureInfo.standard(FailureType.CANCELLED), VerificationNotification::failureInfo);
        }

        /// This test aims to assert that the finished callback is invoked even
        /// when result handling itself fails unexpectedly, so a session can
        /// never stay in the buffer because its result could not be handled.
        @Test
        @DisplayName("the callback is invoked even when result handling fails")
        void testCallbackInvokedWhenResultHandlingFails() {
            recentlyVerifiedBlocks = new ConcurrentLinkedDeque<>() {
                @Override
                public boolean contains(final Object o) {
                    throw new IllegalStateException("result handling failure");
                }
            };
            final CompletableVerificationSession toTest = session();
            toTest.start();
            assertThat(toTest.cancel()).isTrue();
            assertThat(finishedKeys).containsExactly(new SessionKey(BLOCK_NUMBER, 0L));
        }
    }

    /// Tests for the cancellation contract.
    @Nested
    @DisplayName("Cancellation Tests")
    class CancellationTests {
        /// This test aims to assert that a session which was never started is
        /// not finished, and that cancelling it reports false without throwing
        /// and without any result or callback.
        @Test
        @DisplayName("cancelling a session that was never started reports false")
        void testCancelBeforeStartReportsFalse() {
            final CompletableVerificationSession toTest = session();
            assertThat(toTest.isFinished()).isFalse();
            assertThat(toTest.cancel()).isFalse();
            assertThat(blockMessaging.getSentVerificationNotifications()).isEmpty();
            assertThat(finishedKeys).isEmpty();
        }

        /// This test aims to assert that only the first cancellation of a
        /// running session reports true: a second cancellation reports false
        /// and causes no second result or callback.
        @Test
        @DisplayName("a second cancellation reports false and reports nothing twice")
        void testSecondCancelReportsFalse() {
            final CompletableVerificationSession toTest = session();
            toTest.start();
            assertThat(toTest.cancel()).isTrue();
            assertThat(toTest.cancel()).isFalse();
            assertThat(blockMessaging.getSentVerificationNotifications()).hasSize(1);
            assertThat(finishedKeys).hasSize(1);
        }

        /// This test aims to assert that cancelling a session which already
        /// produced its result reports false and does not report a second
        /// result nor invoke the callback again.
        @Test
        @DisplayName("cancelling a finished session reports false")
        void testCancelAfterCompletionReportsFalse() {
            final CompletableVerificationSession toTest = session();
            toTest.start();
            toTest.markEndOfBlockReceived();
            toTest.getBlockItemsDeque().offer(block().asBlockItems());
            executor.executeSerially();
            assertThat(toTest.isFinished()).isTrue();
            assertThat(toTest.cancel()).isFalse();
            assertThat(blockMessaging.getSentVerificationNotifications()).hasSize(1);
            assertThat(finishedKeys).hasSize(1);
        }
    }
}
