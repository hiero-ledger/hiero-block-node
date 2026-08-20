// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.block.node.app.fixtures.TestConfigurationBuilder;
import org.hiero.block.node.app.fixtures.TestUtils;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.block.verification.BadBlockDumper;
import org.hiero.block.node.block.verification.VerificationConfig;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/// Tests for the [SessionResultHandler].
@DisplayName("Session Result Handler Tests")
class SessionResultHandlerTest {
    /// The number of the block the handler under test reports for.
    private static final long BLOCK_NUMBER = 10L;
    /// Captures the notifications the handler sends.
    private TestBlockMessagingFacility blockMessaging;
    /// The flag the owning session raises when the batch ending the block has been received.
    private AtomicBoolean endOfBlockReceived;
    /// The instance under test.
    private SessionResultHandler toTest;

    /// Setup before each test.
    @BeforeEach
    void setUp() {
        blockMessaging = new TestBlockMessagingFacility();
        final BlockNodeContext context = new BlockNodeContext(
                null, null, null, blockMessaging, null, null, null, null, null, null, null, null, null);
        final MetricsHolder metrics = MetricsHolder.create(TestUtils.createMetrics());
        final VerificationConfig verificationConfig = new TestConfigurationBuilder()
                .withConfigDataType(VerificationConfig.class)
                .getOrCreateConfig()
                .getConfigData(VerificationConfig.class);
        final BadBlockDumper badBlockDumper = new BadBlockDumper(verificationConfig, "test");
        endOfBlockReceived = new AtomicBoolean(false);
        toTest = new SessionResultHandler(
                context,
                verificationConfig,
                metrics.sessionResultMetrics(),
                badBlockDumper,
                new AtomicLong(-1),
                new ConcurrentLinkedDeque<>(),
                BLOCK_NUMBER,
                BlockSource.PUBLISHER,
                new ConcurrentSkipListSet<>(),
                new SessionKey(BLOCK_NUMBER, 0L),
                endOfBlockReceived);
    }

    /// Tests for the classification of sessions that were stopped before
    /// producing a result. Only the session knows whether the batch ending
    /// the block was received, so the handler owns the refinement into
    /// [FailureType#CANCELLED] and [FailureType#CANCELLED_INCOMPLETE].
    @Nested
    @DisplayName("Cancellation Classification Tests")
    class CancellationClassificationTests {
        /// This test aims to assert that when the session's stage chain is
        /// cancelled directly (the handler receives a [CancellationException])
        /// and the batch ending the block was never received, the handler
        /// reports a failure of type [FailureType#CANCELLED_INCOMPLETE].
        @Test
        @DisplayName("accept() reports CANCELLED_INCOMPLETE on CancellationException when end of block not received")
        void testCancellationExceptionIncompleteBlock() {
            toTest.accept(null, new CancellationException());
            assertSingleFailureOfType(FailureType.CANCELLED_INCOMPLETE);
        }

        /// This test aims to assert that when the session's stage chain is
        /// cancelled directly (the handler receives a [CancellationException])
        /// and the batch ending the block was received, the handler reports a
        /// failure of type [FailureType#CANCELLED].
        @Test
        @DisplayName("accept() reports CANCELLED on CancellationException when end of block received")
        void testCancellationExceptionCompleteBlock() {
            endOfBlockReceived.set(true);
            toTest.accept(null, new CancellationException());
            assertSingleFailureOfType(FailureType.CANCELLED);
        }

        /// This test aims to assert that when a stage reports a plain
        /// cancellation (a [VerificationSessionFailedException] of type
        /// [SessionFailureType#CANCELLED], reachable when a stage is
        /// interrupted without the chain itself being cancelled, e.g. on an
        /// executor shutdown) and the batch ending the block was never
        /// received, the handler refines the failure to
        /// [FailureType#CANCELLED_INCOMPLETE].
        @Test
        @DisplayName(
                "accept() refines a stage-reported CANCELLED to CANCELLED_INCOMPLETE when end of block not received")
        void testStageCancellationIncompleteBlock() {
            toTest.accept(null, stageCancellation());
            assertSingleFailureOfType(FailureType.CANCELLED_INCOMPLETE);
        }

        /// This test aims to assert that when a stage reports a plain
        /// cancellation (a [VerificationSessionFailedException] of type
        /// [SessionFailureType#CANCELLED], reachable when a stage is
        /// interrupted without the chain itself being cancelled, e.g. on an
        /// executor shutdown) and the batch ending the block was received,
        /// the handler keeps the failure as [FailureType#CANCELLED].
        @Test
        @DisplayName("accept() keeps a stage-reported CANCELLED when end of block received")
        void testStageCancellationCompleteBlock() {
            endOfBlockReceived.set(true);
            toTest.accept(null, stageCancellation());
            assertSingleFailureOfType(FailureType.CANCELLED);
        }

        /// This test aims to assert that the refinement applies only to
        /// stage-reported cancellations: a stage failure of any other type is
        /// reported as is, regardless of whether the batch ending the block
        /// was received.
        @ParameterizedTest
        @EnumSource(value = SessionFailureType.class, names = "CANCELLED", mode = EnumSource.Mode.EXCLUDE)
        @DisplayName("accept() does not refine stage failures of other types")
        void testOtherStageFailureNotRefined(final SessionFailureType failureType) {
            endOfBlockReceived.set(true);
            toTest.accept(
                    null,
                    new CompletionException(
                            new VerificationSessionFailedException(BLOCK_NUMBER, failureType, BlockSource.PUBLISHER)));
            assertSingleFailureOfType(failureType.asFailureType());
        }

        /// Builds the throwable the handler receives when a stage reports a
        /// plain cancellation.
        private CompletionException stageCancellation() {
            return new CompletionException(new VerificationSessionFailedException(
                    BLOCK_NUMBER, SessionFailureType.CANCELLED, BlockSource.PUBLISHER));
        }

        /// Asserts that exactly one failure notification was sent, carrying
        /// the given standard failure type for the expected block.
        private void assertSingleFailureOfType(final FailureType expectedType) {
            final List<VerificationNotification> notifications = blockMessaging.getSentVerificationNotifications(1);
            assertThat(notifications)
                    .hasSize(1)
                    .first()
                    .returns(false, VerificationNotification::success)
                    .returns(FailureInfo.standard(expectedType), VerificationNotification::failureInfo)
                    .returns(BLOCK_NUMBER, VerificationNotification::blockNumber)
                    .returns(BlockSource.PUBLISHER, VerificationNotification::source)
                    .returns(null, VerificationNotification::block)
                    .returns(null, VerificationNotification::blockHash);
        }
    }
}
