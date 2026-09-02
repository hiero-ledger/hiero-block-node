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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
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
        /// This test aims to assert that when the session's stage chain is cancelled
        /// directly (the handler receives a [CancellationException]), the reported
        /// failure type is resolved purely from whether the batch ending the block
        /// was received: [FailureType#CANCELLED] when it was, so the supplier
        /// considers the block delivered, and [FailureType#CANCELLED_INCOMPLETE]
        /// when it was not, so the block was abandoned before it ever finished.
        /// Both values of the end of block flag are exercised.
        @ParameterizedTest
        @CsvSource({"true, CANCELLED", "false, CANCELLED_INCOMPLETE"})
        @DisplayName("accept() resolves the type of a direct cancellation from the end of block flag")
        void testDirectCancellationResolution(final boolean isEndOfBlockReceived, final FailureType expectedType) {
            endOfBlockReceived.set(isEndOfBlockReceived);
            toTest.accept(null, new CancellationException());
            assertSingleFailureOfType(expectedType);
        }

        /// This test aims to assert that the handler is the sole authority on the
        /// cancellation type. When a stage reports a cancellation (a
        /// [VerificationSessionFailedException] of either cancellation type,
        /// reachable when a stage is interrupted without the chain itself being
        /// cancelled, e.g. on an executor shutdown), the reported type is never
        /// trusted as is: for every combination of reported cancellation type and
        /// end of block flag, the resulting type is resolved purely from whether the
        /// batch ending the block was received, [FailureType#CANCELLED] when it was
        /// and [FailureType#CANCELLED_INCOMPLETE] when it was not. All four
        /// combinations are exercised.
        @ParameterizedTest
        @CsvSource({
            "CANCELLED, true, CANCELLED",
            "CANCELLED, false, CANCELLED_INCOMPLETE",
            "CANCELLED_INCOMPLETE, true, CANCELLED",
            "CANCELLED_INCOMPLETE, false, CANCELLED_INCOMPLETE"
        })
        @DisplayName("accept() resolves the type of a stage-reported cancellation from the end of block flag")
        void testStageCancellationResolution(
                final SessionFailureType reportedType,
                final boolean isEndOfBlockReceived,
                final FailureType expectedType) {
            endOfBlockReceived.set(isEndOfBlockReceived);
            toTest.accept(
                    null,
                    new CompletionException(
                            new VerificationSessionFailedException(BLOCK_NUMBER, reportedType, BlockSource.PUBLISHER)));
            assertSingleFailureOfType(expectedType);
        }

        /// This test aims to assert that the resolution applies only to
        /// stage-reported cancellations: a stage failure of any non-cancellation
        /// type is reported as is, regardless of whether the batch ending the
        /// block was received.
        @ParameterizedTest
        @EnumSource(
                value = SessionFailureType.class,
                names = {"CANCELLED", "CANCELLED_INCOMPLETE"},
                mode = EnumSource.Mode.EXCLUDE)
        @DisplayName("accept() does not refine stage failures of non-cancellation types")
        void testOtherStageFailureNotRefined(final SessionFailureType failureType) {
            endOfBlockReceived.set(true);
            toTest.accept(
                    null,
                    new CompletionException(
                            new VerificationSessionFailedException(BLOCK_NUMBER, failureType, BlockSource.PUBLISHER)));
            assertSingleFailureOfType(failureType.asFailureType());
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
