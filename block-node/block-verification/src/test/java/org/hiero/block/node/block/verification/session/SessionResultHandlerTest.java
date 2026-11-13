// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.TestConfigurationBuilder;
import org.hiero.block.node.app.fixtures.TestUtils;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.block.verification.BadBlockDumper;
import org.hiero.block.node.block.verification.VerificationConfig;
import org.hiero.block.node.block.verification.VerificationOutbox;
import org.hiero.block.node.block.verification.metrics.MetricsHolder;
import org.hiero.block.node.block.verification.session.BlockVerificationSession.SessionKey;
import org.hiero.block.node.block.verification.verifier.BlockVerificationResult;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification.FailureInfo;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification.FailureType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

/// Tests for the [SessionResultHandler].
@DisplayName("Session Result Handler Tests")
class SessionResultHandlerTest {
    /// The number of the block the handler under test reports for.
    private static final long BLOCK_NUMBER = 10L;
    /// Captures the notifications the handler sends, and whether the result handled flag
    /// was up when each was sent.
    private FlagRecordingMessagingFacility blockMessaging;
    /// The last verified block handed to the handler under test.
    private AtomicLong lastVerifiedBlock;
    /// The recently verified blocks handed to the handler under test.
    private ConcurrentLinkedDeque<Long> recentlyVerifiedBlocks;
    /// The flag the owning session raises when the batch ending the block has been received.
    private AtomicBoolean endOfBlockReceived;
    /// The flag the handler under test raises once it has handled the result.
    private AtomicBoolean resultHandled;
    /// The instance under test.
    private SessionResultHandler toTest;

    /// Setup before each test.
    @BeforeEach
    void setUp() {
        resultHandled = new AtomicBoolean(false);
        blockMessaging = new FlagRecordingMessagingFacility(resultHandled);
        lastVerifiedBlock = new AtomicLong(BLOCK_NUMBER - 1);
        recentlyVerifiedBlocks = new ConcurrentLinkedDeque<>();
        final MetricsHolder metrics = MetricsHolder.create(TestUtils.createMetrics());
        final VerificationConfig verificationConfig = new TestConfigurationBuilder()
                .withConfigDataType(VerificationConfig.class)
                .getOrCreateConfig()
                .getConfigData(VerificationConfig.class);
        final BadBlockDumper badBlockDumper = new BadBlockDumper(verificationConfig, "test");
        endOfBlockReceived = new AtomicBoolean(false);
        toTest = new SessionResultHandler(
                new VerificationOutbox(blockMessaging),
                verificationConfig,
                metrics.sessionResultMetrics(),
                badBlockDumper,
                lastVerifiedBlock,
                recentlyVerifiedBlocks,
                BLOCK_NUMBER,
                BlockSource.PUBLISHER,
                new SessionKey(BLOCK_NUMBER, 0L),
                endOfBlockReceived,
                resultHandled);
    }

    /// Tests for the flag the handler raises once it has handled the result, which makes
    /// its session complete.
    @Nested
    @DisplayName("Result Handled Flag Tests")
    class ResultHandledFlagTests {
        /// This test aims to assert that the handler raises the result handled flag after it
        /// has sent the notification, whatever outcome it handles: a success, a stage
        /// failure, a cancellation, or neither a result nor a failure. The flag is down
        /// before the handler runs and still down while the notification is sent, so a
        /// session never becomes complete before its notification is out, and it is up
        /// once the handler returns.
        @ParameterizedTest(name = "{0}")
        @EnumSource(Outcome.class)
        @DisplayName("accept() raises the result handled flag once the notification is sent")
        void testRaisesTheResultHandledFlagOnceTheNotificationIsSent(final Outcome outcome) {
            assertThat(resultHandled).isFalse();
            final BlockVerificationResult result =
                    switch (outcome) {
                        case SUCCESS ->
                            new BlockVerificationResult(
                                    BLOCK_NUMBER,
                                    Bytes.wrap(new byte[32]),
                                    BlockUnparsed.DEFAULT,
                                    BlockSource.PUBLISHER);
                        case STAGE_FAILURE, CANCELLATION, NEITHER -> null;
                    };
            final Throwable failure =
                    switch (outcome) {
                        case SUCCESS, NEITHER -> null;
                        case STAGE_FAILURE ->
                            new CompletionException(new VerificationSessionFailedException(
                                    BLOCK_NUMBER, SessionFailureType.BAD_BLOCK_PROOF, BlockSource.PUBLISHER));
                        case CANCELLATION -> new CancellationException();
                    };
            toTest.accept(result, failure);
            assertThat(blockMessaging.getSentVerificationNotifications(1)).hasSize(1);
            assertThat(blockMessaging.resultHandledWhenSent()).containsExactly(false);
            assertThat(resultHandled).isTrue();
        }
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

    /// Tests for sending the result.
    @Nested
    @DisplayName("Result Reporting Tests")
    class ResultReportingTests {
        /// This test aims to assert that a successful result is sent as a success
        /// notification carrying the block and its root hash, and that only then the
        /// block is recorded as recently verified and the last verified block advances
        /// to it.
        @Test
        @DisplayName("accept() sends a success and advances the last verified block")
        void testSendsASuccessAndAdvancesTheLastVerifiedBlock() {
            final Bytes rootHash = Bytes.wrap(new byte[32]);
            toTest.accept(
                    new BlockVerificationResult(BLOCK_NUMBER, rootHash, BlockUnparsed.DEFAULT, BlockSource.PUBLISHER),
                    null);
            assertThat(blockMessaging.getSentVerificationNotifications(1))
                    .hasSize(1)
                    .first()
                    .returns(true, VerificationNotification::success)
                    .returns(BLOCK_NUMBER, VerificationNotification::blockNumber)
                    .returns(BlockUnparsed.DEFAULT, VerificationNotification::block)
                    .returns(rootHash, VerificationNotification::blockHash);
            assertThat(recentlyVerifiedBlocks).containsExactly(BLOCK_NUMBER);
            assertThat(lastVerifiedBlock.get()).isEqualTo(BLOCK_NUMBER);
        }

        /// This test aims to assert that a failed result is sent as a failure notification
        /// and changes neither the recently verified blocks nor the last verified block.
        @Test
        @DisplayName("accept() sends a failure and leaves the last verified block alone")
        void testSendsAFailureAndLeavesTheLastVerifiedBlockAlone() {
            toTest.accept(
                    null,
                    new CompletionException(new VerificationSessionFailedException(
                            BLOCK_NUMBER, SessionFailureType.BAD_BLOCK_PROOF, BlockSource.PUBLISHER)));
            assertThat(blockMessaging.getSentVerificationNotifications(1))
                    .hasSize(1)
                    .first()
                    .returns(false, VerificationNotification::success)
                    .returns(FailureInfo.standard(FailureType.BAD_BLOCK_PROOF), VerificationNotification::failureInfo);
            assertThat(recentlyVerifiedBlocks).isEmpty();
            assertThat(lastVerifiedBlock.get()).isEqualTo(BLOCK_NUMBER - 1);
        }
    }

    /// The outcomes a result handler can be handed.
    enum Outcome {
        /// A successful result.
        SUCCESS,
        /// A failure a stage reported.
        STAGE_FAILURE,
        /// A cancellation of the stage chain.
        CANCELLATION,
        /// Neither a result nor a failure.
        NEITHER
    }

    /// A messaging facility that records, for every verification notification it is
    /// sent, whether the result handled flag was up at that moment, and otherwise behaves
    /// like the [TestBlockMessagingFacility].
    private static final class FlagRecordingMessagingFacility extends TestBlockMessagingFacility {
        /// The flag whose value is recorded.
        private final AtomicBoolean resultHandled;
        /// The value of the flag at every send, in order.
        private final List<Boolean> resultHandledWhenSent;

        /// Constructor.
        ///
        /// @param resultHandled the flag whose value is recorded at every send
        private FlagRecordingMessagingFacility(final AtomicBoolean resultHandled) {
            this.resultHandled = resultHandled;
            this.resultHandledWhenSent = new ArrayList<>();
        }

        /// {@inheritDoc}
        @Override
        public void sendBlockVerification(final VerificationNotification notification) {
            resultHandledWhenSent.add(resultHandled.get());
            super.sendBlockVerification(notification);
        }

        /// The value of the flag at every send so far, in order.
        ///
        /// @return the recorded values
        private List<Boolean> resultHandledWhenSent() {
            return resultHandledWhenSent;
        }
    }
}
