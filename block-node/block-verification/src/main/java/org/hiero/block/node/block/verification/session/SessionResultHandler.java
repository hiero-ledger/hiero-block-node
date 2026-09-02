// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import com.hedera.hapi.node.base.SemanticVersion;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.node.block.verification.BadBlockDumper;
import org.hiero.block.node.block.verification.VerificationConfig;
import org.hiero.block.node.block.verification.metrics.SessionResultMetrics;
import org.hiero.block.node.block.verification.session.BlockVerificationSession.SessionKey;
import org.hiero.block.node.block.verification.verifier.BlockVerificationResult;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification.FailureInfo;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification.FailureType;

/// The final stage of a [CompletableVerificationSession].
/// This stage handles the result of the verification process of a block.
public final class SessionResultHandler implements BiConsumer<BlockVerificationResult, Throwable> {
    /// Logger for the handler.
    private static final Logger LOGGER = System.getLogger(SessionResultHandler.class.getName());
    /// Message format for the informational log emitted for every cancelled session.
    private static final String CANCELLED_SESSION_MESSAGE =
            "Session for block {0} with source {1} was cancelled with type {2}";
    /// Message format for the warning log emitted for every session that failed
    /// with anything other than a cancellation.
    private static final String EXCEPTIONAL_SESSION_COMPLETION_MESSAGE =
            "Session for block %d with source %s completed exceptionally";
    /// The block node context, for access to messaging.
    private final BlockNodeContext context;
    /// The configuration for verification, source of the buffer sizes.
    private final VerificationConfig verificationConfig;
    /// Metrics recorded by this stage.
    private final SessionResultMetrics sessionResultMetrics;
    /// Dumps failing block bytes to disk for diagnostics.
    private final BadBlockDumper badBlockDumper;
    /// The last successfully verified block, shared across sessions.
    final AtomicLong lastVerifiedBlock;
    /// The number of the block this session verified.
    final long blockNumber;
    /// The source of the block.
    final BlockSource blockSource;
    /// The set of recently verified blocks, used for the informational failure check.
    final ConcurrentLinkedDeque<Long> recentlyVerifiedBlocks;
    /// The set this handler adds the session's key to once the result has been handled.
    private final ConcurrentSkipListSet<SessionKey> finishedSessions;
    /// The composite key of the owning session.
    private final SessionKey sessionKey;
    /// Flag raised by the owning session when the batch ending the block has
    /// been received, used to discriminate a cancelled complete block from an
    /// incomplete one.
    private final AtomicBoolean endOfBlockReceived;

    /// Constructor.
    ///
    /// @param context the block node context, must not be null
    /// @param verificationConfig the configuration for verification, must not be null
    /// @param sessionResultMetrics metrics recorded by this stage, must not be null
    /// @param badBlockDumper the bad block dumper for diagnostics, must not be null
    /// @param lastVerifiedBlock the last successfully verified block, must not be null
    /// @param recentlyVerifiedBlocks the set of recently verified blocks, must not be null
    /// @param blockNumber the number of the block the session verified, must be non-negative
    /// @param blockSource the source of the block, must not be null
    /// @param finishedSessions the set to add the session's key to when handled, must not be null
    /// @param sessionKey the composite key of the owning session, must not be null
    /// @param endOfBlockReceived flag raised by the owning session when the batch ending the
    ///     block has been received, must not be null
    public SessionResultHandler(
            final BlockNodeContext context,
            final VerificationConfig verificationConfig,
            final SessionResultMetrics sessionResultMetrics,
            final BadBlockDumper badBlockDumper,
            final AtomicLong lastVerifiedBlock,
            final ConcurrentLinkedDeque<Long> recentlyVerifiedBlocks,
            final long blockNumber,
            final BlockSource blockSource,
            final ConcurrentSkipListSet<SessionKey> finishedSessions,
            final SessionKey sessionKey,
            final AtomicBoolean endOfBlockReceived) {
        this.context = Objects.requireNonNull(context);
        this.verificationConfig = Objects.requireNonNull(verificationConfig);
        this.sessionResultMetrics = Objects.requireNonNull(sessionResultMetrics);
        this.badBlockDumper = Objects.requireNonNull(badBlockDumper);
        this.lastVerifiedBlock = Objects.requireNonNull(lastVerifiedBlock);
        this.blockSource = Objects.requireNonNull(blockSource);
        this.recentlyVerifiedBlocks = Objects.requireNonNull(recentlyVerifiedBlocks);
        this.finishedSessions = Objects.requireNonNull(finishedSessions);
        this.sessionKey = Objects.requireNonNull(sessionKey);
        this.endOfBlockReceived = Objects.requireNonNull(endOfBlockReceived);
        if (blockNumber < 0) {
            throw new IllegalArgumentException("Block number must be non-negative");
        } else {
            this.blockNumber = blockNumber;
        }
    }

    /// Accept a [BlockVerificationResult] in case of successful verification of a block,
    /// or a [Throwable] in case of an error or failure.
    /// The end result is propagated to messaging.
    @Override
    public void accept(final BlockVerificationResult verificationResult, final Throwable throwable) {
        try {
            if (handle(verificationResult, throwable)) {
                sessionResultMetrics.verificationBlocksError().increment();
            }
        } catch (final RuntimeException e) {
            // @todo mark the plugin unhealthy if we have reached this catch block
            final String message = "Failed to handle verification session with id %d result for block %d with source %s"
                    .formatted(sessionKey.uniqueId(), blockNumber, blockSource);
            LOGGER.log(Level.WARNING, message, e);
            final VerificationNotification notification = new VerificationNotification(
                    false,
                    getFailureInfo(blockNumber, SessionFailureType.UNKNOWN_ERROR),
                    blockNumber,
                    null,
                    null,
                    blockSource);
            safeSendNotification(notification);
            sessionResultMetrics.verificationBlocksError().increment();
        } finally {
            finishedSessions.add(sessionKey);
        }
    }

    /// Send a notification to messaging. Any failure to send is logged and
    /// swallowed; this method never throws.
    ///
    /// @param notification the notification to send
    private void safeSendNotification(final VerificationNotification notification) {
        try {
            context.blockMessaging().sendBlockVerification(notification);
        } catch (final RuntimeException e) {
            final String message =
                    "Failed to send verification notification for completed session with id %d for block %d with source %s"
                            .formatted(sessionKey.uniqueId(), blockNumber, blockSource);
            LOGGER.log(Level.WARNING, message, e);
        }
    }

    /// Handle the result of the session, routing to the success or failure path.
    ///
    /// @param verificationResult the successful result, may be null when a failure occurred
    /// @param throwable the failure, may be null when verification succeeded
    /// @return `true` if an unknown error occurred and the error metric must be incremented
    private boolean handle(final BlockVerificationResult verificationResult, final Throwable throwable) {
        final boolean hasUnknownErrorOccurred;
        if (throwable != null) {
            hasUnknownErrorOccurred = handleThrowable(throwable);
        } else if (verificationResult != null) {
            hasUnknownErrorOccurred = handleResult(verificationResult);
        } else {
            // This should not happen
            final String message =
                    "Received neither result, nor throwable for a verification session for block %d with source %s"
                            .formatted(blockNumber, blockSource);
            LOGGER.log(Level.WARNING, message);
            hasUnknownErrorOccurred = handleThrowable(
                    new VerificationSessionFailedException(blockNumber, SessionFailureType.UNKNOWN_ERROR, blockSource));
        }
        return hasUnknownErrorOccurred;
    }

    /// Handle a failed result. A failure notification carrying the appropriate
    /// failure type is sent, the failed metric is incremented, and a bad block
    /// dump is attempted when block items were captured at the failure site.
    /// Cancellations, whether observed as a direct cancellation of the stage
    /// chain or reported by a stage as a failure of a cancellation type, are
    /// expected lifecycle events and are logged at INFO; every other failure is
    /// logged at WARNING.
    ///
    /// @param throwable the failure to handle
    /// @return `true` if the reported failure type was [FailureType#UNKNOWN_ERROR]
    private boolean handleThrowable(final Throwable throwable) {
        SemanticVersion hapiVersion = null;
        List<BlockItemUnparsed> blockItems = null;
        VerificationNotification notification = null;
        try {
            notification = switch (throwable) {
                case CancellationException ignored -> processCancellation();
                case CompletionException ce -> {
                    if (ce.getCause() instanceof VerificationSessionFailedException vfe) {
                        hapiVersion = vfe.getHapiVersion();
                        blockItems = vfe.getBlockItems();
                        yield processVerificationSessionFailedCompletion(vfe);
                    } else {
                        yield processUnknownExceptionalCompletion(ce);
                    }
                }
                default -> processUnknownExceptionalCompletion(throwable);
            };
            safeSendNotification(notification);
            sessionResultMetrics.verificationBlocksFailed().increment();
        } finally {
            if (notification != null) {
                badBlockDumper.attemptDump(notification, hapiVersion, blockItems);
            }
        }
        return notification.failureInfo().failureType() == FailureType.UNKNOWN_ERROR;
    }

    /// Process a session that was stopped before producing a result, regardless of
    /// whether the stop was observed as a direct cancellation of the stage chain or
    /// reported by a stage as a failure of a cancellation type. A reported type is
    /// never trusted as is: only the owning session knows whether the batch ending
    /// the block was received, so this handler always resolves the type itself (see
    /// [#resolveCancellationType()]). A cancellation is an expected lifecycle event,
    /// e.g. an eviction from a full active sessions buffer, so it is logged at INFO.
    ///
    /// @return a failure notification carrying the resolved cancellation type
    private VerificationNotification processCancellation() {
        final SessionFailureType cancellationType = resolveCancellationType();
        LOGGER.log(Level.INFO, CANCELLED_SESSION_MESSAGE, blockNumber, blockSource, cancellationType);
        return new VerificationNotification(
                false, getFailureInfo(blockNumber, cancellationType), blockNumber, null, null, blockSource);
    }

    /// Process a session that failed with a known failure type, reported by one of
    /// its stages. Failures of a cancellation type are routed to
    /// [#processCancellation()]; any other known failure is logged at WARNING and
    /// reported with the failure type exactly as the stage supplied it.
    ///
    /// @param vfe the failure reported by the stage
    /// @return a failure notification carrying the resolved failure type
    private VerificationNotification processVerificationSessionFailedCompletion(
            final VerificationSessionFailedException vfe) {
        if (vfe.getFailureType() == SessionFailureType.CANCELLED
                || vfe.getFailureType() == SessionFailureType.CANCELLED_INCOMPLETE) {
            return processCancellation();
        } else {
            final String message = EXCEPTIONAL_SESSION_COMPLETION_MESSAGE.formatted(blockNumber, blockSource);
            LOGGER.log(Level.WARNING, message, vfe);
            return new VerificationNotification(
                    false,
                    getFailureInfo(vfe.getBlockNumber(), vfe.getFailureType()),
                    vfe.getBlockNumber(),
                    null,
                    null,
                    vfe.getBlockSource());
        }
    }

    /// Process a session that completed with an unexpected throwable carrying no
    /// known failure type. The throwable is logged at WARNING and the failure is
    /// reported as [SessionFailureType#UNKNOWN_ERROR].
    ///
    /// @param throwable the unexpected throwable the session completed with
    /// @return a failure notification carrying the unknown error failure type
    private VerificationNotification processUnknownExceptionalCompletion(final Throwable throwable) {
        final String message = EXCEPTIONAL_SESSION_COMPLETION_MESSAGE.formatted(blockNumber, blockSource);
        LOGGER.log(Level.WARNING, message, throwable);
        return new VerificationNotification(
                false,
                getFailureInfo(blockNumber, SessionFailureType.UNKNOWN_ERROR),
                blockNumber,
                null,
                null,
                blockSource);
    }

    /// Handle a successful verification result. A success notification is sent,
    /// the block is marked as recently verified, the last verified block is
    /// advanced, and the verified metric is incremented.
    ///
    /// @param verificationResult the successful result to handle
    /// @return always `false`, no unknown error can occur on the success path
    private boolean handleResult(final BlockVerificationResult verificationResult) {
        final long verifiedBlockNumber = verificationResult.blockNumber();
        final VerificationNotification notification = new VerificationNotification(
                true,
                null,
                verifiedBlockNumber,
                verificationResult.rootHash(),
                verificationResult.block(),
                verificationResult.source());
        // @todo safe sending of notification should return a result.
        //    If we were unable to send a notification, we should mark unhealthy
        //    once we have the updates to health plugin
        safeSendNotification(notification);
        markRecentlyVerified(verifiedBlockNumber);
        // Note that the below CAS has an interaction with the `allSourcesRequireOrdering` config.
        // If that is set to `false`, it is possible that gaps can happen, because sources, other than publisher,
        // can supply a valid block, much higher than last verified. This concern is understood and accepted.
        final long lastVerified = lastVerifiedBlock.get();
        if (verifiedBlockNumber > lastVerified) {
            if (!lastVerifiedBlock.compareAndSet(lastVerified, lastVerified + 1)) {
                final String message =
                        "Failed to increment last verified block number from {0}, for block {1}, current value is {2}";
                LOGGER.log(Level.INFO, message, lastVerified, verifiedBlockNumber, lastVerifiedBlock.get());
            }
        }
        sessionResultMetrics.verificationBlocksVerified().increment();
        return false;
    }

    /// Mark block as recently verified and keep the recently verified blocks
    /// deque size within limits.
    private void markRecentlyVerified(final long blockNumber) {
        recentlyVerifiedBlocks.offer(blockNumber);
        if (recentlyVerifiedBlocks.size() > verificationConfig.recentlyVerifiedBlocksBufferSize()) {
            recentlyVerifiedBlocks.pollFirst();
        }
    }

    /// Resolve the failure type for a session that was stopped before
    /// producing a result. A cancellation of a complete block means the
    /// supplier considers the block delivered, while an incomplete block was
    /// abandoned by the supplier before it ever finished.
    ///
    /// @return [SessionFailureType#CANCELLED] when the batch ending the block
    ///     was received, [SessionFailureType#CANCELLED_INCOMPLETE] otherwise
    private SessionFailureType resolveCancellationType() {
        return endOfBlockReceived.get() ? SessionFailureType.CANCELLED : SessionFailureType.CANCELLED_INCOMPLETE;
    }

    /// Construct a [FailureInfo] in case of a failed session.
    /// If the block that just failed was recently verified, the failure is considered `informational`.
    private FailureInfo getFailureInfo(final long blockNumber, final SessionFailureType sessionFailureType) {
        final FailureType failureType = sessionFailureType.asFailureType();
        return recentlyVerifiedBlocks.contains(blockNumber)
                ? FailureInfo.informational(failureType)
                : FailureInfo.standard(failureType);
    }
}
