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
            final SessionKey sessionKey) {
        this.context = Objects.requireNonNull(context);
        this.verificationConfig = Objects.requireNonNull(verificationConfig);
        this.sessionResultMetrics = Objects.requireNonNull(sessionResultMetrics);
        this.badBlockDumper = Objects.requireNonNull(badBlockDumper);
        this.lastVerifiedBlock = Objects.requireNonNull(lastVerifiedBlock);
        this.blockSource = Objects.requireNonNull(blockSource);
        this.recentlyVerifiedBlocks = Objects.requireNonNull(recentlyVerifiedBlocks);
        this.finishedSessions = Objects.requireNonNull(finishedSessions);
        this.sessionKey = Objects.requireNonNull(sessionKey);
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
    ///
    /// @param throwable the failure to handle
    /// @return `true` if the reported failure type was [FailureType#UNKNOWN_ERROR]
    private boolean handleThrowable(final Throwable throwable) {
        final String message =
                "Session for block %d with source %s completed exceptionally".formatted(blockNumber, blockSource);
        final SemanticVersion hapiVersion;
        final List<BlockItemUnparsed> blockItems;
        if (throwable instanceof CompletionException ce
                && ce.getCause() instanceof VerificationSessionFailedException vfe) {
            hapiVersion = vfe.getHapiVersion();
            blockItems = vfe.getBlockItems();
        } else {
            hapiVersion = null;
            blockItems = null;
        }
        VerificationNotification notification = null;
        try {
            notification = switch (throwable) {
                case CancellationException ignored ->
                    new VerificationNotification(
                            false,
                            getFailureInfo(blockNumber, SessionFailureType.CANCELLED),
                            blockNumber,
                            null,
                            null,
                            blockSource);
                case CompletionException ce -> {
                    LOGGER.log(Level.WARNING, message, ce.getCause() != null ? ce.getCause() : ce);
                    yield processCompletionException(ce);
                }
                default -> {
                    LOGGER.log(Level.WARNING, message, throwable);
                    yield new VerificationNotification(
                            false,
                            getFailureInfo(blockNumber, SessionFailureType.UNKNOWN_ERROR),
                            blockNumber,
                            null,
                            null,
                            blockSource);
                }
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

    /// Process a completion exception case.
    ///
    /// @param ce the completion exception wrapping the actual cause
    /// @return a failure notification carrying the failure type of the cause, or
    ///     [SessionFailureType#UNKNOWN_ERROR] when the cause is not a known failure
    private VerificationNotification processCompletionException(final CompletionException ce) {
        final Throwable cause = ce.getCause();
        if (cause instanceof VerificationSessionFailedException vfe) {
            // instanceof covers null also
            return new VerificationNotification(
                    false,
                    getFailureInfo(vfe.getBlockNumber(), vfe.getFailureType()),
                    vfe.getBlockNumber(),
                    null,
                    null,
                    vfe.getBlockSource());
        } else {
            return new VerificationNotification(
                    false,
                    getFailureInfo(blockNumber, SessionFailureType.UNKNOWN_ERROR),
                    blockNumber,
                    null,
                    null,
                    blockSource);
        }
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

    /// Construct a [FailureInfo] in case of a failed session.
    /// If the block that just failed was recently verified, the failure is considered `informational`.
    private FailureInfo getFailureInfo(final long blockNumber, final SessionFailureType sessionFailureType) {
        final FailureType failureType = sessionFailureType.asFailureType();
        return recentlyVerifiedBlocks.contains(blockNumber)
                ? FailureInfo.informational(failureType)
                : FailureInfo.standard(failureType);
    }
}
