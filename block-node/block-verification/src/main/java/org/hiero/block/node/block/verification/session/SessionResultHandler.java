// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import com.hedera.hapi.node.base.SemanticVersion;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.node.block.verification.BadBlockDumper;
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
    private static final Logger LOGGER = System.getLogger(SessionResultHandler.class.getName());
    private static final int MAX_RECENTLY_VERIFIED_BLOCKS = 100; // todo(2528) make configurable?
    private final BlockNodeContext context;
    private final SessionResultMetrics sessionResultMetrics;
    private final BadBlockDumper badBlockDumper;
    final AtomicLong lastVerifiedBlock;
    final long blockNumber;
    final BlockSource blockSource;
    final ConcurrentSkipListSet<Long> recentlyVerifiedBlocks;
    private final ConcurrentSkipListSet<SessionKey> finishedSessions;
    private final SessionKey sessionKey;

    /// Constructor.
    public SessionResultHandler(
            final BlockNodeContext context,
            final SessionResultMetrics sessionResultMetrics,
            final BadBlockDumper badBlockDumper,
            final AtomicLong lastVerifiedBlock,
            final ConcurrentSkipListSet<Long> recentlyVerifiedBlocks,
            final long blockNumber,
            final BlockSource blockSource,
            final ConcurrentSkipListSet<SessionKey> finishedSessions,
            final SessionKey sessionKey) {
        this.context = Objects.requireNonNull(context);
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
            // todo(2528) this try-catch is to be resilient in case of unhandled cases
            //    we can reconsider it, but this should never happen. We can consider to continue, as we do now
            //    by sending a notification, or simply fail. In general, if we have reached here, we most likely
            //    have an issue we need to investigate. We can consider to signal server health that we are
            //    not healthy and log the error instead of trying to send a notification.
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

    /// Send a notification to messaging.
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

    /// Handle the result of the session.
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

    /// Handle a failed result.
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

    /// Handle a successful verification result.
    private boolean handleResult(final BlockVerificationResult verificationResult) {
        final VerificationNotification notification = new VerificationNotification(
                true,
                null,
                verificationResult.blockNumber(),
                verificationResult.rootHash(),
                verificationResult.block(),
                verificationResult.source());
        safeSendNotification(notification);
        // todo(2528) shall we mark verified after the CAS below?
        markRecentlyVerified(verificationResult.blockNumber());
        // todo(2528) ensure the correct CAS and sequence of actions
        final long lastVerified = lastVerifiedBlock.get();
        if (verificationResult.blockNumber() > lastVerified) {
            final boolean incremented = lastVerifiedBlock.compareAndSet(lastVerified, lastVerified + 1);
            if (!incremented) {
                // todo(2528) think about this case
                LOGGER.log(Level.INFO, "Failed to increment last verified block number");
            }
        }
        sessionResultMetrics.verificationBlocksVerified().increment();
        return false;
    }

    /// Mark block as recently verified and keep the recently verified blocks
    /// set size within limits.
    private void markRecentlyVerified(final long blockNumber) {
        if (blockNumber >= lastVerifiedBlock.get() - MAX_RECENTLY_VERIFIED_BLOCKS) {
            // only add to the recently verified blocks if we are close to the
            // last verified block
            recentlyVerifiedBlocks.add(blockNumber);
        }
        if (recentlyVerifiedBlocks.size() > MAX_RECENTLY_VERIFIED_BLOCKS) {
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
