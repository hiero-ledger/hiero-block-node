// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification.session;

import static java.lang.System.Logger.Level.INFO;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.verification.BadBlockDumper;
import org.hiero.block.node.verification.VerificationConfig;
import org.hiero.block.node.verification.VerificationDataProvider;
import org.hiero.block.node.verification.hasher.BlockHasher;
import org.hiero.block.node.verification.metrics.MetricsHolder;
import org.hiero.block.node.verification.verifier.BlockVerificationResult;
import org.hiero.block.node.verification.verifier.BlockVerifier;

/// An implementation of the [BlockVerificationSession] interface.
public final class CompletableVerificationSession implements BlockVerificationSession {
    private static final System.Logger LOGGER = System.getLogger(CompletableVerificationSession.class.getName());
    private final SessionKey sessionKey;
    private final MetricsHolder metricsHolder;
    private final long blockNumber;
    private final BlockSource blockSource;
    private final ExecutorService executor;
    private final AtomicBoolean isCancelled;
    private final BlockNodeContext context;
    private final AtomicLong lastVerifiedBlock;
    private final ConcurrentSkipListSet<Long> recentlyVerifiedBlocks;
    private final VerificationDataProvider verificationDataProvider;
    private final ConcurrentLinkedDeque<BlockItems> blockItemsDeque;
    private final ConcurrentSkipListSet<SessionKey> finishedSessions;
    private final VerificationConfig verificationConfig;
    private final BadBlockDumper badBlockDumper;
    private volatile CompletableFuture<BlockVerificationResult> sessionCompletionChain;

    /// Constructor.
    public CompletableVerificationSession(
            final long uniqueId,
            final long blockNumber,
            final MetricsHolder metricsHolder,
            final BlockSource blockSource,
            final VerificationDataProvider verificationDataProvider,
            final AtomicLong lastVerifiedBlock,
            final ConcurrentSkipListSet<Long> recentlyVerifiedBlocks,
            final ExecutorService executor,
            final BlockNodeContext context,
            final VerificationConfig verificationConfig,
            final ConcurrentSkipListSet<SessionKey> finishedSessions,
            final BadBlockDumper badBlockDumper) {
        if (blockNumber < 0) {
            throw new IllegalArgumentException("Block number must be non-negative");
        }
        this.sessionKey = new SessionKey(blockNumber, uniqueId);
        this.blockNumber = blockNumber;
        this.context = Objects.requireNonNull(context);
        this.lastVerifiedBlock = Objects.requireNonNull(lastVerifiedBlock);
        this.recentlyVerifiedBlocks = Objects.requireNonNull(recentlyVerifiedBlocks);
        this.verificationDataProvider = Objects.requireNonNull(verificationDataProvider);
        this.metricsHolder = Objects.requireNonNull(metricsHolder);
        this.blockSource = Objects.requireNonNull(blockSource);
        this.executor = Objects.requireNonNull(executor);
        this.isCancelled = new AtomicBoolean(false);
        this.blockItemsDeque = new ConcurrentLinkedDeque<>();
        this.verificationConfig = Objects.requireNonNull(verificationConfig);
        this.finishedSessions = Objects.requireNonNull(finishedSessions);
        this.badBlockDumper = Objects.requireNonNull(badBlockDumper);
    }

    @Override
    public SessionKey sessionKey() {
        return sessionKey;
    }

    /// {@inheritDoc}
    /// ---
    /// This session type constructs a chain of [CompletableFuture]s.
    /// This chain is constructed of distinct stages which we need to pass
    /// through to verify the integrity of a block:
    /// ```
    /// BlockHasher -> BlockVerifier -> ResultOrderingManager -> SessionResultHandler
    /// ```
    /// 1. `BlockHasher` - dynamically hashes the block's items and produces a
    ///    [org.hiero.block.node.verification.hasher.HashingResult].
    /// 1. `BlockVerifier` - receives the result of the hasher and verifies all
    ///    block proofs.
    /// 1. `ResultOrderingManager` - receives the result of the verifier and waits
    ///    for an order in case hashing and verification have passed.
    /// 1. `SessionResultHandler` - receives the result of the chain. Propagates the
    ///    result to messaging and manages some internal state.
    @Override
    public void start() {
        final BlockHasher hasher = new BlockHasher(
                isCancelled,
                blockItemsDeque,
                metricsHolder.hashingMetrics(),
                blockNumber,
                blockSource,
                verificationDataProvider);
        final BlockVerifier verifier = new BlockVerifier(
                isCancelled, metricsHolder.proofVerificationMetrics(), System.nanoTime(), verificationDataProvider);
        final ResultOrderingManager resultOrderManager =
                new ResultOrderingManager(isCancelled, lastVerifiedBlock, verificationConfig);
        final SessionResultHandler sessionResultHandler = new SessionResultHandler(
                context,
                metricsHolder.sessionResultMetrics(),
                badBlockDumper,
                lastVerifiedBlock,
                recentlyVerifiedBlocks,
                blockNumber,
                blockSource,
                finishedSessions,
                sessionKey);
        final CompletableFuture<BlockVerificationResult> completionChain = CompletableFuture.supplyAsync(
                        hasher, executor)
                .thenApply(verifier)
                .thenApply(resultOrderManager);
        completionChain.whenComplete(sessionResultHandler);
        sessionCompletionChain = completionChain;
    }

    @Override
    public void cancel() {
        // todo(2528) verify correct cancellation of session
        final CompletableFuture<BlockVerificationResult> localChain = sessionCompletionChain;
        try {
            if (localChain != null) {
                localChain.cancel(true);
            } else {
                final String message = "Session with id %d for block %d with source %s canceled before it was started"
                        .formatted(sessionKey.uniqueId(), blockNumber, blockSource);
                throw new IllegalStateException(message);
            }
        } finally {
            isCancelled.set(true);
        }
    }

    @Override
    public ConcurrentLinkedDeque<BlockItems> getBlockItemsDeque() {
        return blockItemsDeque;
    }

    @Override
    public boolean complete() {
        final boolean result;
        try {
            final CompletableFuture<BlockVerificationResult> localSession = sessionCompletionChain;
            if (localSession != null) {
                if (localSession.isDone() && !localSession.isCancelled()) {
                    if (localSession.isCompletedExceptionally()) {
                        // just call exception now, the whenComplete stage will handle a possible
                        // thrown exception
                        localSession.exceptionNow();
                    } else {
                        final BlockVerificationResult blockVerificationResult = localSession.resultNow();
                        if (blockVerificationResult == null) {
                            final String message =
                                    "Verification session with id %d for block %d with source %s completes with null result"
                                            .formatted(sessionKey.uniqueId(), blockNumber, blockSource);
                            LOGGER.log(INFO, message);
                        }
                    }
                    result = true;
                } else
                    // if canceled, the session is completed, otherwise it is not done yet
                    result = localSession.isCancelled();
            } else {
                final String message = "Session with id %d for block %d with source %s completed before it was started"
                        .formatted(sessionKey.uniqueId(), blockNumber, blockSource);
                throw new IllegalStateException(message);
            }
        } catch (final RuntimeException e) {
            // If an exception occurs during the completion of the session, we still want to move forward
            final String message =
                    "Exception occurred during completion of verification session with id %d for block %d with source %s"
                            .formatted(sessionKey.uniqueId(), blockNumber, blockSource);
            LOGGER.log(INFO, message, e);
            return true;
        }
        return result;
    }
}
