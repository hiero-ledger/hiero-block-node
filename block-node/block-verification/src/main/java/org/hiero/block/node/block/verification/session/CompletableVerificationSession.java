// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import static java.lang.System.Logger.Level.INFO;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.block.node.block.verification.BadBlockDumper;
import org.hiero.block.node.block.verification.VerificationConfig;
import org.hiero.block.node.block.verification.VerificationDataProvider;
import org.hiero.block.node.block.verification.hasher.BlockHasher;
import org.hiero.block.node.block.verification.metrics.MetricsHolder;
import org.hiero.block.node.block.verification.verifier.BlockVerificationResult;
import org.hiero.block.node.block.verification.verifier.BlockVerifier;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;

/// An implementation of the [BlockVerificationSession] interface.
public final class CompletableVerificationSession implements BlockVerificationSession {
    /// Logger for the session.
    private static final System.Logger LOGGER = System.getLogger(CompletableVerificationSession.class.getName());
    /// The composite key of this session (block number and unique id).
    private final SessionKey sessionKey;
    /// The holder for all verification metrics, distributed to the stages.
    private final MetricsHolder metricsHolder;
    /// The number of the block this session verifies.
    private final long blockNumber;
    /// The source of the block.
    private final BlockSource blockSource;
    /// The executor the stage chain runs on.
    private final ExecutorService executor;
    /// Cancellation flag shared with all stages of the session.
    private final AtomicBoolean isCancelled;
    /// The block node context, for access to core facilities.
    private final BlockNodeContext context;
    /// The last successfully verified block, shared across sessions.
    private final AtomicLong lastVerifiedBlock;
    /// The set of recently verified blocks, shared across sessions.
    private final ConcurrentLinkedDeque<Long> recentlyVerifiedBlocks;
    /// Provider of the verification data (TSS data and RSA public keys).
    private final VerificationDataProvider verificationDataProvider;
    /// The deque through which the block's item batches are supplied to the hashing stage.
    private final ConcurrentLinkedDeque<BlockItems> blockItemsDeque;
    /// The set this session adds its key to once its result has been handled.
    private final ConcurrentSkipListSet<SessionKey> finishedSessions;
    /// The configuration for verification.
    private final VerificationConfig verificationConfig;
    /// Dumps failing block bytes to disk for diagnostics.
    private final BadBlockDumper badBlockDumper;
    /// The stage chain, saved just before the terminating stage so it can be cancelled.
    private volatile CompletableFuture<BlockVerificationResult> sessionCompletionChain;

    /// Constructor.
    ///
    /// @param uniqueId the unique id for this session
    /// @param blockNumber the number of the block to verify, must be non-negative
    /// @param metricsHolder the holder for all verification metrics, must not be null
    /// @param blockSource the source of the block, must not be null
    /// @param verificationDataProvider provider of the verification data, must not be null
    /// @param lastVerifiedBlock the last successfully verified block, must not be null
    /// @param recentlyVerifiedBlocks the set of recently verified blocks, must not be null
    /// @param executor the executor the stage chain runs on, must not be null
    /// @param context the block node context, must not be null
    /// @param verificationConfig the configuration for verification, must not be null
    /// @param finishedSessions the set to add this session's key to when finished, must not be null
    /// @param badBlockDumper the bad block dumper for diagnostics, must not be null
    public CompletableVerificationSession(
            final long uniqueId,
            final long blockNumber,
            final MetricsHolder metricsHolder,
            final BlockSource blockSource,
            final VerificationDataProvider verificationDataProvider,
            final AtomicLong lastVerifiedBlock,
            final ConcurrentLinkedDeque<Long> recentlyVerifiedBlocks,
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

    /// {@inheritDoc}
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
    ///    [org.hiero.block.node.block.verification.hasher.HashingResult].
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
                verificationConfig,
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
        // Note that we save the completion chain just before the terminating stage `whenComplete`
        // so we can cancel that to work around a bug in CompletableFuture.
        completionChain.whenComplete(sessionResultHandler);
        sessionCompletionChain = completionChain;
    }

    /// {@inheritDoc}
    /// ---
    /// Cancels the stage chain and raises the shared cancellation flag so that any
    /// stage currently running can observe it and stop.
    ///
    /// @throws IllegalStateException if the session was never started
    @Override
    public void cancel() {
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

    /// {@inheritDoc}
    @Override
    public ConcurrentLinkedDeque<BlockItems> getBlockItemsDeque() {
        return blockItemsDeque;
    }

    /// {@inheritDoc}
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
