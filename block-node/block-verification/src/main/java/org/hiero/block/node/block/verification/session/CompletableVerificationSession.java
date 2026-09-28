// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import static java.lang.System.Logger.Level.WARNING;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
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
    /// The priority of the session, derived from the delivery path.
    private final SessionPriority priority;
    /// The executor the stage chain runs on.
    private final ExecutorService executor;
    /// Cancellation flag shared with all stages of the session.
    private final AtomicBoolean isCancelled;
    /// Claimed by the one caller of [#cancel()] that actually cancels the chain.
    private final AtomicBoolean cancelRequested;
    /// Flag raised when the batch ending the block has been received, shared
    /// with the result handling stage so it can discriminate between a
    /// cancelled complete block and an incomplete one.
    private final AtomicBoolean endOfBlockReceived;
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
    /// Invoked with this session's key once its result has been handled.
    private final Consumer<SessionKey> onFinished;
    /// The configuration for verification.
    private final VerificationConfig verificationConfig;
    /// Dumps failing block bytes to disk for diagnostics.
    private final BadBlockDumper badBlockDumper;
    /// The stage chain, saved just before the terminating stages so it can be cancelled.
    private volatile CompletableFuture<BlockVerificationResult> sessionCompletionChain;

    /// Constructor.
    ///
    /// @param uniqueId the unique id for this session
    /// @param blockNumber the number of the block to verify, must be non-negative
    /// @param metricsHolder the holder for all verification metrics, must not be null
    /// @param blockSource the source of the block, must not be null
    /// @param priority the priority of the session, must not be null
    /// @param verificationDataProvider provider of the verification data, must not be null
    /// @param lastVerifiedBlock the last successfully verified block, must not be null
    /// @param recentlyVerifiedBlocks the set of recently verified blocks, must not be null
    /// @param executor the executor the stage chain runs on, must not be null
    /// @param context the block node context, must not be null
    /// @param verificationConfig the configuration for verification, must not be null
    /// @param onFinished invoked with this session's key once its result has been
    ///     handled, on whichever thread handled it; must be non-blocking, must not be null
    /// @param badBlockDumper the bad block dumper for diagnostics, must not be null
    public CompletableVerificationSession(
            final long uniqueId,
            final long blockNumber,
            final MetricsHolder metricsHolder,
            final BlockSource blockSource,
            final SessionPriority priority,
            final VerificationDataProvider verificationDataProvider,
            final AtomicLong lastVerifiedBlock,
            final ConcurrentLinkedDeque<Long> recentlyVerifiedBlocks,
            final ExecutorService executor,
            final BlockNodeContext context,
            final VerificationConfig verificationConfig,
            final Consumer<SessionKey> onFinished,
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
        this.priority = Objects.requireNonNull(priority);
        this.executor = Objects.requireNonNull(executor);
        this.isCancelled = new AtomicBoolean(false);
        this.cancelRequested = new AtomicBoolean(false);
        this.endOfBlockReceived = new AtomicBoolean(false);
        this.blockItemsDeque = new ConcurrentLinkedDeque<>();
        this.verificationConfig = Objects.requireNonNull(verificationConfig);
        this.onFinished = Objects.requireNonNull(onFinished);
        this.badBlockDumper = Objects.requireNonNull(badBlockDumper);
    }

    /// {@inheritDoc}
    @Override
    public SessionKey sessionKey() {
        return sessionKey;
    }

    /// {@inheritDoc}
    @Override
    public SessionPriority priority() {
        return priority;
    }

    /// {@inheritDoc}
    @Override
    public BlockSource blockSource() {
        return blockSource;
    }

    /// {@inheritDoc}
    @Override
    public boolean isEndOfBlockReceived() {
        return endOfBlockReceived.get();
    }

    /// {@inheritDoc}
    /// ---
    /// The chain is done before any of its terminating stages runs, so a
    /// finished session may still be handling its result or about to remove
    /// itself from the active sessions buffer.
    @Override
    public boolean isFinished() {
        final CompletableFuture<BlockVerificationResult> localChain = sessionCompletionChain;
        return localChain != null && localChain.isDone();
    }

    /// {@inheritDoc}
    /// ---
    /// This session type constructs a chain of [CompletableFuture]s.
    /// This chain is constructed of distinct stages which we need to pass
    /// through to verify the integrity of a block:
    /// ```
    /// BlockHasher -> BlockVerifier -> ResultOrderingManager -> SessionResultHandler -> onFinished
    /// ```
    /// 1. `BlockHasher` - dynamically hashes the block's items and produces a
    ///    [org.hiero.block.node.block.verification.hasher.HashingResult].
    /// 1. `BlockVerifier` - receives the result of the hasher and verifies all
    ///    block proofs.
    /// 1. `ResultOrderingManager` - receives the result of the verifier and waits
    ///    for an order in case hashing and verification have passed.
    /// 1. `SessionResultHandler` - receives the result of the chain. Propagates the
    ///    result to messaging and manages some internal state.
    /// 1. `onFinished` - receives the key of this session once the result has been
    ///    handled, whatever the outcome, so the session can leave the active
    ///    sessions buffer.
    ///
    /// The first three stages run on one executor thread. The two terminating
    /// stages run on that same thread when the chain completes on its own, and
    /// on the cancelling thread when the chain is cancelled.
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
                sessionKey,
                endOfBlockReceived);
        final CompletableFuture<BlockVerificationResult> completionChain = CompletableFuture.supplyAsync(
                        hasher, executor)
                .thenApply(verifier)
                .thenApply(resultOrderManager);
        // Note that we save the completion chain just before the terminating stages: cancelling it
        // runs them with a CancellationException, whereas cancelling a dependent stage would skip them.
        completionChain
                .whenComplete(sessionResultHandler)
                .whenComplete((result, throwable) -> onFinished.accept(sessionKey));
        sessionCompletionChain = completionChain;
    }

    /// {@inheritDoc}
    /// ---
    /// Cancels the stage chain and raises the shared cancellation flag so that any
    /// stage currently running can observe it and stop. Only the first caller can
    /// cancel the chain, and only while the chain has not produced its result:
    /// cancelling a chain that has already completed is a no-op that reports
    /// `false`, the result of the session was, or will be, handled normally.
    @Override
    public boolean cancel() {
        final CompletableFuture<BlockVerificationResult> localChain = sessionCompletionChain;
        try {
            final boolean result;
            if (localChain == null) {
                final String message =
                        "Session with id %d for block %d with source %s cannot be cancelled before it starts"
                                .formatted(sessionKey.uniqueId(), blockNumber, blockSource);
                LOGGER.log(WARNING, message);
                result = false;
            } else if (cancelRequested.compareAndSet(false, true)) {
                result = localChain.cancel(true);
            } else {
                result = false;
            }
            return result;
        } finally {
            isCancelled.set(true);
        }
    }

    /// {@inheritDoc}
    @Override
    public void markEndOfBlockReceived() {
        endOfBlockReceived.set(true);
    }

    /// {@inheritDoc}
    @Override
    public ConcurrentLinkedDeque<BlockItems> getBlockItemsDeque() {
        return blockItemsDeque;
    }
}
