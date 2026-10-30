// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.block.node.block.verification.BadBlockDumper;
import org.hiero.block.node.block.verification.VerificationConfig;
import org.hiero.block.node.block.verification.VerificationDataProvider;
import org.hiero.block.node.block.verification.VerificationOutbox;
import org.hiero.block.node.block.verification.hasher.BlockHasher;
import org.hiero.block.node.block.verification.metrics.MetricsHolder;
import org.hiero.block.node.block.verification.verifier.BlockVerificationResult;
import org.hiero.block.node.block.verification.verifier.BlockVerifier;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;

/// An implementation of the [BlockVerificationSession] interface.
public final class CompletableVerificationSession implements BlockVerificationSession {
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
    /// Cancellation flag shared with all stages of the session. Raised once, by the first
    /// [#cancel()] call that finds the stage chain cancelled.
    private final AtomicBoolean isCancelled;
    /// Flag raised when the batch ending the block has been received, shared
    /// with the result handling stage so it can discriminate between a
    /// cancelled complete block and an incomplete one.
    private final AtomicBoolean endOfBlockReceived;
    /// The outbox the result handling stage sends the session's notification through.
    private final VerificationOutbox outbox;
    /// The last successfully verified block, shared across sessions.
    private final AtomicLong lastVerifiedBlock;
    /// The set of recently verified blocks, shared across sessions.
    private final ConcurrentLinkedDeque<Long> recentlyVerifiedBlocks;
    /// Provider of the verification data (TSS data and RSA public keys).
    private final VerificationDataProvider verificationDataProvider;
    /// The deque through which the block's item batches are supplied to the hashing stage.
    private final ConcurrentLinkedDeque<BlockItems> blockItemsDeque;
    /// The configuration for verification.
    private final VerificationConfig verificationConfig;
    /// Dumps failing block bytes to disk for diagnostics.
    private final BadBlockDumper badBlockDumper;
    /// The stage chain, saved just before the terminating stage so it can be cancelled.
    private volatile CompletableFuture<BlockVerificationResult> sessionCompletionChain;
    /// Flag the result handling stage raises once it has handled the session's result and
    /// sent its notification; the session is complete from then on.
    private final AtomicBoolean resultHandled;

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
    /// @param outbox the outbox to send the session's notification through, must not be null
    /// @param verificationConfig the configuration for verification, must not be null
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
            final VerificationOutbox outbox,
            final VerificationConfig verificationConfig,
            final BadBlockDumper badBlockDumper) {
        if (blockNumber < 0) {
            throw new IllegalArgumentException("Block number must be non-negative");
        }
        this.sessionKey = new SessionKey(blockNumber, uniqueId);
        this.blockNumber = blockNumber;
        this.outbox = Objects.requireNonNull(outbox);
        this.lastVerifiedBlock = Objects.requireNonNull(lastVerifiedBlock);
        this.recentlyVerifiedBlocks = Objects.requireNonNull(recentlyVerifiedBlocks);
        this.verificationDataProvider = Objects.requireNonNull(verificationDataProvider);
        this.metricsHolder = Objects.requireNonNull(metricsHolder);
        this.blockSource = Objects.requireNonNull(blockSource);
        this.executor = Objects.requireNonNull(executor);
        this.isCancelled = new AtomicBoolean(false);
        this.endOfBlockReceived = new AtomicBoolean(false);
        this.resultHandled = new AtomicBoolean(false);
        this.blockItemsDeque = new ConcurrentLinkedDeque<>();
        this.verificationConfig = Objects.requireNonNull(verificationConfig);
        this.badBlockDumper = Objects.requireNonNull(badBlockDumper);
    }

    /// {@inheritDoc}
    @Override
    public SessionKey sessionKey() {
        return sessionKey;
    }

    /// {@inheritDoc}
    @Override
    public BlockSource blockSource() {
        return blockSource;
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
    /// 1. `SessionResultHandler` - receives the result of the chain. Sends the
    ///    result to messaging through the outbox and manages some internal state.
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
                outbox,
                verificationConfig,
                metricsHolder.sessionResultMetrics(),
                badBlockDumper,
                lastVerifiedBlock,
                recentlyVerifiedBlocks,
                blockNumber,
                blockSource,
                sessionKey,
                endOfBlockReceived,
                resultHandled);
        final CompletableFuture<BlockVerificationResult> completionChain = CompletableFuture.supplyAsync(
                        hasher, executor)
                .thenApply(verifier)
                .thenApply(resultOrderManager);
        // The result handling stage runs exactly once whenever the chain completes, normally,
        // exceptionally or by cancellation, on the thread that completes it. Completing or
        // cancelling the result handling stage itself would skip it instead, so that stage is
        // deliberately not kept, and only the chain before it is saved and ever cancelled.
        completionChain.whenComplete(sessionResultHandler);
        sessionCompletionChain = completionChain;
    }

    /// {@inheritDoc}
    /// ---
    /// Cancels the stage chain and raises the cancellation flag shared with all stages, so
    /// that a stage still running (hashing, verifying or waiting for its turn) observes it
    /// and stops. Cancelling the chain runs the result handling stage on the thread that
    /// cancels it, so the notification for the cancelled session has been sent when the
    /// call that stopped the chain returns.
    ///
    /// A cancelled [CompletableFuture] answers true to every later cancellation as well, so
    /// its answer alone cannot tell which call stopped it. The flag can: it is raised only
    /// by the first call that finds the chain cancelled, and only that call returns true.
    /// Of calls made at the same time, exactly one returns true, though not necessarily the
    /// one that stopped the chain.
    ///
    /// @throws IllegalStateException if the session was never started
    @Override
    public boolean cancel() {
        final CompletableFuture<BlockVerificationResult> localChain = sessionCompletionChain;
        final boolean stopped;
        if (localChain != null) {
            // cancel(true) returns true once the chain is cancelled, whether by this call or an earlier
            // one; only the first call to reach the compare-and-set then raises the flag and returns
            // true. It returns false when the chain completed on its own first, normally or
            // exceptionally, and the flag is left down. That is safe: the chain is the last stage before
            // the result handling stage, so once it has completed on its own every stage has finished,
            // and no stage is left to observe the flag.
            stopped = localChain.cancel(true) && isCancelled.compareAndSet(false, true);
        } else {
            final String message = "Session with id %d for block %d with source %s canceled before it was started"
                    .formatted(sessionKey.uniqueId(), blockNumber, blockSource);
            throw new IllegalStateException(message);
        }
        return stopped;
    }

    /// {@inheritDoc}
    @Override
    public void markEndOfBlockReceived() {
        endOfBlockReceived.set(true);
    }

    /// {@inheritDoc}
    @Override
    public boolean isEndOfBlockReceived() {
        return endOfBlockReceived.get();
    }

    /// {@inheritDoc}
    /// ---
    /// The result handling stage raises the session's result handled flag once it has
    /// handled the result and sent the notification, so a session whose notification is
    /// still being sent is not complete yet. The flag is the stage's own record that it
    /// ran, not a reading of any future's state.
    @Override
    public boolean isComplete() {
        return resultHandled.get();
    }

    /// {@inheritDoc}
    @Override
    public ConcurrentLinkedDeque<BlockItems> getBlockItemsDeque() {
        return blockItemsDeque;
    }
}
