// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import org.hiero.block.node.block.verification.VerificationConfig;
import org.hiero.block.node.block.verification.verifier.BlockVerificationResult;
import org.hiero.block.node.spi.blockmessaging.BlockSource;

/// The third stage of a [org.hiero.block.node.block.verification.session.CompletableVerificationSession].
/// This stage simply awaits the right turn of the successful block verification's result propagation.
public final class ResultOrderingManager implements Function<BlockVerificationResult, BlockVerificationResult> {
    private final AtomicLong lastVerifiedBlock;
    private final VerificationConfig verificationConfig;
    private final AtomicBoolean isCanceled;

    /// Constructor.
    public ResultOrderingManager(
            final AtomicBoolean isCanceled,
            final AtomicLong lastVerifiedBlock,
            final VerificationConfig verificationConfig) {
        this.isCanceled = Objects.requireNonNull(isCanceled);
        this.lastVerifiedBlock = Objects.requireNonNull(lastVerifiedBlock);
        this.verificationConfig = Objects.requireNonNull(verificationConfig);
    }

    /// Accept a successful [BlockVerificationResult].
    /// Wait until it is our turn to propagate the success.
    /// We follow the last verified block value. If we are below or equal to that, we do not enforce ordering
    /// and simply continue to the next stage. If we are higher than the next expected block to verify successfully,
    /// we have to wait until our turn arrives.
    /// _NOTE_: strict ordering is always required for [BlockSource#PUBLISHER], but based on the
    /// [VerificationConfig#allSourcesRequireOrdering()] setting, we can disable ordering for other sources.
    @Override
    public BlockVerificationResult apply(final BlockVerificationResult blockVerificationResult) {
        checkValidNextExpectedBlock(lastVerifiedBlock, blockVerificationResult.blockNumber());
        while (!isCanceled()) {
            final long nextExpectedBlock = lastVerifiedBlock.get() + 1;
            if (shouldPark(blockVerificationResult, nextExpectedBlock)) {
                // Shortly park to avoid busy-waiting
                LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(500));
            } else {
                return blockVerificationResult;
            }
        }
        throw new VerificationSessionFailedException(
                blockVerificationResult.blockNumber(), SessionFailureType.CANCELLED, blockVerificationResult.source());
    }

    private void checkValidNextExpectedBlock(final AtomicLong lastVerifiedBlock, final long currentVerifiedBlock) {
        // Note, this method will have an effect only at application start.
        // It is possible to create a small gap because of a race condition.
        // This is acceptable, as it will be filled shortly thereafter and only
        // happens once on startup.
        final long lastVerified = lastVerifiedBlock.get();
        if (lastVerified < 0) {
            lastVerifiedBlock.compareAndSet(lastVerified, currentVerifiedBlock);
        }
    }

    private boolean shouldPark(final BlockVerificationResult verificationResult, final long nextExpectedBlock) {
        // spotless:off
        final long verifiedBlockNumber = verificationResult.blockNumber();
        return (verifiedBlockNumber >= verificationConfig.firstOrderedBlock())
            && (verifiedBlockNumber > nextExpectedBlock)
            && (verificationResult.source() == BlockSource.PUBLISHER || verificationConfig.allSourcesRequireOrdering());
        // spotless:on
    }

    private boolean isCanceled() {
        return isCanceled.get() || Thread.currentThread().isInterrupted();
    }
}
