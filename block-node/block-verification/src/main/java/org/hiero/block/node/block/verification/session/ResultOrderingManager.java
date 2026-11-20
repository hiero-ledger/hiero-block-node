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

/// The third stage of a [org.hiero.block.node.block.verification.session.CompletableVerificationSession].
/// This stage simply awaits the right turn of the successful block verification's result propagation.
public final class ResultOrderingManager implements Function<BlockVerificationResult, BlockVerificationResult> {
    /// The last successfully verified block, shared across sessions; ordering is enforced against it.
    private final AtomicLong lastVerifiedBlock;
    /// The configuration for verification, source of the ordering settings.
    private final VerificationConfig verificationConfig;
    /// Cancellation flag shared with the owning session.
    private final AtomicBoolean isCanceled;

    /// Constructor.
    ///
    /// @param isCanceled cancellation flag shared with the owning session, must not be null
    /// @param lastVerifiedBlock the last successfully verified block, must not be null
    /// @param verificationConfig the configuration for verification, must not be null
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
    /// We follow the last verified block value, and wait while the [OrderingRules] say
    /// the result has to wait for its turn: the same rules the eviction policy applies to
    /// decide which sessions wait. A result at or below the next expected block, below
    /// [VerificationConfig#firstOrderedBlock()], or from a source not subject to ordering
    /// continues to the next stage at once.
    @Override
    public BlockVerificationResult apply(final BlockVerificationResult blockVerificationResult) {
        checkValidNextExpectedBlock(lastVerifiedBlock, blockVerificationResult.blockNumber());
        while (!isCanceled()) {
            if (OrderingRules.waitsForOrder(
                    verificationConfig,
                    blockVerificationResult.blockNumber(),
                    blockVerificationResult.source(),
                    lastVerifiedBlock.get())) {
                // Shortly park to avoid busy-waiting
                LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(500));
            } else {
                return blockVerificationResult;
            }
        }
        throw new VerificationSessionFailedException(
                blockVerificationResult.blockNumber(), SessionFailureType.CANCELLED, blockVerificationResult.source());
    }

    /// Establish the last verified block if it is not yet known.
    /// This method will have an effect only at application start, when no last
    /// verified block has been established: the first successful verification
    /// establishes it. It is possible to create a small gap because of a race
    /// condition. This is acceptable, as it will be filled shortly thereafter and
    /// only happens once on startup.
    ///
    /// @param lastVerifiedBlock the last successfully verified block reference
    /// @param currentVerifiedBlock the block that just verified successfully
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

    /// Returns `true` if the owning session has been cancelled or the current thread interrupted.
    private boolean isCanceled() {
        return isCanceled.get() || Thread.currentThread().isInterrupted();
    }
}
