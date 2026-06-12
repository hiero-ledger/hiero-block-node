// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import org.hiero.block.node.block.verification.verifier.BlockVerificationResult;

/// The third stage of a [org.hiero.block.node.block.verification.session.CompletableVerificationSession].
/// This stage simply awaits the right turn of the successful block verification's result propagation.
public final class ResultOrderingManager implements Function<BlockVerificationResult, BlockVerificationResult> {
    private final AtomicLong lastVerifiedBlock;
    private final AtomicBoolean isCanceled;

    /// Constructor.
    public ResultOrderingManager(final AtomicBoolean isCanceled, final AtomicLong lastVerifiedBlock) {
        this.isCanceled = Objects.requireNonNull(isCanceled);
        this.lastVerifiedBlock = Objects.requireNonNull(lastVerifiedBlock);
    }

    /// Accept a successful [BlockVerificationResult].
    /// Wait until it is our turn to propagate the success.
    /// We follow the last verified block value. If we are below or equal to that, we do not enforce ordering
    /// and simply continue to the next stage. If we are higher than the next expected block to verify successfully,
    /// we have to wait until our turn arrives.
    @Override
    public BlockVerificationResult apply(final BlockVerificationResult blockVerificationResult) {
        while (!isCanceled()) {
            final long nextExpectedBlock = lastVerifiedBlock.get() + 1;
            if (blockVerificationResult.blockNumber() > nextExpectedBlock) {
                // Shortly park to avoid busy-waiting
                LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(500));
            } else {
                return blockVerificationResult;
            }
        }
        throw new VerificationSessionFailedException(
                blockVerificationResult.blockNumber(), SessionFailureType.CANCELLED, blockVerificationResult.source());
    }

    private boolean isCanceled() {
        return isCanceled.get() || Thread.currentThread().isInterrupted();
    }
}
