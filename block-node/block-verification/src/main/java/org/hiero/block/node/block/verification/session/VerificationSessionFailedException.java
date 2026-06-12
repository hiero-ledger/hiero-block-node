// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import org.hiero.block.node.spi.blockmessaging.BlockSource;

/// An exception thrown when the verification chain experiences a situation
/// where we cannot continue or recover from.
public class VerificationSessionFailedException extends RuntimeException {
    private static final String DEFAULT_MESSAGE = "Verification session for block %d with source %s failed with %s";
    private final long blockNumber;
    private final SessionFailureType failureType;
    private final BlockSource blockSource;

    public VerificationSessionFailedException(
            final long blockNumber, final SessionFailureType failureType, final BlockSource blockSource) {
        this.blockNumber = blockNumber;
        this.failureType = failureType;
        this.blockSource = blockSource;
        super(DEFAULT_MESSAGE.formatted(blockNumber, blockSource, failureType));
    }

    public VerificationSessionFailedException(
            final long blockNumber,
            final SessionFailureType failureType,
            final BlockSource blockSource,
            final String message) {
        this.blockNumber = blockNumber;
        this.failureType = failureType;
        this.blockSource = blockSource;
        super(message);
    }

    public VerificationSessionFailedException(
            final long blockNumber,
            final SessionFailureType failureType,
            final BlockSource blockSource,
            final Throwable cause) {
        this(
                blockNumber,
                failureType,
                blockSource,
                DEFAULT_MESSAGE.formatted(blockNumber, blockSource, failureType),
                cause);
    }

    public VerificationSessionFailedException(
            final long blockNumber,
            final SessionFailureType failureType,
            final BlockSource blockSource,
            final String message,
            final Throwable cause) {
        this.blockNumber = blockNumber;
        this.failureType = failureType;
        this.blockSource = blockSource;
        super(message, cause);
    }

    public long getBlockNumber() {
        return blockNumber;
    }

    public SessionFailureType getFailureType() {
        return failureType;
    }

    public BlockSource getBlockSource() {
        return blockSource;
    }
}
