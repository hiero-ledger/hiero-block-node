// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import com.hedera.hapi.node.base.SemanticVersion;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.spi.blockmessaging.BlockSource;

/// An exception thrown when the verification chain experiences a situation
/// where we cannot continue or recover from.
public class VerificationSessionFailedException extends RuntimeException {
    private static final String DEFAULT_MESSAGE = "Verification session for block %d with source %s failed with %s";
    private final long blockNumber;
    private final SessionFailureType failureType;
    private final BlockSource blockSource;
    /// The raw block bytes captured at the failure site, for diagnostic dump. May be {@code null}
    /// when the block had not yet been fully assembled (e.g. failures during hashing).
    private final BlockUnparsed block;
    /// The HAPI proto version from the block header, for diagnostic dump. May be {@code null}
    /// when the header had not yet been parsed at the failure site.
    private final SemanticVersion hapiVersion;

    public VerificationSessionFailedException(
            final long blockNumber, final SessionFailureType failureType, final BlockSource blockSource) {
        this(blockNumber, failureType, blockSource, (BlockUnparsed) null);
    }

    public VerificationSessionFailedException(
            final long blockNumber,
            final SessionFailureType failureType,
            final BlockSource blockSource,
            final BlockUnparsed block) {
        this(blockNumber, failureType, blockSource, block, null);
    }

    public VerificationSessionFailedException(
            final long blockNumber,
            final SessionFailureType failureType,
            final BlockSource blockSource,
            final BlockUnparsed block,
            final SemanticVersion hapiVersion) {
        this.blockNumber = blockNumber;
        this.failureType = failureType;
        this.blockSource = blockSource;
        this.block = block;
        this.hapiVersion = hapiVersion;
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
        this.block = null;
        this.hapiVersion = null;
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
        this.block = null;
        this.hapiVersion = null;
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

    public BlockUnparsed getBlock() {
        return block;
    }

    public SemanticVersion getHapiVersion() {
        return hapiVersion;
    }
}
