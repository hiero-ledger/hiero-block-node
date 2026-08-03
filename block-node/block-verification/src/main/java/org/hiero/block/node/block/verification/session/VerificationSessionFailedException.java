// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import com.hedera.hapi.node.base.SemanticVersion;
import java.util.List;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.node.spi.blockmessaging.BlockSource;

/// An exception thrown when the verification chain experiences a situation
/// where we cannot continue or recover from.
public class VerificationSessionFailedException extends RuntimeException {
    /// The default message template: block number, source, and failure type.
    private static final String DEFAULT_MESSAGE = "Verification session for block %d with source %s failed with %s";
    /// The number of the block whose verification failed.
    private final long blockNumber;
    /// The reason for the failure.
    private final SessionFailureType failureType;
    /// The source of the block whose verification failed.
    private final BlockSource blockSource;
    /// The raw block items captured at the failure site, for diagnostic dump. May be `null`
    /// when no block items were available at the failure site.
    private final List<BlockItemUnparsed> blockItems;
    /// The HAPI proto version from the block header, for diagnostic dump. May be `null`
    /// when the header had not yet been parsed at the failure site.
    private final SemanticVersion hapiVersion;

    /// Constructor with the default message and no diagnostic data.
    ///
    /// @param blockNumber the number of the block whose verification failed
    /// @param failureType the reason for the failure
    /// @param blockSource the source of the block
    public VerificationSessionFailedException(
            final long blockNumber, final SessionFailureType failureType, final BlockSource blockSource) {
        this(blockNumber, failureType, blockSource, (List<BlockItemUnparsed>) null);
    }

    /// Constructor with the default message and captured block items.
    ///
    /// @param blockNumber the number of the block whose verification failed
    /// @param failureType the reason for the failure
    /// @param blockSource the source of the block
    /// @param blockItems the raw block items captured at the failure site, may be `null`
    public VerificationSessionFailedException(
            final long blockNumber,
            final SessionFailureType failureType,
            final BlockSource blockSource,
            final List<BlockItemUnparsed> blockItems) {
        this(blockNumber, failureType, blockSource, blockItems, null);
    }

    /// Constructor with the default message and full diagnostic data.
    ///
    /// @param blockNumber the number of the block whose verification failed
    /// @param failureType the reason for the failure
    /// @param blockSource the source of the block
    /// @param blockItems the raw block items captured at the failure site, may be `null`
    /// @param hapiVersion the HAPI proto version from the block header, may be `null`
    public VerificationSessionFailedException(
            final long blockNumber,
            final SessionFailureType failureType,
            final BlockSource blockSource,
            final List<BlockItemUnparsed> blockItems,
            final SemanticVersion hapiVersion) {
        this.blockNumber = blockNumber;
        this.failureType = failureType;
        this.blockSource = blockSource;
        this.blockItems = blockItems;
        this.hapiVersion = hapiVersion;
        super(DEFAULT_MESSAGE.formatted(blockNumber, blockSource, failureType));
    }

    /// Constructor with a custom message and no diagnostic data.
    ///
    /// @param blockNumber the number of the block whose verification failed
    /// @param failureType the reason for the failure
    /// @param blockSource the source of the block
    /// @param message the exception message
    public VerificationSessionFailedException(
            final long blockNumber,
            final SessionFailureType failureType,
            final BlockSource blockSource,
            final String message) {
        this.blockNumber = blockNumber;
        this.failureType = failureType;
        this.blockSource = blockSource;
        this.blockItems = null;
        this.hapiVersion = null;
        super(message);
    }

    /// Constructor with the default message and a cause.
    ///
    /// @param blockNumber the number of the block whose verification failed
    /// @param failureType the reason for the failure
    /// @param blockSource the source of the block
    /// @param cause the underlying cause of the failure
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

    /// Constructor with a custom message and a cause.
    ///
    /// @param blockNumber the number of the block whose verification failed
    /// @param failureType the reason for the failure
    /// @param blockSource the source of the block
    /// @param message the exception message
    /// @param cause the underlying cause of the failure
    public VerificationSessionFailedException(
            final long blockNumber,
            final SessionFailureType failureType,
            final BlockSource blockSource,
            final String message,
            final Throwable cause) {
        this.blockNumber = blockNumber;
        this.failureType = failureType;
        this.blockSource = blockSource;
        this.blockItems = null;
        this.hapiVersion = null;
        super(message, cause);
    }

    /// Get the number of the block whose verification failed.
    /// @return the block number
    public long getBlockNumber() {
        return blockNumber;
    }

    /// Get the reason for the failure.
    /// @return the failure type
    public SessionFailureType getFailureType() {
        return failureType;
    }

    /// Get the source of the block whose verification failed.
    /// @return the block source
    public BlockSource getBlockSource() {
        return blockSource;
    }

    /// Get the raw block items captured at the failure site.
    /// @return the block items, may be `null` when none were available
    public List<BlockItemUnparsed> getBlockItems() {
        return blockItems;
    }

    /// Get the HAPI proto version from the block header.
    /// @return the HAPI version, may be `null` when the header had not been parsed
    public SemanticVersion getHapiVersion() {
        return hapiVersion;
    }
}
