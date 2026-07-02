// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification.session;

import com.hedera.hapi.node.base.SemanticVersion;
import java.util.List;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.node.spi.blockmessaging.BlockSource;

/// An exception thrown when the verification chain experiences a situation
/// where we cannot continue or recover from.
public class VerificationSessionFailedException extends RuntimeException {
    private static final String DEFAULT_MESSAGE = "Verification session for block %d with source %s failed with %s";
    private final long blockNumber;
    private final SessionFailureType failureType;
    private final BlockSource blockSource;
    /// The raw block items captured at the failure site, for diagnostic dump. May be {@code null}
    /// when no block items were available at the failure site.
    private final List<BlockItemUnparsed> blockItems;
    /// The HAPI proto version from the block header, for diagnostic dump. May be {@code null}
    /// when the header had not yet been parsed at the failure site.
    private final SemanticVersion hapiVersion;

    public VerificationSessionFailedException(
            final long blockNumber, final SessionFailureType failureType, final BlockSource blockSource) {
        this(blockNumber, failureType, blockSource, (List<BlockItemUnparsed>) null);
    }

    public VerificationSessionFailedException(
            final long blockNumber,
            final SessionFailureType failureType,
            final BlockSource blockSource,
            final List<BlockItemUnparsed> blockItems) {
        this(blockNumber, failureType, blockSource, blockItems, null);
    }

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
        this.blockItems = null;
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

    public List<BlockItemUnparsed> getBlockItems() {
        return blockItems;
    }

    public SemanticVersion getHapiVersion() {
        return hapiVersion;
    }
}
