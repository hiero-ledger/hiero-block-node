// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.blockmessaging;

import com.hedera.pbj.runtime.io.buffer.Bytes;

/// Simple record for block verification notifications.
///
/// @param success     `true` if the block was verified successfully, `false` otherwise
/// @param failureType the type of the failure; must be `null` if verification is successful and `non-null` otherwise
/// @param blockNumber the block number this notification is for
/// @param blockHash   the hash of the block, if verification is successful
/// @param block       the block, if verification is successful
/// @param source      the source of the message
public record VerificationNotification(
        boolean success,
        FailureType failureType,
        long blockNumber,
        Bytes blockHash,
        org.hiero.block.internal.BlockUnparsed block,
        BlockSource source) {
    public VerificationNotification {
        if (success && failureType != null) {
            throw new IllegalArgumentException("Verification is successful, but a failure reason is provided");
        }
        if (!success && failureType == null) {
            throw new IllegalArgumentException("Verification failed, but no failure reason is provided");
        }
    }

    /// The type of failure when verification fails.
    public enum FailureType {
        /// This type indicates that the proof was bad
        BAD_BLOCK_PROOF,
        /// This type indicates that the block could not be parsed
        UNABLE_TO_PARSE,
        /// This type indicates that the block is missing a mandatory item
        MISSING_MANDATORY_ITEM,
    }
}
