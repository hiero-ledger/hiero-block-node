// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.blockmessaging;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.Objects;

/// Simple record for block verification notifications.
///
/// @param success     `true` if the block was verified successfully, `false` otherwise
/// @param failureInfo the failure info; must be `null` if verification is successful and `non-null` otherwise
/// @param blockNumber the block number this notification is for
/// @param blockHash   the hash of the block, if verification is successful
/// @param block       the block, if verification is successful; may also be present on failure for diagnostics
/// @param source      the source of the message
public record VerificationNotification(
        boolean success,
        FailureInfo failureInfo,
        long blockNumber,
        Bytes blockHash,
        org.hiero.block.internal.BlockUnparsed block,
        BlockSource source) {
    public VerificationNotification {
        if (success && failureInfo != null) {
            throw new IllegalArgumentException("Verification is successful, but a failure reason is provided");
        }
        if (!success && failureInfo == null) {
            throw new IllegalArgumentException("Verification failed, but no failure reason is provided");
        }
    }

    /// The type of failure when verification fails.
    public enum FailureType {
        /// This type indicates that the block could not be parsed
        UNABLE_TO_PARSE,
        /// This type indicates that the block is missing a mandatory item
        MISSING_MANDATORY_ITEM,
        /// This type indicates that the block is missing a mandatory field (null)
        MISSING_MANDATORY_FIELD,
        /// This type indicates we have missing verification data. This includes we do not have TSS Data, RSA public
        /// keys, or we have witnessed a [java.security.NoSuchAlgorithmException].
        MISSING_VERIFICATION_DATA,
        /// This type indicates that the proof(s) provided for the block are not recognized
        UNRECOGNIZED_PROOF_TYPE,
        /// This type indicates that the block is of unsupported HAPI version
        UNSUPPORTED_HAPI_VERSION,
        /// This type indicates that the session was cancelled
        CANCELLED,
        /// This type indicates that an unknown error occurred
        UNKNOWN_ERROR,
        /// This type indicates that the block footer is absent when finalization is attempted
        MISSING_FOOTER,
        /// This type indicates that the proof structure is malformed (e.g., duplicate proofs, RSA engine error)
        MALFORMED_PROOF_STRUCTURE,
        /// This type indicates that the state-proof merkle-path chain fails verification
        STATE_PROOF_INVALID,
        /// This type indicates that the cryptographic signature (RSA or TSS) does not match
        SIGNATURE_MISMATCH,
        /// This type indicates that the block header is structurally invalid or internally inconsistent
        /// (e.g., the block number in the header does not match the expected block number)
        INVALID_BLOCK_HEADER
    }

    /// A simple record that shows us the reason of failed verification.
    ///
    /// We can see if the failure is informational, meaning it happened after
    /// successful verification of the same block (within reasonable recency)
    /// alongside the type of the failure.
    public record FailureInfo(FailureType failureType, boolean isInformational) {
        public FailureInfo {
            Objects.requireNonNull(failureType, "failureType cannot be null");
        }

        public static FailureInfo standard(final FailureType failureType) {
            return new FailureInfo(failureType, false);
        }

        public static FailureInfo informational(final FailureType failureType) {
            return new FailureInfo(failureType, true);
        }
    }
}
