// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import org.hiero.block.node.spi.blockmessaging.VerificationNotification.FailureType;

/// Failure types for a failed session.
/// These types provide a reason for the failure.
public enum SessionFailureType {
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
    /// This type indicates the block footer was absent when finalization was attempted
    MISSING_FOOTER,
    /// This type indicates a duplicate, malformed, or structurally invalid proof
    MALFORMED_PROOF_STRUCTURE,
    /// This type indicates the state-proof merkle-path chain failed integrity check
    STATE_PROOF_INVALID,
    /// This type indicates an RSA or TSS cryptographic signature did not match
    SIGNATURE_MISMATCH,
    /// This type indicates that the block header is structurally invalid or internally inconsistent
    /// (e.g., the block number in the header does not match the expected block number)
    INVALID_BLOCK_HEADER;

    /// Map `this` to a corresponding [FailureType].
    /// @return the corresponding [FailureType]
    public FailureType asFailureType() {
        return switch (this) {
            case UNABLE_TO_PARSE -> FailureType.UNABLE_TO_PARSE;
            case MISSING_MANDATORY_ITEM -> FailureType.MISSING_MANDATORY_ITEM;
            case MISSING_MANDATORY_FIELD -> FailureType.MISSING_MANDATORY_FIELD;
            case MISSING_VERIFICATION_DATA -> FailureType.MISSING_VERIFICATION_DATA;
            case UNRECOGNIZED_PROOF_TYPE -> FailureType.UNRECOGNIZED_PROOF_TYPE;
            case UNSUPPORTED_HAPI_VERSION -> FailureType.UNSUPPORTED_HAPI_VERSION;
            case CANCELLED -> FailureType.CANCELLED;
            case UNKNOWN_ERROR -> FailureType.UNKNOWN_ERROR;
            case MISSING_FOOTER -> FailureType.MISSING_FOOTER;
            case MALFORMED_PROOF_STRUCTURE -> FailureType.MALFORMED_PROOF_STRUCTURE;
            case STATE_PROOF_INVALID -> FailureType.STATE_PROOF_INVALID;
            case SIGNATURE_MISMATCH -> FailureType.SIGNATURE_MISMATCH;
            case INVALID_BLOCK_HEADER -> FailureType.INVALID_BLOCK_HEADER;
        };
    }
}
