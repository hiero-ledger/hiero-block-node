// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.verification;

/**
 * An enum representing the status of block verification.
 */
public enum BlockVerificationStatus {
    /**
     * The Block has been verified.
     */
    VERIFIED,
    /**
     * The Block failed verification, either due to an invalid signature or an invalid hash.
     */
    INVALID_HASH_OR_SIGNATURE
}
