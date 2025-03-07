// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.verification.session;

/**
 * Defines the types of block verification sessions.
 */
public enum BlockVerificationSessionType {
    /**
     * An asynchronous block verification session, where the verification is done in a separate thread.
     */
    ASYNC,
    /**
     * A synchronous block verification session, where the verification is done in the same thread.
     */
    SYNC
}
