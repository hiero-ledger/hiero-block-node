// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import java.util.concurrent.ConcurrentLinkedDeque;
import org.hiero.block.node.spi.blockmessaging.BlockItems;

/// This interface defines a verification session for a block.
/// Sessions run async. They can be canceled. Once a session completes, it will
/// report the result thereof to messaging.
public interface BlockVerificationSession {
    /// The key of the session.
    /// A composite key of block number and a unique session id.
    /// @return the session key
    SessionKey sessionKey();

    /// Start the session.
    ///
    /// The verification process consists of four stages:
    /// 1. Producing of a root hash of the block
    /// 1. Proof verification of the block
    /// 1. Ordering of the result of the verification
    /// 1. Reporting of the result of the verification
    ///
    /// Note that we follow which was the last (successfully) verified block. If
    /// an active session passes item hashing and proof verification, it will wait
    /// its order. This means that if the last verified block was 50, and a session
    /// for block 52 just passed hashing and verification, it will keep waiting for
    /// its order until a session for block 51 is started, passes hashing and
    /// verification successfully, and its result is propagated to messaging.
    /// Note also that failures are propagated to messaging immediately and no
    /// ordering is enforced there. That is also true for successes, which are lower
    /// than or equal to the last verified block!
    void start();

    /// Cancel and stop the session.
    void cancel();

    /// Get the block items deque.
    /// Through this deque, we are able to offer the next block items of the block.
    ConcurrentLinkedDeque<BlockItems> getBlockItemsDeque();

    /// Complete a finished session.
    ///
    /// This method attempts to complete a finished session. If a session has
    /// marked itself as finished, we attempt to get a result from the future
    /// to gracefully complete the session. Iff the session is not done when
    /// this method is called, we will return `false`.
    /// @return a [Boolean] indicating whether the session was completed
    boolean complete();

    /// A composite key for a [BlockVerificationSession].
    /// We allow multiple sessions for the same block.
    /// We first compare by the block number. If that conflicts, then we compare
    /// by the unique id, which will always be different for a different sesion.
    record SessionKey(long blockNumber, long uniqueId) implements Comparable<SessionKey> {
        @Override
        public int compareTo(final SessionKey other) {
            if (this == other) {
                return 0;
            } else if (other == null) {
                return 1; // null comes before non-null.
            } else {
                final int blockNumberCompare = Long.compare(this.blockNumber, other.blockNumber);
                if (blockNumberCompare != 0) {
                    return blockNumberCompare;
                } else {
                    return Long.compare(this.uniqueId, other.uniqueId);
                }
            }
        }
    }
}
