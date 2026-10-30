// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import java.util.concurrent.ConcurrentLinkedDeque;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;

/// This interface defines a verification session for a block.
/// Sessions run async. They can be canceled. Once a session completes, it will
/// report the result thereof to messaging.
public interface BlockVerificationSession {
    /// The key of the session.
    /// A composite key of block number and a unique session id.
    /// @return the session key
    SessionKey sessionKey();

    /// The source the block of this session came from. The active sessions buffer
    /// chooses the lane of the session and how it is admitted from it.
    ///
    /// @return the source of the block, never null
    BlockSource blockSource();

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
    ///
    /// @return true for exactly one call once the session is stopped by cancellation, and
    ///     false for every other call, as well as for any call made after the session
    ///     produced its result on its own
    boolean cancel();

    /// Mark that the end of block has been received for this session.
    ///
    /// The supplier of the block items must call this method as soon as it
    /// supplies the batch that ends the block. If the session ends without
    /// producing a result, this mark determines whether the failure is
    /// reported as [SessionFailureType#CANCELLED] (the complete block was
    /// received) or [SessionFailureType#CANCELLED_INCOMPLETE] (the block was never
    /// fully received).
    void markEndOfBlockReceived();

    /// Whether the end of block has been received for this session, see
    /// [#markEndOfBlockReceived()].
    ///
    /// @return true once the end of block has been marked
    boolean isEndOfBlockReceived();

    /// Whether the session is complete: its result, a success, a failure or a
    /// cancellation, has been handled and its notification sent. A complete session
    /// can be cleared from the active sessions buffer.
    ///
    /// @return true once the session is complete
    boolean isComplete();

    /// Get the block items deque.
    /// Through this deque, we are able to offer the next block items of the block.
    ConcurrentLinkedDeque<BlockItems> getBlockItemsDeque();

    /// A composite key for a [BlockVerificationSession].
    /// We allow multiple sessions for the same block.
    /// We first compare by the block number. If that conflicts, then we compare
    /// by the unique id, which will always be different for a different session.
    /// @param blockNumber the number of the block the session verifies
    /// @param uniqueId a unique id distinguishing sessions for the same block
    record SessionKey(long blockNumber, long uniqueId) implements Comparable<SessionKey> {
        /// {@inheritDoc}
        /// ---
        /// Compares first by block number, then by unique id. A `null` other
        /// sorts before any non-null key.
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
