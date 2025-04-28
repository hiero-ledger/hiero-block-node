// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.publisher;

/**
 * Interface for handling updates from the sessions.
 */
public interface UpdateCallback {
    enum UpdateType {
        BLOCK_ITEMS_RECEIVED,
        START_BLOCK,
        END_BLOCK,
        WHOLE_BLOCK,
        SESSION_ADDED,
        SESSION_CLOSED,
    }

    /**
     * Called when the update happens.
     *
     * @param session the session that is calling update, null if not from a session
     * @param updateType  the type of update
     * @param blockNumber the block number, Always the current block number of session issuing the update.
     */
    void update(BlockStreamProducerSession session, UpdateType updateType, long blockNumber);
}
