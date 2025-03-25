// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.publisher;

import static org.hiero.block.node.spi.BlockNodePlugin.UNKNOWN_BLOCK_NUMBER;

/**
 * Interface for handling updates from the sessions.
 */
public interface UpdateCallback {
    enum UpdateType {
        BLOCK_ITEMS_RECEIVED,
        START_BLOCK,
        END_BLOCK,
        SESSION_ADDED,
        SESSION_CLOSED,
    }

    /**
     * Called when the update happens.
     *
     * @param session the session that is calling update, null if not from a session
     * @param updateType  the type of update
     * @param blockNumber the block number, if update type is START_BLOCK or END_BLOCK
     */
    void update(BlockStreamProducerSession session, UpdateType updateType, long blockNumber);

    /**
     * Called when the update happens.
     *
     * @param session the session that is calling update, null if not from a session
     * @param updateType the type of update
     */
    default void update(BlockStreamProducerSession session, UpdateType updateType) {
        update(session, updateType, UNKNOWN_BLOCK_NUMBER);
    }
}
