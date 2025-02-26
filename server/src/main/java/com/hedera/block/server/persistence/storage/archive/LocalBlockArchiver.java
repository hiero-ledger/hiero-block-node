// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.persistence.storage.archive;

/**
 * An interface that defines an asynchronous local block archiver.
 */
public interface LocalBlockArchiver {
    /**
     * This method will notify the archiver that a block has been persisted.
     *
     * @param blockNumber the block number that has been persisted
     */
    void notifyBlockPersisted(final long blockNumber);
}
