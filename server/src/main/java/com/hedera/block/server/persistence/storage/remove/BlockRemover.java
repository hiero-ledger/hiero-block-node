// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.persistence.storage.remove;

import java.io.IOException;

/** The BlockRemover interface defines the contract for removing a block from storage. */
public interface BlockRemover {
    /**
     * Remove an unverified block under the unverified root with the given block
     * number.
     *
     * @param blockNumber the block number of the block to remove
     * @return true if the block was removed successfully, false otherwise
     * @throws IOException when failing to remove a block
     * @throws IllegalArgumentException if the blockNumber IS NOT a whole number
     */
    boolean removeUnverified(final long blockNumber) throws IOException;
}
