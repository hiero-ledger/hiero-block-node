// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi;

import com.hedera.hapi.node.base.NodeAddressBook;
import org.hiero.block.api.TssData;

/**
 * Interface for the Application and block node plugins to exchange state information. The `ApplicationStateFacility`
 * is passed to all block node plugins in the BlockNodeCOntext.
 * */
public interface ApplicationStateFacility {
    /**
     * Used by plugins to update the TssData for this application. i.e. `TssBootstrapPlugin`, and `VerificationPlugin`
     * The update will be forwarded to all plugins using the BlockNodePlugin.onContextUpdate() of the plugins
     *
     * @param tssData - The TssData to be updated on the `BlockNodeContext`
     * */
    void updateTssData(TssData tssData);

    /**
     * Used by plugins to update the consensus node NodeAddressBook for this application,
     * i.e. {@code RsaRosterBootstrapPlugin}. The update will be forwarded to all plugins using
     * {@link BlockNodePlugin#onContextUpdate(BlockNodeContext)}.
     *
     * @param nodeAddressBook The NodeAddressBook to be stored in the BlockNodeContext.
     * @return true if the addressbook is queued for update, false if it was not
     */
    boolean updateAddressBook(NodeAddressBook nodeAddressBook);

    /**
     * Add a block number to the range of blocks available in the node.
     * @param blockNumber the block number to add
     */
    default void addBlock(long blockNumber) {}

    /**
     * Add a range of block numbers to the range of blocks available in the node.
     * @param start the start block number of the range (inclusive)
     * @param end the end block number of the range (inclusive)
     */
    default void addRange(long start, long end) {}

    /**
     * Remove a block number from the range of blocks available in the node.
     * @param blockNumber the block number to remove
     */
    default void removeBlock(long blockNumber) {}

    /**
     * Remove a range of block numbers from the range of blocks available in the node.
     * @param start the start block number of the range (inclusive)
     * @param end the end block number of the range (inclusive)
     */
    default void removeRange(long start, long end) {}

    /**
     * Get the set of block ranges that are available in the node.
     * @return the set of available block ranges
     */
    default org.hiero.block.node.spi.historicalblocks.BlockRangeSet availableBlocks() {
        return null;
    }
}
