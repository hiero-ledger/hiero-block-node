// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.blockmessaging;

/**
 * Interface for handling block items.
 */
public interface BlockItemHandler {
    /**
     * Handle a list of block items. Always called on handler thread. Each registered handler will have its own virtual
     * thread.
     * <p>
     * When you get block items they could be the start of a block, starting with a header or middle block items, or end
     * with a block proof. The first block items you as a listener receive could be any of these and might be in the
     * middle of a block. You may get a full complete block header to proof, or it is possible to get a half complete
     * block because the sender had an issue and could not send the rest of the block. So if you get a block header and
     * have not received the block proof, you should assume that the block is incomplete and throw away the data.
     * Hopefully you will get that block resent directly after or shortly later but that is not guaranteed.
     *
     * @param blockItems the immutable list of block items to handle
     */
    void handleBlockItemsReceived(BlockItems blockItems);
}
