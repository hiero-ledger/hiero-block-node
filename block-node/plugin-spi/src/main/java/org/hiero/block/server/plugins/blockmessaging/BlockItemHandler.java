// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.plugins.blockmessaging;

import com.hedera.hapi.block.BlockItemUnparsed;
import java.util.List;

/**
 * Interface for handling block items.
 */
public interface BlockItemHandler {
    /**
     * Handle a list of block items. Always called on handler thread. Each registered handler will have its own virtual
     * thread.
     *
     * @param blockItems the immutable list of block items to handle
     */
    void handleBlockItemsReceived(List<BlockItemUnparsed> blockItems);
}
