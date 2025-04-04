// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.blockmessaging;

/**
 * This interface is used to handle block items without applying back pressure. If the handler can not keep up with the
 * incoming block items, it will receive a call on the {@link #onTooFarBehindError()} method. After that point the
 * handler will not get calls to {@link BlockItemHandler#handleBlockItemsReceived(BlockItems)} anymore.
 */
public interface NoBackPressureBlockItemHandler extends BlockItemHandler {
    /**
     * Called when the block item handler is too far behind the current block number. This can happen if the handler is
     * not able to process the block items fast enough. After this call, the handler will not receive any more block
     * items.
     */
    void onTooFarBehindError();
}
