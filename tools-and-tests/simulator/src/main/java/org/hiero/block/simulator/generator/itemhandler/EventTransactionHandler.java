// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.generator.itemhandler;

import com.google.protobuf.ByteString;
import com.hedera.hapi.block.stream.protoc.BlockItem;

/**
 * Handler for event transactions in the block stream.
 * Creates and manages event transaction items representing blockchain transactions.
 */
public class EventTransactionHandler extends AbstractBlockItemHandler {
    @Override
    public BlockItem getItem() {
        if (blockItem == null) {
            blockItem = BlockItem.newBuilder()
                    .setSignedTransaction(ByteString.EMPTY)
                    .build();
        }
        return blockItem;
    }
}
