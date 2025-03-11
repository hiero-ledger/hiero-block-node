// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.generator.itemhandler;

import com.hedera.hapi.block.stream.protoc.BlockItem;
import com.hedera.hapi.platform.event.legacy.EventTransaction;

/**
 * Handler for event transactions in the block stream.
 * Creates and manages event transaction items representing blockchain transactions.
 */
public class EventTransactionHandler extends AbstractBlockItemHandler {
    @Override
    public BlockItem getItem() {
        if (blockItem == null) {
            blockItem = BlockItem.newBuilder()
                    .setEventTransaction(createEventTransaction())
                    .build();
        }
        return blockItem;
    }

    private EventTransaction createEventTransaction() {
        // For now, we stick with empty EventTransaction, because otherwise we need to provide encoded transaction,
        // which we don't have.
        // This transaction data should correspond with the results in the transaction result item and others.
        return EventTransaction.newBuilder().build();
    }
}
