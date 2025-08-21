// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.generator.itemhandler;

import com.google.protobuf.ByteString;
import com.hedera.hapi.block.stream.protoc.BlockItem;
import com.hedera.hapi.node.transaction.SignedTransaction;
import com.hedera.pbj.runtime.io.WritableSequentialData;
import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Handler for event transactions in the block stream.
 * Creates and manages event transaction items representing blockchain transactions.
 */
public class SignedTransactionHandler extends AbstractBlockItemHandler {
    @Override
    public BlockItem getItem() {
        if (blockItem == null) {
            final var out = new ByteArrayOutputStream();
            WritableSequentialData data = new WritableStreamingData(out);
            try {
                SignedTransaction.PROTOBUF.write(createSignedTransaction(), data);
            } catch (IOException e) {
                throw new AssertionError("Failed to get transaction bytes", e);
            }

            // Create a new BlockItem with the signed transaction
            // Note: The SignedTransaction is created with an empty body for now
            blockItem = BlockItem.newBuilder()
                    .setSignedTransaction(ByteString.copyFrom(out.toByteArray()))
                    .build();
        }
        return blockItem;
    }

    private SignedTransaction createSignedTransaction() {
        // For now, we stick with empty EventTransaction, because otherwise we need to provide encoded transaction,
        // which we don't have.
        // This transaction data should correspond with the results in the transaction result item and others.
        return SignedTransaction.newBuilder().build();
    }
}
