// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.generator.itemhandler;

import static java.util.Objects.requireNonNull;

import com.google.protobuf.ByteString;
import com.hedera.hapi.block.stream.protoc.BlockItem;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Handler for the genesis ledger-id publication signed transaction.
 * Wraps a pre-serialized {@code SignedTransaction} (carrying a {@code LedgerIdPublicationTransactionBody})
 * into a signed-transaction block item, so a verifier can self-provision its TSS roster from block 0.
 */
public class LedgerIdPublicationHandler extends AbstractBlockItemHandler {

    private final Bytes signedTransactionBytes;

    /**
     * Constructs a new LedgerIdPublicationHandler.
     *
     * @param signedTransactionBytes the serialized ledger-id publication {@code SignedTransaction}
     * @throws NullPointerException if signedTransactionBytes is null
     */
    public LedgerIdPublicationHandler(@NonNull final Bytes signedTransactionBytes) {
        this.signedTransactionBytes = requireNonNull(signedTransactionBytes);
    }

    @Override
    public BlockItem getItem() {
        if (blockItem == null) {
            blockItem = BlockItem.newBuilder()
                    .setSignedTransaction(ByteString.copyFrom(signedTransactionBytes.toByteArray()))
                    .build();
        }
        return blockItem;
    }
}
