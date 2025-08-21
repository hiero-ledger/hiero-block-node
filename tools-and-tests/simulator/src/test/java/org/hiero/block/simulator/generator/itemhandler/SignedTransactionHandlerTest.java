// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.generator.itemhandler;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import com.hedera.hapi.block.stream.protoc.BlockItem;
import org.junit.jupiter.api.Test;

class SignedTransactionHandlerTest {

    @Test
    void testGetItem() {
        SignedTransactionHandler handler = new SignedTransactionHandler();
        BlockItem item = handler.getItem();

        assertNotNull(item);
        assertTrue(item.hasSignedTransaction());

        ByteString transaction = item.getSignedTransaction();
        assertNotNull(transaction);
    }

    @Test
    void testGetItemCaching() {
        SignedTransactionHandler handler = new SignedTransactionHandler();
        BlockItem item1 = handler.getItem();
        BlockItem item2 = handler.getItem();

        assertSame(item1, item2, "getItem should return cached instance");
    }
}
