// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.simulator.generator.itemhandler;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.block.stream.protoc.BlockItem;
import com.hedera.hapi.platform.event.legacy.EventTransaction;
import org.junit.jupiter.api.Test;

class EventTransactionHandlerTest {

    @Test
    void testGetItem() {
        EventTransactionHandler handler = new EventTransactionHandler();
        BlockItem item = handler.getItem();

        assertNotNull(item);
        assertTrue(item.hasEventTransaction());

        EventTransaction transaction = item.getEventTransaction();
        assertNotNull(transaction);
    }

    @Test
    void testGetItemCaching() {
        EventTransactionHandler handler = new EventTransactionHandler();
        BlockItem item1 = handler.getItem();
        BlockItem item2 = handler.getItem();

        assertSame(item1, item2, "getItem should return cached instance");
    }
}
