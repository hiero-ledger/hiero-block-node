// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.generator.itemhandler;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.block.stream.input.protoc.EventHeader;
import com.hedera.hapi.block.stream.protoc.BlockItem;
import com.hedera.hapi.platform.event.legacy.EventCore;
import org.junit.jupiter.api.Test;

class EventHeaderHandlerTest {

    @Test
    void testGetItem() {
        EventHeaderHandler handler = new EventHeaderHandler();
        BlockItem item = handler.getItem();

        assertNotNull(item);
        assertTrue(item.hasEventHeader());

        EventHeader header = item.getEventHeader();
        assertNotNull(header.getEventCore());

        EventCore core = header.getEventCore();
        assertTrue(core.getCreatorNodeId() >= 1 && core.getCreatorNodeId() < 32);
    }

    @Test
    void testGetItemCaching() {
        EventHeaderHandler handler = new EventHeaderHandler();
        BlockItem item1 = handler.getItem();
        BlockItem item2 = handler.getItem();

        assertSame(item1, item2, "getItem should return cached instance");
    }
}
