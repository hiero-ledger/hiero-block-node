// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.hedera.pbj.runtime.OneOf;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;
import java.util.stream.Collectors;
import org.hiero.block.node.messaging.BlockItemBatchRingEvent;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.hapi.block.node.BlockItemUnparsed;
import org.hiero.hapi.block.node.BlockItemUnparsed.ItemOneOfType;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the {@link BlockItemBatchRingEvent} class.
 */
public class BlockItemBatchRingEventTest {

    /**
     * Test to verify that the {@link BlockItemBatchRingEvent} class can be created and used correctly.
     */
    @Test
    void testSetAndGet() {
        BlockItemBatchRingEvent event = new BlockItemBatchRingEvent();
        // verify that the toString method returns a non-empty string for empty event
        assertEquals(
                "BlockItemBatchRingEvent{empty}",
                event.toString(),
                "The toString method should return a non-empty string");
        // should be null before set
        assertNull(event.get(), "The get method should return null if no value has been set");
        // create a BlockNotification instance
        BlockItemUnparsed item1 = new BlockItemUnparsed(new OneOf<>(ItemOneOfType.BLOCK_HEADER, Bytes.wrap("fake")));
        BlockItemUnparsed item2 = new BlockItemUnparsed(new OneOf<>(ItemOneOfType.BLOCK_PROOF, Bytes.wrap("fake")));
        List<BlockItemUnparsed> items = List.of(item1, item2);
        final BlockItems blockItems = new BlockItems(items, 0);
        // set the items
        event.set(blockItems);
        // verify that the get method returns the same notification
        assertEquals(
                blockItems, event.get(), "The set and get methods should return the same BlockNotification instance");
        // verify that the toString method returns a non-empty string
        assertEquals(
                "BlockItemBatchRingEvent{"
                        + items.stream().map(BlockItemUnparsed::toString).collect(Collectors.joining(", "))
                        + '}',
                event.toString(),
                "The toString method should return a non-empty string");
    }
}
