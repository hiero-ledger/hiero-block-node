// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.hiero.block.node.messaging.BlockNotificationRingEvent;
import org.hiero.block.node.spi.blockmessaging.BlockNotification;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the {@link BlockNotificationRingEvent} class.
 */
public class BlockNotificationRingEventTest {
    @Test
    void testSetAndGet() {
        BlockNotificationRingEvent event = new BlockNotificationRingEvent();
        // should be null before set
        assertNull(event.get(), "The get method should return null if no value has been set");
        // create a BlockNotification instance
        BlockNotification notification = new BlockNotification(1, BlockNotification.Type.BLOCK_VERIFIED);
        // set the notification
        event.set(notification);
        // verify that the get method returns the same notification
        assertEquals(
                notification, event.get(), "The set and get methods should return the same BlockNotification instance");
    }
}
