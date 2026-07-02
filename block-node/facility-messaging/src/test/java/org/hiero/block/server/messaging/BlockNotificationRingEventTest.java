// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.messaging.BlockNotificationRingEvent;
import org.hiero.block.node.spi.blockmessaging.BackfilledBlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the {@link BlockNotificationRingEvent} class.
 */
class BlockNotificationRingEventTest {
    /**
     * Tests that a new BlockNotificationRingEvent has a null notification.
     */
    @Test
    @DisplayName("New BlockNotificationRingEvent should have a null notification")
    void newEventShouldHaveNullNotification() {
        final BlockNotificationRingEvent event = new BlockNotificationRingEvent();
        assertNull(event.get());
    }

    /**
     * Tests setting and getting a verification notification.
     */
    @Test
    @DisplayName("Should set and get a verification notification correctly")
    void shouldSetAndGetVerificationNotification() {
        final BlockNotificationRingEvent event = new BlockNotificationRingEvent();
        final VerificationNotification notification =
                new VerificationNotification(true, null, 1, null, null, BlockSource.PUBLISHER);
        event.set(notification);
        assertEquals(notification, event.get());
    }

    /**
     * Tests setting and getting a persisted notification.
     */
    @Test
    @DisplayName("Should set and get a persisted notification correctly")
    void shouldSetAndGetPersistedNotification() {
        final BlockNotificationRingEvent event = new BlockNotificationRingEvent();
        final PersistedNotification notification = new PersistedNotification(2, true, 10, BlockSource.PUBLISHER);
        event.set(notification);
        assertEquals(notification, event.get());
    }

    /**
     * Tests setting and getting a backfilled notification.
     */
    @Test
    @DisplayName("Should set and get a backfilled notification correctly")
    void shouldSetAndGetBackfilledNotification() {
        final BlockNotificationRingEvent event = new BlockNotificationRingEvent();
        final BackfilledBlockNotification notification =
                new BackfilledBlockNotification(1, BlockUnparsed.newBuilder().build());
        event.set(notification);
        assertEquals(notification, event.get());
    }

    /**
     * Tests that setting a new notification replaces the previous one, allowing the event to be
     * reused across different notification types as the ring buffer recycles it.
     */
    @Test
    @DisplayName("Setting a new notification should replace the previous one")
    void settingNewNotificationShouldReplacePrevious() {
        final BlockNotificationRingEvent event = new BlockNotificationRingEvent();
        final PersistedNotification persistedNotification =
                new PersistedNotification(2, true, 10, BlockSource.PUBLISHER);
        final VerificationNotification verificationNotification =
                new VerificationNotification(true, null, 1, null, null, BlockSource.PUBLISHER);

        event.set(persistedNotification);
        assertEquals(persistedNotification, event.get());

        event.set(verificationNotification);
        assertEquals(verificationNotification, event.get());
    }

    /**
     * Tests that the event can be reused by setting different notifications of the same type.
     */
    @Test
    @DisplayName("Should allow reuse with different notifications of the same type")
    void shouldAllowReuseWithDifferentNotificationsOfSameType() {
        final BlockNotificationRingEvent event = new BlockNotificationRingEvent();
        final VerificationNotification notification1 =
                new VerificationNotification(true, null, 1, null, null, BlockSource.PUBLISHER);
        final VerificationNotification notification2 =
                new VerificationNotification(true, null, 1, null, null, BlockSource.PUBLISHER);

        event.set(notification1);
        assertEquals(notification1, event.get());

        event.set(notification2);
        assertEquals(notification2, event.get());
    }
}
