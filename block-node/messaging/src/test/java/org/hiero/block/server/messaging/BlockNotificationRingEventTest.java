// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.hiero.block.node.messaging.BlockNotificationRingEvent;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the {@link BlockNotificationRingEvent} class.
 */
class BlockNotificationRingEventTest {

    /**
     * Tests that a new BlockNotificationRingEvent has null values for both notification types.
     */
    @Test
    @DisplayName("New BlockNotificationRingEvent should have null values")
    void newEventShouldHaveNullValues() {
        final BlockNotificationRingEvent event = new BlockNotificationRingEvent();

        assertNull(event.getVerificationNotification());
        assertNull(event.getPersistedNotification());
    }

    /**
     * Tests setting and getting a verification notification.
     */
    @Test
    @DisplayName("Should set and get verification notification correctly")
    void shouldSetAndGetVerificationNotification() {
        final BlockNotificationRingEvent event = new BlockNotificationRingEvent();
        final VerificationNotification notification = new VerificationNotification(true, 1, null, null);

        event.set(notification);

        assertEquals(notification, event.getVerificationNotification());
        assertNull(event.getPersistedNotification(), "Persisted notification should be null");
    }

    /**
     * Tests setting and getting a persisted notification.
     */
    @Test
    @DisplayName("Should set and get persisted notification correctly")
    void shouldSetAndGetPersistedNotification() {
        final BlockNotificationRingEvent event = new BlockNotificationRingEvent();
        final PersistedNotification notification = new PersistedNotification(1, 2, 10);

        event.set(notification);

        assertEquals(notification, event.getPersistedNotification());
        assertNull(event.getVerificationNotification(), "Verification notification should be null");
    }

    /**
     * Tests that setting a verification notification clears any existing persisted notification.
     */
    @Test
    @DisplayName("Setting verification notification should clear persisted notification")
    void settingVerificationNotificationShouldClearPersistedNotification() {
        final BlockNotificationRingEvent event = new BlockNotificationRingEvent();
        final PersistedNotification persistedNotification = new PersistedNotification(1, 2, 10);
        final VerificationNotification verificationNotification = new VerificationNotification(true, 1, null, null);

        // First set persisted notification
        event.set(persistedNotification);
        assertEquals(persistedNotification, event.getPersistedNotification());

        // Then set verification notification
        event.set(verificationNotification);

        assertEquals(verificationNotification, event.getVerificationNotification());
        assertNull(event.getPersistedNotification(), "Persisted notification should be cleared");
    }

    /**
     * Tests that setting a persisted notification clears any existing verification notification.
     */
    @Test
    @DisplayName("Setting persisted notification should clear verification notification")
    void settingPersistedNotificationShouldClearVerificationNotification() {
        final BlockNotificationRingEvent event = new BlockNotificationRingEvent();
        final VerificationNotification verificationNotification = new VerificationNotification(true, 1, null, null);
        final PersistedNotification persistedNotification = new PersistedNotification(1, 2, 10);

        // First set verification notification
        event.set(verificationNotification);
        assertEquals(verificationNotification, event.getVerificationNotification());

        // Then set persisted notification
        event.set(persistedNotification);

        assertEquals(persistedNotification, event.getPersistedNotification());
        assertNull(event.getVerificationNotification(), "Verification notification should be cleared");
    }

    /**
     * Tests that the event can be reused by setting different verification notifications.
     */
    @Test
    @DisplayName("Should allow reuse with different verification notifications")
    void shouldAllowReuseWithDifferentVerificationNotifications() {
        final BlockNotificationRingEvent event = new BlockNotificationRingEvent();
        final VerificationNotification notification1 = new VerificationNotification(true, 1, null, null);
        final VerificationNotification notification2 = new VerificationNotification(true, 1, null, null);

        event.set(notification1);
        assertEquals(notification1, event.getVerificationNotification());

        // Set another verification notification
        event.set(notification2);
        assertEquals(notification2, event.getVerificationNotification());
        assertNull(event.getPersistedNotification());
    }

    /**
     * Tests that the event can be reused by setting different persisted notifications.
     */
    @Test
    @DisplayName("Should allow reuse with different persisted notifications")
    void shouldAllowReuseWithDifferentPersistedNotifications() {
        final BlockNotificationRingEvent event = new BlockNotificationRingEvent();
        final PersistedNotification notification1 = new PersistedNotification(1, 2, 10);
        final PersistedNotification notification2 = new PersistedNotification(1, 2, 10);

        event.set(notification1);
        assertEquals(notification1, event.getPersistedNotification());

        // Set another persisted notification
        event.set(notification2);
        assertEquals(notification2, event.getPersistedNotification());
        assertNull(event.getVerificationNotification());
    }
}
