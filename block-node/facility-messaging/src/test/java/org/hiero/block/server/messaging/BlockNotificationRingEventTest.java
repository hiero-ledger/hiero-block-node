// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import org.hiero.block.api.BlockRange;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.messaging.BlockNotificationRingEvent;
import org.hiero.block.node.spi.blockmessaging.AddressBookHistoryNotification;
import org.hiero.block.node.spi.blockmessaging.AvailableBlocksNotification;
import org.hiero.block.node.spi.blockmessaging.BackfilledBlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.NewestBlockKnownToNetworkNotification;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.PublisherStatusUpdateNotification;
import org.hiero.block.node.spi.blockmessaging.PublisherStatusUpdateNotification.UpdateType;
import org.hiero.block.node.spi.blockmessaging.StoredBlocksNotification;
import org.hiero.block.node.spi.blockmessaging.TssDataNotification;
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
        assertNull(event.getBackfilledBlockNotification());
        assertNull(event.getNewestBlockKnownToNetworkNotification());
        assertNull(event.getPublisherStatusUpdateNotification());
        assertNull(event.getTssDataNotification());
        assertNull(event.getAddressBookHistoryNotification());
        assertNull(event.getStoredBlocksNotification());
        assertNull(event.getAvailableBlocksNotification());
    }

    /**
     * Tests setting and getting a verification notification.
     */
    @Test
    @DisplayName("Should set and get verification notification correctly")
    void shouldSetAndGetVerificationNotification() {
        final BlockNotificationRingEvent event = new BlockNotificationRingEvent();
        final VerificationNotification notification =
                new VerificationNotification(true, null, 1, null, null, BlockSource.PUBLISHER);
        event.set(notification);
        assertEquals(notification, event.getVerificationNotification());
        assertNull(event.getPersistedNotification(), "Persisted notification should be null");
        assertNull(event.getBackfilledBlockNotification(), "Backfilled notification should be null");
        assertNull(event.getPublisherStatusUpdateNotification(), "Publisher status update notification should be null");
        assertNull(
                event.getNewestBlockKnownToNetworkNotification(),
                "Newest block known to network notification should be null");
    }

    /**
     * Tests setting and getting a persisted notification.
     */
    @Test
    @DisplayName("Should set and get persisted notification correctly")
    void shouldSetAndGetPersistedNotification() {
        final BlockNotificationRingEvent event = new BlockNotificationRingEvent();
        final PersistedNotification notification = new PersistedNotification(2, true, 10, BlockSource.PUBLISHER);
        event.set(notification);
        assertEquals(notification, event.getPersistedNotification());
        assertNull(event.getVerificationNotification(), "Verification notification should be null");
        assertNull(event.getBackfilledBlockNotification(), "Backfilled notification should be null");
        assertNull(event.getPublisherStatusUpdateNotification(), "Publisher status update notification should be null");
        assertNull(
                event.getNewestBlockKnownToNetworkNotification(),
                "Newest block known to network notification should be null");
    }

    /**
     * Tests setting and getting a backfilled notification.
     */
    @Test
    @DisplayName("Should set and get backfilled notification correctly")
    void shouldSetAndGetBackfilledNotification() {
        final BlockNotificationRingEvent event = new BlockNotificationRingEvent();
        final BackfilledBlockNotification notification =
                new BackfilledBlockNotification(1, BlockUnparsed.newBuilder().build());
        event.set(notification);
        assertEquals(notification, event.getBackfilledBlockNotification());
        assertNull(event.getVerificationNotification(), "Verification notification should be null");
        assertNull(event.getPersistedNotification(), "Persisted notification should be null");
        assertNull(event.getPublisherStatusUpdateNotification(), "Publisher status update notification should be null");
        assertNull(
                event.getNewestBlockKnownToNetworkNotification(),
                "Newest block known to network notification should be null");
    }

    /**
     * Tests setting and getting a backfilled notification.
     */
    @Test
    @DisplayName("Should set and get NewestBlockKnownToNetworkNotification notification correctly")
    void shouldSetAndGetNewestBlockKnownToNetworkNotification() {
        final BlockNotificationRingEvent event = new BlockNotificationRingEvent();
        final NewestBlockKnownToNetworkNotification notification = new NewestBlockKnownToNetworkNotification(10L);
        event.set(notification);
        assertEquals(notification, event.getNewestBlockKnownToNetworkNotification());
        assertNull(event.getVerificationNotification(), "Verification notification should be null");
        assertNull(event.getPersistedNotification(), "Persisted notification should be null");
        assertNull(event.getBackfilledBlockNotification(), "Backfilled notification should be null");
        assertNull(event.getPublisherStatusUpdateNotification(), "Publisher status update notification should be null");
    }

    /**
     * Tests setting and getting a publisher status update notification.
     */
    @Test
    @DisplayName("Should set and get publisher status update notification correctly")
    void shouldSetAndGetPublisherStatusUpdateNotification() {
        final BlockNotificationRingEvent event = new BlockNotificationRingEvent();
        final PublisherStatusUpdateNotification notification =
                new PublisherStatusUpdateNotification(UpdateType.PUBLISHER_CONNECTED, 1);
        event.set(notification);
        assertEquals(notification, event.getPublisherStatusUpdateNotification());
        assertNull(event.getVerificationNotification(), "Verification notification should be null");
        assertNull(event.getPersistedNotification(), "Persisted notification should be null");
        assertNull(event.getBackfilledBlockNotification(), "Backfilled notification should be null");
        assertNull(
                event.getNewestBlockKnownToNetworkNotification(),
                "Newest block known to network notification should be null");
    }

    /**
     * Tests that setting a verification notification clears any existing persisted notification.
     */
    @Test
    @DisplayName("Setting verification notification should clear persisted notification")
    void settingVerificationNotificationShouldClearPersistedNotification() {
        final BlockNotificationRingEvent event = new BlockNotificationRingEvent();
        final PersistedNotification persistedNotification =
                new PersistedNotification(2, true, 10, BlockSource.PUBLISHER);
        final VerificationNotification verificationNotification =
                new VerificationNotification(true, null, 1, null, null, BlockSource.PUBLISHER);

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
        final VerificationNotification verificationNotification =
                new VerificationNotification(true, null, 1, null, null, BlockSource.PUBLISHER);
        final PersistedNotification persistedNotification =
                new PersistedNotification(2, true, 10, BlockSource.PUBLISHER);

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
        final VerificationNotification notification1 =
                new VerificationNotification(true, null, 1, null, null, BlockSource.PUBLISHER);
        final VerificationNotification notification2 =
                new VerificationNotification(true, null, 1, null, null, BlockSource.PUBLISHER);

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
        final PersistedNotification notification1 = new PersistedNotification(2, true, 10, BlockSource.PUBLISHER);
        final PersistedNotification notification2 = new PersistedNotification(2, true, 10, BlockSource.PUBLISHER);

        event.set(notification1);
        assertEquals(notification1, event.getPersistedNotification());

        // Set another persisted notification
        event.set(notification2);
        assertEquals(notification2, event.getPersistedNotification());
        assertNull(event.getVerificationNotification());
    }

    /**
     * Tests setting and getting a TSS data update notification.
     */
    @Test
    @DisplayName("Should set and get TssDataNotification correctly")
    void shouldSetAndGetTssDataNotification() {
        final BlockNotificationRingEvent event = new BlockNotificationRingEvent();
        final TssDataNotification notification = new TssDataNotification(null);
        event.set(notification);
        assertEquals(notification, event.getTssDataNotification());
        assertNull(event.getVerificationNotification(), "Verification notification should be null");
        assertNull(event.getPersistedNotification(), "Persisted notification should be null");
        assertNull(event.getBackfilledBlockNotification(), "Backfilled notification should be null");
        assertNull(event.getNewestBlockKnownToNetworkNotification(), "Newest block notification should be null");
        assertNull(event.getPublisherStatusUpdateNotification(), "Publisher status notification should be null");
        assertNull(event.getAddressBookHistoryNotification(), "Address book history notification should be null");
        assertNull(event.getStoredBlocksNotification(), "Stored blocks notification should be null");
        assertNull(event.getAvailableBlocksNotification(), "Available blocks notification should be null");
    }

    /**
     * Tests setting and getting an address book history update notification.
     */
    @Test
    @DisplayName("Should set and get AddressBookHistoryNotification correctly")
    void shouldSetAndGetAddressBookHistoryNotification() {
        final BlockNotificationRingEvent event = new BlockNotificationRingEvent();
        final AddressBookHistoryNotification notification = new AddressBookHistoryNotification(null);
        event.set(notification);
        assertEquals(notification, event.getAddressBookHistoryNotification());
        assertNull(event.getVerificationNotification(), "Verification notification should be null");
        assertNull(event.getPersistedNotification(), "Persisted notification should be null");
        assertNull(event.getBackfilledBlockNotification(), "Backfilled notification should be null");
        assertNull(event.getNewestBlockKnownToNetworkNotification(), "Newest block notification should be null");
        assertNull(event.getPublisherStatusUpdateNotification(), "Publisher status notification should be null");
        assertNull(event.getTssDataNotification(), "TSS data notification should be null");
        assertNull(event.getStoredBlocksNotification(), "Stored blocks notification should be null");
        assertNull(event.getAvailableBlocksNotification(), "Available blocks notification should be null");
    }

    /**
     * Tests setting and getting a stored blocks update notification.
     */
    @Test
    @DisplayName("Should set and get StoredBlocksNotification correctly")
    void shouldSetAndGetStoredBlocksNotification() {
        final BlockNotificationRingEvent event = new BlockNotificationRingEvent();
        final StoredBlocksNotification notification = new StoredBlocksNotification(List.of());
        event.set(notification);
        assertEquals(notification, event.getStoredBlocksNotification());
        assertNull(event.getVerificationNotification(), "Verification notification should be null");
        assertNull(event.getPersistedNotification(), "Persisted notification should be null");
        assertNull(event.getBackfilledBlockNotification(), "Backfilled notification should be null");
        assertNull(event.getNewestBlockKnownToNetworkNotification(), "Newest block notification should be null");
        assertNull(event.getPublisherStatusUpdateNotification(), "Publisher status notification should be null");
        assertNull(event.getTssDataNotification(), "TSS data notification should be null");
        assertNull(event.getAddressBookHistoryNotification(), "Address book history notification should be null");
        assertNull(event.getAvailableBlocksNotification(), "Available blocks notification should be null");
    }

    /**
     * Tests setting and getting an available blocks update notification.
     */
    @Test
    @DisplayName("Should set and get AvailableBlocksNotification correctly")
    void shouldSetAndGetAvailableBlocksNotification() {
        final BlockNotificationRingEvent event = new BlockNotificationRingEvent();
        final List<BlockRange> ranges =
                List.of(BlockRange.newBuilder().rangeStart(1L).rangeEnd(100L).build());
        final AvailableBlocksNotification notification = new AvailableBlocksNotification(ranges);
        event.set(notification);
        assertEquals(notification, event.getAvailableBlocksNotification());
        assertNull(event.getVerificationNotification(), "Verification notification should be null");
        assertNull(event.getPersistedNotification(), "Persisted notification should be null");
        assertNull(event.getBackfilledBlockNotification(), "Backfilled notification should be null");
        assertNull(event.getNewestBlockKnownToNetworkNotification(), "Newest block notification should be null");
        assertNull(event.getPublisherStatusUpdateNotification(), "Publisher status notification should be null");
        assertNull(event.getTssDataNotification(), "TSS data notification should be null");
        assertNull(event.getAddressBookHistoryNotification(), "Address book history notification should be null");
        assertNull(event.getStoredBlocksNotification(), "Stored blocks notification should be null");
    }

    /**
     * Tests that setting a TSS data notification clears a previously set block-stream notification.
     */
    @Test
    @DisplayName("Setting TssDataNotification should clear block-stream notifications")
    void settingTssDataNotificationShouldClearBlockStreamNotifications() {
        final BlockNotificationRingEvent event = new BlockNotificationRingEvent();
        event.set(new PersistedNotification(1, true, 0, BlockSource.PUBLISHER));
        assertEquals(1L, event.getPersistedNotification().blockNumber());

        event.set(new TssDataNotification(null));
        assertNull(event.getPersistedNotification(), "Persisted notification should be cleared");
        assertNull(event.getVerificationNotification(), "Verification notification should be cleared");
    }
}
