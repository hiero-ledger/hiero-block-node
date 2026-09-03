// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.messaging;

import org.hiero.block.node.spi.blockmessaging.AddressBookHistoryNotification;
import org.hiero.block.node.spi.blockmessaging.AvailableBlocksNotification;
import org.hiero.block.node.spi.blockmessaging.BackfilledBlockNotification;
import org.hiero.block.node.spi.blockmessaging.NewestBlockKnownToNetworkNotification;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.PublisherStatusUpdateNotification;
import org.hiero.block.node.spi.blockmessaging.StoredBlocksNotification;
import org.hiero.block.node.spi.blockmessaging.TssDataNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;

/**
 * Simple mutable container for notifications. The notifications ring buffer is
 * made up of these events. Only one of the fields should be set at any time.
 * These notifications are published to downstream subscribers through the LMAX
 * Disruptor.
 */
public final class BlockNotificationRingEvent {
    /** The block verification notification to be published to downstream subscribers through the LMAX Disruptor. */
    private VerificationNotification verificationNotification;
    /** The block persistence notification to be published to downstream subscribers through the LMAX Disruptor. */
    private PersistedNotification persistedNotification;
    /** The Backfilled block notification to be published to downstream subscribers through the LMAX Disruptor. */
    private BackfilledBlockNotification backfilledBlockNotification;
    /** The newest block known to network notification to be published to downstream subscribers through the LMAX Disruptor. */
    private NewestBlockKnownToNetworkNotification newestBlockKnownToNetworkNotification;
    /** The publisher status update notification to be published to downstream subscribers through the LMAX Disruptor. */
    private PublisherStatusUpdateNotification publisherStatusUpdateNotification;
    /** The TSS data update notification to be published to downstream subscribers through the LMAX Disruptor. */
    private TssDataNotification tssDataNotification;
    /** The address book history update notification to be published to downstream subscribers through the LMAX Disruptor. */
    private AddressBookHistoryNotification addressBookHistoryNotification;
    /** The stored blocks update notification to be published to downstream subscribers through the LMAX Disruptor. */
    private StoredBlocksNotification storedBlocksNotification;
    /** The available blocks update notification to be published to downstream subscribers through the LMAX Disruptor. */
    private AvailableBlocksNotification availableBlocksNotification;

    /**
     * Sets the given notification to be published to downstream subscribers
     * through the LMAX Disruptor.
     *
     * @param notification to set
     */
    public void set(final VerificationNotification notification) {
        this.verificationNotification = notification;
        this.persistedNotification = null;
        this.backfilledBlockNotification = null;
        this.newestBlockKnownToNetworkNotification = null;
        this.publisherStatusUpdateNotification = null;
        this.tssDataNotification = null;
        this.addressBookHistoryNotification = null;
        this.storedBlocksNotification = null;
        this.availableBlocksNotification = null;
    }

    /**
     * Sets the given notification to be published to downstream subscribers
     * through the LMAX Disruptor.
     *
     * @param notification to set
     */
    public void set(final PersistedNotification notification) {
        this.persistedNotification = notification;
        this.verificationNotification = null;
        this.backfilledBlockNotification = null;
        this.newestBlockKnownToNetworkNotification = null;
        this.publisherStatusUpdateNotification = null;
        this.tssDataNotification = null;
        this.addressBookHistoryNotification = null;
        this.storedBlocksNotification = null;
        this.availableBlocksNotification = null;
    }

    /**
     * Sets the given notification to be published to downstream subscribers
     * through the LMAX Disruptor.
     *
     * @param notification to set
     */
    public void set(final BackfilledBlockNotification notification) {
        this.backfilledBlockNotification = notification;
        this.verificationNotification = null;
        this.persistedNotification = null;
        this.newestBlockKnownToNetworkNotification = null;
        this.publisherStatusUpdateNotification = null;
        this.tssDataNotification = null;
        this.addressBookHistoryNotification = null;
        this.storedBlocksNotification = null;
        this.availableBlocksNotification = null;
    }

    /**
     * Sets the given notification to be published to downstream subscribers
     * through the LMAX Disruptor.
     *
     * @param notification to set
     */
    public void set(final NewestBlockKnownToNetworkNotification notification) {
        this.newestBlockKnownToNetworkNotification = notification;
        this.backfilledBlockNotification = null;
        this.verificationNotification = null;
        this.persistedNotification = null;
        this.publisherStatusUpdateNotification = null;
        this.tssDataNotification = null;
        this.addressBookHistoryNotification = null;
        this.storedBlocksNotification = null;
        this.availableBlocksNotification = null;
    }

    /**
     * Sets the given notification to be published to downstream subscribers
     * through the LMAX Disruptor.
     *
     * @param notification to set
     */
    public void set(final PublisherStatusUpdateNotification notification) {
        this.publisherStatusUpdateNotification = notification;
        this.newestBlockKnownToNetworkNotification = null;
        this.backfilledBlockNotification = null;
        this.verificationNotification = null;
        this.persistedNotification = null;
        this.tssDataNotification = null;
        this.addressBookHistoryNotification = null;
        this.storedBlocksNotification = null;
        this.availableBlocksNotification = null;
    }

    /**
     * Sets the given notification to be published to downstream subscribers
     * through the LMAX Disruptor.
     *
     * @param notification to set
     */
    public void set(final TssDataNotification notification) {
        this.tssDataNotification = notification;
        this.verificationNotification = null;
        this.persistedNotification = null;
        this.backfilledBlockNotification = null;
        this.newestBlockKnownToNetworkNotification = null;
        this.publisherStatusUpdateNotification = null;
        this.addressBookHistoryNotification = null;
        this.storedBlocksNotification = null;
        this.availableBlocksNotification = null;
    }

    /**
     * Sets the given notification to be published to downstream subscribers
     * through the LMAX Disruptor.
     *
     * @param notification to set
     */
    public void set(final AddressBookHistoryNotification notification) {
        this.addressBookHistoryNotification = notification;
        this.verificationNotification = null;
        this.persistedNotification = null;
        this.backfilledBlockNotification = null;
        this.newestBlockKnownToNetworkNotification = null;
        this.publisherStatusUpdateNotification = null;
        this.tssDataNotification = null;
        this.storedBlocksNotification = null;
        this.availableBlocksNotification = null;
    }

    /**
     * Sets the given notification to be published to downstream subscribers
     * through the LMAX Disruptor.
     *
     * @param notification to set
     */
    public void set(final StoredBlocksNotification notification) {
        this.storedBlocksNotification = notification;
        this.verificationNotification = null;
        this.persistedNotification = null;
        this.backfilledBlockNotification = null;
        this.newestBlockKnownToNetworkNotification = null;
        this.publisherStatusUpdateNotification = null;
        this.tssDataNotification = null;
        this.addressBookHistoryNotification = null;
        this.availableBlocksNotification = null;
    }

    /**
     * Sets the given notification to be published to downstream subscribers
     * through the LMAX Disruptor.
     *
     * @param notification to set
     */
    public void set(final AvailableBlocksNotification notification) {
        this.availableBlocksNotification = notification;
        this.verificationNotification = null;
        this.persistedNotification = null;
        this.backfilledBlockNotification = null;
        this.newestBlockKnownToNetworkNotification = null;
        this.publisherStatusUpdateNotification = null;
        this.tssDataNotification = null;
        this.addressBookHistoryNotification = null;
        this.storedBlocksNotification = null;
    }

    /**
     * Gets the verification notification of the event from the LMAX Disruptor
     * on the consumer side.
     * If the event is not a {@link VerificationNotification}, this
     * will return null.
     *
     * @return the value of the event
     */
    public VerificationNotification getVerificationNotification() {
        return verificationNotification;
    }

    /**
     * Gets the persisted notification of the event from the LMAX Disruptor on
     * the consumer side.
     * If the event is not a {@link PersistedNotification}, this
     * will return null.
     *
     * @return the value of the event
     */
    public PersistedNotification getPersistedNotification() {
        return persistedNotification;
    }

    /**
     * Gets the backfilled block notification of the event from the LMAX
     * Disruptor on the consumer side.
     * If the event is not a {@link BackfilledBlockNotification}, this
     * will return null.
     *
     * @return the value of the event
     */
    public BackfilledBlockNotification getBackfilledBlockNotification() {
        return backfilledBlockNotification;
    }

    /**
     * Gets the newest block known to network notification of the event from the
     * LMAX Disruptor on the consumer side.
     * If the event is not a {@link NewestBlockKnownToNetworkNotification}, this
     * will return null.
     *
     * @return the value of the event
     */
    public NewestBlockKnownToNetworkNotification getNewestBlockKnownToNetworkNotification() {
        return newestBlockKnownToNetworkNotification;
    }

    /**
     * Gets the publisher status update notification of the event from the LMAX
     * Disruptor on the consumer side.
     * If the event is not a {@link PublisherStatusUpdateNotification}, this
     * will return null.
     *
     * @return the value of the event
     */
    public PublisherStatusUpdateNotification getPublisherStatusUpdateNotification() {
        return publisherStatusUpdateNotification;
    }

    /**
     * Gets the TSS data update notification of the event from the LMAX Disruptor
     * on the consumer side.
     * If the event is not a {@link TssDataNotification}, this will return null.
     *
     * @return the value of the event
     */
    public TssDataNotification getTssDataNotification() {
        return tssDataNotification;
    }

    /**
     * Gets the address book history update notification of the event from the LMAX Disruptor
     * on the consumer side.
     * If the event is not a {@link AddressBookHistoryNotification}, this will return null.
     *
     * @return the value of the event
     */
    public AddressBookHistoryNotification getAddressBookHistoryNotification() {
        return addressBookHistoryNotification;
    }

    /**
     * Gets the stored blocks update notification of the event from the LMAX Disruptor
     * on the consumer side.
     * If the event is not a {@link StoredBlocksNotification}, this will return null.
     *
     * @return the value of the event
     */
    public StoredBlocksNotification getStoredBlocksNotification() {
        return storedBlocksNotification;
    }

    /**
     * Gets the available blocks update notification of the event from the LMAX Disruptor
     * on the consumer side.
     * If the event is not a {@link AvailableBlocksNotification}, this will return null.
     *
     * @return the value of the event
     */
    public AvailableBlocksNotification getAvailableBlocksNotification() {
        return availableBlocksNotification;
    }
}
