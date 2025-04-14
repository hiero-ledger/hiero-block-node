// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.messaging;

import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;

/**
 * Simple mutable container for batch of block items. The ring buffer is made up of these events.
 */
public class BlockNotificationRingEvent {
    /** The block verification notification to be published to downstream subscribers through the LMAX Disruptor. */
    private VerificationNotification verificationNotification;
    /** The block persistence notification to be published to downstream subscribers through the LMAX Disruptor. */
    private PersistedNotification persistedNotification;

    /** Constructor for the BlockNotificationRingEvent class. */
    public BlockNotificationRingEvent() {}

    /**
     * Sets the given value to be published to downstream subscribers through the LMAX Disruptor.
     *
     * @param verificationNotification the value to set
     */
    public void set(final VerificationNotification verificationNotification) {
        this.verificationNotification = verificationNotification;
        this.persistedNotification = null;
    }

    /**
     * Sets the given value to be published to downstream subscribers through the LMAX Disruptor.
     *
     * @param persistedNotification the value to set
     */
    public void set(final PersistedNotification persistedNotification) {
        this.verificationNotification = null;
        this.persistedNotification = persistedNotification;
    }

    /**
     * Gets the verification notification of the event from the LMAX Disruptor on the consumer side. If the event is a
     * persisted notification, this will return null.
     *
     * @return the value of the event
     */
    public VerificationNotification getVerificationNotification() {
        return verificationNotification;
    }

    /**
     * Gets the persisted notification of the event from the LMAX Disruptor on the consumer side. If the event is a
     * verification notification, this will return null.
     *
     * @return the value of the event
     */
    public PersistedNotification getPersistedNotification() {
        return persistedNotification;
    }
}
