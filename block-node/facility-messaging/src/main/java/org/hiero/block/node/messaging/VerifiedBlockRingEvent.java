// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.messaging;

import org.hiero.block.node.spi.blockmessaging.VerificationNotification;

/**
 * Simple mutable container for a verified block: the result of verifying a block, which carries the whole block when
 * verification succeeded. The verified block ring buffer is made up of these events.
 */
public final class VerifiedBlockRingEvent {
    /** The block verification notification to be published to downstream subscribers through the LMAX Disruptor. */
    private VerificationNotification verificationNotification;

    /**
     * Sets the given notification to be published to downstream subscribers through the LMAX Disruptor.
     *
     * @param notification the notification to set
     */
    public void set(final VerificationNotification notification) {
        this.verificationNotification = notification;
    }

    /**
     * Gets the notification of the event from the LMAX Disruptor on the consumer side.
     *
     * @return the notification of the event
     */
    public VerificationNotification get() {
        return verificationNotification;
    }
}
