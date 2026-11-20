// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification;

import static java.lang.System.Logger.Level.WARNING;

import java.util.Objects;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;

/// The one place the verification plugin sends its notifications from.
///
/// A send happens on the calling thread. A success is only ever sent from a
/// session thread, before the session advances the last verified block, so when
/// messaging has no room for it, the send waits and every later block waits in
/// the ordering stage of its session. A failure, a cancellation included, can be
/// sent from an ingress thread of the plugin; failures travel on the
/// notifications channel, whose sender never waits.
public final class VerificationOutbox {
    /// Logger for the outbox.
    private static final System.Logger LOGGER = System.getLogger(VerificationOutbox.class.getName());
    /// The messaging facility the notifications are sent to.
    private final BlockMessagingFacility blockMessaging;

    /// Constructor.
    ///
    /// @param blockMessaging the messaging facility to send to, must not be null
    public VerificationOutbox(final BlockMessagingFacility blockMessaging) {
        this.blockMessaging = Objects.requireNonNull(blockMessaging);
    }

    /// Send a notification. A failure to send is logged and swallowed; this method
    /// never throws.
    ///
    /// @param notification the notification to send, must not be null
    public void send(final VerificationNotification notification) {
        try {
            blockMessaging.sendBlockVerification(notification);
        } catch (final RuntimeException e) {
            final String message = "Failed to send verification notification for block %d with source %s"
                    .formatted(notification.blockNumber(), notification.source());
            LOGGER.log(WARNING, message, e);
        }
    }
}
