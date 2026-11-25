// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification.FailureInfo;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification.FailureType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Tests for the [VerificationOutbox].
@DisplayName("Verification Outbox Tests")
class VerificationOutboxTest {
    /// This test aims to assert that a notification given to the outbox is sent to
    /// messaging on the calling thread, unchanged, so it has been sent when the call
    /// returns.
    @Test
    @DisplayName("send() sends the notification to messaging before it returns")
    void testSendsTheNotificationBeforeItReturns() {
        final TestBlockMessagingFacility messaging = new TestBlockMessagingFacility();
        final VerificationNotification notification = failure(7L);
        new VerificationOutbox(messaging).send(notification);
        assertThat(messaging.getSentVerificationNotifications()).containsExactly(notification);
    }

    /// This test aims to assert that a failure to send is swallowed: the outbox never
    /// throws to its caller, so a session's result handling and the plugin's ingress
    /// threads carry on.
    @Test
    @DisplayName("send() never throws when messaging fails")
    void testNeverThrowsWhenMessagingFails() {
        final TestBlockMessagingFacility failingMessaging = new TestBlockMessagingFacility() {
            @Override
            public void sendBlockVerification(final VerificationNotification notification) {
                throw new IllegalStateException("send failed");
            }
        };
        final VerificationOutbox toTest = new VerificationOutbox(failingMessaging);
        assertThatNoException().isThrownBy(() -> toTest.send(failure(7L)));
    }

    /// A failure notification for a block.
    private static VerificationNotification failure(final long blockNumber) {
        return new VerificationNotification(
                false,
                FailureInfo.standard(FailureType.BAD_BLOCK_PROOF),
                blockNumber,
                null,
                null,
                BlockSource.BACKFILL);
    }
}
