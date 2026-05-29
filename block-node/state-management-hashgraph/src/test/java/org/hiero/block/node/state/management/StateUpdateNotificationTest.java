// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.management;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.concurrent.atomic.AtomicReference;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.StateUpdateNotification;
import org.hiero.block.node.spi.blockmessaging.StateUpdateNotification.StateUpdateType;
import org.junit.jupiter.api.Test;

/**
 * Exercises the new {@code sendStateUpdate} send method on
 * {@link org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility} and the
 * {@code handleStateUpdate} hook on {@link BlockNotificationHandler}. Uses the
 * synchronous test fixture so the handler call is observable inline.
 */
class StateUpdateNotificationTest {

    @Test
    void sendStateUpdateRoutesToRegisteredHandler() {
        final TestBlockMessagingFacility facility = new TestBlockMessagingFacility();
        final AtomicReference<StateUpdateNotification> observed = new AtomicReference<>();

        final BlockNotificationHandler handler = new BlockNotificationHandler() {
            @Override
            public void handleStateUpdate(final StateUpdateNotification notification) {
                observed.set(notification);
            }
        };
        facility.registerBlockNotificationHandler(handler, false, "test");

        final StateUpdateNotification notification =
                new StateUpdateNotification(StateUpdateType.VERIFIED, 42L, 7L, Bytes.fromHex("deadbeef"), 1024L);
        facility.sendStateUpdate(notification);

        assertThat(observed.get()).isEqualTo(notification);
        assertThat(facility.getSentStateUpdateNotifications()).containsExactly(notification);
    }
}