// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.async.TestThreadPoolManager;
import org.hiero.block.node.messaging.BlockMessagingFacilityImpl;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.BlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Proves that a notification type defined entirely outside `spi-plugins` and `facility-messaging`
/// — standing in for one a third-party plugin would define — can be sent and received through
/// [BlockMessagingFacility] using only the public [BlockNotification]/[BlockNotificationHandler]
/// contract, with no change to either module.
///
/// See `docs/design/architecture/generic-block-notifications.md` for the design this validates.
class CustomNotificationTest {

    /// Stand-in for a notification type defined by a plugin outside this module. Neither
    /// `spi-plugins` nor `facility-messaging` know about this type at compile time.
    private record PingNotification(int sequence) implements BlockNotification {}

    @Test
    @DisplayName("A plugin-defined notification type is delivered in order, exactly once, on the handler's own thread")
    void customNotificationTypeIsDeliveredCorrectly() throws InterruptedException {
        final int notificationCount = 20;
        final List<PingNotification> received = new CopyOnWriteArrayList<>();
        final List<String> receivingThreadNames = new CopyOnWriteArrayList<>();
        final CountDownLatch latch = new CountDownLatch(notificationCount);

        final BlockNotificationHandler handler = new BlockNotificationHandler() {
            @Override
            public void handleNotification(final BlockNotification notification) {
                if (notification instanceof PingNotification ping) {
                    received.add(ping);
                    receivingThreadNames.add(Thread.currentThread().getName());
                    latch.countDown();
                }
            }
        };

        final TestThreadPoolManager<BlockingExecutor, ScheduledBlockingExecutor> threadPoolManager =
                new TestThreadPoolManager<>(
                        new BlockingExecutor(new LinkedBlockingQueue<>()),
                        new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
        final BlockNodeContext context = TestConfig.generateContext(threadPoolManager);
        final BlockMessagingFacility messagingService = new BlockMessagingFacilityImpl();
        messagingService.init(context, null);
        messagingService.registerBlockNotificationHandler(handler, false, "ping-handler");
        messagingService.start();

        for (int i = 0; i < notificationCount; i++) {
            messagingService.sendBlockNotification(new PingNotification(i));
        }
        // BlockingExecutor (the test thread pool) only runs queued tasks once asked to; this drains
        // the queued sendBlockNotification publish tasks onto a single thread, preserving order.
        threadPoolManager
                .executor()
                .executeAsync(false, 10_000L, true, true, () -> Executors.newSingleThreadExecutor());

        assertTrue(latch.await(5, TimeUnit.SECONDS), "All notifications should have been delivered");
        messagingService.stop();

        assertEquals(notificationCount, received.size(), "Every notification should be delivered exactly once");
        for (int i = 0; i < notificationCount; i++) {
            assertEquals(i, received.get(i).sequence(), "Notifications should be delivered in order");
        }
        assertEquals(
                1,
                receivingThreadNames.stream().distinct().count(),
                "All notifications for one handler should arrive on the same dedicated thread");
    }
}
