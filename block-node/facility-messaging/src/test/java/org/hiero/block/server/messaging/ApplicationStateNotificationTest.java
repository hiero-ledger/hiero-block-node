// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.LockSupport;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.async.TestThreadPoolManager;
import org.hiero.block.node.messaging.BlockMessagingFacilityImpl;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.AddressBookHistoryNotification;
import org.hiero.block.node.spi.blockmessaging.ApplicationStateNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.AvailableBlocksNotification;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.PublisherStatusUpdateNotification;
import org.hiero.block.node.spi.blockmessaging.PublisherStatusUpdateNotification.UpdateType;
import org.hiero.block.node.spi.blockmessaging.StoredBlocksNotification;
import org.hiero.block.node.spi.blockmessaging.TssDataNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for application state notification delivery through {@link BlockMessagingFacilityImpl}.
 * Covers registration, delivery of all four application state notification types, isolation from
 * block-stream events, unregistration, and multi-handler fan-out.
 */
public class ApplicationStateNotificationTest {

    private TestThreadPoolManager<BlockingExecutor, ScheduledBlockingExecutor> threadPoolManager;
    private BlockNodeContext context;

    @BeforeEach
    void setup() {
        threadPoolManager = new TestThreadPoolManager<>(
                new BlockingExecutor(new LinkedBlockingQueue<>()),
                new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
        context = TestConfig.generateContext(threadPoolManager);
    }

    /**
     * Default method implementations on {@link ApplicationStateNotificationHandler} must be no-ops
     * that do not throw.
     */
    @Test
    void defaultMethodsAreNoOps() {
        final ApplicationStateNotificationHandler handler = new ApplicationStateNotificationHandler() {};
        handler.handleTssDataUpdate(new TssDataNotification(null));
        handler.handleAddressBookHistoryUpdate(new AddressBookHistoryNotification(null));
        handler.handleStoredBlocksUpdate(new StoredBlocksNotification(List.of()));
        handler.handleAvailableBlocksUpdate(new AvailableBlocksNotification(List.of()));
    }

    /**
     * A handler pre-registered before {@code start()} must receive all four application state
     * notification types after the service starts.
     */
    @Test
    void preRegisteredHandlerReceivesAllFourTypes() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(4);
        final AtomicIntegerArray counters = new AtomicIntegerArray(4);
        final ApplicationStateNotificationHandler handler = new ApplicationStateNotificationHandler() {
            @Override
            public void handleTssDataUpdate(final TssDataNotification n) {
                counters.incrementAndGet(0);
                latch.countDown();
            }

            @Override
            public void handleAddressBookHistoryUpdate(final AddressBookHistoryNotification n) {
                counters.incrementAndGet(1);
                latch.countDown();
            }

            @Override
            public void handleStoredBlocksUpdate(final StoredBlocksNotification n) {
                counters.incrementAndGet(2);
                latch.countDown();
            }

            @Override
            public void handleAvailableBlocksUpdate(final AvailableBlocksNotification n) {
                counters.incrementAndGet(3);
                latch.countDown();
            }
        };

        final BlockMessagingFacility service = new BlockMessagingFacilityImpl();
        service.init(context, null);
        service.registerApplicationStateNotificationHandler(handler, false, "pre-reg");
        service.start();

        sendAllApplicationStateNotifications(service);
        threadPoolManager
                .executor()
                .executeAsync(false, 10_000L, true, true, () -> Executors.newSingleThreadExecutor());

        assertTrue(latch.await(20, TimeUnit.SECONDS), "Handler did not receive all four notification types");
        assertEquals(1, counters.get(0), "TSS data count");
        assertEquals(1, counters.get(1), "Address book history count");
        assertEquals(1, counters.get(2), "Stored blocks count");
        assertEquals(1, counters.get(3), "Available blocks count");
        service.stop();
    }

    /**
     * A handler registered dynamically after {@code start()} must also receive all four
     * application state notification types.
     */
    @Test
    void dynamicallyRegisteredHandlerReceivesAllFourTypes() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(4);
        final AtomicIntegerArray counters = new AtomicIntegerArray(4);
        final ApplicationStateNotificationHandler handler = new ApplicationStateNotificationHandler() {
            @Override
            public void handleTssDataUpdate(final TssDataNotification n) {
                counters.incrementAndGet(0);
                latch.countDown();
            }

            @Override
            public void handleAddressBookHistoryUpdate(final AddressBookHistoryNotification n) {
                counters.incrementAndGet(1);
                latch.countDown();
            }

            @Override
            public void handleStoredBlocksUpdate(final StoredBlocksNotification n) {
                counters.incrementAndGet(2);
                latch.countDown();
            }

            @Override
            public void handleAvailableBlocksUpdate(final AvailableBlocksNotification n) {
                counters.incrementAndGet(3);
                latch.countDown();
            }
        };

        final BlockMessagingFacility service = new BlockMessagingFacilityImpl();
        service.init(context, null);
        service.start();
        // register AFTER start
        service.registerApplicationStateNotificationHandler(handler, false, "dynamic");

        sendAllApplicationStateNotifications(service);
        threadPoolManager
                .executor()
                .executeAsync(false, 10_000L, true, true, () -> Executors.newSingleThreadExecutor());

        assertTrue(latch.await(20, TimeUnit.SECONDS), "Dynamically registered handler did not receive all types");
        assertEquals(1, counters.get(0));
        assertEquals(1, counters.get(1));
        assertEquals(1, counters.get(2));
        assertEquals(1, counters.get(3));
        service.stop();
    }

    /**
     * A CPU-intensive application state handler (platform thread) must receive notifications just
     * like a virtual-thread handler.
     */
    @Test
    void cpuIntensiveHandlerReceivesNotifications() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final ApplicationStateNotificationHandler handler = new ApplicationStateNotificationHandler() {
            @Override
            public void handleTssDataUpdate(final TssDataNotification n) {
                latch.countDown();
            }
        };

        final BlockMessagingFacility service = new BlockMessagingFacilityImpl();
        service.init(context, null);
        service.registerApplicationStateNotificationHandler(handler, true, "cpu-intensive"); // platform thread
        service.start();

        service.sendTssDataUpdate(new TssDataNotification(null));
        threadPoolManager
                .executor()
                .executeAsync(false, 10_000L, true, true, () -> Executors.newSingleThreadExecutor());

        assertTrue(latch.await(20, TimeUnit.SECONDS), "CPU-intensive handler did not receive notification");
        service.stop();
    }

    /**
     * Multiple handlers must all receive the same application state notifications (fan-out).
     */
    @Test
    void multipleHandlersAllReceiveNotifications() throws InterruptedException {
        final int handlerCount = 3;
        final CountDownLatch latch = new CountDownLatch(handlerCount);
        final AtomicIntegerArray counters = new AtomicIntegerArray(handlerCount);

        final BlockMessagingFacility service = new BlockMessagingFacilityImpl();
        service.init(context, null);
        for (int i = 0; i < handlerCount; i++) {
            final int idx = i;
            service.registerApplicationStateNotificationHandler(
                    new ApplicationStateNotificationHandler() {
                        @Override
                        public void handleTssDataUpdate(final TssDataNotification n) {
                            counters.incrementAndGet(idx);
                            latch.countDown();
                        }
                    },
                    false,
                    "handler-" + i);
        }
        service.start();

        service.sendTssDataUpdate(new TssDataNotification(null));
        threadPoolManager
                .executor()
                .executeAsync(false, 10_000L, true, true, () -> Executors.newSingleThreadExecutor());

        assertTrue(latch.await(20, TimeUnit.SECONDS), "Not all handlers received the notification");
        for (int i = 0; i < handlerCount; i++) {
            assertEquals(1, counters.get(i), "Handler " + i + " count");
        }
        service.stop();
    }

    /**
     * Unregistering a handler must stop further delivery to it while not affecting other handlers.
     */
    @Test
    void unregisteredHandlerStopsReceivingNotifications() throws InterruptedException {
        final CountDownLatch firstBatchLatch = new CountDownLatch(1);
        final AtomicIntegerArray counters = new AtomicIntegerArray(1);
        final ApplicationStateNotificationHandler handler = new ApplicationStateNotificationHandler() {
            @Override
            public void handleTssDataUpdate(final TssDataNotification n) {
                counters.incrementAndGet(0);
                firstBatchLatch.countDown();
            }
        };

        final BlockMessagingFacility service = new BlockMessagingFacilityImpl();
        service.init(context, null);
        service.registerApplicationStateNotificationHandler(handler, false, "to-unregister");
        service.start();

        // First batch — handler should receive it.
        service.sendTssDataUpdate(new TssDataNotification(null));
        threadPoolManager
                .executor()
                .executeAsync(false, 10_000L, true, true, () -> Executors.newSingleThreadExecutor());
        assertTrue(firstBatchLatch.await(20, TimeUnit.SECONDS));
        assertEquals(1, counters.get(0));

        // Unregister, then send more.
        service.unregisterApplicationStateNotificationHandler(handler);
        LockSupport.parkNanos(10_000_000L); // 10 ms for unregister to propagate

        service.sendTssDataUpdate(new TssDataNotification(null));
        threadPoolManager
                .executor()
                .executeAsync(false, 10_000L, true, true, () -> Executors.newSingleThreadExecutor());
        LockSupport.parkNanos(100_000_000L); // 100 ms — unregistered, no latch to gate on

        assertEquals(1, counters.get(0), "Handler should not receive events after unregistration");
        service.stop();
    }

    /**
     * An {@link ApplicationStateNotificationHandler} must silently ignore block-stream notification
     * events; only the application state notification used as a sentinel must be delivered.
     */
    @Test
    void applicationStateHandlerIgnoresBlockStreamEvents() throws InterruptedException {
        final CountDownLatch sentinelLatch = new CountDownLatch(1);
        final AtomicIntegerArray counters = new AtomicIntegerArray(1);
        final ApplicationStateNotificationHandler handler = new ApplicationStateNotificationHandler() {
            @Override
            public void handleTssDataUpdate(final TssDataNotification n) {
                // Sentinel: only this should arrive.
                counters.incrementAndGet(0);
                sentinelLatch.countDown();
            }
        };

        final BlockMessagingFacility service = new BlockMessagingFacilityImpl();
        service.init(context, null);
        service.registerApplicationStateNotificationHandler(handler, false, "app-state-handler");
        service.start();

        // Send three block-stream notifications, then one TSS as sentinel.
        service.sendBlockVerification(new VerificationNotification(true, null, 1L, null, null, BlockSource.PUBLISHER));
        service.sendBlockPersisted(new PersistedNotification(1L, true, 0, BlockSource.PUBLISHER));
        service.sendPublisherStatusUpdate(new PublisherStatusUpdateNotification(UpdateType.PUBLISHER_CONNECTED, 1));
        service.sendTssDataUpdate(new TssDataNotification(null));
        threadPoolManager
                .executor()
                .executeAsync(false, 10_000L, true, true, () -> Executors.newSingleThreadExecutor());

        // Wait for sentinel; only 1 application state notification should have been delivered.
        assertTrue(sentinelLatch.await(20, TimeUnit.SECONDS));
        assertEquals(
                1,
                counters.get(0),
                "Application state handler should only receive TSS sentinel, not block-stream events");
        service.stop();
    }

    /**
     * A {@link BlockNotificationHandler} must silently ignore application state notification
     * events; only the verification notification used as a sentinel must trigger the block handler.
     */
    @Test
    void blockHandlerIgnoresApplicationStateEvents() throws InterruptedException {
        final CountDownLatch verificationLatch = new CountDownLatch(1);
        final AtomicIntegerArray counters = new AtomicIntegerArray(1);
        final BlockNotificationHandler blockHandler = new BlockNotificationHandler() {
            @Override
            public void handleVerification(final VerificationNotification n) {
                counters.incrementAndGet(0);
                verificationLatch.countDown();
            }
        };

        final BlockMessagingFacility service = new BlockMessagingFacilityImpl();
        service.init(context, null);
        service.registerBlockNotificationHandler(blockHandler, false, "block-handler");
        service.start();

        // Send all four application state notifications, then one verification as sentinel.
        sendAllApplicationStateNotifications(service);
        service.sendBlockVerification(new VerificationNotification(true, null, 1L, null, null, BlockSource.PUBLISHER));
        threadPoolManager
                .executor()
                .executeAsync(false, 10_000L, true, true, () -> Executors.newSingleThreadExecutor());

        // Wait for sentinel; block handler should only have fired once (the verification).
        assertTrue(verificationLatch.await(20, TimeUnit.SECONDS));
        assertEquals(
                1,
                counters.get(0),
                "Block handler should only receive verification sentinel, not application state events");
        service.stop();
    }

    // ---- helpers ----

    private static void sendAllApplicationStateNotifications(final BlockMessagingFacility service) {
        service.sendTssDataUpdate(new TssDataNotification(null));
        service.sendAddressBookHistoryUpdate(new AddressBookHistoryNotification(null));
        service.sendStoredBlocksUpdate(new StoredBlocksNotification(List.of()));
        service.sendAvailableBlocksUpdate(new AvailableBlocksNotification(List.of()));
    }
}
