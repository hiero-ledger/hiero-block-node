// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.hiero.block.server.messaging.BlockNotification.Type;
import org.junit.jupiter.api.Test;

public class MessagingServiceBlockNotificationTest {

    /**
     * Simple test to verify that the messaging service can handle multiple block notification handlers and that
     *
     * @throws Throwable if there was an error during the test
     */
    @Test
    void testSimpleBlockNotificationHandlers() throws Throwable {
        CountDownLatch latch = new CountDownLatch(3);
        MessagingService messagingService = MessagingService.createMessagingService();
        List<TestBlockNotificationHandler> testHandlers = List.of(
                new TestBlockNotificationHandler(1, 2000, latch::countDown, 0),
                new TestBlockNotificationHandler(2, 2000, latch::countDown, 1),
                new TestBlockNotificationHandler(3, 2000, latch::countDown, 2));
        // Register the handlers
        for (TestBlockNotificationHandler handler : testHandlers) {
            messagingService.registerBlockNotificationHandler(handler);
        }
        // start the messaging service
        messagingService.start();
        // collect any exceptions thrown by threads
        AtomicReference<Throwable> threadException = new AtomicReference<>();
        // start thread to send 100 items to each handler
        new Thread(() -> {
                    try {
                        for (int i = 0; i < 2000; i++) {
                            messagingService.sendBlockNotification(new BlockNotification(i, Type.BLOCK_PERSISTED));
                            // check that the back pressure is working and this thread is being held back by the slowest
                            // handler
                            if (i == 1500) {
                                // we expect at this point we are sending item 1500
                                // handler 0 that is fast has processed almost 1500
                                final int count0 = testHandlers.get(0).counter.get();
                                assertTrue(
                                        count0 > 1400,
                                        "Handler 1 should have processed more than 1400 items, count = " + count0);
                                // handler 2 that is slow has processed no more than 1100
                                final int count2 = testHandlers.get(2).counter.get();
                                assertTrue(
                                        count2 < 1100,
                                        "Handler 3 should not have processed more than 1100 items, count = " + count2);
                            }
                        }
                    } catch (Throwable e) {
                        threadException.set(e);
                        // important to countdown latch here if we fail otherwise we hang
                        for (int j = 0; j <= latch.getCount() + 1; j++) {
                            latch.countDown();
                        }
                    }
                })
                .start();
        // wait for all handlers to finish
        try {
            System.out.println("Wait");
            latch.await();
            System.out.println("All handlers finished");

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        // shutdown the messaging service
        messagingService.shutdown();
        // throw any exception thrown by the thread
        if (threadException.get() != null) {
            throw threadException.get();
        }
        // verify that all handlers received the expected number of items
        for (TestBlockNotificationHandler handler : testHandlers) {
            assertEquals(2000, handler.counter.get());
        }
    }

    /**
     * TestBlockNotificationHandler is a test implementation of the BlockNotificationHandler interface. It keeps track of the number of
     * items received and calls a callback when the expected number of items is reached. It has an optional delay to simulate
     * processing time.
     */
    private static class TestBlockNotificationHandler implements BlockNotificationHandler {
        final AtomicInteger counter = new AtomicInteger(0);
        final int handlerId;
        final int expectedCount;
        final int delayMs;
        final Runnable completeCallback;

        public TestBlockNotificationHandler(int handlerId, int expectedCount, Runnable completeCallback, int delayMs) {
            this.handlerId = handlerId;
            this.expectedCount = expectedCount;
            this.completeCallback = completeCallback;
            this.delayMs = delayMs;
        }

        @Override
        public void handleBlockNotification(BlockNotification notification) {
            // Increment the counter for this handler
            int count = counter.incrementAndGet();
            // Check if the expected count is reached
            if (count >= expectedCount) {
                // Call the complete callback
                completeCallback.run();
            }
            //                        System.out.println("Handler " + handlerId + " received " + count + " items -
            // "+notification);
            // Simulate some processing delay
            if (delayMs > 0) {
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
