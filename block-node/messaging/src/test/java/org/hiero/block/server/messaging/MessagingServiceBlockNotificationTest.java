// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.Thread.State;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.stream.IntStream;
import org.hiero.block.server.messaging.BlockNotification.Type;
import org.hiero.block.server.messaging.impl.MessagingServiceImpl;
import org.junit.jupiter.api.Test;

/**
 * Test class for the MessagingService to verify that it can handle block notifications and back pressure. All these
 * tests are super hard to get right as they are highly concurrent. So makes it very hard to not be timing dependent.
 */
public class MessagingServiceBlockNotificationTest {

    /**
     * The number of items to send to the messaging service. This is twice the size of the ring buffer, so that we can
     * test the back pressure and the slow handler.
     */
    public static final int TEST_DATA_COUNT = MessagingServiceImpl.getConfig().queueSize() * 2;

    /**
     * Simple test to verify that the messaging service can handle multiple block notification handlers and that
     * they all receive the same number of items.
     *
     * @throws InterruptedException if the test latch is interrupted
     */
    @Test
    void testSimpleBlockNotificationHandlers() throws InterruptedException {
        final int expectedCount = TEST_DATA_COUNT;
        // latch to wait for all handlers to finish
        CountDownLatch latch = new CountDownLatch(3);
        AtomicIntegerArray counters = new AtomicIntegerArray(3);
        // create a list of handlers that will call the latch countdown when they reach the expected count
        var testHandlers = IntStream.range(0, 3)
                .mapToObj(i -> (BlockNotificationHandler) notification -> {
                    if (expectedCount == counters.incrementAndGet(i)) {
                        // call the latch countdown when we reach the expected count
                        latch.countDown();
                    }
                })
                .toList();
        // Create MessagingService to test and register the handlers
        MessagingService messagingService = MessagingService.createMessagingService();
        testHandlers.forEach(messagingService::registerBlockNotificationHandler);
        // start the messaging service
        messagingService.start();
        // send TEST_DATA_COUNT block notifications
        for (int i = 0; i < TEST_DATA_COUNT; i++) {
            messagingService.sendBlockNotification(new BlockNotification(i, Type.BLOCK_PERSISTED));
        }
        // wait for all handlers to finish
        assertTrue(
                latch.await(20, TimeUnit.SECONDS),
                "Did not finish in time, should " + "have been way faster than 20sec timeout");
        // shutdown the messaging service
        messagingService.shutdown();
        // verify that all handlers received the expected number of items
        for (int i = 0; i < testHandlers.size(); i++) {
            assertEquals(expectedCount, counters.get(i));
        }
    }

    /**
     * Test to verify that the messaging service can handle multiple block notification handlers and that
     * the slow handler is slowed down by 25% and the fast handler is not slowed down.
     *
     * @throws InterruptedException if the test latch is interrupted
     */
    @Test
    void testFastAndSlowBlockNotificationHandlers() throws InterruptedException {
        final int expectedCount = TEST_DATA_COUNT;
        // latch to wait for all handlers to finish
        final CountDownLatch latch = new CountDownLatch(1);
        // create counters for received items
        final AtomicInteger counterFast = new AtomicInteger(0);
        final AtomicInteger counterSlow = new AtomicInteger(0);
        // create object for slowing down the slow handler
        final Semaphore holdBackSlowHandler = new Semaphore(0);
        // create a simple fast handler
        BlockNotificationHandler fastHandler = notification -> {
            if (expectedCount == counterFast.incrementAndGet()) {
                // call the latch countdown when we reach the expected count
                latch.countDown();
            }
        };
        // create a slow handler, that waits for the semaphore to be released on each call
        BlockNotificationHandler slowHandler = notification -> {
            if (expectedCount == counterSlow.incrementAndGet()) {
                // call the latch countdown when we reach the expected count
                latch.countDown();
            }
            // slow down the handler, if sending is not finished
            try {
                holdBackSlowHandler.acquire();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        };
        // Create MessagingService to test and register the handlers
        MessagingService messagingService = MessagingService.createMessagingService();
        messagingService.registerBlockNotificationHandler(fastHandler);
        messagingService.registerBlockNotificationHandler(slowHandler);
        // start the messaging service
        messagingService.start();
        // send TEST_DATA_COUNT block notifications
        for (int i = 0; i < TEST_DATA_COUNT; i++) {
            messagingService.sendBlockNotification(new BlockNotification(i, Type.BLOCK_PERSISTED));
            // release the slow handler 3 out of 4 times so it is slowed down by 25%
            if (i % 4 == 0) {
                holdBackSlowHandler.release(3);
            }
        }
        // mark sending finished and release the slow handler
        holdBackSlowHandler.release(TEST_DATA_COUNT);
        // wait for all handlers to finish
        assertTrue(
                latch.await(20, TimeUnit.SECONDS),
                "Did not finish in time, should " + "have been way faster than 20sec timeout");
        // shutdown the messaging service
        messagingService.shutdown();
        // verify that all handlers received the expected number of items
        assertEquals(expectedCount, counterFast.get());
        assertEquals(expectedCount, counterSlow.get());
    }

    /**
     * Test to verify that the messaging service can handle backpressure on block notifications.
     * The test will create a slow handler that will apply backpressure to the messaging service
     * and verify that the messaging service applies backpressure to the notification send.
     *
     * @throws InterruptedException if the test latch is interrupted
     */
    @Test
    void testBlockNotificationBackpressure() throws InterruptedException {
        // latch to wait for all handlers to finish
        final CountDownLatch latch = new CountDownLatch(1);
        // create counters for sent and received items
        final AtomicInteger sentCounter = new AtomicInteger(0);
        final AtomicInteger receivedCounter = new AtomicInteger(0);
        // create object for slowing down the slow handler
        final Semaphore holdBackSlowHandler = new Semaphore(0);
        // create a slow handler, that waits for the semaphore to be released on each call
        BlockNotificationHandler slowHandler = notification -> {
            if (TEST_DATA_COUNT == receivedCounter.incrementAndGet()) {
                // call the latch countdown when we reach the expected count
                latch.countDown();
            }
            // slow down the handler, if sending is not finished
            try {
                holdBackSlowHandler.acquire();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        };
        // Create MessagingService to test and register the handlers
        MessagingService messagingService = MessagingService.createMessagingService();
        messagingService.registerBlockNotificationHandler(slowHandler);
        // start the messaging service
        messagingService.start();
        // send TEST_DATA_COUNT block notifications in a background thread
        Thread senderThread = new Thread(() -> {
            for (int i = 0; i < TEST_DATA_COUNT; i++) {
                messagingService.sendBlockNotification(new BlockNotification(i, Type.BLOCK_PERSISTED));
                sentCounter.incrementAndGet();
                // release the slow handler every other time so it is slowed down by 50%
                if (i % 4 == 0) {
                    holdBackSlowHandler.release(1);
                }
            }
        });
        senderThread.start();
        // wait for the sender thread to get stuck in back pressure
        while (senderThread.getState() == State.RUNNABLE) {
            try {
                //noinspection BusyWait
                Thread.sleep(2);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        // the send thread should get blocked in back pressure
        final var stateAfter = senderThread.getState();
        assertNotEquals(
                State.RUNNABLE,
                stateAfter,
                "Sender thread should be blocked in back pressure, but is in state: " + stateAfter);
        // only 250 ish notifications should have been sent to the messaging service
        assertTrue(sentCounter.get() > 100, "sentCounter = " + sentCounter.get() + " is not > 100");
        assertTrue(
                sentCounter.get() < TEST_DATA_COUNT,
                "sentCounter = " + sentCounter.get() + " is not < " + TEST_DATA_COUNT);
        // mark sending finished and release the slow handler
        holdBackSlowHandler.release(TEST_DATA_COUNT);
        // wait for all handlers to finish
        assertTrue(
                latch.await(20, TimeUnit.SECONDS),
                "Did not finish in time, should " + "have been way faster than 20sec timeout");
        // shutdown the messaging service
        messagingService.shutdown();
    }
}
