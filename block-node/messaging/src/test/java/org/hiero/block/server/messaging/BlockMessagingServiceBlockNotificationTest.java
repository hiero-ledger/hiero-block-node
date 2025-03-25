// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.messaging;

import static org.hiero.block.node.spi.blockmessaging.BlockNotification.Type.BLOCK_PERSISTED;
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
import org.hiero.block.node.messaging.BlockMessagingFacilityImpl;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.BlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.junit.jupiter.api.Test;

/**
 * Test class for the MessagingService to verify that it can handle block notifications and back pressure. All these
 * tests are super hard to get right as they are highly concurrent. So makes it very hard to not be timing dependent.
 */
public class BlockMessagingServiceBlockNotificationTest {

    /**
     * The number of items to send to the messaging service. This is twice the size of the ring buffer, so that we can
     * test the back pressure and the slow handler.
     */
    public static final int TEST_DATA_COUNT =
            BlockMessagingFacilityImpl.getConfig().queueSize() * 2;

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
        BlockMessagingFacility messagingService = new BlockMessagingFacilityImpl();
        testHandlers.forEach(handler -> messagingService.registerBlockNotificationHandler(handler, false, null));
        // start the messaging service
        messagingService.start();
        // send TEST_DATA_COUNT block notifications
        for (int i = 0; i < TEST_DATA_COUNT; i++) {
            messagingService.sendBlockNotification(new BlockNotification(i, BLOCK_PERSISTED, null));
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
        final CountDownLatch latch = new CountDownLatch(2);
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
        BlockMessagingFacility messagingService = new BlockMessagingFacilityImpl();
        messagingService.registerBlockNotificationHandler(fastHandler, false, null);
        messagingService.registerBlockNotificationHandler(slowHandler, false, null);
        // start the messaging service
        messagingService.start();
        // send TEST_DATA_COUNT block notifications
        for (int i = 0; i < TEST_DATA_COUNT; i++) {
            messagingService.sendBlockNotification(new BlockNotification(i, BLOCK_PERSISTED, null));
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
        // wait for the handlers to finish processing
        for (int i = 0; i < 10 && (counterFast.get() != expectedCount || counterSlow.get() != expectedCount); i++) {
            try {
                //noinspection BusyWait
                Thread.sleep(50);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
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
        BlockMessagingFacility messagingService = new BlockMessagingFacilityImpl();
        messagingService.registerBlockNotificationHandler(slowHandler, false, null);
        // start the messaging service
        messagingService.start();
        // send TEST_DATA_COUNT block notifications in a background thread
        Thread senderThread = new Thread(() -> {
            for (int i = 0; i < TEST_DATA_COUNT; i++) {
                messagingService.sendBlockNotification(new BlockNotification(i, BLOCK_PERSISTED, null));
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

    /**
     * Test to verify that the messaging service can handle dynamic registration and un-registration of block
     * notification handlers.
     *
     * @throws InterruptedException if the test latch is interrupted
     */
    @Test
    void testDynamicBlockNotificationHandlersUnregister() throws InterruptedException {
        // latch to wait for both handlers to finish
        final CountDownLatch latch = new CountDownLatch(1);
        // Create a couple handlers
        final AtomicInteger handler1Counter = new AtomicInteger(0);
        final AtomicInteger handler1Sum = new AtomicInteger(0);
        final AtomicInteger handler2Counter = new AtomicInteger(0);
        final AtomicInteger handler2Sum = new AtomicInteger(0);
        final BlockNotificationHandler handler1 = notification -> {
            // process notifications
            int receivedValue = (int) notification.blockNumber();
            // add up all the received values
            handler1Sum.addAndGet(receivedValue);
            // update count of calls
            handler1Counter.incrementAndGet();
        };
        final BlockNotificationHandler handler2 = notification -> {
            // process notifications
            int receivedValue = (int) notification.blockNumber();
            // add up all the received values
            handler2Sum.addAndGet(receivedValue);
            // check if we are done
            if (handler2Counter.incrementAndGet() == TEST_DATA_COUNT - 1) {
                latch.countDown();
            }
        };
        // create message service to test, add handlers and start the service
        final BlockMessagingFacility messagingService = new BlockMessagingFacilityImpl();
        messagingService.registerBlockNotificationHandler(handler1, false, null);
        messagingService.registerBlockNotificationHandler(handler2, false, null);
        messagingService.start();
        // send 2000 notifications to the service
        for (int i = 0; i < TEST_DATA_COUNT; i++) {
            if (i == 5) {
                // unregister the first handler, so it will not get any more notifications
                messagingService.unregisterBlockNotificationHandler(handler1);
                // wait for a bit to let the handler unregister
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            messagingService.sendBlockNotification(new BlockNotification(i, BLOCK_PERSISTED, null));
            // have to slow down production to make test reliable
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        // wait for handler 2 to finish
        assertTrue(
                latch.await(20, TimeUnit.SECONDS),
                "Did not finish in time, should have been way faster than 20sec timeout");
        // shutdown the messaging service
        messagingService.shutdown();
        final int expectedTotal = IntStream.range(0, TEST_DATA_COUNT).sum();
        // check that the first handler did not process all notifications
        assertTrue(
                handler1Counter.get() < TEST_DATA_COUNT,
                "Handler 1 did not receive less items got [" + handler1Counter.get() + "] of [" + TEST_DATA_COUNT
                        + "]");
        assertTrue(
                handler1Sum.get() < expectedTotal,
                "Handler 1 did not receive less item value sum got [" + handler1Sum.get() + "] of [" + expectedTotal
                        + "]");
        // check that the second handler processed all notifications
        assertEquals(TEST_DATA_COUNT, handler2Counter.get());
        assertEquals(expectedTotal, handler2Sum.get());
    }
}
