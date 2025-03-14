// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.block.BlockItemUnparsed;
import com.hedera.hapi.block.BlockItemUnparsed.ItemOneOfType;
import com.hedera.pbj.runtime.OneOf;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Tests for the Block Item functionality of the MessagingService.
 */
public class MessagingServiceBlockItemTest {

    /**
     * Simple test to verify that the messaging service can handle multiple block item handlers and that
     */
    @Test
    void testSimpleBlockItemHandlers() {
        // get number of threads before
        final int numThreadBefore = Thread.activeCount();
        // Create a latch to wait for all handlers to finish
        CountDownLatch latch = new CountDownLatch(3);
        MessagingService messagingService = MessagingService.createMessagingService();
        // Create a list of 3 test handlers
        List<TestBlockItemHandler> testHandlers = List.of(
                new TestBlockItemHandler(1, 100, latch::countDown, 0),
                new TestBlockItemHandler(2, 100, latch::countDown, 10),
                new TestBlockItemHandler(3, 100, latch::countDown, 20));
        // Register the handlers
        for (TestBlockItemHandler handler : testHandlers) {
            messagingService.registerBlockItemHandler(handler);
        }
        // start the messaging service
        messagingService.start();
        // start thread to send 100 items to each handler
        new Thread(() -> {
                    for (int i = 0; i < 50; i++) {
                        messagingService.sendBlockItems(
                                List.of(createBlockItem(i + .1d), createBlockItem(i + .2d), createBlockItem(i + .3d)));
                    }
                    // sleep for 10 ms to see if it hurts anything
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    for (int i = 50; i < 100; i++) {
                        messagingService.sendBlockItems(
                                List.of(createBlockItem(i + .1d), createBlockItem(i + .2d), createBlockItem(i + .3d)));
                    }
                })
                .start();
        // wait for all handlers to finish
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        // shutdown the messaging service
        messagingService.shutdown();
        // verify that all handlers received the expected number of items
        for (TestBlockItemHandler handler : testHandlers) {
            assertEquals(100, handler.counter.get());
        }
        // assert that the number of threads is the same as before
        assertEquals(numThreadBefore, Thread.activeCount());
    }

    /**
     * Simple test of a dynamic block item handler.
     */
    @Test
    void testSimpleDynamicBlockItemHandler() {
        // get number of threads before
        final int numThreadBefore = Thread.activeCount();
        // create a latch to wait for all handlers to finish
        CountDownLatch latch = new CountDownLatch(1);
        MessagingService messagingService = MessagingService.createMessagingService();
        // create two new dynamic handlers
        final TestBlockItemHandler dynamicHandler1 = new TestBlockItemHandler(4, 100, latch::countDown, 0);
        // start the messaging service
        messagingService.start();
        // register the dynamic handlers
        messagingService.registerDynamicNoBackpressureBlockItemHandler(dynamicHandler1);
        // start thread to send 100 items to each handler
        new Thread(() -> {
                    for (int i = 0; i < 100; i++) {
                        messagingService.sendBlockItems(
                                List.of(createBlockItem(i + .1d), createBlockItem(i + .2d), createBlockItem(i + .3d)));
                    }
                })
                .start();
        // wait for all handlers to finish
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        // verify that all handlers received the expected number of items
        assertEquals(100, dynamicHandler1.counter.get());
        // shutdown the messaging service
        messagingService.shutdown();
        // assert that the number of threads is the same as before
        assertEquals(numThreadBefore, Thread.activeCount());
    }

    /**
     * Test the dynamic block item handlers. Registering after some items have already been sent. The dynamic handler
     * will only see events sent after it is registered.
     */
    @Test
    void testDynamicBlockItemHandlers() {
        // get number of threads before
        final int numThreadBefore = Thread.activeCount();
        // create a latch to wait for all handlers to finish
        CountDownLatch latch = new CountDownLatch(2);
        MessagingService messagingService = MessagingService.createMessagingService();
        // create new non-dynamic handler
        TestBlockItemHandler nonDynamicHandler = new TestBlockItemHandler(0, 100, latch::countDown, 0);
        messagingService.registerBlockItemHandler(nonDynamicHandler);
        // create new dynamic handler
        final TestBlockItemHandler dynamicHandler1 = new TestBlockItemHandler(1, 50, latch::countDown, 0);
        // start the messaging service
        messagingService.start();
        // start thread to send 100 items to each handler
        new Thread(() -> {
                    for (int i = 0; i < 50; i++) {
                        messagingService.sendBlockItems(
                                List.of(createBlockItem(i + .1d), createBlockItem(i + .2d), createBlockItem(i + .3d)));
                    }
                    // sleep for 30 ms for everything to catch up
                    try {
                        Thread.sleep(30);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    // register the dynamic handlers
                    messagingService.registerDynamicNoBackpressureBlockItemHandler(dynamicHandler1);
                    // send rest of items
                    for (int i = 50; i < 100; i++) {
                        messagingService.sendBlockItems(
                                List.of(createBlockItem(i + .1d), createBlockItem(i + .2d), createBlockItem(i + .3d)));
                    }
                })
                .start();
        // wait for all handlers to finish
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        // shutdown the messaging service
        messagingService.shutdown();
        // verify that all handlers received the expected number of items
        assertEquals(100, nonDynamicHandler.counter.get());
        // assert that the number of threads is the same as before
        assertEquals(numThreadBefore, Thread.activeCount());
    }

    /**
     * Test mix of dynamic block item handlers and non-dynamic handlers. Registering after some items have already been
     * sent.
     */
    @Test
    void testDynamicBlockItemHandlerMix() {
        // get number of threads before
        final int numThreadBefore = Thread.activeCount();
        // create a latch to wait for all handlers to finish
        CountDownLatch latch = new CountDownLatch(5);
        MessagingService messagingService = MessagingService.createMessagingService();
        List<TestBlockItemHandler> testHandlers = new ArrayList<>(List.of(
                new TestBlockItemHandler(1, 200, latch::countDown, 0),
                new TestBlockItemHandler(2, 200, latch::countDown, 1),
                new TestBlockItemHandler(3, 200, latch::countDown, 2)));
        // Register the handlers
        for (TestBlockItemHandler handler : testHandlers) {
            messagingService.registerBlockItemHandler(handler);
        }
        // create two new dynamic handlers
        final TestBlockItemHandler dynamicHandler1 = new TestBlockItemHandler(4, 100, latch::countDown, 0);
        final TestBlockItemHandler dynamicHandler2 = new TestBlockItemHandler(5, 100, latch::countDown, 2);
        // start the messaging service
        messagingService.start();
        // start thread to send 100 items to each handler
        new Thread(() -> {
                    for (int i = 0; i < 100; i++) {
                        messagingService.sendBlockItems(
                                List.of(createBlockItem(i + .1d), createBlockItem(i + .2d), createBlockItem(i + .3d)));
                    }
                    // sleep for 30 ms for everything to catch up
                    try {
                        Thread.sleep(30);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    // register the dynamic handlers
                    messagingService.registerDynamicNoBackpressureBlockItemHandler(dynamicHandler1);
                    messagingService.registerDynamicNoBackpressureBlockItemHandler(dynamicHandler2);
                    // send rest of items
                    for (int i = 100; i < 200; i++) {
                        messagingService.sendBlockItems(
                                List.of(createBlockItem(i + .1d), createBlockItem(i + .2d), createBlockItem(i + .3d)));
                    }
                })
                .start();
        // wait for all handlers to finish
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        // shutdown the messaging service
        messagingService.shutdown();
        // verify that all handlers received the expected number of items
        for (TestBlockItemHandler handler : testHandlers) {
            assertEquals(200, handler.counter.get());
        }
        // the two dynamic handlers should have received 100 items each
        assertEquals(100, dynamicHandler1.counter.get());
        assertEquals(100, dynamicHandler2.counter.get());
        // assert that the number of threads is the same as before
        assertEquals(numThreadBefore, Thread.activeCount());
    }

    /**
     * Test the dynamic block item handlers with more items than the ring size.
     * This test will create 3 handlers and send 500 items to ring, then wait for handlers to receive them all.
     * Then it will create two new dynamic handlers and register them. Then send the remaining 1500 items to the ring.
     * The test will verify that all handlers received the expected number of items.
     */
    @Test
    void testDynamicBlockItemHandlersWithMoreItemsThanRingSize() {
        // get number of threads before
        final int numThreadBefore = Thread.activeCount();
        // create a latch to wait for all handlers to finish
        CountDownLatch latch = new CountDownLatch(5);
        MessagingService messagingService = MessagingService.createMessagingService();
        List<TestBlockItemHandler> testHandlers = new ArrayList<>(List.of(
                new TestBlockItemHandler(1, 2000, latch::countDown, 0),
                new TestBlockItemHandler(2, 2000, latch::countDown, 1),
                new TestBlockItemHandler(3, 2000, latch::countDown, 2)));
        // Register the handlers
        for (TestBlockItemHandler handler : testHandlers) {
            messagingService.registerBlockItemHandler(handler);
        }
        // create two new dynamic handlers
        final TestBlockItemHandler dynamicHandler1 = new TestBlockItemHandler(4, 1500, latch::countDown, 1);
        final TestBlockItemHandler dynamicHandler2 = new TestBlockItemHandler(5, 1500, latch::countDown, 2);
        // start the messaging service
        messagingService.start();
        // start thread to send 100 items to each handler
        new Thread(() -> {
                    for (int i = 0; i < 500; i++) {
                        messagingService.sendBlockItems(
                                List.of(createBlockItem(i + .1d), createBlockItem(i + .2d), createBlockItem(i + .3d)));
                    }
                    // sleep for 30 ms for everything to catch up
                    try {
                        Thread.sleep(30);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    // register the dynamic handlers
                    messagingService.registerDynamicNoBackpressureBlockItemHandler(dynamicHandler1);
                    messagingService.registerDynamicNoBackpressureBlockItemHandler(dynamicHandler2);
                    for (int i = 500; i < 2000; i++) {
                        messagingService.sendBlockItems(
                                List.of(createBlockItem(i + .1d), createBlockItem(i + .2d), createBlockItem(i + .3d)));
                    }
                })
                .start();
        // wait for all handlers to finish
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        // shutdown the messaging service
        messagingService.shutdown();
        // verify that all handlers received the expected number of items
        for (TestBlockItemHandler handler : testHandlers) {
            assertEquals(2000, handler.counter.get());
        }
        // the two dynamic handlers should have received 1500 items each
        assertEquals(1500, dynamicHandler1.counter.get());
        assertEquals(1500, dynamicHandler2.counter.get());
        // assert that the number of threads is the same as before
        assertEquals(numThreadBefore, Thread.activeCount());
        // assert that the number of threads is the same as before
        assertEquals(numThreadBefore, Thread.activeCount());
    }

    /**
     * Test the dynamic block item handlers with one being unregistered.
     */
    @Test
    void testDynamicBlockItemHandlerUnregister() {
        // get number of threads before
        final int numThreadBefore = Thread.activeCount();
        // create a latch to wait for all handlers to finish
        CountDownLatch latch = new CountDownLatch(2);
        MessagingService messagingService = MessagingService.createMessagingService();
        // create two new dynamic handlers
        final TestBlockItemHandler dynamicHandler1 = new TestBlockItemHandler(1, 200, latch::countDown, 0);
        final TestBlockItemHandler dynamicHandler2 = new TestBlockItemHandler(2, 300, latch::countDown, 0);
        // start the messaging service
        messagingService.start();
        // register the dynamic handlers
        messagingService.registerDynamicNoBackpressureBlockItemHandler(dynamicHandler1);
        messagingService.registerDynamicNoBackpressureBlockItemHandler(dynamicHandler2);
        // start thread to send 100 items to each handler
        new Thread(() -> {
                    // send 100 items
                    for (int i = 0; i < 200; i++) {
                        messagingService.sendBlockItems(
                                List.of(createBlockItem(i + .1d), createBlockItem(i + .2d), createBlockItem(i + .3d)));
                    }
                    // wait for dynamicHandler1 to receive 200 items
                    while (dynamicHandler1.counter.get() < 200) {
                        try {
                            //noinspection BusyWait
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    // unregister the first dynamic handler
                    messagingService.unregisterDynamicNoBackpressureBlockItemHandler(dynamicHandler1);
                    // send 100 more items
                    for (int i = 200; i < 300; i++) {
                        messagingService.sendBlockItems(
                                List.of(createBlockItem(i + .1d), createBlockItem(i + .2d), createBlockItem(i + .3d)));
                    }
                })
                .start();
        // wait for all handlers to finish
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        // verify that handler 1 received at least 200 items and less than 300 items. It can take the thread a
        // small-time to stop so might receive a few extra items
        assertTrue(dynamicHandler1.counter.get() >= 200);
        assertTrue(dynamicHandler1.counter.get() < 300);
        // verify that handler 2 received all 300 items
        assertEquals(300, dynamicHandler2.counter.get());
        // shutdown the messaging service
        messagingService.shutdown();
        // assert that the number of threads is the same as before
        assertEquals(numThreadBefore, Thread.activeCount());
    }

    /**
     * Test the dynamic block item handlers with one falling behind.
     */
    @Test
    void testDynamicBlockItemHandlerFallBehind() {
        // get number of threads before
        final int numThreadBefore = Thread.activeCount();
        // create a latch to wait for all handlers to finish
        CountDownLatch latch = new CountDownLatch(2);
        MessagingService messagingService = MessagingService.createMessagingService();
        // create one new non-dynamic handler
        TestBlockItemHandler nonDynamicHandler = new TestBlockItemHandler(1, 2000, latch::countDown, 0);
        messagingService.registerBlockItemHandler(nonDynamicHandler);
        // start the messaging service
        messagingService.start();
        // create new dynamic handler
        final TestBlockItemHandler dynamicHandler = new TestBlockItemHandler(2, 2000, latch::countDown, 20);
        messagingService.registerDynamicNoBackpressureBlockItemHandler(dynamicHandler);
        // start thread to send 2000 items to each handler, need to be bigger than the ring size
        new Thread(() -> {
                    // send 2000 items
                    for (int i = 0; i < 2000; i++) {
                        messagingService.sendBlockItems(List.of(createBlockItem(i + .1d), createBlockItem(i + .2d)));
                        // slow the data rate down, this is important otherwise the dynamic handler is behind
                        // immediately which is not a good test. Can speed up after tooFarBehind is set to save test
                        // time
                        if (!dynamicHandler.tooFarBehind.get()) {
                            try {
                                Thread.sleep(5);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    }
                })
                .start();
        // wait for all handlers to finish
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        // verify that nonDynamicHandler received all 2000 items
        assertEquals(2000, nonDynamicHandler.counter.get());
        // verify that dynamicHandler2 received less than 2000 items
        assertTrue(dynamicHandler.counter.get() < 2000);
        // verify that dynamicHandler2 got too far behind
        assertTrue(dynamicHandler.tooFarBehind.get());
        // shutdown the messaging service
        messagingService.shutdown();
        // assert that the number of threads is the same as before
        assertEquals(numThreadBefore, Thread.activeCount());
    }

    /**
     * Create a fake testing BlockItemUnparsed object with the given id.
     *
     * @param id the id of the block item
     * @return a BlockItemUnparsed object with the given id
     */
    private static BlockItemUnparsed createBlockItem(double id) {
        // Create a BlockItemUnparsed object with the given id
        return new BlockItemUnparsed(new OneOf<>(ItemOneOfType.BLOCK_HEADER, id));
    }

    /**
     * TestBlockItemHandler is a test implementation of the BlockItemHandler interface. It keeps track of the number of
     * items received and calls a callback when the expected number of items is reached. It has a optional delay to simulate
     * processing time.
     */
    private static class TestBlockItemHandler implements NoBackPressureBlockItemHandler {
        final AtomicInteger counter = new AtomicInteger(0);
        final int handlerId;
        final int expectedCount;
        final int delayMs;
        final Runnable completeCallback;
        final AtomicBoolean tooFarBehind = new AtomicBoolean(false);

        public TestBlockItemHandler(int handlerId, int expectedCount, Runnable completeCallback, int delayMs) {
            this.handlerId = handlerId;
            this.expectedCount = expectedCount;
            this.completeCallback = completeCallback;
            this.delayMs = delayMs;
        }

        @Override
        public void handleBlockItemsReceived(List<BlockItemUnparsed> blockItems) {
            // Increment the counter for this handler
            int count = counter.incrementAndGet();
            // Check if the expected count is reached
            if (count >= expectedCount) {
                // Call the complete callback
                completeCallback.run();
            }
            // Simulate some processing delay
            if (delayMs > 0) {
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        @Override
        public void onTooFarBehindError() {
            System.out.println("TestBlockItemHandler.onTooFarBehindError " + handlerId);
            new Exception().printStackTrace(System.out);
            // mark this handler as too far behind
            boolean wasUnsetBefore = tooFarBehind.compareAndSet(false, true);
            // Call the complete callback, but only once as this is used to count down a latch
            if (wasUnsetBefore) completeCallback.run();
        }
    }
}
