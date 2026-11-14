// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.messaging;

import static org.hiero.block.server.messaging.DynamicBlockItemTest.intToBytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.hedera.pbj.runtime.OneOf;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.IntStream;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockItemUnparsed.ItemOneOfType;
import org.hiero.block.node.messaging.BlockMessagingFacilityImpl;
import org.hiero.block.node.messaging.MessagingConfig;
import org.hiero.block.node.spi.blockmessaging.BlockItemHandler;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.NoBackPressureBlockItemHandler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for the Block Item functionality of the MessagingService.
 */
public class BlockItemTest {
    /**
     * The number of items to send to the messaging service. This is twice the size of the ring buffer, so that we can
     * test the back pressure and the slow handler.
     */
    public static final int TEST_DATA_COUNT =
            TestConfig.getConfig().getConfigData(MessagingConfig.class).blockItemQueueSize() * 2;

    /**
     * Simple test to verify that the messaging service can handle multiple block notification handlers and that
     * they all receive the same number of items.
     *
     * @throws InterruptedException if the test latch is interrupted
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testSimpleBlockItemHandlersRegisteredBeforeStart(boolean registerBeforeStart) throws InterruptedException {
        final int expectedCount = TEST_DATA_COUNT;
        // latch to wait for all handlers to finish
        CountDownLatch latch = new CountDownLatch(3);
        AtomicIntegerArray counters = new AtomicIntegerArray(3);
        // create a list of handlers that will call the latch countdown when they reach the expected count
        var testHandlers = IntStream.range(0, 3)
                .mapToObj(i -> (BlockItemHandler) (notification) -> {
                    if (expectedCount == counters.incrementAndGet(i)) {
                        // call the latch countdown when we reach the expected count
                        latch.countDown();
                    }
                })
                .toList();
        // Create MessagingService to test
        BlockMessagingFacility messagingService = new BlockMessagingFacilityImpl();
        messagingService.init(TestConfig.getBlockNodeContext(), null);
        // Register the handlers, if registerBeforeStart is true
        if (registerBeforeStart) {
            testHandlers.forEach(handler -> messagingService.registerBlockItemHandler(handler, false, "testHandler"));
        }
        // start the messaging service
        messagingService.start();
        // register the handlers if not already registered
        if (!registerBeforeStart) {
            testHandlers.forEach(handler -> messagingService.registerBlockItemHandler(handler, false, "testHandler"));
        }
        // send TEST_DATA_COUNT block notifications
        for (int i = 0; i < TEST_DATA_COUNT; i++) {
            messagingService.sendBlockItems(new BlockItems(
                    List.of(new BlockItemUnparsed(new OneOf<>(ItemOneOfType.BLOCK_HEADER, intToBytes(i)))),
                    i,
                    true,
                    false));
            // we can not send data at full speed as it is just to reliable for testing, sender can out pace the
            // receivers and cause exceptions once in a while
            Thread.sleep(1);
        }
        // wait for all handlers to finish
        assertTrue(
                latch.await(20, TimeUnit.SECONDS),
                "Did not finish in time, should " + "have been way faster than 20sec timeout");
        // shutdown the messaging service
        messagingService.stop();
        // verify that all handlers received the expected number of items
        for (int i = 0; i < testHandlers.size(); i++) {
            assertEquals(expectedCount, counters.get(i));
        }
    }

    /**
     * Test mix of back-pressure and non-back-pressure block item handlers. Registering after some items have already been
     * sent.
     *
     * @throws InterruptedException if the test is interrupted
     */
    @Test
    void testDynamicBlockItemHandlerMix() throws InterruptedException {
        final int expectedCount = TEST_DATA_COUNT;
        // latch to wait for all handlers to finish
        CountDownLatch latch = new CountDownLatch(3);
        AtomicIntegerArray counters = new AtomicIntegerArray(3);
        // create a list of handlers that will call the latch countdown when they reach the expected count
        var testHandlers = IntStream.range(0, 3)
                .mapToObj(i -> new NoBackPressureBlockItemHandler() {
                    @Override
                    public void onTooFarBehindError() {
                        fail("Should not be called");
                    }

                    @Override
                    public void handleBlockItemsReceived(BlockItems blockItems) {
                        if (expectedCount == counters.incrementAndGet(i)) {
                            // call the latch countdown when we reach the expected count
                            latch.countDown();
                        }
                    }
                })
                .toList();
        // Create MessagingService to test and register the handlers
        BlockMessagingFacility messagingService = new BlockMessagingFacilityImpl();
        messagingService.init(TestConfig.getBlockNodeContext(), null);
        messagingService.registerBlockItemHandler(testHandlers.get(0), false, "testHandler0");
        messagingService.registerBlockItemHandler(testHandlers.get(1), false, "testHandler1");
        messagingService.registerNoBackpressureBlockItemHandler(testHandlers.get(2), false, "testHandler2");
        // start the messaging service
        messagingService.start();
        // send TEST_DATA_COUNT block notifications
        for (int i = 0; i < TEST_DATA_COUNT; i++) {
            messagingService.sendBlockItems(new BlockItems(
                    List.of(new BlockItemUnparsed(new OneOf<>(ItemOneOfType.BLOCK_HEADER, intToBytes(i)))),
                    i,
                    true,
                    false));
            // we can not send data at full speed as it is just to reliable for testing, sender can out pace the
            // receivers and cause exceptions once in a while
            Thread.sleep(1);
        }
        // wait for all handlers to finish
        assertTrue(
                latch.await(20, TimeUnit.SECONDS),
                "Did not finish in time, should " + "have been way faster than 20sec timeout");
        // shutdown the messaging service
        messagingService.stop();
        // verify that all handlers received the expected number of items
        for (int i = 0; i < testHandlers.size(); i++) {
            assertEquals(expectedCount, counters.get(i));
        }
    }

    /**
     * Handler threads are named after the handler, or "Unknown" when the handler has no name.
     */
    @Test
    void handlerThreadIsNamedAfterHandler() throws InterruptedException {
        final List<Thread> handlerThreads = captureHandlerThreads();
        assertEquals("MessageHandler:FooBar", handlerThreads.get(0).getName());
        assertEquals("MessageHandler:Unknown", handlerThreads.get(1).getName());
    }

    /**
     * Every handler runs on its own platform thread, whatever its CPU intensive hint. A virtual thread waiting on the ring
     * keeps the carrier threads busy.
     */
    @Test
    void handlersRunOnPlatformThreads() throws InterruptedException {
        final List<Thread> handlerThreads = captureHandlerThreads();
        assertFalse(handlerThreads.get(0).isVirtual(), "Non CPU intensive handler should run on a platform thread");
        assertFalse(handlerThreads.get(1).isVirtual(), "CPU intensive handler should run on a platform thread");
    }

    /**
     * Register a non CPU intensive handler named "FooBar" and an unnamed CPU intensive handler, send one batch and
     * return the threads the two handlers ran on, in that order.
     */
    private static List<Thread> captureHandlerThreads() throws InterruptedException {
        final BlockMessagingFacility service = new BlockMessagingFacilityImpl();
        service.init(TestConfig.getBlockNodeContext(), null);
        final AtomicReferenceArray<Thread> handlerThreads = new AtomicReferenceArray<>(2);
        final CountDownLatch bothHandled = new CountDownLatch(2);
        service.registerBlockItemHandler(
                (blockItems) -> {
                    handlerThreads.set(0, Thread.currentThread());
                    bothHandled.countDown();
                },
                false,
                "FooBar");
        service.registerBlockItemHandler(
                (blockItems) -> {
                    handlerThreads.set(1, Thread.currentThread());
                    bothHandled.countDown();
                },
                true,
                null);
        service.start();
        try {
            service.sendBlockItems(new BlockItems(
                    List.of(new BlockItemUnparsed(new OneOf<>(ItemOneOfType.BLOCK_HEADER, intToBytes(1)))),
                    0,
                    true,
                    false));
            assertTrue(bothHandled.await(10, TimeUnit.SECONDS), "Handlers did not receive the batch");
        } finally {
            service.stop();
        }
        return List.of(handlerThreads.get(0), handlerThreads.get(1));
    }
}
