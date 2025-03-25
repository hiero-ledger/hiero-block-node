// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.messaging;

import static org.hiero.block.server.messaging.BlockMessagingServiceDynamicBlockItemTest.intToBytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.hedera.pbj.runtime.OneOf;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import org.hiero.block.node.messaging.BlockMessagingFacilityImpl;
import org.hiero.block.node.spi.blockmessaging.BlockItemHandler;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.NoBackPressureBlockItemHandler;
import org.hiero.hapi.block.node.BlockItemUnparsed;
import org.hiero.hapi.block.node.BlockItemUnparsed.ItemOneOfType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for the Block Item functionality of the MessagingService.
 */
public class BlockMessagingServiceBlockItemTest {
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
                    List.of(new BlockItemUnparsed(new OneOf<>(ItemOneOfType.BLOCK_HEADER, intToBytes(i)))), -1));
            // we can not send data at full speed as it is just to reliable for testing, sender can out pace the
            // receivers and cause exceptions once in a while
            Thread.sleep(1);
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
        messagingService.registerBlockItemHandler(testHandlers.get(0), false, "testHandler0");
        messagingService.registerBlockItemHandler(testHandlers.get(1), false, "testHandler1");
        messagingService.registerNoBackpressureBlockItemHandler(testHandlers.get(2), false, "testHandler2");
        // start the messaging service
        messagingService.start();
        // send TEST_DATA_COUNT block notifications
        for (int i = 0; i < TEST_DATA_COUNT; i++) {
            messagingService.sendBlockItems(new BlockItems(
                    List.of(new BlockItemUnparsed(new OneOf<>(ItemOneOfType.BLOCK_HEADER, intToBytes(i)))), -1));
            // we can not send data at full speed as it is just to reliable for testing, sender can out pace the
            // receivers and cause exceptions once in a while
            Thread.sleep(1);
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
     * Test to verify that thread names are set correctly for block item handlers and that the correct thread type is
     * used, i.e. virtual vs non-virtual.
     */
    @Test
    public void testThreadNameAndVirtualVsNonVirtual() {
        // Create a MessagingService instance
        BlockMessagingFacility service = new BlockMessagingFacilityImpl();
        // collect thread names and virtual vs non-virtual flags
        final AtomicReference<String> threadName1 = new AtomicReference<>();
        final AtomicReference<String> threadName2 = new AtomicReference<>();
        final AtomicBoolean handler1WasVirtual = new AtomicBoolean(false);
        final AtomicBoolean handler2WasVirtual = new AtomicBoolean(false);
        // Register a block item handler that just throws an exception
        service.registerBlockItemHandler(
                (blockItems) -> {
                    threadName1.set(Thread.currentThread().getName());
                    handler1WasVirtual.set(Thread.currentThread().isVirtual());
                },
                false,
                "FooBar");
        service.registerBlockItemHandler(
                (blockItems) -> {
                    threadName2.set(Thread.currentThread().getName());
                    handler2WasVirtual.set(Thread.currentThread().isVirtual());
                },
                true,
                null);
        // start services and send empty block items
        service.start();
        // send empty block items
        service.sendBlockItems(new BlockItems(
                List.of(new BlockItemUnparsed(new OneOf<>(ItemOneOfType.BLOCK_HEADER, intToBytes(1)))), -1));
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        service.shutdown();
        // check thread names
        assertEquals("MessageHandler:FooBar", threadName1.get());
        assertEquals("MessageHandler:Unknown", threadName2.get());
        // check that the first handler was virtual
        assertTrue(handler1WasVirtual.get(), "Handler 1 should be virtual");
        // check that the second handler was not virtual
        assertFalse(handler2WasVirtual.get(), "Handler 2 should not be virtual");
    }
}
