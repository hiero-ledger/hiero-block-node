// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.messaging;

import static org.hiero.block.server.messaging.MessagingServiceDynamicBlockItemTest.intToBytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.hedera.hapi.block.BlockItemUnparsed;
import com.hedera.hapi.block.BlockItemUnparsed.ItemOneOfType;
import com.hedera.pbj.runtime.OneOf;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.stream.IntStream;
import org.hiero.block.server.messaging.impl.MessagingServiceImpl;
import org.junit.jupiter.api.Test;

/**
 * Tests for the Block Item functionality of the MessagingService.
 */
public class MessagingServiceBlockItemTest {
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
    void testSimpleBlockItemHandlers() throws InterruptedException {
        final int expectedCount = TEST_DATA_COUNT;
        // latch to wait for all handlers to finish
        CountDownLatch latch = new CountDownLatch(3);
        AtomicIntegerArray counters = new AtomicIntegerArray(3);
        // create a list of handlers that will call the latch countdown when they reach the expected count
        var testHandlers = IntStream.range(0, 3)
                .mapToObj(i -> (BlockItemHandler) notification -> {
                    if (expectedCount == counters.incrementAndGet(i)) {
                        // call the latch countdown when we reach the expected count
                        latch.countDown();
                    }
                })
                .toList();
        // Create MessagingService to test and register the handlers
        MessagingService messagingService = MessagingService.createMessagingService();
        testHandlers.forEach(messagingService::registerBlockItemHandler);
        // start the messaging service
        messagingService.start();
        // send TEST_DATA_COUNT block notifications
        for (int i = 0; i < TEST_DATA_COUNT; i++) {
            messagingService.sendBlockItems(
                    List.of(new BlockItemUnparsed(new OneOf<>(ItemOneOfType.BLOCK_HEADER, intToBytes(i)))));
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
     * Test mix of dynamic block item handlers and non-dynamic handlers. Registering after some items have already been
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
                    public void handleBlockItemsReceived(List<BlockItemUnparsed> blockItems) {
                        if (expectedCount == counters.incrementAndGet(i)) {
                            // call the latch countdown when we reach the expected count
                            latch.countDown();
                        }
                    }
                })
                .toList();
        // Create MessagingService to test and register the handlers
        MessagingService messagingService = MessagingService.createMessagingService();
        messagingService.registerBlockItemHandler(testHandlers.get(0));
        messagingService.registerBlockItemHandler(testHandlers.get(1));
        messagingService.registerDynamicNoBackpressureBlockItemHandler(testHandlers.get(2));
        // start the messaging service
        messagingService.start();
        // send TEST_DATA_COUNT block notifications
        for (int i = 0; i < TEST_DATA_COUNT; i++) {
            messagingService.sendBlockItems(
                    List.of(new BlockItemUnparsed(new OneOf<>(ItemOneOfType.BLOCK_HEADER, intToBytes(i)))));
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
}
