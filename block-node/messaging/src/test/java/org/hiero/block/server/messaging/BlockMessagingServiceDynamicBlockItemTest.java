// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.messaging;

import static org.hiero.block.server.messaging.TestConfig.BLOCK_NODE_CONTEXT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.hedera.pbj.runtime.OneOf;
import com.hedera.pbj.runtime.io.buffer.BufferedData;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockItemUnparsed.ItemOneOfType;
import org.hiero.block.node.messaging.BlockMessagingFacilityImpl;
import org.hiero.block.node.messaging.MessagingConfig;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.NoBackPressureBlockItemHandler;
import org.junit.jupiter.api.Test;

public class BlockMessagingServiceDynamicBlockItemTest {

    /**
     * The number of items to send to the messaging service. This is twice the size of the ring buffer, so that we can
     * test the back pressure and the slow handler.
     */
    public static final int TEST_DATA_COUNT =
            TestConfig.getConfig().getConfigData(MessagingConfig.class).blockItemQueueSize() * 2;

    /**
     * Test to verify that the messaging service can handle dynamic handlers with no back pressure and a slow handler is
     * removed and gets onTooFarBehindError() callback.
     *
     * @throws InterruptedException if the test is interrupted
     */
    @SuppressWarnings("DataFlowIssue")
    @Test
    void testDynamicHandlers() throws InterruptedException {
        // latch to wait for both handlers to finish
        final CountDownLatch latch = new CountDownLatch(2);
        // barrier to synchronize the fast handler and sender
        final CyclicBarrier barrier = new CyclicBarrier(2);
        // Lock object to hold back slow handler
        final Object holdBackSlowHandler = new Object();
        // Create a slow handler that will take a long time to process items
        final AtomicInteger slowHandlerCounter = new AtomicInteger(0);
        final AtomicInteger onTooFarBehindErrorCalled = new AtomicInteger(0);
        final NoBackPressureBlockItemHandler slowHandler = new NoBackPressureBlockItemHandler() {
            @Override
            public void handleBlockItemsReceived(BlockItems items) {
                // process items
                int receivedValue = bytesToInt(items.blockItems().getFirst().blockHeader());
                // add up all the received values
                slowHandlerCounter.addAndGet(receivedValue);
                // slow us down, waiting for fast handler to release us every 100 it gets
                synchronized (holdBackSlowHandler) {
                    try {
                        holdBackSlowHandler.wait(50);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }

            @Override
            public void onTooFarBehindError() {
                onTooFarBehindErrorCalled.incrementAndGet();
                latch.countDown();
            }
        };

        // Create a fast handler that will process items quickly
        final AtomicInteger fastHandlerCounter = new AtomicInteger(0);
        final NoBackPressureBlockItemHandler fastHandler = new NoBackPressureBlockItemHandler() {
            @Override
            public void onTooFarBehindError() {
                fail("Should never be called");
            }

            @Override
            public void handleBlockItemsReceived(BlockItems blockItems) {
                int receivedValue =
                        bytesToInt(blockItems.blockItems().getFirst().blockHeader());
                fastHandlerCounter.addAndGet(receivedValue);
                // every 100 items we will release the slow handler
                if (receivedValue % 100 == 0) {
                    synchronized (holdBackSlowHandler) {
                        holdBackSlowHandler.notify();
                    }
                }
                // check if we are done
                if (receivedValue == TEST_DATA_COUNT - 1) {
                    latch.countDown();
                }
                // wait for the sender to finish sending, so we are in lock step
                try {
                    barrier.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                    throw new RuntimeException(e);
                }
            }
        };
        // create message service to test, add handlers and start the service
        final BlockMessagingFacility messagingService = new BlockMessagingFacilityImpl();
        messagingService.init(BLOCK_NODE_CONTEXT, null);
        messagingService.registerNoBackpressureBlockItemHandler(slowHandler, false, null);
        messagingService.registerNoBackpressureBlockItemHandler(fastHandler, false, null);
        messagingService.start();
        // send 2000 items to the service, in lock step with fast handler
        for (int i = 0; i < TEST_DATA_COUNT; i++) {
            messagingService.sendBlockItems(new BlockItems(
                    List.of(new BlockItemUnparsed(new OneOf<>(ItemOneOfType.BLOCK_HEADER, intToBytes(i)))), -1));
            // notify the fast handler that we are done sending an item, so we stay in lock step
            try {
                barrier.await(5, TimeUnit.SECONDS);
            } catch (BrokenBarrierException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        }
        // wait for both handlers to finish
        assertTrue(
                latch.await(20, TimeUnit.SECONDS),
                "Did not finish in time, should " + "have been way faster than 20sec timeout");
        // shutdown the messaging service
        messagingService.stop();
        // check that the fast handler processed all items
        final int expectedTotal = IntStream.range(0, TEST_DATA_COUNT).sum();
        assertEquals(expectedTotal, fastHandlerCounter.get());
        // check that the slow handler NOT processed all items
        assertTrue(slowHandlerCounter.get() < expectedTotal);
        // check that the slow handler was called onTooFarBehindError, once and only once
        assertEquals(1, onTooFarBehindErrorCalled.get());
    }

    /**
     * Test the dynamic block item handlers with one being unregistered.
     */
    @SuppressWarnings("DataFlowIssue")
    @Test
    void testDynamicHandlersUnregister() throws InterruptedException {
        // latch to wait for both handlers to finish
        final CountDownLatch latch = new CountDownLatch(1);
        // Create a couple handlers
        final AtomicInteger handler1Counter = new AtomicInteger(0);
        final AtomicInteger handler1Sum = new AtomicInteger(0);
        final AtomicInteger handler2Counter = new AtomicInteger(0);
        final AtomicInteger handler2Sum = new AtomicInteger(0);
        final NoBackPressureBlockItemHandler handler1 = new NoBackPressureBlockItemHandler() {
            @Override
            public void handleBlockItemsReceived(BlockItems items) {
                // process items
                int receivedValue = bytesToInt(items.blockItems().getFirst().blockHeader());
                // add up all the received values
                handler1Sum.addAndGet(receivedValue);
                // update count of calls
                handler1Counter.incrementAndGet();
            }

            @Override
            public void onTooFarBehindError() {
                fail("Should never be called");
            }
        };
        final NoBackPressureBlockItemHandler handler2 = new NoBackPressureBlockItemHandler() {
            @Override
            public void handleBlockItemsReceived(BlockItems blockItems) {
                // process items
                int receivedValue =
                        bytesToInt(blockItems.blockItems().getFirst().blockHeader());
                // add up all the received values
                handler2Sum.addAndGet(receivedValue);
                // check if we are done
                if (handler2Counter.incrementAndGet() == TEST_DATA_COUNT - 1) {
                    latch.countDown();
                }
            }

            @Override
            public void onTooFarBehindError() {
                fail("Should never be called");
            }
        };
        // create message service to test, add handlers and start the service
        final BlockMessagingFacility messagingService = new BlockMessagingFacilityImpl();
        messagingService.init(BLOCK_NODE_CONTEXT, null);
        messagingService.registerNoBackpressureBlockItemHandler(handler1, false, null);
        messagingService.registerNoBackpressureBlockItemHandler(handler2, false, null);
        messagingService.start();
        // send 2000 items to the service, in lock step with fast handler
        for (int i = 0; i < TEST_DATA_COUNT; i++) {
            if (i == 5) {
                // unregister the first handler, so it will not get any more items
                messagingService.unregisterBlockItemHandler(handler1);
                // wait for a bit to let the handler unregister
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            messagingService.sendBlockItems(new BlockItems(
                    List.of(new BlockItemUnparsed(new OneOf<>(ItemOneOfType.BLOCK_HEADER, intToBytes(i)))), -1));
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
                "Did not finish in time, should " + "have been way faster than 20sec timeout");
        // shutdown the messaging service
        messagingService.stop();
        final int expectedTotal = IntStream.range(0, TEST_DATA_COUNT).sum();
        // check that the first handler did not process all items
        assertTrue(
                handler1Counter.get() < TEST_DATA_COUNT,
                "Handler 1 did not receive less items got [" + handler1Counter.get() + "] of [" + TEST_DATA_COUNT
                        + "]");
        assertTrue(
                handler1Sum.get() < expectedTotal,
                "Handler 1 did not receive less item value sum got [" + handler1Sum.get() + "] of [" + expectedTotal
                        + "]");
        // check that the second handler processed all items
        assertEquals(TEST_DATA_COUNT, handler2Counter.get());
        assertEquals(expectedTotal, handler2Sum.get());
    }

    /**
     * Utility method to convert an int to a Bytes object. So that we can send a simple int inside a BlockItem.
     *
     * @param value the int to convert
     * @return the Bytes object
     */
    public static Bytes intToBytes(int value) {
        BufferedData data = BufferedData.allocate(Integer.BYTES);
        data.writeInt(value);
        return data.getBytes(0, Integer.BYTES);
    }

    /**
     * Utility method to convert a Bytes object to an int. So that we can read a simple int inside a BlockItem.
     *
     * @param bytes the Bytes object
     * @return the int
     */
    public static int bytesToInt(Bytes bytes) {
        return bytes.getInt(0);
    }
}
