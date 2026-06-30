// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.hedera.pbj.runtime.OneOf;
import com.hedera.pbj.runtime.io.buffer.BufferedData;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
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

public class DynamicBlockItemTest {

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
        messagingService.init(TestConfig.getBlockNodeContext(), null);
        messagingService.registerNoBackpressureBlockItemHandler(slowHandler, false, null);
        messagingService.registerNoBackpressureBlockItemHandler(fastHandler, false, null);
        messagingService.start();
        // send 2000 items to the service, in lock step with fast handler
        for (int i = 0; i < TEST_DATA_COUNT; i++) {
            messagingService.sendBlockItems(new BlockItems(
                    List.of(new BlockItemUnparsed(new OneOf<>(ItemOneOfType.BLOCK_HEADER, intToBytes(i)))),
                    i,
                    true,
                    false));
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
        messagingService.init(TestConfig.getBlockNodeContext(), null);
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
                    List.of(new BlockItemUnparsed(new OneOf<>(ItemOneOfType.BLOCK_HEADER, intToBytes(i)))),
                    i,
                    true,
                    false));
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
     * Verify that a no-backpressure handler that blocks indefinitely does NOT gate the ring buffer.
     * The publisher must be able to fill and wrap the ring buffer without deadlocking, even while the
     * handler is stuck. This is the core invariant of the no-backpressure contract.
     */
    @Test
    void testNoBackpressureHandlerDoesNotGateRingBuffer() throws Exception {
        final int ringSize =
                TestConfig.getConfig().getConfigData(MessagingConfig.class).blockItemQueueSize();
        // This latch is never released — the handler blocks forever.
        final CountDownLatch blockForever = new CountDownLatch(1);
        final AtomicBoolean tooFarBehindCalled = new AtomicBoolean(false);

        final NoBackPressureBlockItemHandler blockingHandler = new NoBackPressureBlockItemHandler() {
            @Override
            public void handleBlockItemsReceived(BlockItems items) {
                try {
                    blockForever.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            @Override
            public void onTooFarBehindError() {
                tooFarBehindCalled.set(true);
                blockForever.countDown(); // release so stop() can clean up
            }
        };

        final BlockMessagingFacility messagingService = new BlockMessagingFacilityImpl();
        messagingService.init(TestConfig.getBlockNodeContext(), null);
        messagingService.registerNoBackpressureBlockItemHandler(blockingHandler, false, "blocking");
        messagingService.start();

        // Publish more events than the ring buffer size; if the handler is a gating sequence this deadlocks.
        final ExecutorService publisher = Executors.newSingleThreadExecutor();
        final Future<?> publishFuture = publisher.submit(() -> {
            for (int i = 0; i < ringSize * 2; i++) {
                messagingService.sendBlockItems(new BlockItems(
                        List.of(new BlockItemUnparsed(new OneOf<>(ItemOneOfType.BLOCK_HEADER, intToBytes(i)))),
                        i,
                        true,
                        false));
            }
        });

        // If the handler were a gating sequence this would time out.
        publishFuture.get(10, TimeUnit.SECONDS);

        blockForever.countDown(); // ensure handler thread can exit
        messagingService.stop();
        publisher.shutdown();
    }

    /**
     * Verify that the lag eviction check fires BEFORE dispatching to the handler, not after.
     * When a no-backpressure handler falls more than 80% behind the ring head, it must be evicted
     * and onTooFarBehindError() called even if the handler would otherwise block indefinitely.
     * No further events should be dispatched to the handler after eviction.
     *
     * <p>Design note: the ring head advances purely from publishing — no second "fast" handler is
     * needed. A second handler would be unreliable on a loaded CI runner where the publisher can
     * outpace both virtual threads, inadvertently evicting the "fast" handler too.
     */
    @Test
    void testNoBackpressureEvictionCheckedBeforeDispatch() throws Exception {
        final int ringSize =
                TestConfig.getConfig().getConfigData(MessagingConfig.class).blockItemQueueSize();
        final CountDownLatch evicted = new CountDownLatch(1);
        // Once evicted is set, any further dispatch is a bug.
        final AtomicBoolean dispatchAfterEviction = new AtomicBoolean(false);

        final NoBackPressureBlockItemHandler slowHandler = new NoBackPressureBlockItemHandler() {
            @Override
            public void handleBlockItemsReceived(BlockItems items) {
                if (evicted.getCount() == 0) {
                    dispatchAfterEviction.set(true);
                }
                // Block long enough to fall far behind, but not forever.
                try {
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            @Override
            public void onTooFarBehindError() {
                evicted.countDown();
            }
        };

        final BlockMessagingFacility messagingService = new BlockMessagingFacilityImpl();
        messagingService.init(TestConfig.getBlockNodeContext(), null);
        messagingService.registerNoBackpressureBlockItemHandler(slowHandler, false, "slow");
        messagingService.start();

        // Publishing twice the ring size with no delay: the publisher fills and wraps the ring
        // far faster than the 5ms/event slow handler can drain it, guaranteeing >80% lag.
        final int totalItems = ringSize * 2;
        for (int i = 0; i < totalItems; i++) {
            messagingService.sendBlockItems(new BlockItems(
                    List.of(new BlockItemUnparsed(new OneOf<>(ItemOneOfType.BLOCK_HEADER, intToBytes(i)))),
                    i,
                    true,
                    false));
        }

        assertTrue(evicted.await(15, TimeUnit.SECONDS), "Slow handler was never evicted");
        assertFalse(dispatchAfterEviction.get(), "Events were dispatched to handler after eviction");

        messagingService.stop();
    }

    /**
     * Regression test: a regular BlockItemHandler must still receive all published events after the
     * no-backpressure fix. Verifies that the gating sequence plumbing for regular handlers is not
     * accidentally broken by the fix (e.g. sequences not added, removed too early, or stop() skipping cleanup).
     */
    @Test
    void testRegularHandlerReceivesAllEvents() throws Exception {
        final int totalItems = TEST_DATA_COUNT;
        final CountDownLatch allReceived = new CountDownLatch(1);
        final AtomicInteger receiveCount = new AtomicInteger(0);

        final BlockItemHandler gatingHandler = items -> {
            if (receiveCount.incrementAndGet() >= totalItems) {
                allReceived.countDown();
            }
        };

        final BlockMessagingFacility messagingService = new BlockMessagingFacilityImpl();
        messagingService.init(TestConfig.getBlockNodeContext(), null);
        messagingService.registerBlockItemHandler(gatingHandler, false, "regular");
        messagingService.start();

        for (int i = 0; i < totalItems; i++) {
            messagingService.sendBlockItems(new BlockItems(
                    List.of(new BlockItemUnparsed(new OneOf<>(ItemOneOfType.BLOCK_HEADER, intToBytes(i)))),
                    i,
                    true,
                    false));
        }

        assertTrue(allReceived.await(20, TimeUnit.SECONDS), "Regular handler did not receive all events");
        assertEquals(totalItems, receiveCount.get());
        messagingService.stop();
    }

    /**
     * Regression test: a no-backpressure handler that uses an internal blocking queue (mimicking
     * {@code LiveBlockHandler}) must never receive the same {@link BlockItems} more than once.
     *
     * <p>Root cause: {@code handleBlockItemsReceived} may call a blocking {@code put()} on an internal
     * bounded queue. While the messaging thread is blocked inside {@code put()}, the ring buffer head
     * keeps advancing (non-gating — the producer never stalls). If the head advances more than one full
     * ring revolution before {@code put()} returns, the ring wraps and <em>overwrites</em> slots the
     * handler has not yet read. When {@code put()} finally unblocks the handler reads those overwritten
     * slots — which now hold <em>newer</em> events — and enqueues them. Later the handler's sequence
     * cursor naturally reaches those same newer events and dispatches them a <em>second time</em>,
     * causing the consumer to see duplicate {@link BlockItems}.
     *
     * <p>The eviction threshold (80 %) is meant to prevent wrap-around, but the check only runs between
     * {@code put()} calls, not during one. This test verifies that each published value is received
     * <em>at most once</em>.
     */
    @Test
    void testNoBackpressureHandlerWithBlockingInternalQueueDoesNotDeliverDuplicates() throws Exception {
        final int ringSize =
                TestConfig.getConfig().getConfigData(MessagingConfig.class).blockItemQueueSize();
        // Internal transfer queue with a small capacity to force the handler to block in put() quickly.
        // The eviction fires at > 80 % lag (> 0.8 * ringSize events behind). We size the internal
        // queue smaller than ringSize * 0.2 so that a fast producer can fill it before 80 % lag is
        // reached, thereby causing the handler to block during put() while the ring keeps advancing.
        final int internalQueueCapacity = Math.max(2, ringSize / 10);
        final java.util.concurrent.BlockingQueue<BlockItems> internalQueue =
                new java.util.concurrent.ArrayBlockingQueue<>(internalQueueCapacity);

        final AtomicBoolean evicted = new AtomicBoolean(false);
        // Track every sequence value dispatched; a duplicate means the same event payload was delivered twice.
        final java.util.concurrent.ConcurrentHashMap<Integer, Integer> seenValues =
                new java.util.concurrent.ConcurrentHashMap<>();
        final AtomicInteger duplicateCount = new AtomicInteger(0);
        final CountDownLatch evictedLatch = new CountDownLatch(1);

        final NoBackPressureBlockItemHandler blockingHandler = new NoBackPressureBlockItemHandler() {
            @Override
            public void handleBlockItemsReceived(BlockItems items) {
                int value = bytesToInt(items.blockItems().getFirst().blockHeader());
                // Record any duplicate deliveries before blocking in put().
                if (seenValues.put(value, value) != null) {
                    duplicateCount.incrementAndGet();
                }
                try {
                    // Blocking put — this is the pattern used by LiveBlockHandler.
                    internalQueue.put(items);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            @Override
            public void onTooFarBehindError() {
                evicted.set(true);
                evictedLatch.countDown();
            }
        };

        // A fast consumer drains the internal queue slowly enough to let it fill but not so slow
        // that the test takes forever. The goal is to keep the queue nearly full so the handler
        // blocks on put() while the ring advances.
        final AtomicBoolean consumerRunning = new AtomicBoolean(true);
        final Thread consumerThread = Thread.ofVirtual().start(() -> {
            while (consumerRunning.get() || !internalQueue.isEmpty()) {
                try {
                    internalQueue.poll(1, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });

        final BlockMessagingFacility messagingService = new BlockMessagingFacilityImpl();
        messagingService.init(TestConfig.getBlockNodeContext(), null);
        messagingService.registerNoBackpressureBlockItemHandler(blockingHandler, false, "blocking-internal-queue");
        messagingService.start();

        // Publish ringSize * 2 events as fast as possible; this should outpace the handler and
        // trigger the eviction (or expose duplicate delivery if eviction misfires).
        final int totalItems = ringSize * 2;
        for (int i = 0; i < totalItems; i++) {
            messagingService.sendBlockItems(new BlockItems(
                    List.of(new BlockItemUnparsed(new OneOf<>(ItemOneOfType.BLOCK_HEADER, intToBytes(i)))),
                    i,
                    true,
                    false));
        }

        // Either eviction fires (expected happy path) or the test continues to completion.
        // Either way, no value should have been delivered twice.
        evictedLatch.await(15, TimeUnit.SECONDS);

        consumerRunning.set(false);
        consumerThread.interrupt();
        consumerThread.join(2000);
        messagingService.stop();

        assertEquals(
                0,
                duplicateCount.get(),
                "Handler received " + duplicateCount.get() + " duplicate BlockItems deliveries. "
                        + "A blocking put() inside handleBlockItemsReceived allows the ring buffer to "
                        + "wrap and overwrite unread slots, causing the same event to be dispatched twice.");
    }

    /**
     * Verify that a fast no-backpressure handler receives every item that is sent. Removing the gating
     * sequence means the ring buffer can wrap and overwrite unread slots, so a handler that keeps up must
     * still see all events without any being silently dropped.
     */
    @Test
    void testNoBackpressureHandlerReceivesAllItemsWhenFastEnough() throws Exception {
        final int totalItems = TEST_DATA_COUNT;
        final CountDownLatch allReceived = new CountDownLatch(1);
        final AtomicInteger receiveCount = new AtomicInteger(0);
        final AtomicInteger receiveSum = new AtomicInteger(0);

        final NoBackPressureBlockItemHandler fastHandler = new NoBackPressureBlockItemHandler() {
            @Override
            public void handleBlockItemsReceived(BlockItems items) {
                int value = bytesToInt(items.blockItems().getFirst().blockHeader());
                receiveSum.addAndGet(value);
                if (receiveCount.incrementAndGet() >= totalItems) {
                    allReceived.countDown();
                }
            }

            @Override
            public void onTooFarBehindError() {
                fail("Fast handler should never be evicted");
            }
        };

        final BlockMessagingFacility messagingService = new BlockMessagingFacilityImpl();
        messagingService.init(TestConfig.getBlockNodeContext(), null);
        messagingService.registerNoBackpressureBlockItemHandler(fastHandler, false, "fast-all-items");
        messagingService.start();

        for (int i = 0; i < totalItems; i++) {
            messagingService.sendBlockItems(new BlockItems(
                    List.of(new BlockItemUnparsed(new OneOf<>(ItemOneOfType.BLOCK_HEADER, intToBytes(i)))),
                    i,
                    true,
                    false));
        }
        // Pause for 100 microseconds to let blocks get sent.
        LockSupport.parkNanos(100_000L);
        messagingService.stop();
        assertTrue(
                allReceived.await(20, TimeUnit.SECONDS),
                "No-backpressure handler did not receive all " + totalItems + " items");
        assertEquals(totalItems, receiveCount.get(), "Item count mismatch");
        assertEquals(
                IntStream.range(0, totalItems).sum(), receiveSum.get(), "Item sum mismatch — some items were lost");
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
