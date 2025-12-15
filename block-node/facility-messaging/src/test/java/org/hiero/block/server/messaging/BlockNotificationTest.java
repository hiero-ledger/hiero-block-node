// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.Thread.State;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.IntStream;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.async.TestThreadPoolManager;
import org.hiero.block.node.messaging.BlockMessagingFacilityImpl;
import org.hiero.block.node.messaging.MessagingConfig;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Test class for the MessagingService to verify that it can handle block notifications and back pressure. All these
 * tests are super hard to get right as they are highly concurrent. So makes it very hard to not be timing dependent.
 */
public class BlockNotificationTest {

    /**
     * The number of items to send to the messaging service. This is twice the size of the ring buffer, so that we can
     * test the back pressure and the slow handler.
     */
    private static final int TEST_DATA_COUNT =
            TestConfig.getConfig().getConfigData(MessagingConfig.class).blockItemQueueSize() * 2;

    private static final int MAX_NOTIFICATION_COUNT =
            TestConfig.getConfig().getConfigData(MessagingConfig.class).blockNotificationQueueSize();

    /** The thread pool manager to use when testing */
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
        // create counters for sent items
        final AtomicInteger sentCounter = new AtomicInteger(0);
        // create a list of handlers that will call the latch countdown when they reach the expected count
        final List<BlockNotificationHandler> testHandlers = new ArrayList<>(3);
        for (int i = 0; i < 3; i++) {
            testHandlers.add(new LatchCountdownHandler(expectedCount, counters, latch, i, null));
        }
        // Create MessagingService to test and register the handlers
        BlockMessagingFacility messagingService = new BlockMessagingFacilityImpl();
        messagingService.init(context, null);
        testHandlers.forEach(handler -> messagingService.registerBlockNotificationHandler(handler, false, null));
        // start the messaging service
        messagingService.start();
        // send TEST_DATA_COUNT block notifications
        Thread senderThread = new SendingThread(messagingService, sentCounter, counters, null, 0, 0, 0);
        senderThread.start();
        waitForSenderBlockedOrComplete(senderThread);
        threadPoolManager
                .executor()
                .executeAsync(false, 10_000L, true, true, () -> Executors.newSingleThreadExecutor());
        // wait for all handlers to finish
        waitToFinish(latch);
        // shutdown the messaging service
        messagingService.stop();
        // verify that all handlers received the expected number of items
        for (int i = 0; i < testHandlers.size(); i++) {
            assertEquals(expectedCount, counters.get(i));
        }
    }

    /**
     * Test to verify that the messaging service can handle multiple block notification handlers
     * with both slow and fast responding handlers.
     *
     * @throws InterruptedException if the test latch is interrupted
     */
    @Test
    void testFastAndSlowBlockNotificationHandlers() throws InterruptedException {
        final int expectedCount = TEST_DATA_COUNT;
        // latch to wait for all handlers to finish
        final CountDownLatch latch = new CountDownLatch(2);
        // create counters for sent items
        final AtomicInteger sentCounter = new AtomicInteger(0);
        // create counters for received items
        AtomicIntegerArray counters = new AtomicIntegerArray(2);
        // create object for slowing down the slow handler
        final Semaphore holdBackSlowHandler = new Semaphore(0);
        // create a simple fast handler
        BlockNotificationHandler fastHandler = new LatchCountdownHandler(expectedCount, counters, latch, 0, null);
        // create a slow handler, that waits for the semaphore to be released on each call
        BlockNotificationHandler slowHandler =
                new LatchCountdownHandler(expectedCount, counters, latch, 1, holdBackSlowHandler);
        // Create MessagingService to test and register the handlers
        BlockMessagingFacility messagingService = new BlockMessagingFacilityImpl();
        messagingService.init(context, null);
        messagingService.registerBlockNotificationHandler(fastHandler, false, null);
        messagingService.registerBlockNotificationHandler(slowHandler, false, null);
        // start the messaging service
        messagingService.start();
        // send TEST_DATA_COUNT block notifications
        Thread senderThread =
                new SendingThread(messagingService, sentCounter, counters, holdBackSlowHandler, 1, 0.25D, 1);
        senderThread.start();
        waitForSenderBlockedOrComplete(senderThread);
        // mark sending finished and release the slow handler
        holdBackSlowHandler.release(TEST_DATA_COUNT);
        threadPoolManager
                .executor()
                .executeAsync(false, 10_000L, true, true, () -> Executors.newSingleThreadExecutor());
        // wait for all handlers to finish
        waitToFinish(latch);
        // shutdown the messaging service
        messagingService.stop();
        // wait up to 500ms (25*20) for the handlers to finish processing
        for (int i = 0; i < 25 && (counters.get(0) != expectedCount || counters.get(1) != expectedCount); i++) {
            LockSupport.parkNanos(20_000_000);
        }
        // verify that all handlers received the expected number of items
        assertEquals(expectedCount, counters.get(0));
        assertEquals(expectedCount, counters.get(1));
    }

    /**
     * Test to verify that the messaging service can handle backpressure on block notifications.
     * The test will create a slow handler that will apply backpressure to the messaging service
     * and verify that the messaging service applies backpressure to the notification send.
     *
     * @throws InterruptedException if the test latch is interrupted
     */
    @Test
    @Disabled("Disabled until new backpressure mechanism is in place")
    void testBlockNotificationBackpressure() throws InterruptedException {
        // latch to wait for all handlers to finish
        final CountDownLatch latch = new CountDownLatch(1);
        // create counters for sent and received items
        final AtomicInteger sentCounter = new AtomicInteger(0);
        final AtomicIntegerArray counters = new AtomicIntegerArray(1);
        // create object for slowing down the slow handler
        final Semaphore holdBackSlowHandler = new Semaphore(0);
        // create a slow handler, that waits for the semaphore to be released on each call
        BlockNotificationHandler slowHandler =
                new LatchCountdownHandler(TEST_DATA_COUNT, counters, latch, 0, holdBackSlowHandler);
        // Create MessagingService to test and register the handlers
        BlockMessagingFacility messagingService = new BlockMessagingFacilityImpl();
        messagingService.init(TestConfig.BLOCK_NODE_CONTEXT, null);
        messagingService.registerBlockNotificationHandler(slowHandler, false, null);
        // start the messaging service
        messagingService.start();
        // send TEST_DATA_COUNT block notifications in a background thread
        Thread senderThread =
                new SendingThread(messagingService, sentCounter, counters, holdBackSlowHandler, 0, 0.75D, 3);
        senderThread.start();
        final State stateAfter = waitForSenderBlockedOrComplete(senderThread, 1, TimeUnit.SECONDS);
        assertNotEquals(State.RUNNABLE, stateAfter, "Sender thread should not be runnable.");
        assertNotEquals(State.TERMINATED, stateAfter, "Sender thread should not be terminated.");
        int amountSent = sentCounter.get();
        // enough notifications should have been sent to the messaging service, avoid fixed numbers
        // because that breaks if the config changes.
        final String tooFewMessage = "sentCounter should be at least %d, but is %d.";
        assertTrue(amountSent >= MAX_NOTIFICATION_COUNT, tooFewMessage.formatted(MAX_NOTIFICATION_COUNT, amountSent));
        final String tooManyMessage = "sentCounter should be less than %d, but is %d.";
        assertTrue(amountSent < TEST_DATA_COUNT, tooManyMessage.formatted(TEST_DATA_COUNT, amountSent));
        // mark sending finished and release the slow handler
        holdBackSlowHandler.release(TEST_DATA_COUNT);
        // wait for all handlers to finish
        waitToFinish(latch);
        // shutdown the messaging service
        messagingService.stop();
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
        final BlockNotificationHandler handler1 = new BlockNotificationHandler() {
            @Override
            public void handleVerification(VerificationNotification notification) {
                // process notifications
                int receivedValue = (int) notification.blockNumber();
                // add up all the received values
                handler1Sum.addAndGet(receivedValue);
                // update count of calls
                handler1Counter.incrementAndGet();
            }
        };
        final BlockNotificationHandler handler2 = new BlockNotificationHandler() {
            @Override
            public void handleVerification(VerificationNotification notification) {
                // process notifications
                int receivedValue = (int) notification.blockNumber();
                // add up all the received values
                handler2Sum.addAndGet(receivedValue);
                // check if we are done
                if (handler2Counter.incrementAndGet() == TEST_DATA_COUNT - 1) {
                    latch.countDown();
                }
            }
        };
        // create message service to test, add handlers and start the service
        final BlockMessagingFacility messagingService = new BlockMessagingFacilityImpl();
        messagingService.init(context, null);
        messagingService.registerBlockNotificationHandler(handler1, false, null);
        messagingService.registerBlockNotificationHandler(handler2, false, null);
        messagingService.start();
        // send 2000 notifications to the service
        for (int i = 0; i < TEST_DATA_COUNT; i++) {
            if (i == 5) {
                // unregister the first handler, so it will not get any more notifications
                messagingService.unregisterBlockNotificationHandler(handler1);
                // wait for a bit to let the handler unregister
                LockSupport.parkNanos(500_000);
            }
            messagingService.sendBlockVerification(
                    new VerificationNotification(true, i, null, null, BlockSource.PUBLISHER));
            // have to slow down production to make test reliable
            LockSupport.parkNanos(500_000);
        }
        threadPoolManager
                .executor()
                .executeAsync(false, 10_000L, true, true, () -> Executors.newSingleThreadExecutor());
        // wait for handler number 2 to finish
        waitToFinish(latch);
        // shutdown the messaging service
        messagingService.stop();
        // check that the first handler did not process all notifications
        final String tooManyItemsMessage = "Handler 1 did not receive less items got [%d] of [%d]";
        assertTrue(
                handler1Counter.get() < TEST_DATA_COUNT,
                tooManyItemsMessage.formatted(handler1Counter.get(), TEST_DATA_COUNT));
        final int expectedBlockNumberSum = IntStream.range(0, TEST_DATA_COUNT).sum();
        final String totalTooLargeMessage = "Handler 1 did not receive less item value sum got [%d] of [%d]";
        assertTrue(
                handler1Sum.get() < expectedBlockNumberSum,
                totalTooLargeMessage.formatted(handler1Sum.get(), expectedBlockNumberSum));
        // check that the second handler processed all notifications
        assertEquals(TEST_DATA_COUNT, handler2Counter.get());
        assertEquals(expectedBlockNumberSum, handler2Sum.get());
    }

    private static void waitToFinish(final CountDownLatch latch) throws InterruptedException {
        assertTrue(
                latch.await(20, TimeUnit.SECONDS),
                "Did not finish in time; should have been much faster than the 20 second timeout");
    }

    private static void waitForSenderBlockedOrComplete(final Thread senderThread) {
        waitForSenderBlockedOrComplete(senderThread, 500, TimeUnit.MILLISECONDS);
    }

    private static Thread.State waitForSenderBlockedOrComplete(
            final Thread senderThread, long maxWait, TimeUnit waitUnit) {
        // wait for the sender thread to get stuck in back pressure or finish
        // but don't wait forever.
        final Duration waitDuration = Duration.of(maxWait, waitUnit.toChronoUnit());
        final Instant endTime = Instant.now().plus(waitDuration);
        Thread.State currentState = senderThread.getState();
        while (!(currentState == State.BLOCKED || currentState == State.TERMINATED)
                && Instant.now().isBefore(endTime)) {
            LockSupport.parkNanos(1_000_000);
            currentState = senderThread.getState();
        }
        return currentState;
    }

    private static class SendingThread extends Thread {
        private final BlockMessagingFacility messagingService;
        private final AtomicInteger sentCounter;
        private final AtomicIntegerArray counters;
        private final Semaphore pauseControl;
        private final int slowIndex;
        private final double slowRatio;
        private final int releaseCount;

        SendingThread(
                final BlockMessagingFacility messagingService,
                final AtomicInteger sentCounter,
                final AtomicIntegerArray counters,
                final Semaphore pauseControl,
                int slowIndex,
                double slowRatio,
                int releaseCount) {
            this.messagingService = messagingService;
            this.sentCounter = sentCounter;
            this.counters = counters;
            this.pauseControl = pauseControl;
            this.slowIndex = slowIndex;
            this.slowRatio = slowRatio;
            this.releaseCount = releaseCount;
        }

        @Override
        public void run() {
            for (int i = 0; i < TEST_DATA_COUNT; i++) {
                messagingService.sendBlockVerification(
                        new VerificationNotification(true, i, null, null, BlockSource.PUBLISHER));
                int totalSent = sentCounter.incrementAndGet();
                if (pauseControl != null) {
                    // release the pause control occasionally, to slow a handler by some amount.
                    // but don't release if the receiver is too far ahead
                    if (i % 4 == 0 && counters.get(slowIndex) <= (totalSent * (1.0D - slowRatio))) {
                        pauseControl.release(releaseCount);
                    }
                }
                LockSupport.parkNanos(10000); // park briefly (10μs) on every cycle, so we don't overrun.
            }
        }
    }

    private static class LatchCountdownHandler implements BlockNotificationHandler {
        private static final String DEADLOCK_TIMEOUT_MESSAGE =
                "Semaphore not released within 1 second, deadlock avoided in test";

        private final int expectedCount;
        private final AtomicIntegerArray counters;
        private final int identifier;
        private final CountDownLatch latch;
        private final Semaphore slowdownSemaphore;

        public LatchCountdownHandler(
                final int expectedCount,
                final AtomicIntegerArray counters,
                final CountDownLatch latch,
                int identifier,
                final Semaphore slowdownSemaphore) {
            this.expectedCount = expectedCount;
            this.counters = Objects.requireNonNull(counters);
            this.latch = Objects.requireNonNull(latch);
            this.identifier = identifier;
            this.slowdownSemaphore = slowdownSemaphore; // null if no slowdown desired.
        }

        @Override
        public void handleVerification(VerificationNotification notification) {
            if (expectedCount <= counters.incrementAndGet(identifier)) {
                // call the latch countdown when we reach the expected count
                latch.countDown();
            }
            // slow down the handler, if sending is not finished
            if (slowdownSemaphore != null) {
                try {
                    if (!slowdownSemaphore.tryAcquire(2, java.util.concurrent.TimeUnit.SECONDS)) {
                        throw new TimeoutException(DEADLOCK_TIMEOUT_MESSAGE);
                    }
                } catch (InterruptedException | TimeoutException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
