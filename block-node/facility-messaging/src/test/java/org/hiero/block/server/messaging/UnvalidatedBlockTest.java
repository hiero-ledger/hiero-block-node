// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.hedera.pbj.runtime.OneOf;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.stream.LongStream;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockItemUnparsed.ItemOneOfType;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.async.TestThreadPoolManager;
import org.hiero.block.node.messaging.BlockMessagingFacilityImpl;
import org.hiero.block.node.messaging.MessagingConfig;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.BackfilledBlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockItemHandler;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.metrics.LongCounter;
import org.hiero.metrics.core.DoubleMeasurementSnapshot;
import org.hiero.metrics.core.MeasurementSnapshot;
import org.hiero.metrics.core.MetricKey;
import org.hiero.metrics.core.MetricRegistry;
import org.hiero.metrics.core.MetricRegistrySnapshot;
import org.hiero.metrics.core.MetricSnapshot;
import org.hiero.metrics.core.MetricsExporter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for the unvalidated block ring of {@link BlockMessagingFacilityImpl}. Backfilled blocks travel on a ring of
 * their own, published directly from the sending threads, and reach the handlers registered with
 * {@link BlockMessagingFacilityImpl#registerUnvalidatedBlockHandler} through their
 * {@link BlockNotificationHandler#handleBackfilled} method, on a thread of their own.
 */
class UnvalidatedBlockTest {
    /** How long to wait for handlers to receive messages and for senders to finish. */
    private static final long AWAIT_TIMEOUT_SECONDS = 10;
    /** How long a sender must keep waiting for the test to accept that a full ring holds it back. */
    private static final long STILL_WAITING_MILLIS = 200;
    /** The first block number of the second sender in the tests with several senders. */
    private static final long SECOND_SENDER_FIRST_BLOCK = 1_000_000L;

    /** The thread pool manager, its executor stands in for the message forwarder of the facility */
    private TestThreadPoolManager<BlockingExecutor, ScheduledBlockingExecutor> threadPoolManager;
    /** Reads gauge values from the metric registry of the facility */
    private GaugeReadingExporter metricsExporter;
    /** The context the facility under test was initialized with */
    private BlockNodeContext context;
    /** The configuration of the facility under test */
    private MessagingConfig messagingConfig;
    /** The size of the unvalidated block ring */
    private int queueSize;
    /** The messaging facility under test */
    private BlockMessagingFacilityImpl facility;
    /** Every holding handler of the test, released after the test so no handler thread stays parked */
    private final List<HoldingHandler> holdingHandlers = new ArrayList<>();

    @BeforeEach
    void setUp() {
        threadPoolManager = new TestThreadPoolManager<>(
                new BlockingExecutor(new LinkedBlockingQueue<>()),
                new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
        metricsExporter = new GaugeReadingExporter();
        final MetricRegistry metricRegistry =
                MetricRegistry.builder().setMetricsExporter(metricsExporter).build();
        context = new BlockNodeContext(
                TestConfig.getConfig(), metricRegistry, null, null, null, null, null, threadPoolManager, null);
        messagingConfig = context.configuration().getConfigData(MessagingConfig.class);
        queueSize = messagingConfig.unvalidatedBlockQueueSize();
        facility = new BlockMessagingFacilityImpl();
        facility.init(context, null);
    }

    @AfterEach
    void tearDown() {
        facility.stop();
        holdingHandlers.forEach(HoldingHandler::release);
    }

    @Nested
    @DisplayName("Delivery")
    class Delivery {
        /**
         * This test aims to assert that every backfilled block reaches every registered unvalidated block handler, in
         * the order the blocks were sent, whether the handlers were registered before or after the facility started.
         * More blocks than the ring holds are sent, so the ring wraps around.
         */
        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        @DisplayName("Every unvalidated block handler receives every backfilled block in order")
        void everyHandlerReceivesEveryBlockInOrder(final boolean registerBeforeStart) throws InterruptedException {
            final int blockCount = queueSize * 2 + 1;
            final RecordingHandler first = new RecordingHandler(blockCount);
            final RecordingHandler second = new RecordingHandler(blockCount);
            if (registerBeforeStart) {
                facility.registerUnvalidatedBlockHandler(first, false, "first");
                facility.registerUnvalidatedBlockHandler(second, false, "second");
                facility.start();
            } else {
                facility.start();
                facility.registerUnvalidatedBlockHandler(first, false, "first");
                facility.registerUnvalidatedBlockHandler(second, false, "second");
            }

            sendInBackground(0, blockCount, new AtomicInteger());

            final List<Long> expected = blockNumbers(0, blockCount);
            assertTrue(first.awaitAll(), "First handler is missing blocks: " + first.received.size());
            assertTrue(second.awaitAll(), "Second handler is missing blocks: " + second.received.size());
            assertEquals(expected, first.received);
            assertEquals(expected, second.received);
        }

        /**
         * This test aims to assert that a handler registered only as a block notification handler is not attached to
         * the unvalidated block ring: it receives no backfilled blocks and does not hold the ring back. The handler
         * would hold on to the first backfilled block it receives, so if it were attached the sender would wait on
         * the full ring instead of finishing.
         */
        @Test
        @DisplayName("A handler registered only for block notifications gets no backfilled blocks")
        void notificationOnlyHandlerGetsNoBlocks() throws InterruptedException {
            final int blockCount = queueSize * 2;
            final HoldingHandler notificationOnly = newHoldingHandler();
            final RecordingHandler unvalidated = new RecordingHandler(blockCount);
            facility.registerBlockNotificationHandler(notificationOnly, false, "notification-only");
            facility.registerUnvalidatedBlockHandler(unvalidated, false, "unvalidated");
            facility.start();

            final Thread sender = sendInBackground(0, blockCount, new AtomicInteger());

            sender.join(TimeUnit.SECONDS.toMillis(AWAIT_TIMEOUT_SECONDS));
            assertFalse(sender.isAlive(), "Sender is waiting on a handler that is not an unvalidated block handler");
            assertTrue(unvalidated.awaitAll(), "Unvalidated block handler is missing blocks");
            assertTrue(notificationOnly.received.isEmpty(), "Notification handler received backfilled blocks");
        }

        /**
         * This test aims to assert that backfilled blocks are published directly from the sending thread and never
         * go through the message forwarder. The forwarder of this test only runs its tasks when the test asks it to,
         * which this test never does, yet every block is delivered and no task is ever submitted to the forwarder.
         */
        @Test
        @DisplayName("Backfilled blocks are delivered without the message forwarder")
        void blocksAreDeliveredWithoutTheMessageForwarder() throws InterruptedException {
            final RecordingHandler handler = new RecordingHandler(queueSize);
            facility.registerUnvalidatedBlockHandler(handler, false, "recording");
            facility.start();

            for (long blockNumber = 0; blockNumber < queueSize; blockNumber++) {
                facility.sendBackfilledBlockNotification(backfilledBlock(blockNumber));
            }

            assertTrue(handler.awaitAll(), "Handler is missing blocks: " + handler.received.size());
            assertFalse(threadPoolManager.executor().wasAnyTaskSubmitted(), "A task went through the forwarder");
        }

        /**
         * This test aims to assert that a handler receives backfilled blocks on a thread of its own, named after the
         * unvalidated block ring, and not on the thread that delivers its block notifications.
         */
        @Test
        @DisplayName("Backfilled blocks arrive on a thread of their own")
        void blocksArriveOnAThreadOfTheirOwn() throws InterruptedException {
            final RecordingHandler handler = new RecordingHandler(1);
            facility.registerBlockNotificationHandler(handler, false, "recording");
            facility.registerUnvalidatedBlockHandler(handler, false, "recording");
            facility.start();

            facility.sendBackfilledBlockNotification(backfilledBlock(0));
            facility.sendBlockPersisted(new PersistedNotification(0, true, 1, BlockSource.BACKFILL));
            runForwardedSends();

            assertTrue(handler.awaitAll(), "Handler did not receive the backfilled block");
            awaitCondition(() -> handler.persistedThreadName != null, "Handler did not receive the notification");
            assertEquals("MessageHandler:UnvalidatedBlocks:recording", handler.backfilledThreadName);
            assertNotEquals(handler.backfilledThreadName, handler.persistedThreadName);
        }
    }

    @Nested
    @DisplayName("Back pressure")
    class BackPressure {
        /**
         * This test aims to assert that a handler that falls behind makes the sender of backfilled blocks wait once the
         * ring is full, that the gauge reports the ring as full meanwhile, and that every block is delivered in order
         * once the handler catches up, after which the gauge reports the ring as empty.
         */
        @Test
        @DisplayName("A full ring makes the sender wait until the handler catches up")
        void fullRingMakesTheSenderWait() throws InterruptedException {
            final HoldingHandler handler = newHoldingHandler();
            facility.registerUnvalidatedBlockHandler(handler, false, "holding");
            facility.start();
            final AtomicInteger sent = new AtomicInteger();

            final Thread sender = sendInBackground(0, queueSize * 2, sent);

            awaitRingFull(handler, sent);
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(STILL_WAITING_MILLIS));
            assertEquals(queueSize, sent.get(), "Sender did not wait on the full ring");
            assertTrue(sender.isAlive(), "Sender finished while the ring was full");
            assertEquals(100.0, metricsExporter.unvalidatedBlockQueuePercentUsed());

            handler.release();
            sender.join(TimeUnit.SECONDS.toMillis(AWAIT_TIMEOUT_SECONDS));
            assertFalse(sender.isAlive(), "Sender is still waiting after the handler caught up");
            awaitCondition(() -> handler.received.size() == queueSize * 2, "Handler is missing blocks");
            assertEquals(blockNumbers(0, queueSize * 2), handler.received);
            awaitCondition(
                    () -> metricsExporter.unvalidatedBlockQueuePercentUsed() == 0.0,
                    "Gauge does not report the drained ring as empty");
        }

        /**
         * This test aims to assert that unregistering a handler that holds the ring full releases the waiting sender,
         * even though the handler still holds on to its block, that the remaining handler still receives every block,
         * and that the unregistered handler receives none of the blocks sent after it was unregistered.
         */
        @Test
        @DisplayName("Unregistering the handler that holds the ring releases the sender")
        void unregisteringTheHoldingHandlerReleasesTheSender() throws InterruptedException {
            final int blockCount = queueSize * 2;
            final HoldingHandler holding = newHoldingHandler();
            final RecordingHandler recording = new RecordingHandler(blockCount);
            facility.registerUnvalidatedBlockHandler(holding, false, "holding");
            facility.registerUnvalidatedBlockHandler(recording, false, "recording");
            facility.start();
            final AtomicInteger sent = new AtomicInteger();

            final Thread sender = sendInBackground(0, blockCount, sent);
            awaitRingFull(holding, sent);
            facility.unregisterUnvalidatedBlockHandler(holding);

            sender.join(TimeUnit.SECONDS.toMillis(AWAIT_TIMEOUT_SECONDS));
            assertFalse(sender.isAlive(), "Sender is still waiting after the holding handler was unregistered");
            assertTrue(recording.awaitAll(), "Remaining handler is missing blocks: " + recording.received.size());
            assertEquals(blockNumbers(0, blockCount), recording.received);
            for (final long blockNumber : holding.received) {
                assertTrue(blockNumber < queueSize, "Unregistered handler received block " + blockNumber);
            }
        }

        /**
         * This test aims to assert that stopping the facility releases a sender that waits on a full ring, even when
         * the handler that holds the ring does not react to being stopped, so stopping the node never hangs on a
         * backfilled block that no handler will take.
         */
        @Test
        @DisplayName("Stopping the facility releases a sender waiting on a full ring")
        void stoppingTheFacilityReleasesAWaitingSender() throws InterruptedException {
            final HoldingHandler handler = newHoldingHandler();
            facility.registerUnvalidatedBlockHandler(handler, false, "holding");
            facility.start();
            final AtomicInteger sent = new AtomicInteger();

            final Thread sender = sendInBackground(0, queueSize + 1, sent);
            awaitRingFull(handler, sent);
            facility.stop();

            sender.join(TimeUnit.SECONDS.toMillis(AWAIT_TIMEOUT_SECONDS));
            assertFalse(sender.isAlive(), "Sender is still waiting after the facility stopped");
            assertEquals(queueSize + 1, sent.get());
        }
    }

    @Nested
    @DisplayName("Several senders")
    class SeveralSenders {
        /**
         * This test aims to assert that backfilled blocks sent from two threads at the same time are each delivered
         * exactly once, and that the blocks of each sender arrive in the order that sender sent them.
         */
        @Test
        @DisplayName("Blocks from concurrent senders are each delivered once, in sender order")
        void blocksFromConcurrentSendersAreEachDeliveredOnce() throws InterruptedException {
            final int blocksPerSender = queueSize * 4;
            final RecordingHandler handler = new RecordingHandler(blocksPerSender * 2);
            facility.registerUnvalidatedBlockHandler(handler, false, "recording");
            facility.start();

            final Thread firstSender = sendInBackground(0, blocksPerSender, new AtomicInteger());
            final Thread secondSender =
                    sendInBackground(SECOND_SENDER_FIRST_BLOCK, blocksPerSender, new AtomicInteger());

            firstSender.join(TimeUnit.SECONDS.toMillis(AWAIT_TIMEOUT_SECONDS));
            secondSender.join(TimeUnit.SECONDS.toMillis(AWAIT_TIMEOUT_SECONDS));
            assertTrue(handler.awaitAll(), "Handler is missing blocks: " + handler.received.size());
            assertEquals(blocksPerSender * 2, new HashSet<>(handler.received).size(), "A block arrived twice");
            assertEquals(
                    blockNumbers(0, blocksPerSender),
                    handler.received.stream()
                            .filter(blockNumber -> blockNumber < SECOND_SENDER_FIRST_BLOCK)
                            .toList());
            assertEquals(
                    blockNumbers(SECOND_SENDER_FIRST_BLOCK, blocksPerSender),
                    handler.received.stream()
                            .filter(blockNumber -> blockNumber >= SECOND_SENDER_FIRST_BLOCK)
                            .toList());
        }
    }

    @Nested
    @DisplayName("Isolation from the other rings")
    class Isolation {
        /**
         * This test aims to assert that a full unvalidated block ring never holds back the live block items: while a
         * sender of backfilled blocks waits on the full ring, block items still flow to their handler, through more
         * batches than the block item ring holds.
         */
        @Test
        @DisplayName("A full unvalidated block ring does not hold back block items")
        void fullRingDoesNotHoldBackBlockItems() throws InterruptedException {
            final int batchCount = messagingConfig.blockItemQueueSize() * 2;
            final HoldingHandler holding = newHoldingHandler();
            final CountDownLatch batchesReceived = new CountDownLatch(batchCount);
            final BlockItemHandler blockItemHandler = blockItems -> batchesReceived.countDown();
            facility.registerUnvalidatedBlockHandler(holding, false, "holding");
            facility.registerBlockItemHandler(blockItemHandler, false, "block-items");
            facility.start();
            final AtomicInteger sent = new AtomicInteger();
            final Thread sender = sendInBackground(0, queueSize + 1, sent);
            awaitRingFull(holding, sent);

            for (int blockNumber = 0; blockNumber < batchCount; blockNumber++) {
                facility.sendBlockItems(blockItems(blockNumber));
            }

            assertTrue(
                    batchesReceived.await(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS),
                    "Block item handler is missing batches: " + batchesReceived.getCount());
            assertTrue(sender.isAlive(), "The unvalidated block ring was not full while block items were sent");
        }

        /**
         * This test aims to assert that a full unvalidated block ring never holds back block notifications, not even
         * those of the handler that holds the ring full: its block notifications arrive on another thread.
         */
        @Test
        @DisplayName("A full unvalidated block ring does not hold back block notifications")
        void fullRingDoesNotHoldBackBlockNotifications() {
            final int notificationCount = messagingConfig.blockNotificationQueueSize() * 2;
            final HoldingHandler holding = newHoldingHandler();
            facility.registerBlockNotificationHandler(holding, false, "holding");
            facility.registerUnvalidatedBlockHandler(holding, false, "holding");
            facility.start();
            final AtomicInteger sent = new AtomicInteger();
            final Thread sender = sendInBackground(0, queueSize + 1, sent);
            awaitRingFull(holding, sent);

            for (long blockNumber = 0; blockNumber < notificationCount; blockNumber++) {
                facility.sendBlockPersisted(new PersistedNotification(blockNumber, true, 1, BlockSource.BACKFILL));
            }
            runForwardedSends();

            awaitCondition(
                    () -> holding.persisted.size() == notificationCount,
                    "Holding handler is missing block notifications");
            assertTrue(sender.isAlive(), "The unvalidated block ring was not full while notifications were sent");
        }

        /**
         * This test aims to assert that a handler busy with a block notification still receives backfilled blocks:
         * backfilled blocks no longer queue behind the block notifications of the handler.
         */
        @Test
        @DisplayName("A handler busy with a block notification still receives backfilled blocks")
        void handlerBusyWithANotificationStillReceivesBlocks() {
            final NotificationHoldingHandler handler = new NotificationHoldingHandler();
            facility.registerBlockNotificationHandler(handler, false, "notification-holding");
            facility.registerUnvalidatedBlockHandler(handler, false, "notification-holding");
            facility.start();
            facility.sendBlockPersisted(new PersistedNotification(0, true, 1, BlockSource.BACKFILL));
            runForwardedSends();
            awaitCondition(() -> handler.holding.getCount() == 0, "Handler never received the notification");

            for (long blockNumber = 0; blockNumber < queueSize * 2; blockNumber++) {
                facility.sendBackfilledBlockNotification(backfilledBlock(blockNumber));
            }

            awaitCondition(
                    () -> handler.received.size() == queueSize * 2,
                    "Handler busy with a notification is missing backfilled blocks");
            assertEquals(blockNumbers(0, queueSize * 2), handler.received);
            handler.release.countDown();
        }
    }

    @Nested
    @DisplayName("Handler failures")
    class HandlerFailures {
        /**
         * This test aims to assert that a handler that fails on one backfilled block skips only that block: it still
         * receives the blocks after it, and the skipped block is counted.
         */
        @Test
        @DisplayName("A handler that fails on a block still receives the next blocks")
        void handlerThatFailsOnABlockStillReceivesTheNextBlocks() {
            final FailOnFirstHandler handler = new FailOnFirstHandler();
            facility.registerUnvalidatedBlockHandler(handler, false, "fails-on-first");
            facility.start();

            for (long blockNumber = 0; blockNumber < 3; blockNumber++) {
                facility.sendBackfilledBlockNotification(backfilledBlock(blockNumber));
            }

            awaitCondition(() -> handler.received.size() == 2, "Handler stopped after the failed block");
            assertEquals(List.of(1L, 2L), handler.received);
            assertEquals(1L, counterValue(BlockMessagingFacilityImpl.METRIC_MESSAGING_HANDLER_FAILURES));
        }
    }

    @Nested
    @DisplayName("Metrics")
    class Metrics {
        /**
         * This test aims to assert that every backfilled block sent is counted once it is on the ring, as the
         * backfilled notification counter did before the blocks had a ring of their own.
         */
        @Test
        @DisplayName("Every sent backfilled block is counted")
        void everySentBlockIsCounted() {
            facility.start();

            for (long blockNumber = 0; blockNumber < queueSize; blockNumber++) {
                facility.sendBackfilledBlockNotification(backfilledBlock(blockNumber));
            }

            assertEquals(
                    queueSize,
                    counterValue(BlockMessagingFacilityImpl.METRIC_MESSAGING_BLOCK_BACKFILLED_NOTIFICATIONS));
        }
    }

    /**
     * Creates a holding handler that is released after the test.
     *
     * @return the new holding handler
     */
    private HoldingHandler newHoldingHandler() {
        final HoldingHandler handler = new HoldingHandler();
        holdingHandlers.add(handler);
        return handler;
    }

    /**
     * Waits until the handler holds on to its first block and the sender has filled the ring, so the next send waits.
     *
     * @param handler the handler holding the ring full
     * @param sent the number of blocks the sender has sent so far
     */
    private void awaitRingFull(final HoldingHandler handler, final AtomicInteger sent) {
        awaitCondition(() -> handler.holding.getCount() == 0, "Handler never received the first block");
        awaitCondition(() -> sent.get() == queueSize, "Sender did not fill the ring");
    }

    /**
     * Runs the sends handed to the message forwarder, on a thread of their own.
     */
    private void runForwardedSends() {
        threadPoolManager.executor().executeAsync(false, 10_000L, false, false, Executors::newSingleThreadExecutor);
    }

    /**
     * Sends backfilled blocks with consecutive numbers from a thread of its own, so a full ring makes that thread wait
     * instead of the test.
     *
     * @param firstBlock the number of the first block to send
     * @param count the number of blocks to send
     * @param sent counted up after every block sent
     * @return the sending thread
     */
    private Thread sendInBackground(final long firstBlock, final int count, final AtomicInteger sent) {
        return Thread.ofVirtual().start(() -> {
            for (long blockNumber = firstBlock; blockNumber < firstBlock + count; blockNumber++) {
                facility.sendBackfilledBlockNotification(backfilledBlock(blockNumber));
                sent.incrementAndGet();
            }
        });
    }

    /**
     * Reads the value of a counter of the facility under test.
     *
     * @param key the key of the counter
     * @return the value of the counter
     */
    private long counterValue(final MetricKey<LongCounter> key) {
        return context.metricRegistry().getMetric(key).getOrCreateNotLabeled().get();
    }

    /**
     * Waits until the condition holds, failing the test if it does not hold in time.
     *
     * @param condition the condition to wait for
     * @param message the failure message if the condition does not hold in time
     */
    private static void awaitCondition(final BooleanSupplier condition, final String message) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(AWAIT_TIMEOUT_SECONDS);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                fail(message);
            } else {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
            }
        }
    }

    /**
     * Creates a backfilled block notification with an empty block.
     *
     * @param blockNumber the number of the block
     * @return the backfilled block notification
     */
    private static BackfilledBlockNotification backfilledBlock(final long blockNumber) {
        return new BackfilledBlockNotification(blockNumber, BlockUnparsed.DEFAULT);
    }

    /**
     * Creates a batch of block items holding a single item.
     *
     * @param blockNumber the number of the block the batch belongs to
     * @return the batch of block items
     */
    private static BlockItems blockItems(final long blockNumber) {
        return new BlockItems(
                List.of(new BlockItemUnparsed(new OneOf<>(ItemOneOfType.BLOCK_HEADER, null))), blockNumber, true, true);
    }

    /**
     * Lists consecutive block numbers.
     *
     * @param firstBlock the first block number
     * @param count the number of block numbers
     * @return the block numbers from the first block on
     */
    private static List<Long> blockNumbers(final long firstBlock, final int count) {
        return LongStream.range(firstBlock, firstBlock + count).boxed().toList();
    }

    /** Block notification handler that records the backfilled blocks it receives and the threads it is called on. */
    private static final class RecordingHandler implements BlockNotificationHandler {
        /** The numbers of the backfilled blocks received, in the order received */
        private final List<Long> received = new CopyOnWriteArrayList<>();
        /** Counted down for every backfilled block received */
        private final CountDownLatch allReceived;
        /** The name of the thread the last backfilled block arrived on */
        private volatile String backfilledThreadName;
        /** The name of the thread the last persisted notification arrived on */
        private volatile String persistedThreadName;

        /**
         * Create a handler expecting the given number of backfilled blocks.
         *
         * @param expectedBlocks the number of backfilled blocks to expect
         */
        private RecordingHandler(final int expectedBlocks) {
            this.allReceived = new CountDownLatch(expectedBlocks);
        }

        @Override
        public void handleBackfilled(final BackfilledBlockNotification notification) {
            backfilledThreadName = Thread.currentThread().getName();
            received.add(notification.blockNumber());
            allReceived.countDown();
        }

        @Override
        public void handlePersisted(final PersistedNotification notification) {
            persistedThreadName = Thread.currentThread().getName();
        }

        /**
         * Waits for all the expected backfilled blocks.
         *
         * @return true if all the expected blocks arrived in time
         * @throws InterruptedException if the waiting thread is interrupted
         */
        private boolean awaitAll() throws InterruptedException {
            return allReceived.await(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
    }

    /**
     * Block notification handler that holds on to the first backfilled block it receives until released, so the
     * unvalidated block ring fills up behind it. Interrupts do not end the hold, like a handler stuck in work that does
     * not react to interrupts, so only the facility can free the ring from it. It records every backfilled block and
     * persisted notification.
     */
    private static final class HoldingHandler implements BlockNotificationHandler {
        /** Counted down when the handler starts holding on to its first block */
        private final CountDownLatch holding = new CountDownLatch(1);
        /** Counted down to let the handler move on */
        private final CountDownLatch released = new CountDownLatch(1);
        /** The numbers of the backfilled blocks received, in the order received */
        private final List<Long> received = new CopyOnWriteArrayList<>();
        /** The block numbers of the persisted notifications received */
        private final List<Long> persisted = new CopyOnWriteArrayList<>();

        @Override
        public void handleBackfilled(final BackfilledBlockNotification notification) {
            received.add(notification.blockNumber());
            if (holding.getCount() > 0) {
                holding.countDown();
                holdUntilReleased();
            }
        }

        @Override
        public void handlePersisted(final PersistedNotification notification) {
            persisted.add(notification.blockNumber());
        }

        /** Lets the handler move on from the block it holds on to. */
        private void release() {
            released.countDown();
        }

        /** Holds on to the current block until released, an interrupt is kept for later but does not end the hold. */
        private void holdUntilReleased() {
            boolean interrupted = false;
            while (released.getCount() > 0) {
                try {
                    released.await();
                } catch (final InterruptedException e) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Block notification handler that holds on to the first persisted notification it receives until released, and
     * records every backfilled block it receives meanwhile.
     */
    private static final class NotificationHoldingHandler implements BlockNotificationHandler {
        /** Counted down when the handler starts holding on to its first notification */
        private final CountDownLatch holding = new CountDownLatch(1);
        /** Counted down by the test to let the handler move on */
        private final CountDownLatch release = new CountDownLatch(1);
        /** The numbers of the backfilled blocks received, in the order received */
        private final List<Long> received = new CopyOnWriteArrayList<>();

        @Override
        public void handleBackfilled(final BackfilledBlockNotification notification) {
            received.add(notification.blockNumber());
        }

        @Override
        public void handlePersisted(final PersistedNotification notification) {
            if (holding.getCount() > 0) {
                holding.countDown();
                awaitRelease(release);
            }
        }
    }

    /** Block notification handler that fails on the first backfilled block and records the blocks after it. */
    private static final class FailOnFirstHandler implements BlockNotificationHandler {
        /** Whether the handler has failed already */
        private final AtomicBoolean failed = new AtomicBoolean(false);
        /** The numbers of the backfilled blocks received after the failure, in the order received */
        private final List<Long> received = new CopyOnWriteArrayList<>();

        @Override
        public void handleBackfilled(final BackfilledBlockNotification notification) {
            if (failed.compareAndSet(false, true)) {
                throw new IllegalStateException("Simulated failure on the first backfilled block");
            } else {
                received.add(notification.blockNumber());
            }
        }
    }

    /**
     * Waits until the latch is released. Unregistering a handler or stopping the facility interrupts the handler
     * thread, which also ends the wait.
     *
     * @param release the latch to wait for
     */
    private static void awaitRelease(final CountDownLatch release) {
        try {
            release.await();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** A metrics exporter that reads the current value of the unvalidated block queue gauge. */
    private static final class GaugeReadingExporter implements MetricsExporter {
        /** Supplies snapshots of the metric registry */
        private Supplier<MetricRegistrySnapshot> snapshotSupplier;

        @Override
        public void setSnapshotSupplier(final Supplier<MetricRegistrySnapshot> snapshotSupplier) {
            this.snapshotSupplier = snapshotSupplier;
        }

        /**
         * Reads the current value of the unvalidated block queue gauge.
         *
         * @return the percent of the unvalidated block queue in use
         */
        private double unvalidatedBlockQueuePercentUsed() {
            final Set<Double> values = new HashSet<>();
            for (final MetricSnapshot snapshot : snapshotSupplier.get()) {
                if (snapshot.name().endsWith("messaging_unvalidated_block_queue_percent_used")) {
                    for (final MeasurementSnapshot measurement : snapshot) {
                        if (measurement instanceof DoubleMeasurementSnapshot doubleMeasurement) {
                            values.add(doubleMeasurement.get());
                        }
                    }
                }
            }
            assertEquals(1, values.size(), "Expected exactly one unvalidated block queue gauge value: " + values);
            return values.iterator().next();
        }

        @Override
        public void close() {}
    }
}
