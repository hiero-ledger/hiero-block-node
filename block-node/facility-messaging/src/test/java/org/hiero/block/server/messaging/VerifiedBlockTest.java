// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.hedera.pbj.runtime.OneOf;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.time.Duration;
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
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
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
 * Tests for the verified block ring of {@link BlockMessagingFacilityImpl}. Block verification notifications travel on a
 * ring of their own, published directly from the sending threads, and reach the handlers registered with
 * {@link BlockMessagingFacilityImpl#registerVerifiedBlockHandler} through their
 * {@link BlockNotificationHandler#handleVerification} method, on a thread of their own.
 */
class VerifiedBlockTest {
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
    /** The size of the verified block ring */
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
        queueSize = messagingConfig.verifiedBlockQueueSize();
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
         * This test aims to assert that every block verification notification reaches every registered verified block
         * handler, in the order the notifications were sent, whether the handlers were registered before or after the
         * facility started. More notifications than the ring holds are sent, so the ring wraps around.
         */
        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        @DisplayName("Every verified block handler receives every verification notification in order")
        void everyHandlerReceivesEveryNotificationInOrder(final boolean registerBeforeStart)
                throws InterruptedException {
            final int blockCount = queueSize * 2 + 1;
            final RecordingHandler first = new RecordingHandler(blockCount);
            final RecordingHandler second = new RecordingHandler(blockCount);
            if (registerBeforeStart) {
                facility.registerVerifiedBlockHandler(first, false, "first");
                facility.registerVerifiedBlockHandler(second, false, "second");
                facility.start();
            } else {
                facility.start();
                facility.registerVerifiedBlockHandler(first, false, "first");
                facility.registerVerifiedBlockHandler(second, false, "second");
            }

            sendInBackground(0, blockCount, new AtomicInteger());

            final List<Long> expected = blockNumbers(0, blockCount);
            assertTrue(first.awaitAll(), "First handler is missing notifications: " + first.received.size());
            assertTrue(second.awaitAll(), "Second handler is missing notifications: " + second.received.size());
            assertEquals(expected, first.received);
            assertEquals(expected, second.received);
        }

        /**
         * This test aims to assert that a verified block handler receives the very notification that was sent, with
         * the whole block it carries, and not a copy without the block.
         */
        @Test
        @DisplayName("A verification notification arrives unchanged, with its block")
        void notificationArrivesWithItsBlock() throws InterruptedException {
            final RecordingHandler handler = new RecordingHandler(1);
            facility.registerVerifiedBlockHandler(handler, false, "recording");
            facility.start();
            final VerificationNotification sent = verifiedBlock(0);

            facility.sendBlockVerification(sent);

            assertTrue(handler.awaitAll(), "Handler did not receive the notification");
            assertSame(sent, handler.notifications.getFirst());
            assertSame(sent.block(), handler.notifications.getFirst().block());
        }

        /**
         * This test aims to assert that a handler registered only as a block notification handler is not attached to
         * the verified block ring: it receives no verification notifications and does not hold the ring back. The
         * handler would hold on to the first verification notification it receives, so if it were attached the sender
         * would wait on the full ring instead of finishing.
         */
        @Test
        @DisplayName("A handler registered only for block notifications gets no verification notifications")
        void notificationOnlyHandlerGetsNoVerifications() throws InterruptedException {
            final int blockCount = queueSize * 2;
            final HoldingHandler notificationOnly = newHoldingHandler();
            final RecordingHandler verified = new RecordingHandler(blockCount);
            facility.registerBlockNotificationHandler(notificationOnly, false, "notification-only");
            facility.registerVerifiedBlockHandler(verified, false, "verified");
            facility.start();

            final Thread sender = sendInBackground(0, blockCount, new AtomicInteger());

            sender.join(TimeUnit.SECONDS.toMillis(AWAIT_TIMEOUT_SECONDS));
            assertFalse(sender.isAlive(), "Sender is waiting on a handler that is not a verified block handler");
            assertTrue(verified.awaitAll(), "Verified block handler is missing notifications");
            assertTrue(notificationOnly.received.isEmpty(), "Notification handler received verification notifications");
        }

        /**
         * This test aims to assert that verification notifications are published directly from the sending thread and
         * never go through the message forwarder. The forwarder of this test only runs its tasks when the test asks it
         * to, which this test never does, yet every notification is delivered and no task is ever submitted to the
         * forwarder.
         */
        @Test
        @DisplayName("Verification notifications are delivered without the message forwarder")
        void notificationsAreDeliveredWithoutTheMessageForwarder() throws InterruptedException {
            final RecordingHandler handler = new RecordingHandler(queueSize);
            facility.registerVerifiedBlockHandler(handler, false, "recording");
            facility.start();

            for (long blockNumber = 0; blockNumber < queueSize; blockNumber++) {
                facility.sendBlockVerification(verifiedBlock(blockNumber));
            }

            assertTrue(handler.awaitAll(), "Handler is missing notifications: " + handler.received.size());
            assertFalse(threadPoolManager.executor().wasAnyTaskSubmitted(), "A task went through the forwarder");
        }

        /**
         * This test aims to assert that a handler receives verification notifications on a thread of its own, named
         * after the verified block ring, and not on the thread that delivers its block notifications.
         */
        @Test
        @DisplayName("Verification notifications arrive on a thread of their own")
        void notificationsArriveOnAThreadOfTheirOwn() throws InterruptedException {
            final RecordingHandler handler = new RecordingHandler(1);
            facility.registerBlockNotificationHandler(handler, false, "recording");
            facility.registerVerifiedBlockHandler(handler, false, "recording");
            facility.start();

            facility.sendBlockVerification(verifiedBlock(0));
            facility.sendBlockPersisted(new PersistedNotification(0, true, 1, BlockSource.PUBLISHER));
            runForwardedSends();

            assertTrue(handler.awaitAll(), "Handler did not receive the verification notification");
            awaitCondition(() -> handler.persistedThreadName != null, "Handler did not receive the notification");
            assertEquals("MessageHandler:VerifiedBlocks:recording", handler.verificationThreadName);
            assertNotEquals(handler.verificationThreadName, handler.persistedThreadName);
        }
    }

    @Nested
    @DisplayName("Back pressure")
    class BackPressure {
        /**
         * This test aims to assert that a handler that falls behind makes the sender of verification notifications
         * wait once the ring is full, that the gauge reports the ring as full meanwhile, and that every notification
         * is delivered in order once the handler catches up, after which the gauge reports the ring as empty.
         */
        @Test
        @DisplayName("A full ring makes the sender wait until the handler catches up")
        void fullRingMakesTheSenderWait() throws InterruptedException {
            final HoldingHandler handler = newHoldingHandler();
            facility.registerVerifiedBlockHandler(handler, false, "holding");
            facility.start();
            final AtomicInteger sent = new AtomicInteger();

            final Thread sender = sendInBackground(0, queueSize * 2, sent);

            awaitRingFull(handler, sent);
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(STILL_WAITING_MILLIS));
            assertEquals(queueSize, sent.get(), "Sender did not wait on the full ring");
            assertTrue(sender.isAlive(), "Sender finished while the ring was full");
            assertEquals(100.0, metricsExporter.verifiedBlockQueuePercentUsed());

            handler.release();
            sender.join(TimeUnit.SECONDS.toMillis(AWAIT_TIMEOUT_SECONDS));
            assertFalse(sender.isAlive(), "Sender is still waiting after the handler caught up");
            awaitCondition(() -> handler.received.size() == queueSize * 2, "Handler is missing notifications");
            assertEquals(blockNumbers(0, queueSize * 2), handler.received);
            awaitCondition(
                    () -> metricsExporter.verifiedBlockQueuePercentUsed() == 0.0,
                    "Gauge does not report the drained ring as empty");
        }

        /**
         * This test aims to assert that unregistering a handler that holds the ring full releases the waiting sender,
         * even though the handler still holds on to its notification, that the remaining handler still receives every
         * notification, and that the unregistered handler receives none of the notifications sent after it was
         * unregistered. Once released, the unregistered handler may still finish the batch it had already taken, which
         * only holds notifications sent before the ring was full, and then its thread must stop.
         */
        @Test
        @DisplayName("Unregistering the handler that holds the ring releases the sender")
        void unregisteringTheHoldingHandlerReleasesTheSender() throws InterruptedException {
            final int blockCount = queueSize * 2;
            final HoldingHandler holding = newHoldingHandler();
            final RecordingHandler recording = new RecordingHandler(blockCount);
            facility.registerVerifiedBlockHandler(holding, false, "holding");
            facility.registerVerifiedBlockHandler(recording, false, "recording");
            facility.start();
            final AtomicInteger sent = new AtomicInteger();

            final Thread sender = sendInBackground(0, blockCount, sent);
            awaitRingFull(holding, sent);
            facility.unregisterVerifiedBlockHandler(holding);

            sender.join(TimeUnit.SECONDS.toMillis(AWAIT_TIMEOUT_SECONDS));
            assertFalse(sender.isAlive(), "Sender is still waiting after the holding handler was unregistered");
            assertTrue(
                    recording.awaitAll(), "Remaining handler is missing notifications: " + recording.received.size());
            assertEquals(blockNumbers(0, blockCount), recording.received);
            holding.release();
            assertTrue(
                    holding.holdingThread.join(Duration.ofSeconds(AWAIT_TIMEOUT_SECONDS)),
                    "Thread of the unregistered handler did not stop");
            for (final long blockNumber : holding.received) {
                assertTrue(blockNumber < queueSize, "Unregistered handler received block " + blockNumber);
            }
        }

        /**
         * This test aims to assert that stopping the facility releases a sender that waits on a full ring, even when
         * the handler that holds the ring does not react to being stopped, so stopping the node never hangs on a
         * verification notification that no handler will take.
         */
        @Test
        @DisplayName("Stopping the facility releases a sender waiting on a full ring")
        void stoppingTheFacilityReleasesAWaitingSender() throws InterruptedException {
            final HoldingHandler handler = newHoldingHandler();
            facility.registerVerifiedBlockHandler(handler, false, "holding");
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
         * This test aims to assert that verification notifications sent from two threads at the same time are each
         * delivered exactly once, and that the notifications of each sender arrive in the order that sender sent them.
         * Verification sends from several threads: its sessions, its block item thread and its unvalidated block
         * thread.
         */
        @Test
        @DisplayName("Notifications from concurrent senders are each delivered once, in sender order")
        void notificationsFromConcurrentSendersAreEachDeliveredOnce() throws InterruptedException {
            final int blocksPerSender = queueSize * 4;
            final RecordingHandler handler = new RecordingHandler(blocksPerSender * 2);
            facility.registerVerifiedBlockHandler(handler, false, "recording");
            facility.start();

            final Thread firstSender = sendInBackground(0, blocksPerSender, new AtomicInteger());
            final Thread secondSender =
                    sendInBackground(SECOND_SENDER_FIRST_BLOCK, blocksPerSender, new AtomicInteger());

            firstSender.join(TimeUnit.SECONDS.toMillis(AWAIT_TIMEOUT_SECONDS));
            secondSender.join(TimeUnit.SECONDS.toMillis(AWAIT_TIMEOUT_SECONDS));
            assertTrue(handler.awaitAll(), "Handler is missing notifications: " + handler.received.size());
            assertEquals(blocksPerSender * 2, new HashSet<>(handler.received).size(), "A notification arrived twice");
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
         * This test aims to assert that a full verified block ring does not hold back block items that are sent by
         * another thread than the waiting sender: block items still flow to their handler, through more batches than
         * the block item ring holds.
         */
        @Test
        @DisplayName("A full verified block ring does not hold back block items")
        void fullRingDoesNotHoldBackBlockItems() throws InterruptedException {
            final int batchCount = messagingConfig.blockItemQueueSize() * 2;
            final HoldingHandler holding = newHoldingHandler();
            final CountDownLatch batchesReceived = new CountDownLatch(batchCount);
            final BlockItemHandler blockItemHandler = blockItems -> batchesReceived.countDown();
            facility.registerVerifiedBlockHandler(holding, false, "holding");
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
            assertTrue(sender.isAlive(), "The verified block ring was not full while block items were sent");
        }

        /**
         * This test aims to assert that a full verified block ring never holds back block notifications, not even
         * those of the handler that holds the ring full: its block notifications arrive on another thread.
         */
        @Test
        @DisplayName("A full verified block ring does not hold back block notifications")
        void fullRingDoesNotHoldBackBlockNotifications() {
            final int notificationCount = messagingConfig.blockNotificationQueueSize() * 2;
            final HoldingHandler holding = newHoldingHandler();
            facility.registerBlockNotificationHandler(holding, false, "holding");
            facility.registerVerifiedBlockHandler(holding, false, "holding");
            facility.start();
            final AtomicInteger sent = new AtomicInteger();
            final Thread sender = sendInBackground(0, queueSize + 1, sent);
            awaitRingFull(holding, sent);

            for (long blockNumber = 0; blockNumber < notificationCount; blockNumber++) {
                facility.sendBlockPersisted(new PersistedNotification(blockNumber, true, 1, BlockSource.PUBLISHER));
            }
            runForwardedSends();

            awaitCondition(
                    () -> holding.persisted.size() == notificationCount,
                    "Holding handler is missing block notifications");
            assertTrue(sender.isAlive(), "The verified block ring was not full while notifications were sent");
        }

        /**
         * This test aims to assert that a full verified block ring does not hold back unvalidated blocks that are sent
         * by another thread than the waiting sender: backfilled blocks still flow to their handler, through more
         * blocks than the unvalidated block ring holds.
         */
        @Test
        @DisplayName("A full verified block ring does not hold back unvalidated blocks")
        void fullRingDoesNotHoldBackUnvalidatedBlocks() throws InterruptedException {
            final int backfilledCount = messagingConfig.unvalidatedBlockQueueSize() * 2;
            final HoldingHandler holding = newHoldingHandler();
            final CountDownLatch backfilledReceived = new CountDownLatch(backfilledCount);
            final BlockNotificationHandler unvalidatedHandler = new BlockNotificationHandler() {
                @Override
                public void handleBackfilled(final BackfilledBlockNotification notification) {
                    backfilledReceived.countDown();
                }
            };
            facility.registerVerifiedBlockHandler(holding, false, "holding");
            facility.registerUnvalidatedBlockHandler(unvalidatedHandler, false, "unvalidated");
            facility.start();
            final AtomicInteger sent = new AtomicInteger();
            final Thread sender = sendInBackground(0, queueSize + 1, sent);
            awaitRingFull(holding, sent);

            for (long blockNumber = 0; blockNumber < backfilledCount; blockNumber++) {
                facility.sendBackfilledBlockNotification(
                        new BackfilledBlockNotification(blockNumber, BlockUnparsed.DEFAULT));
            }

            assertTrue(
                    backfilledReceived.await(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS),
                    "Unvalidated block handler is missing blocks: " + backfilledReceived.getCount());
            assertTrue(sender.isAlive(), "The verified block ring was not full while unvalidated blocks were sent");
        }

        /**
         * This test aims to assert that a handler busy with a block notification still receives verification
         * notifications: they no longer queue behind the block notifications of the handler.
         */
        @Test
        @DisplayName("A handler busy with a block notification still receives verification notifications")
        void handlerBusyWithANotificationStillReceivesVerifications() {
            final NotificationHoldingHandler handler = new NotificationHoldingHandler();
            facility.registerBlockNotificationHandler(handler, false, "notification-holding");
            facility.registerVerifiedBlockHandler(handler, false, "notification-holding");
            facility.start();
            facility.sendBlockPersisted(new PersistedNotification(0, true, 1, BlockSource.PUBLISHER));
            runForwardedSends();
            awaitCondition(() -> handler.holding.getCount() == 0, "Handler never received the notification");

            for (long blockNumber = 0; blockNumber < queueSize * 2; blockNumber++) {
                facility.sendBlockVerification(verifiedBlock(blockNumber));
            }

            awaitCondition(
                    () -> handler.received.size() == queueSize * 2,
                    "Handler busy with a notification is missing verification notifications");
            assertEquals(blockNumbers(0, queueSize * 2), handler.received);
            handler.release.countDown();
        }
    }

    @Nested
    @DisplayName("Handler failures")
    class HandlerFailures {
        /**
         * This test aims to assert that a handler that fails on one verification notification skips only that
         * notification: it still receives the notifications after it, and the skipped notification is counted.
         */
        @Test
        @DisplayName("A handler that fails on a notification still receives the next notifications")
        void handlerThatFailsOnANotificationStillReceivesTheNextNotifications() {
            final FailOnFirstHandler handler = new FailOnFirstHandler();
            facility.registerVerifiedBlockHandler(handler, false, "fails-on-first");
            facility.start();

            for (long blockNumber = 0; blockNumber < 3; blockNumber++) {
                facility.sendBlockVerification(verifiedBlock(blockNumber));
            }

            awaitCondition(() -> handler.received.size() == 2, "Handler stopped after the failed notification");
            assertEquals(List.of(1L, 2L), handler.received);
            assertEquals(1L, counterValue(BlockMessagingFacilityImpl.METRIC_MESSAGING_HANDLER_FAILURES));
        }
    }

    @Nested
    @DisplayName("Metrics")
    class Metrics {
        /**
         * This test aims to assert that every verification notification sent is counted once it is on the ring, as
         * the verification notification counter did before the notifications had a ring of their own.
         */
        @Test
        @DisplayName("Every sent verification notification is counted")
        void everySentNotificationIsCounted() {
            facility.start();

            for (long blockNumber = 0; blockNumber < queueSize; blockNumber++) {
                facility.sendBlockVerification(verifiedBlock(blockNumber));
            }

            assertEquals(
                    queueSize,
                    counterValue(BlockMessagingFacilityImpl.METRIC_MESSAGING_BLOCK_VERIFICATION_NOTIFICATIONS));
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
     * Waits until the handler holds on to its first notification and the sender has filled the ring, so the next send
     * waits.
     *
     * @param handler the handler holding the ring full
     * @param sent the number of notifications the sender has sent so far
     */
    private void awaitRingFull(final HoldingHandler handler, final AtomicInteger sent) {
        awaitCondition(() -> handler.holding.getCount() == 0, "Handler never received the first notification");
        awaitCondition(() -> sent.get() == queueSize, "Sender did not fill the ring");
    }

    /**
     * Runs the sends handed to the message forwarder, on a thread of their own.
     */
    private void runForwardedSends() {
        threadPoolManager.executor().executeAsync(false, 10_000L, false, false, Executors::newSingleThreadExecutor);
    }

    /**
     * Sends verification notifications with consecutive block numbers from a thread of its own, so a full ring makes
     * that thread wait instead of the test.
     *
     * @param firstBlock the number of the first block to send
     * @param count the number of notifications to send
     * @param sent counted up after every notification sent
     * @return the sending thread
     */
    private Thread sendInBackground(final long firstBlock, final int count, final AtomicInteger sent) {
        return Thread.ofVirtual().start(() -> {
            for (long blockNumber = firstBlock; blockNumber < firstBlock + count; blockNumber++) {
                facility.sendBlockVerification(verifiedBlock(blockNumber));
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
     * Creates a successful verification notification carrying an empty block.
     *
     * @param blockNumber the number of the block
     * @return the verification notification
     */
    private static VerificationNotification verifiedBlock(final long blockNumber) {
        return new VerificationNotification(
                true, null, blockNumber, Bytes.EMPTY, BlockUnparsed.newBuilder().build(), BlockSource.PUBLISHER);
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

    /** Block notification handler that records the verification notifications it receives and the threads used. */
    private static final class RecordingHandler implements BlockNotificationHandler {
        /** The block numbers of the verification notifications received, in the order received */
        private final List<Long> received = new CopyOnWriteArrayList<>();
        /** The verification notifications received, in the order received */
        private final List<VerificationNotification> notifications = new CopyOnWriteArrayList<>();
        /** Counted down for every verification notification received */
        private final CountDownLatch allReceived;
        /** The name of the thread the last verification notification arrived on */
        private volatile String verificationThreadName;
        /** The name of the thread the last persisted notification arrived on */
        private volatile String persistedThreadName;

        /**
         * Create a handler expecting the given number of verification notifications.
         *
         * @param expectedNotifications the number of verification notifications to expect
         */
        private RecordingHandler(final int expectedNotifications) {
            this.allReceived = new CountDownLatch(expectedNotifications);
        }

        @Override
        public void handleVerification(final VerificationNotification notification) {
            verificationThreadName = Thread.currentThread().getName();
            notifications.add(notification);
            received.add(notification.blockNumber());
            allReceived.countDown();
        }

        @Override
        public void handlePersisted(final PersistedNotification notification) {
            persistedThreadName = Thread.currentThread().getName();
        }

        /**
         * Waits for all the expected verification notifications.
         *
         * @return true if all the expected notifications arrived in time
         * @throws InterruptedException if the waiting thread is interrupted
         */
        private boolean awaitAll() throws InterruptedException {
            return allReceived.await(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
    }

    /**
     * Block notification handler that holds on to the first verification notification it receives until released, so
     * the verified block ring fills up behind it. Interrupts do not end the hold, like a handler stuck in work that
     * does not react to interrupts, so only the facility can free the ring from it. It records every verification and
     * persisted notification.
     */
    private static final class HoldingHandler implements BlockNotificationHandler {
        /** Counted down when the handler starts holding on to its first notification */
        private final CountDownLatch holding = new CountDownLatch(1);
        /** Counted down to let the handler move on */
        private final CountDownLatch released = new CountDownLatch(1);
        /** The block numbers of the verification notifications received, in the order received */
        private final List<Long> received = new CopyOnWriteArrayList<>();
        /** The block numbers of the persisted notifications received */
        private final List<Long> persisted = new CopyOnWriteArrayList<>();
        /** The thread the handler held its first notification on */
        private volatile Thread holdingThread;

        @Override
        public void handleVerification(final VerificationNotification notification) {
            received.add(notification.blockNumber());
            if (holding.getCount() > 0) {
                holdingThread = Thread.currentThread();
                holding.countDown();
                holdUntilReleased();
            }
        }

        @Override
        public void handlePersisted(final PersistedNotification notification) {
            persisted.add(notification.blockNumber());
        }

        /** Lets the handler move on from the notification it holds on to. */
        private void release() {
            released.countDown();
        }

        /** Holds on to the current notification until released, an interrupt is kept but does not end the hold. */
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
     * records every verification notification it receives meanwhile.
     */
    private static final class NotificationHoldingHandler implements BlockNotificationHandler {
        /** Counted down when the handler starts holding on to its first notification */
        private final CountDownLatch holding = new CountDownLatch(1);
        /** Counted down by the test to let the handler move on */
        private final CountDownLatch release = new CountDownLatch(1);
        /** The block numbers of the verification notifications received, in the order received */
        private final List<Long> received = new CopyOnWriteArrayList<>();

        @Override
        public void handleVerification(final VerificationNotification notification) {
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

    /** Block notification handler that fails on the first verification notification and records the ones after it. */
    private static final class FailOnFirstHandler implements BlockNotificationHandler {
        /** Whether the handler has failed already */
        private final AtomicBoolean failed = new AtomicBoolean(false);
        /** The block numbers of the verification notifications received after the failure, in the order received */
        private final List<Long> received = new CopyOnWriteArrayList<>();

        @Override
        public void handleVerification(final VerificationNotification notification) {
            if (failed.compareAndSet(false, true)) {
                throw new IllegalStateException("Simulated failure on the first verification notification");
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

    /** A metrics exporter that reads the current value of the verified block queue gauge. */
    private static final class GaugeReadingExporter implements MetricsExporter {
        /** Supplies snapshots of the metric registry */
        private Supplier<MetricRegistrySnapshot> snapshotSupplier;

        @Override
        public void setSnapshotSupplier(final Supplier<MetricRegistrySnapshot> snapshotSupplier) {
            this.snapshotSupplier = snapshotSupplier;
        }

        /**
         * Reads the current value of the verified block queue gauge.
         *
         * @return the percent of the verified block queue in use
         */
        private double verifiedBlockQueuePercentUsed() {
            final Set<Double> values = new HashSet<>();
            for (final MetricSnapshot snapshot : snapshotSupplier.get()) {
                if (snapshot.name().endsWith("messaging_verified_block_queue_percent_used")) {
                    for (final MeasurementSnapshot measurement : snapshot) {
                        if (measurement instanceof DoubleMeasurementSnapshot doubleMeasurement) {
                            values.add(doubleMeasurement.get());
                        }
                    }
                }
            }
            assertEquals(1, values.size(), "Expected exactly one verified block queue gauge value: " + values);
            return values.iterator().next();
        }

        @Override
        public void close() {}
    }
}
