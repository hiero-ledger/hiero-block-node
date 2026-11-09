// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.publisher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.config.api.Configuration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.internal.BlockItemSetUnparsed;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.node.app.fixtures.TestUtils;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.async.TestThreadPoolManager;
import org.hiero.block.node.app.fixtures.pipeline.TestResponsePipeline;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.app.fixtures.plugintest.TestApplicationStateFacility;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.stream.publisher.LiveStreamPublisherManager.MetricsHolder;
import org.hiero.block.node.stream.publisher.fixtures.TestStreamPublisherManager;
import org.hiero.metrics.core.MetricRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/// Tests that the queue forwarder and the flow-control refresh task of [LiveStreamPublisherManager]
/// recover after they stop, instead of staying dead for the life of the process.
@DisplayName("LiveStreamPublisherManager Recovery Tests")
class LiveStreamPublisherManagerRecoveryTest {

    /// An executor that hands out scripted futures instead of running the forwarder, so each
    /// scenario can decide exactly how the previous forwarder task ended.
    private static final class ScriptedForwarderExecutor extends AbstractExecutorService {
        private final Deque<Future<Long>> scripted = new ConcurrentLinkedDeque<>();
        private volatile boolean rejecting;
        private final AtomicInteger attempts = new AtomicInteger();
        private final AtomicInteger launched = new AtomicInteger();
        private final List<Future<Long>> handedOut = new CopyOnWriteArrayList<>();

        void script(final Future<Long> next) {
            scripted.add(next);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> Future<T> submit(final Callable<T> task) {
            attempts.incrementAndGet();
            if (rejecting) {
                throw new RejectedExecutionException("rejected for test");
            }
            launched.incrementAndGet();
            final Future<Long> next = scripted.isEmpty() ? new CompletableFuture<>() : scripted.poll();
            handedOut.add(next);
            return (Future<T>) next;
        }

        @Override
        public void execute(final Runnable command) {
            throw new UnsupportedOperationException("only submit is scripted");
        }

        @Override
        public void shutdown() {}

        @Override
        public List<Runnable> shutdownNow() {
            return List.of();
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public boolean awaitTermination(final long timeout, final TimeUnit unit) {
            return true;
        }
    }

    private ScriptedForwarderExecutor forwarderExecutor;
    private TestThreadPoolManager<ScriptedForwarderExecutor, ScheduledBlockingExecutor> threadPoolManager;
    private MetricsHolder managerMetrics;
    private PublisherHandler.MetricsHolder handlerMetrics;
    private LiveStreamPublisherManager manager;

    @BeforeEach
    void setup() {
        final Map<String, String> overrides = Map.of(
                "producer.perHandlerMessageBudget", "10",
                "producer.totalMessageBudgetPerInterval", "10000");
        final Configuration config = TestStreamPublisherManager.createTestConfiguration(overrides);
        forwarderExecutor = new ScriptedForwarderExecutor();
        threadPoolManager = new TestThreadPoolManager<>(
                forwarderExecutor, new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
        final SimpleInMemoryHistoricalBlockFacility historicalBlockFacility =
                new SimpleInMemoryHistoricalBlockFacility();
        final BlockNodeContext context = new BlockNodeContext(
                config,
                TestUtils.createMetrics(),
                null,
                new TestBlockMessagingFacility(),
                historicalBlockFacility,
                new TestApplicationStateFacility(),
                null,
                threadPoolManager,
                null);
        historicalBlockFacility.init(context, null);
        final MetricRegistry registry = MetricRegistry.builder().build();
        managerMetrics = MetricsHolder.createMetrics(registry);
        handlerMetrics = PublisherHandler.MetricsHolder.createMetrics(registry);
        manager = new LiveStreamPublisherManager(context, managerMetrics, List.of());
    }

    /// Runs the action on `callers` threads that all start at the same moment.
    private static void runConcurrently(final int callers, final Runnable action) throws Exception {
        final ExecutorService pool = Executors.newFixedThreadPool(callers);
        try {
            final CountDownLatch ready = new CountDownLatch(callers);
            final CountDownLatch go = new CountDownLatch(1);
            final List<Future<Object>> tasks = new ArrayList<>();
            for (int i = 0; i < callers; i++) {
                tasks.add(pool.submit(() -> {
                    ready.countDown();
                    go.await();
                    action.run();
                    return null;
                }));
            }
            ready.await();
            go.countDown();
            for (final Future<Object> task : tasks) {
                task.get(30, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }
    }

    /// A messaging facility whose first send fails after recording the batch, which ends a real forwarder
    /// task with an exception while the rest of the block is still queued.
    private static final class FailFirstSendMessagingFacility extends TestBlockMessagingFacility {
        private final AtomicBoolean failNext = new AtomicBoolean(true);

        @Override
        public void sendBlockItems(final BlockItems blockItems) {
            super.sendBlockItems(blockItems);
            if (failNext.compareAndSet(true, false)) {
                throw new IllegalStateException("send failed for test");
            }
        }
    }

    private static BlockItemSetUnparsed batchOf(final int marker) {
        return BlockItemSetUnparsed.newBuilder()
                .blockItems(List.of(BlockItemUnparsed.newBuilder()
                        .signedTransaction(Bytes.wrap(new byte[] {(byte) marker}))
                        .build()))
                .build();
    }

    /// Calls `check` until it returns true or the deadline passes.
    private static boolean awaitTrue(final BooleanSupplier check, final long timeoutMillis) {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (!check.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                return false;
            }
            LockSupport.parkNanos(1_000_000L);
        }
        return true;
    }

    @Nested
    @DisplayName("Timer driven forwarder restart")
    class TimerDrivenRestartTests {

        @Test
        @DisplayName("a scheduler tick restarts a finished forwarder when a block is still queued")
        void tickRestartsFinishedForwarderWhenWorkIsPending() {
            forwarderExecutor.script(CompletableFuture.completedFuture(1L));
            manager.checkLogAndRestartForwarderTask();
            assertThat(forwarderExecutor.launched.get()).isEqualTo(1);
            manager.registerQueueForBlock(1L, new ArrayDeque<>(List.of(batchOf(1))), 5L);

            threadPoolManager.scheduledExecutor().executeSerially();

            assertThat(forwarderExecutor.launched.get()).isEqualTo(2);
        }

        @Test
        @DisplayName("a scheduler tick leaves a finished forwarder alone when nothing is queued")
        void tickDoesNothingWhenNothingIsQueued() {
            forwarderExecutor.script(CompletableFuture.completedFuture(1L));
            manager.checkLogAndRestartForwarderTask();
            assertThat(forwarderExecutor.launched.get()).isEqualTo(1);

            threadPoolManager.scheduledExecutor().executeSerially();

            assertThat(forwarderExecutor.launched.get()).isEqualTo(1);
        }

        @Test
        @DisplayName("a scheduler tick does not launch a second forwarder while one is running")
        void tickLeavesARunningForwarderAlone() {
            manager.checkLogAndRestartForwarderTask();
            manager.registerQueueForBlock(1L, new ArrayDeque<>(List.of(batchOf(1))), 5L);

            threadPoolManager.scheduledExecutor().executeSerially();
            threadPoolManager.scheduledExecutor().executeSerially();

            assertThat(forwarderExecutor.launched.get()).isEqualTo(1);
        }

        @Test
        @DisplayName("a real forwarder that dies mid block is restarted by a tick, with no further block traffic")
        void realForwarderIsRestartedByATick() {
            final FailFirstSendMessagingFacility messaging = new FailFirstSendMessagingFacility();
            final ExecutorService realThreads = Executors.newVirtualThreadPerTaskExecutor();
            final ScheduledBlockingExecutor scheduler = new ScheduledBlockingExecutor(new LinkedBlockingQueue<>());
            final TestThreadPoolManager<ExecutorService, ScheduledBlockingExecutor> threads =
                    new TestThreadPoolManager<>(realThreads, scheduler);
            final SimpleInMemoryHistoricalBlockFacility historicalBlockFacility =
                    new SimpleInMemoryHistoricalBlockFacility();
            final BlockNodeContext context = new BlockNodeContext(
                    TestStreamPublisherManager.createTestConfiguration(Map.of()),
                    TestUtils.createMetrics(),
                    null,
                    messaging,
                    historicalBlockFacility,
                    new TestApplicationStateFacility(),
                    null,
                    threads,
                    null);
            historicalBlockFacility.init(context, null);
            final LiveStreamPublisherManager realManager = new LiveStreamPublisherManager(
                    context,
                    MetricsHolder.createMetrics(MetricRegistry.builder().build()),
                    List.of());
            try {
                realManager.registerQueueForBlock(
                        1L, new ArrayDeque<>(List.of(batchOf(1), batchOf(2), batchOf(3))), 5L);
                // Starts the first forwarder, whose first send throws and ends it with batches 2 and 3 still queued.
                realManager.checkLogAndRestartForwarderTask();

                // Only timer ticks run from here on. A tick before the first forwarder has ended does nothing.
                final boolean allSent = awaitTrue(
                        () -> {
                            scheduler.executeSerially();
                            return messaging.getSentBlockItems().size() >= 3;
                        },
                        10_000L);

                assertThat(allSent)
                        .as("the remaining batches reach messaging after a tick")
                        .isTrue();
                assertThat(messaging.getSentBlockItems().stream()
                                .map(items -> items.blockItems()
                                        .getFirst()
                                        .signedTransactionOrThrow()
                                        .toByteArray()[0])
                                .toList())
                        .containsExactly((byte) 1, (byte) 2, (byte) 3);
            } finally {
                realManager.shutdown();
                realThreads.shutdownNow();
            }
        }

        @Test
        @DisplayName("restarts racing with shutdown never leave a forwarder running or launch one afterwards")
        void restartsRacingWithShutdownLeaveNothingRunning() throws Exception {
            final AtomicInteger roles = new AtomicInteger();
            final AtomicBoolean shutDown = new AtomicBoolean();
            final AtomicBoolean finished = new AtomicBoolean();
            final AtomicInteger callsAfterShutdown = new AtomicInteger();

            assertThatNoException()
                    .isThrownBy(() -> runConcurrently(8, () -> {
                        final int role = roles.getAndIncrement();
                        if (role == 0) {
                            // Ends forwarders so the callers keep having something to restart.
                            while (!finished.get()) {
                                for (final Future<Long> handedOut : forwarderExecutor.handedOut) {
                                    if (handedOut instanceof CompletableFuture<Long> completable) {
                                        completable.complete(1L);
                                    }
                                }
                                LockSupport.parkNanos(100_000L);
                            }
                        } else if (role == 1) {
                            awaitTrue(() -> forwarderExecutor.launched.get() >= 3, 10_000L);
                            manager.shutdown();
                            shutDown.set(true);
                            awaitTrue(() -> callsAfterShutdown.get() >= 1_000, 10_000L);
                            finished.set(true);
                        } else {
                            while (!finished.get()) {
                                manager.checkLogAndRestartForwarderTask();
                                manager.ensureFlowControlRefreshScheduled();
                                if (shutDown.get()) {
                                    callsAfterShutdown.incrementAndGet();
                                }
                            }
                        }
                    }));

            final int attemptsAtRest = forwarderExecutor.attempts.get();
            manager.checkLogAndRestartForwarderTask();
            manager.ensureFlowControlRefreshScheduled();

            assertThat(forwarderExecutor.launched.get()).isGreaterThanOrEqualTo(3);
            assertThat(forwarderExecutor.attempts.get())
                    .as("nothing launched after shutdown")
                    .isEqualTo(attemptsAtRest);
            assertThat(forwarderExecutor.handedOut).allSatisfy(future -> assertThat(future.isDone())
                    .as("every forwarder ever launched has ended")
                    .isTrue());
        }
    }

    @Nested
    @DisplayName("Queue forwarder recovery")
    class ForwarderRecoveryTests {

        @Test
        @DisplayName("a forwarder that completed normally is replaced on the next check")
        void completedForwarderIsReplaced() {
            forwarderExecutor.script(CompletableFuture.completedFuture(5L));

            manager.checkLogAndRestartForwarderTask();
            manager.checkLogAndRestartForwarderTask();

            assertThat(forwarderExecutor.launched.get()).isEqualTo(2);
            assertThat(managerMetrics.blockBatchesMessaged().get()).isEqualTo(5L);
        }

        @Test
        @DisplayName("a forwarder that failed is replaced and the failure does not reach the caller")
        void failedForwarderIsReplaced() {
            forwarderExecutor.script(CompletableFuture.failedFuture(new IllegalStateException("boom")));

            manager.checkLogAndRestartForwarderTask();

            assertThatNoException().isThrownBy(() -> manager.checkLogAndRestartForwarderTask());
            assertThat(forwarderExecutor.launched.get()).isEqualTo(2);
            assertThat(managerMetrics.blockBatchesMessaged().get()).isZero();
        }

        @Test
        @DisplayName("a cancelled forwarder is replaced")
        void cancelledForwarderIsReplaced() {
            final CompletableFuture<Long> cancelled = new CompletableFuture<>();
            cancelled.cancel(true);
            forwarderExecutor.script(cancelled);

            manager.checkLogAndRestartForwarderTask();
            manager.checkLogAndRestartForwarderTask();

            assertThat(forwarderExecutor.launched.get()).isEqualTo(2);
        }

        @Test
        @DisplayName("a forwarder that is still running is not replaced")
        void runningForwarderIsNotReplaced() {
            manager.checkLogAndRestartForwarderTask();
            manager.checkLogAndRestartForwarderTask();
            manager.checkLogAndRestartForwarderTask();

            assertThat(forwarderExecutor.attempts.get()).isEqualTo(1);
        }

        @Test
        @DisplayName("a failed launch is retried on the next check")
        void failedLaunchIsRetried() {
            forwarderExecutor.rejecting = true;

            assertThatNoException().isThrownBy(() -> manager.checkLogAndRestartForwarderTask());
            assertThat(forwarderExecutor.launched.get()).isZero();

            forwarderExecutor.rejecting = false;
            manager.checkLogAndRestartForwarderTask();
            manager.checkLogAndRestartForwarderTask();

            assertThat(forwarderExecutor.launched.get()).isEqualTo(1);
            assertThat(forwarderExecutor.attempts.get()).isEqualTo(2);
        }

        @Test
        @DisplayName("nothing is launched after the manager is shut down")
        void noRelaunchAfterShutdown() {
            manager.checkLogAndRestartForwarderTask();
            assertThat(forwarderExecutor.attempts.get()).isEqualTo(1);

            manager.shutdown();
            manager.checkLogAndRestartForwarderTask();

            assertThat(forwarderExecutor.attempts.get()).isEqualTo(1);
        }

        @Test
        @DisplayName("callers racing on a finished forwarder launch exactly one replacement")
        void racingChecksLaunchOneReplacement() throws Exception {
            forwarderExecutor.script(CompletableFuture.completedFuture(1L));
            manager.checkLogAndRestartForwarderTask();
            assertThat(forwarderExecutor.launched.get()).isEqualTo(1);

            runConcurrently(16, manager::checkLogAndRestartForwarderTask);

            assertThat(forwarderExecutor.launched.get()).isEqualTo(2);
        }
    }

    @Nested
    @DisplayName("Flow control refresh recovery")
    class FlowControlRecoveryTests {

        @Test
        @DisplayName("a refresh that throws does not stop the following runs")
        void failedRefreshDoesNotStopTheSchedule() {
            final PublisherHandler handler =
                    manager.addHandler(new TestResponsePipeline<PublishStreamResponse>(), handlerMetrics, null);
            final Runnable realRefresh = manager.flowControlRefreshAction;
            final AtomicInteger runs = new AtomicInteger();
            manager.flowControlRefreshAction = () -> {
                if (runs.getAndIncrement() == 0) {
                    throw new IllegalStateException("boom");
                }
                realRefresh.run();
            };
            handler.setMessageBudget(handler.getMessageBudget(), 5L);

            assertThatNoException()
                    .isThrownBy(() -> threadPoolManager.scheduledExecutor().executeSerially());
            assertThat(handler.getMessageBudget()).isEqualTo(5L);
            assertThat(manager.flowControlRefreshFuture.isDone()).isFalse();

            threadPoolManager.scheduledExecutor().executeSerially();

            assertThat(runs.get()).isEqualTo(2);
            assertThat(handler.getMessageBudget()).isEqualTo(10L);
        }

        @Test
        @DisplayName("a running refresh task is left alone by the activity check")
        void runningRefreshIsNotRescheduled() {
            final var before = manager.flowControlRefreshFuture;
            final long tasksBefore = threadPoolManager.scheduledExecutor().getTaskCount();

            manager.ensureFlowControlRefreshScheduled();

            assertThat((Object) manager.flowControlRefreshFuture).isSameAs(before);
            assertThat(threadPoolManager.scheduledExecutor().getTaskCount()).isEqualTo(tasksBefore);
        }

        @Test
        @DisplayName("a finished refresh task is rescheduled by the activity check")
        void finishedRefreshIsRescheduled() {
            final var before = manager.flowControlRefreshFuture;
            before.cancel(false);
            final long tasksBefore = threadPoolManager.scheduledExecutor().getTaskCount();

            manager.ensureFlowControlRefreshScheduled();

            assertThat((Object) manager.flowControlRefreshFuture).isNotSameAs(before);
            assertThat(manager.flowControlRefreshFuture.isDone()).isFalse();
            assertThat(threadPoolManager.scheduledExecutor().getTaskCount()).isEqualTo(tasksBefore + 1);
        }

        @Test
        @DisplayName("callers racing on a finished refresh task schedule exactly one replacement")
        void racingChecksRescheduleOnce() throws Exception {
            manager.flowControlRefreshFuture.cancel(false);
            final long tasksBefore = threadPoolManager.scheduledExecutor().getTaskCount();

            runConcurrently(16, manager::ensureFlowControlRefreshScheduled);

            assertThat(threadPoolManager.scheduledExecutor().getTaskCount()).isEqualTo(tasksBefore + 1);
            assertThat(manager.flowControlRefreshFuture.isDone()).isFalse();
        }

        @Test
        @DisplayName("the activity check does not reschedule after shutdown")
        void noRescheduleAfterShutdown() {
            manager.shutdown();
            final var cancelledByShutdown = manager.flowControlRefreshFuture;

            manager.ensureFlowControlRefreshScheduled();

            assertThat(cancelledByShutdown.isCancelled()).isTrue();
            assertThat((Object) manager.flowControlRefreshFuture).isSameAs(cancelledByShutdown);
        }

        @Test
        @DisplayName("adding a handler reschedules a finished refresh task")
        void addHandlerRestoresRefresh() {
            final var before = manager.flowControlRefreshFuture;
            before.cancel(false);

            manager.addHandler(new TestResponsePipeline<PublishStreamResponse>(), handlerMetrics, null);

            assertThat((Object) manager.flowControlRefreshFuture).isNotSameAs(before);
            assertThat(manager.flowControlRefreshFuture.isDone()).isFalse();
        }

        @Test
        @DisplayName("the forwarder check reschedules a finished refresh task")
        void forwarderCheckRestoresRefresh() {
            final var before = manager.flowControlRefreshFuture;
            before.cancel(false);

            manager.checkLogAndRestartForwarderTask();

            assertThat((Object) manager.flowControlRefreshFuture).isNotSameAs(before);
            assertThat(manager.flowControlRefreshFuture.isDone()).isFalse();
        }
    }
}
