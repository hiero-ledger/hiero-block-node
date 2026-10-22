// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.publisher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

import com.swirlds.config.api.Configuration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.hiero.block.node.app.fixtures.TestUtils;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.async.TestThreadPoolManager;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.app.fixtures.plugintest.TestApplicationStateFacility;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.spi.BlockNodeContext;
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
        private final Deque<Future<Long>> scripted = new ArrayDeque<>();
        private boolean rejecting;
        private int attempts;
        private int launched;

        void script(final Future<Long> next) {
            scripted.add(next);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> Future<T> submit(final Callable<T> task) {
            attempts++;
            if (rejecting) {
                throw new RejectedExecutionException("rejected for test");
            }
            launched++;
            final Future<Long> next = scripted.isEmpty() ? new CompletableFuture<>() : scripted.poll();
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
        manager = new LiveStreamPublisherManager(context, managerMetrics, List.of());
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

            assertThat(forwarderExecutor.launched).isEqualTo(2);
            assertThat(managerMetrics.blockBatchesMessaged().get()).isEqualTo(5L);
        }

        @Test
        @DisplayName("a forwarder that failed is replaced and the failure does not reach the caller")
        void failedForwarderIsReplaced() {
            forwarderExecutor.script(CompletableFuture.failedFuture(new IllegalStateException("boom")));

            manager.checkLogAndRestartForwarderTask();

            assertThatNoException().isThrownBy(() -> manager.checkLogAndRestartForwarderTask());
            assertThat(forwarderExecutor.launched).isEqualTo(2);
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

            assertThat(forwarderExecutor.launched).isEqualTo(2);
        }

        @Test
        @DisplayName("a forwarder that is still running is not replaced")
        void runningForwarderIsNotReplaced() {
            manager.checkLogAndRestartForwarderTask();
            manager.checkLogAndRestartForwarderTask();
            manager.checkLogAndRestartForwarderTask();

            assertThat(forwarderExecutor.attempts).isEqualTo(1);
        }

        @Test
        @DisplayName("a failed launch is retried on the next check")
        void failedLaunchIsRetried() {
            forwarderExecutor.rejecting = true;

            assertThatNoException().isThrownBy(() -> manager.checkLogAndRestartForwarderTask());
            assertThat(forwarderExecutor.launched).isZero();

            forwarderExecutor.rejecting = false;
            manager.checkLogAndRestartForwarderTask();
            manager.checkLogAndRestartForwarderTask();

            assertThat(forwarderExecutor.launched).isEqualTo(1);
            assertThat(forwarderExecutor.attempts).isEqualTo(2);
        }

        @Test
        @DisplayName("nothing is launched after the manager is shut down")
        void noRelaunchAfterShutdown() {
            manager.checkLogAndRestartForwarderTask();
            assertThat(forwarderExecutor.attempts).isEqualTo(1);

            manager.shutdown();
            manager.checkLogAndRestartForwarderTask();

            assertThat(forwarderExecutor.attempts).isEqualTo(1);
        }
    }
}
