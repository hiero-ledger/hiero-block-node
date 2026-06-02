// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.publisher;

import static org.assertj.core.api.Assertions.assertThat;

import com.swirlds.config.api.Configuration;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import org.hiero.block.api.BlockNodeVersions;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.node.app.fixtures.TestMetricsExporter;
import org.hiero.block.node.app.fixtures.TestUtils;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.async.TestThreadPoolManager;
import org.hiero.block.node.app.fixtures.pipeline.TestResponsePipeline;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.stream.publisher.LiveStreamPublisherManager.MetricsHolder;
import org.hiero.block.node.stream.publisher.fixtures.TestStreamPublisherManager;
import org.hiero.metrics.core.MetricRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/// Tests for flow-control behaviour in [LiveStreamPublisherManager].
@DisplayName("LiveStreamPublisherManager Flow Control Tests")
class LiveStreamPublisherManagerFlowControlTest {

    // =========================================================================
    // Streak and penalty tests
    // =========================================================================

    /// Tests that verify handler streak tracking and penalty application.
    @Nested
    @DisplayName("Streak and Penalty Tests")
    class StreakAndPenaltyTests {

        private TestMetricsExporter metricsExporter;
        private LiveStreamPublisherManager manager;
        private TestThreadPoolManager<BlockingExecutor, ScheduledBlockingExecutor> threadPoolManager;
        private PublisherHandler handler;

        @BeforeEach
        void setup() {
            final Map<String, String> overrides = Map.of(
                    "producer.perHandlerMessageBudget", "10",
                    "producer.totalMessageBudgetPerInterval", "10000",
                    "producer.consecutiveFullBudgetIntervalsForPenalty", "10",
                    "producer.penaltyDurationSeconds", "2",
                    "producer.penaltyResetIntervalSeconds", "600");
            final Configuration config = TestStreamPublisherManager.createTestConfiguration(overrides);
            threadPoolManager = new TestThreadPoolManager<>(
                    new BlockingExecutor(new LinkedBlockingQueue<>()),
                    new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
            final SimpleInMemoryHistoricalBlockFacility historicalBlockFacility =
                    new SimpleInMemoryHistoricalBlockFacility();
            final TestBlockMessagingFacility messagingFacility = new TestBlockMessagingFacility();
            final BlockNodeContext context =
                    generateContext(config, historicalBlockFacility, threadPoolManager, messagingFacility);
            historicalBlockFacility.init(context, null);
            metricsExporter = new TestMetricsExporter();
            final MetricRegistry registry =
                    MetricRegistry.builder().setMetricsExporter(metricsExporter).build();
            final MetricsHolder managerMetrics = MetricsHolder.createMetrics(registry);
            final PublisherHandler.MetricsHolder handlerMetrics =
                    PublisherHandler.MetricsHolder.createMetrics(registry);
            manager = new LiveStreamPublisherManager(context, managerMetrics);
            handler = manager.addHandler(new TestResponsePipeline<PublishStreamResponse>(), handlerMetrics, null);
        }

        @Test
        @DisplayName(
                "penaltyAppliedAfterConsecutiveFullBudgetIntervals - penalty applied after threshold consecutive exhausted intervals")
        void penaltyAppliedAfterConsecutiveFullBudgetIntervals() {
            // Exhaust budget then run the refresh task 10 times (= threshold).
            exhaustBudgetAndRunRefresh(handler, 10);

            assertThat(handler.getMessageBudget()).isEqualTo(-1L);
            assertThat(metricsExporter.getMetricValue(
                            StreamPublisherPlugin.METRIC_PUBLISHER_FLOW_CONTROL_PENALTIES_APPLIED.name()))
                    .isEqualTo(1L);
        }

        @Test
        @DisplayName(
                "streakResetPreventsPrematurePenalty - streak reset prevents penalty when streak is broken before threshold")
        void streakResetPreventsPrematurePenalty() {
            // Run 9 exhaust+refresh cycles to build streak to 9.
            exhaustBudgetAndRunRefresh(handler, 9);

            // Run one refresh without exhausting so the streak resets to 0.
            // After each refresh the budget is restored to perHandlerMessageBudget (10).
            // Budget was last refreshed to 10, so just run the refresh without exhausting.
            threadPoolManager.scheduledExecutor().executeSerially();

            // Run 9 more exhaust+refresh cycles — streak goes to 9, which is still below threshold 10.
            exhaustBudgetAndRunRefresh(handler, 9);

            assertThat(metricsExporter.getMetricValue(
                            StreamPublisherPlugin.METRIC_PUBLISHER_FLOW_CONTROL_PENALTIES_APPLIED.name()))
                    .isZero();
        }

        @Test
        @Timeout(15)
        @DisplayName(
                "secondPenaltyMultipliesDuration - second penalty is applied after penalty expires and streak re-accumulates")
        void secondPenaltyMultipliesDuration() throws InterruptedException {
            // Trigger penalty #1 by exhausting 10 times.
            exhaustBudgetAndRunRefresh(handler, 10);
            assertThat(metricsExporter.getMetricValue(
                            StreamPublisherPlugin.METRIC_PUBLISHER_FLOW_CONTROL_PENALTIES_APPLIED.name()))
                    .isEqualTo(1L);

            // Wait for penalty #1 to expire (penaltyDurationSeconds=2 so penalty = 2*1 = 2s).
            // After expiry the next refresh will see snapshotBudget=-1 which is <=0 (fullyConsumed=true),
            // streak++, but isPenalized will now be false, so it restores the budget to perHandlerBudget.
            Thread.sleep(2_100L);

            // After expiry: run refresh to restore budget from -1 to perHandlerBudget (10).
            // The refresh sees snapshotBudget=-1, fullyConsumed=true, streak becomes 1,
            // penalty threshold not reached (1<10), setMessageBudget(-1, 10) → budget=10.
            threadPoolManager.scheduledExecutor().executeSerially();
            assertThat(handler.getMessageBudget()).isEqualTo(10L);

            // Now accumulate 9 more exhaust+refresh cycles to reach streak=10 again.
            // The first post-expiry refresh already incremented streak to 1, so 9 more = 10.
            exhaustBudgetAndRunRefresh(handler, 9);

            assertThat(metricsExporter.getMetricValue(
                            StreamPublisherPlugin.METRIC_PUBLISHER_FLOW_CONTROL_PENALTIES_APPLIED.name()))
                    .isEqualTo(2L);
        }

        /// Clears the handler budget to 0 (simulating full exhaustion), then calls
        /// executeSerially() the given number of times to trigger refreshFlowControl.
        private void exhaustBudgetAndRunRefresh(final PublisherHandler h, final int cycles) {
            for (int i = 0; i < cycles; i++) {
                h.clearMessageBudget();
                threadPoolManager.scheduledExecutor().executeSerially();
            }
        }
    }

    // =========================================================================
    // Aggregate limit tests
    // =========================================================================

    /// Tests that verify aggregate budget limit behaviour.
    @Nested
    @DisplayName("Aggregate Limit Tests")
    class AggregateLimitTests {

        private TestMetricsExporter metricsExporter;
        private LiveStreamPublisherManager manager;
        private TestThreadPoolManager<BlockingExecutor, ScheduledBlockingExecutor> threadPoolManager;
        private PublisherHandler handler1;
        private PublisherHandler handler2;

        @BeforeEach
        void setup() {
            final Map<String, String> overrides = Map.of(
                    "producer.perHandlerMessageBudget", "10",
                    "producer.totalMessageBudgetPerInterval", "10",
                    "producer.consecutiveFullBudgetIntervalsForPenalty", "10");
            final Configuration config = TestStreamPublisherManager.createTestConfiguration(overrides);
            threadPoolManager = new TestThreadPoolManager<>(
                    new BlockingExecutor(new LinkedBlockingQueue<>()),
                    new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
            final SimpleInMemoryHistoricalBlockFacility historicalBlockFacility =
                    new SimpleInMemoryHistoricalBlockFacility();
            final TestBlockMessagingFacility messagingFacility = new TestBlockMessagingFacility();
            final BlockNodeContext context =
                    generateContext(config, historicalBlockFacility, threadPoolManager, messagingFacility);
            historicalBlockFacility.init(context, null);
            metricsExporter = new TestMetricsExporter();
            final MetricRegistry registry =
                    MetricRegistry.builder().setMetricsExporter(metricsExporter).build();
            final MetricsHolder managerMetrics = MetricsHolder.createMetrics(registry);
            final PublisherHandler.MetricsHolder handlerMetrics =
                    PublisherHandler.MetricsHolder.createMetrics(registry);
            manager = new LiveStreamPublisherManager(context, managerMetrics);
            handler1 = manager.addHandler(new TestResponsePipeline<PublishStreamResponse>(), handlerMetrics, null);
            handler2 = manager.addHandler(new TestResponsePipeline<PublishStreamResponse>(), handlerMetrics, null);
        }

        @Test
        @DisplayName(
                "aggregateLimitClearsPositiveBudgetsAndIncrementsMetric - aggregate limit clears positive budgets and increments aggregate metric")
        void aggregateLimitClearsPositiveBudgetsAndIncrementsMetric() {
            // handler1 has budget fully consumed, handler2 still has full budget=10.
            // totalConsumed = (10-0) + (10-10) = 10 >= totalBudget(10) → aggregate fires.
            handler1.clearMessageBudget();

            threadPoolManager.scheduledExecutor().executeSerially();

            // handler2 had a positive budget; aggregate path clears it.
            assertThat(handler2.getMessageBudget()).isZero();
            assertThat(metricsExporter.getMetricValue(
                            StreamPublisherPlugin.METRIC_PUBLISHER_FLOW_CONTROL_AGGREGATE_PAUSES.name()))
                    .isEqualTo(1L);
        }

        @Test
        @DisplayName(
                "aggregateLimitDoesNotUpdateStreaks - aggregate path does not increment individual handler streaks")
        void aggregateLimitDoesNotUpdateStreaks() {
            // Clear handler1's budget so aggregate triggers on every refresh.
            handler1.clearMessageBudget();

            // Run the refresh many times — aggregate fires every time because
            // clearMessageBudget is called each cycle. Streaks must remain at 0.
            for (int i = 0; i < 10; i++) {
                handler1.clearMessageBudget();
                threadPoolManager.scheduledExecutor().executeSerially();
            }

            assertThat(metricsExporter.getMetricValue(
                            StreamPublisherPlugin.METRIC_PUBLISHER_FLOW_CONTROL_PENALTIES_APPLIED.name()))
                    .isZero();
        }
    }

    // =========================================================================
    // Budget refresh tests
    // =========================================================================

    /// Tests that verify per-handler budget reset behaviour.
    @Nested
    @DisplayName("Budget Refresh Tests")
    class BudgetRefreshTests {

        private TestMetricsExporter metricsExporter;
        private LiveStreamPublisherManager manager;
        private TestThreadPoolManager<BlockingExecutor, ScheduledBlockingExecutor> threadPoolManager;
        private PublisherHandler handler;

        @BeforeEach
        void setup() {
            final Map<String, String> overrides = Map.of(
                    "producer.perHandlerMessageBudget", "10",
                    "producer.totalMessageBudgetPerInterval", "1000");
            final Configuration config = TestStreamPublisherManager.createTestConfiguration(overrides);
            threadPoolManager = new TestThreadPoolManager<>(
                    new BlockingExecutor(new LinkedBlockingQueue<>()),
                    new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
            final SimpleInMemoryHistoricalBlockFacility historicalBlockFacility =
                    new SimpleInMemoryHistoricalBlockFacility();
            final TestBlockMessagingFacility messagingFacility = new TestBlockMessagingFacility();
            final BlockNodeContext context =
                    generateContext(config, historicalBlockFacility, threadPoolManager, messagingFacility);
            historicalBlockFacility.init(context, null);
            metricsExporter = new TestMetricsExporter();
            final MetricRegistry registry =
                    MetricRegistry.builder().setMetricsExporter(metricsExporter).build();
            final MetricsHolder managerMetrics = MetricsHolder.createMetrics(registry);
            final PublisherHandler.MetricsHolder handlerMetrics =
                    PublisherHandler.MetricsHolder.createMetrics(registry);
            manager = new LiveStreamPublisherManager(context, managerMetrics);
            handler = manager.addHandler(new TestResponsePipeline<PublishStreamResponse>(), handlerMetrics, null);
        }

        @Test
        @DisplayName(
                "partiallyConsumedBudgetResetToFullAfterRefresh - partially consumed budget is reset to perHandlerMessageBudget on refresh")
        void partiallyConsumedBudgetResetToFullAfterRefresh() {
            final long initial = handler.getMessageBudget(); // 10
            // Simulate partial consumption (5 remaining out of 10).
            handler.setMessageBudget(initial, 5L);
            assertThat(handler.getMessageBudget()).isEqualTo(5L);

            threadPoolManager.scheduledExecutor().executeSerially();

            assertThat(handler.getMessageBudget()).isEqualTo(10L);
        }

        @Test
        @DisplayName(
                "handlerInShutdownStateIsSkippedByRefresh - refresh task does not overwrite Long.MIN_VALUE shutdown sentinel")
        void handlerInShutdownStateIsSkippedByRefresh() {
            final long initial = handler.getMessageBudget(); // 10
            // Force the handler into the shutdown sentinel state.
            handler.setMessageBudget(initial, Long.MIN_VALUE);
            assertThat(handler.getMessageBudget()).isEqualTo(Long.MIN_VALUE);

            threadPoolManager.scheduledExecutor().executeSerially();

            // Refresh must skip this handler; sentinel must remain.
            assertThat(handler.getMessageBudget()).isEqualTo(Long.MIN_VALUE);
        }
    }

    // =========================================================================
    // Shared helper methods
    // =========================================================================

    @SuppressWarnings("all")
    private BlockNodeContext generateContext(
            final Configuration configuration,
            final SimpleInMemoryHistoricalBlockFacility historicalBlockFacility,
            final TestThreadPoolManager<BlockingExecutor, ScheduledBlockingExecutor> threadPoolManager,
            final TestBlockMessagingFacility messagingFacility) {
        final MetricRegistry metricRegistry = TestUtils.createMetrics();
        return new BlockNodeContext(
                configuration,
                metricRegistry,
                null,
                messagingFacility,
                historicalBlockFacility,
                null,
                null,
                threadPoolManager,
                BlockNodeVersions.DEFAULT,
                null,
                null);
    }
}
