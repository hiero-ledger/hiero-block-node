// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.publisher;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.LinkedBlockingQueue;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.internal.PublishStreamRequestUnparsed;
import org.hiero.block.node.app.fixtures.TestMetricsExporter;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
import org.hiero.block.node.app.fixtures.pipeline.TestResponsePipeline;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.stream.publisher.PublisherHandler.MetricsHolder;
import org.hiero.block.node.stream.publisher.StreamPublisherManager.BlockAction;
import org.hiero.block.node.stream.publisher.fixtures.TestStreamPublisherManager;
import org.hiero.metrics.core.MetricRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/// Tests for flow-control additions to [PublisherHandler].
@DisplayName("PublisherHandler Flow Control Tests")
class PublisherHandlerFlowControlTest {

    private TestMetricsExporter metricsExporter;
    private MetricsHolder metrics;
    private TestResponsePipeline<PublishStreamResponse> replies;
    private TestStreamPublisherManager manager;
    private PublisherHandler toTest;

    @BeforeEach
    void setup() {
        metricsExporter = new TestMetricsExporter();
        metrics = MetricsHolder.createMetrics(
                MetricRegistry.builder().setMetricsExporter(metricsExporter).build());
        replies = new TestResponsePipeline<>();
        manager = new TestStreamPublisherManager(
                new TestBlockMessagingFacility(), new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
        toTest = new PublisherHandler(0L, replies, metrics, manager, null);
        manager.addHandler(toTest);
        // Any onNext call that reaches the processing logic must return cleanly.
        manager.setBlockActionForBlock(BlockAction.SKIP);
    }

    @Test
    @DisplayName("initialBudgetEqualsConfiguredValue - initial budget matches perHandlerMessageBudget from config")
    void initialBudgetEqualsConfiguredValue() {
        assertThat(toTest.getMessageBudget()).isEqualTo(manager.configuration().perHandlerMessageBudget());
    }

    @Test
    @DisplayName("clearMessageBudgetSetsToExactlyZero - clearMessageBudget sets budget to exactly 0")
    void clearMessageBudgetSetsToExactlyZero() {
        toTest.clearMessageBudget();
        assertThat(toTest.getMessageBudget()).isZero();
    }

    @Test
    @DisplayName("setMessageBudgetCASSucceedsWhenExpectedMatches - CAS succeeds when expected value matches current")
    void setMessageBudgetCASSucceedsWhenExpectedMatches() {
        final long initial = toTest.getMessageBudget();
        toTest.setMessageBudget(initial, 42L);
        assertThat(toTest.getMessageBudget()).isEqualTo(42L);
    }

    @Test
    @DisplayName(
            "setMessageBudgetCASIsNoOpWhenExpectedDoesNotMatch - CAS leaves budget unchanged when expected mismatches")
    void setMessageBudgetCASIsNoOpWhenExpectedDoesNotMatch() {
        final long initial = toTest.getMessageBudget();
        toTest.setMessageBudget(initial + 1L, 42L);
        assertThat(toTest.getMessageBudget()).isEqualTo(initial);
    }

    @Test
    @DisplayName(
            "setMessageBudgetCannotOverwriteShutdownSentinelWhenExpectedDiffers - shutdown sentinel cannot be overwritten when expected is wrong")
    void setMessageBudgetCannotOverwriteShutdownSentinelWhenExpectedDiffers() {
        final long initial = toTest.getMessageBudget();
        // Force the budget to the shutdown sentinel using the initial value as expected.
        toTest.setMessageBudget(initial, Long.MIN_VALUE);
        assertThat(toTest.getMessageBudget()).isEqualTo(Long.MIN_VALUE);
        // Now try to overwrite, but use the original initial as expected (which is wrong
        // because current is Long.MIN_VALUE). CAS must fail.
        toTest.setMessageBudget(initial, 100L);
        assertThat(toTest.getMessageBudget()).isEqualTo(Long.MIN_VALUE);
    }

    @Test
    @DisplayName("onNextDecrementsBudgetByOne - each onNext call decrements message budget by one")
    void onNextDecrementsBudgetByOne() {
        final long before = toTest.getMessageBudget();
        final PublishStreamRequestUnparsed request =
                TestBlockBuilder.generateBlockWithNumber(0L).asPublishStreamRequestUnparsed();
        toTest.onNext(request);
        assertThat(toTest.getMessageBudget()).isEqualTo(before - 1L);
    }

    @Test
    @Timeout(5)
    @DisplayName(
            "onNextParksWhenBudgetIsZeroAndResumesWhenBudgetRestored - onNext parks when budget is zero and resumes when budget is set")
    void onNextParksWhenBudgetIsZeroAndResumesWhenBudgetRestored() throws InterruptedException {
        toTest.clearMessageBudget();

        final PublishStreamRequestUnparsed request =
                TestBlockBuilder.generateBlockWithNumber(0L).asPublishStreamRequestUnparsed();
        final Thread thread = Thread.ofVirtual().start(() -> toTest.onNext(request));

        // Give the virtual thread time to park.
        Thread.sleep(50L);
        assertThat(thread.isAlive()).isTrue();

        // Restore budget so the parked thread can proceed.
        toTest.setMessageBudget(0L, 1L);
        thread.join(2_000L);
        assertThat(thread.isAlive()).isFalse();
    }

    @Test
    @Timeout(5)
    @DisplayName(
            "flowControlIndividualPausesMetricIncrementedOncePerPauseEpisode - metric incremented exactly once per parking episode regardless of park cycles")
    void flowControlIndividualPausesMetricIncrementedOncePerPauseEpisode() throws InterruptedException {
        toTest.clearMessageBudget();

        final PublishStreamRequestUnparsed request =
                TestBlockBuilder.generateBlockWithNumber(0L).asPublishStreamRequestUnparsed();
        final Thread thread = Thread.ofVirtual().start(() -> toTest.onNext(request));

        // Sleep long enough for several park cycles (default refresh interval is 10 ms).
        Thread.sleep(35L);
        assertThat(thread.isAlive()).isTrue();

        toTest.setMessageBudget(0L, 1L);
        thread.join(2_000L);
        assertThat(thread.isAlive()).isFalse();

        assertThat(metricsExporter.getMetricValue(
                        StreamPublisherPlugin.METRIC_PUBLISHER_FLOW_CONTROL_INDIVIDUAL_PAUSES.name()))
                .isEqualTo(1L);
    }
}
