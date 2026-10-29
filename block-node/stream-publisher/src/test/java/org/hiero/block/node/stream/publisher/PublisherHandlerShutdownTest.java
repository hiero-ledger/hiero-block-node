// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.publisher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.api.PublishStreamResponse.EndOfStream.Code;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.pipeline.TestResponsePipeline;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.stream.publisher.fixtures.TestStreamPublisherManager;
import org.hiero.metrics.core.MetricRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Tests that [PublisherHandler] runs every step of its shutdown, in order, even when one of the steps fails.
@DisplayName("PublisherHandler shutdown Tests")
class PublisherHandlerShutdownTest {

    /// A pipeline that records when its steps run and can be told to fail them.
    private static final class ScriptedPipeline extends TestResponsePipeline<PublishStreamResponse> {
        private final List<String> events;
        private RuntimeException onCompleteFailure;
        private RuntimeException closeFailure;

        ScriptedPipeline(final List<String> events) {
            this.events = events;
        }

        @Override
        public void onComplete() {
            super.onComplete();
            events.add("onComplete");
            if (onCompleteFailure != null) {
                throw onCompleteFailure;
            }
        }

        @Override
        public void closeConnection() {
            events.add("closeConnection");
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }

    /// A manager that records handler removal and can be told to fail it.
    private static final class ScriptedManager extends TestStreamPublisherManager {
        private final List<String> events;
        private RuntimeException removeFailure;

        ScriptedManager(
                final TestBlockMessagingFacility messaging,
                final ScheduledBlockingExecutor scheduler,
                final List<String> events) {
            super(messaging, scheduler);
            this.events = events;
        }

        @Override
        public void removeHandler(final long handlerId) {
            events.add("removeHandler");
            if (removeFailure != null) {
                throw removeFailure;
            }
        }
    }

    private static final List<String> ALL_STEPS_IN_ORDER = List.of("removeHandler", "onComplete", "closeConnection");

    private List<String> events;
    private ScheduledBlockingExecutor scheduler;
    private ScriptedPipeline pipeline;
    private ScriptedManager manager;
    private PublisherHandler handler;

    @BeforeEach
    void setup() {
        events = new CopyOnWriteArrayList<>();
        scheduler = new ScheduledBlockingExecutor(new LinkedBlockingQueue<>());
        pipeline = new ScriptedPipeline(events);
        manager = new ScriptedManager(new TestBlockMessagingFacility(), scheduler, events);
        handler = new PublisherHandler(
                1L,
                pipeline,
                PublisherHandler.MetricsHolder.createMetrics(
                        MetricRegistry.builder().build()),
                manager,
                null);
        manager.addHandler(handler);
    }

    /// Ends the stream and runs the delayed shutdown task.
    private void shutDownHandler() {
        handler.endStreamWithCode(Code.SUCCESS, true, 0L);
        assertThatNoException().isThrownBy(scheduler::executeSerially);
    }

    @Test
    @DisplayName("runs removal, onComplete and closeConnection in that order when nothing fails")
    void runsAllStepsInOrder() {
        shutDownHandler();

        assertThat(events).containsExactlyElementsOf(ALL_STEPS_IN_ORDER);
    }

    @Test
    @DisplayName("still closes the connection when onComplete throws")
    void closesConnectionWhenOnCompleteThrows() {
        pipeline.onCompleteFailure = new IllegalStateException("onComplete failed");

        shutDownHandler();

        assertThat(events).containsExactlyElementsOf(ALL_STEPS_IN_ORDER);
    }

    @Test
    @DisplayName("still closes the connection when onComplete fails because the peer reset it")
    void closesConnectionWhenOnCompleteSeesAReset() {
        pipeline.onCompleteFailure = new UncheckedIOException(new SocketException("connection reset"));

        shutDownHandler();

        assertThat(events).containsExactlyElementsOf(ALL_STEPS_IN_ORDER);
    }

    @Test
    @DisplayName("does not let a failing closeConnection escape, for a reset and for other IO errors")
    void closeConnectionFailuresDoNotEscape() {
        pipeline.closeFailure = new UncheckedIOException(new SocketException("connection reset"));
        shutDownHandler();
        assertThat(events).containsExactlyElementsOf(ALL_STEPS_IN_ORDER);

        // A second handler, whose connection fails with an error that is not a reset.
        events.clear();
        pipeline.closeFailure = new UncheckedIOException(new IOException("disk gone"));
        final PublisherHandler other = new PublisherHandler(
                2L,
                pipeline,
                PublisherHandler.MetricsHolder.createMetrics(
                        MetricRegistry.builder().build()),
                manager,
                null);
        manager.addHandler(other);
        other.endStreamWithCode(Code.SUCCESS, true, 0L);
        assertThatNoException().isThrownBy(scheduler::executeSerially);

        assertThat(events).containsExactlyElementsOf(ALL_STEPS_IN_ORDER);
    }

    @Test
    @DisplayName("still completes and closes the pipeline when removing the handler throws")
    void runsPipelineStepsWhenRemovalThrows() {
        manager.removeFailure = new IllegalStateException("removal failed");

        shutDownHandler();

        assertThat(events).containsExactlyElementsOf(ALL_STEPS_IN_ORDER);
    }

    @Test
    @DisplayName("runs every step once when all of them throw")
    void runsEveryStepWhenAllThrow() {
        manager.removeFailure = new IllegalStateException("removal failed");
        pipeline.onCompleteFailure = new IllegalStateException("onComplete failed");
        pipeline.closeFailure = new IllegalStateException("closeConnection failed");

        shutDownHandler();

        assertThat(events).containsExactlyElementsOf(ALL_STEPS_IN_ORDER);
    }
}
