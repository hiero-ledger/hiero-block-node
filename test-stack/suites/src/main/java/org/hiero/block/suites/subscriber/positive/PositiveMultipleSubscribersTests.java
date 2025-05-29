// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.subscriber.positive;

import static org.hiero.block.suites.utils.BlockSimulatorUtils.createBlockSimulator;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Future;
import org.hiero.block.simulator.BlockStreamSimulatorApp;
import org.hiero.block.suites.BaseSuite;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Test class for verifying correct behaviour of the application, when more than one subscriber requests data.
 *
 * <p>Inherits from {@link BaseSuite} to reuse the container setup and teardown logic for the Block
 * Node.
 */
@DisplayName("Positive Multiple Subscribers Tests")
public class PositiveMultipleSubscribersTests extends BaseSuite {
    private final List<Future<?>> simulators = new ArrayList<>();
    private final List<BlockStreamSimulatorApp> simulatorAppsRef = new ArrayList<>();

    /** Default constructor for the {@link PositiveMultipleSubscribersTests} class. */
    public PositiveMultipleSubscribersTests() {}

    @AfterEach
    void teardownEnvironment() {
        simulatorAppsRef.forEach(simulator -> {
            try {
                simulator.stop();
                while (simulator.isRunning()) {
                    Thread.sleep(100);
                }
            } catch (InterruptedException e) {
                // Do nothing, this is not mandatory, we try to shut down cleaner and graceful
            }
        });
        simulators.forEach(simulator -> simulator.cancel(true));
        simulators.clear();
    }

    @Test
    @DisplayName("Should subscribe multiple  to stream and receive both historical and live")
    public void shouldSubscribeMultipleForHistoricalAndLiveBlocks() throws IOException, InterruptedException {
        // ===== Prepare environment =================================================================
        final Map<String, String> consumerConfiguration = Map.of(
                "blockStream.simulatorMode",
                "CONSUMER",
                "consumer.startBlockNumber",
                "0",
                "consumer.endBlockNumber",
                "-1");
        final BlockStreamSimulatorApp publisherSimulator = createBlockSimulator();
        final BlockStreamSimulatorApp consumerSimulator1 = createBlockSimulator(consumerConfiguration);
        final BlockStreamSimulatorApp consumerSimulator2 = createBlockSimulator(consumerConfiguration);

        simulatorAppsRef.add(publisherSimulator);
        simulatorAppsRef.add(consumerSimulator1);
        simulatorAppsRef.add(consumerSimulator2);

        // ===== Start publisher and make sure it's streaming =======================================
        final Future<?> publisherSimulatorThread = startSimulatorInstance(publisherSimulator);
        simulators.add(publisherSimulatorThread);

        // ===== Start consumers and try to request blocks ===========================================
        final Future<?> consumerSimulatorThread1 = startSimulatorInThread(consumerSimulator1);
        final Future<?> consumerSimulatorThread2 = startSimulatorInThread(consumerSimulator2);
        simulators.add(consumerSimulatorThread1);
        simulators.add(consumerSimulatorThread2);

        boolean areConsumingBlocks = false;

        long lastConsumedBlockForConsumer1 =
                publisherSimulator.getStreamStatus().publishedBlocks();
        long lastConsumedBlockForConsumer2 =
                publisherSimulator.getStreamStatus().publishedBlocks();
        int retries = 3;
        // We assign lastConsumedBlock as the last published, so that we can track it later.
        // This will help us determine whether we are actually consuming blocks.
        while (retries > 0) {
            if (consumerSimulator1.getStreamStatus().consumedBlocks() > lastConsumedBlockForConsumer1
                    && consumerSimulator2.getStreamStatus().consumedBlocks() > lastConsumedBlockForConsumer2) {
                lastConsumedBlockForConsumer1 =
                        consumerSimulator1.getStreamStatus().consumedBlocks();
                lastConsumedBlockForConsumer2 =
                        consumerSimulator2.getStreamStatus().consumedBlocks();
                areConsumingBlocks = true;
            }
            retries--;
            Thread.sleep(1000);
        }

        assertTrue(areConsumingBlocks);
        assertTrue(lastConsumedBlockForConsumer1 > 0L);
        assertTrue(lastConsumedBlockForConsumer2 > 0L);
        assertTrue(publisherSimulator.isRunning());
        assertTrue(consumerSimulator1.isRunning());
        assertTrue(consumerSimulator2.isRunning());
    }

    /**
     * Starts a simulator in a thread and make sure that it's running and trying to publish blocks
     *
     * @param simulator instance with configuration depending on the test
     * @return a {@link Future} representing the asynchronous execution of the block stream simulator
     */
    private Future<?> startSimulatorInstance(@NonNull final BlockStreamSimulatorApp simulator) {
        Objects.requireNonNull(simulator);
        final int statusesRequired = 5; // we wait for at least 5 statuses, to avoid flakiness

        final Future<?> simulatorThread = startSimulatorInThread(simulator);
        simulators.add(simulatorThread);
        String simulatorStatus = null;
        while (simulator.getStreamStatus().lastKnownPublisherClientStatuses().size() < statusesRequired) {
            if (!simulator.getStreamStatus().lastKnownPublisherClientStatuses().isEmpty()) {
                simulatorStatus = simulator
                        .getStreamStatus()
                        .lastKnownPublisherClientStatuses()
                        .getLast();
            }
        }
        assertNotNull(simulatorStatus);
        assertTrue(simulator.isRunning());
        return simulatorThread;
    }
}
