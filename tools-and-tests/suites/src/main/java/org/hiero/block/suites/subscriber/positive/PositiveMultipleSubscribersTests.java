// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.subscriber.positive;

import static org.hiero.block.suites.utils.BlockSimulatorUtils.createBlockSimulator;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
    private static final Duration WAIT_TIMEOUT = Duration.ofSeconds(30);

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
        final Future<?> publisherSimulatorThread =
                startPublisherAndAwaitFirstResponse(publisherSimulator, WAIT_TIMEOUT);
        simulators.add(publisherSimulatorThread);

        // ===== Start consumers and try to request blocks ===========================================
        final Future<?> consumerSimulatorThread1 = startSimulatorInThread(consumerSimulator1);
        final Future<?> consumerSimulatorThread2 = startSimulatorInThread(consumerSimulator2);
        simulators.add(consumerSimulatorThread1);
        simulators.add(consumerSimulatorThread2);

        boolean areConsumingBlocks = false;

        long lastConsumedBlockForConsumer1 = 0;
        long lastConsumedBlockForConsumer2 = 0;
        int retries = 10;
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
}
