// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.subscriber.positive;

import static org.hiero.block.suites.utils.BlockSimulatorUtils.createBlockSimulator;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
 * Test class for verifying correct behaviour of the application, when one subscriber requests data.
 *
 * <p>Inherits from {@link BaseSuite} to reuse the container setup and teardown logic for the Block
 * Node.
 */
@DisplayName("Positive Single Subscriber Tests")
public class PositiveSingleSubscriberTests extends BaseSuite {
    private final List<Future<?>> simulators = new ArrayList<>();
    private final List<BlockStreamSimulatorApp> simulatorAppsRef = new ArrayList<>();

    /** Default constructor for the {@link PositiveSingleSubscriberTests} class. */
    public PositiveSingleSubscriberTests() {}

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
    @DisplayName("Should subscribe to stream and receive both historical and live")
    public void shouldSubscribeForHistoricalAndLiveBlocks() throws IOException, InterruptedException {
        // ===== Prepare environment =================================================================
        final Map<String, String> consumerConfiguration = Map.of(
                "blockStream.simulatorMode",
                "CONSUMER",
                "consumer.startBlockNumber",
                "0",
                "consumer.endBlockNumber",
                "-1");
        final BlockStreamSimulatorApp publisherSimulator = createBlockSimulator();
        final BlockStreamSimulatorApp consumerSimulator = createBlockSimulator(consumerConfiguration);

        simulatorAppsRef.add(publisherSimulator);
        simulatorAppsRef.add(consumerSimulator);

        // ===== Start publisher and make sure it's streaming =======================================
        final Future<?> publisherSimulatorThread = startSimulatorInstance(publisherSimulator);
        simulators.add(publisherSimulatorThread);

        // ===== Start consumer and try to request blocks ===========================================
        final Future<?> consumerSimulatorThread = startSimulatorInThread(consumerSimulator);
        simulators.add(consumerSimulatorThread);

        boolean isConsumingBlocks = false;
        // We assign lastConsumedBlock as the last published, so that we can track it later.
        // This will help us determine whether we are actually consuming blocks.
        long lastConsumedBlock = publisherSimulator.getStreamStatus().publishedBlocks();
        int retries = 3;

        while (retries > 0) {
            if (consumerSimulator.getStreamStatus().consumedBlocks() > lastConsumedBlock) {
                lastConsumedBlock = consumerSimulator.getStreamStatus().consumedBlocks();
                isConsumingBlocks = true;
            }
            retries--;
            Thread.sleep(1000);
        }

        assertTrue(isConsumingBlocks);
        assertTrue(lastConsumedBlock > 0L);
        assertTrue(publisherSimulator.isRunning());
        assertTrue(consumerSimulator.isRunning());
    }

    @Test
    @DisplayName("Should subscribe to receive historical blocks")
    public void shouldSubscribeForHistoricalBlocks() throws IOException, InterruptedException {
        // ===== Prepare environment =================================================================
        final long endBlock = 10L;
        final Map<String, String> consumerConfiguration = Map.of(
                "blockStream.simulatorMode",
                "CONSUMER",
                "consumer.startBlockNumber",
                "1",
                "consumer.endBlockNumber",
                "10");
        final BlockStreamSimulatorApp publisherSimulator = createBlockSimulator();
        final BlockStreamSimulatorApp consumerSimulator = createBlockSimulator(consumerConfiguration);

        simulatorAppsRef.add(publisherSimulator);
        simulatorAppsRef.add(consumerSimulator);

        // ===== Start publisher and make sure it's streaming =======================================
        final Future<?> publisherSimulatorThread = startSimulatorInstance(publisherSimulator);
        simulators.add(publisherSimulatorThread);
        boolean publisherReachedEndBlock = false;
        while (!publisherReachedEndBlock) {
            if (publisherSimulator.getStreamStatus().publishedBlocks() > endBlock) {
                publisherReachedEndBlock = true;
            }
        }
        // ===== Start consumer and try to request blocks ===========================================
        final Future<?> consumerSimulatorThread = startSimulatorInThread(consumerSimulator);
        simulators.add(consumerSimulatorThread);

        boolean isConsumingBlocks = false;
        int retries = 3;

        while (retries > 0) {
            if (consumerSimulator.getStreamStatus().consumedBlocks() > 0
                    && !consumerSimulator
                            .getStreamStatus()
                            .lastKnownConsumersStatuses()
                            .isEmpty()) {
                isConsumingBlocks = true;
            }
            retries--;
            Thread.sleep(1000);
        }

        assertTrue(isConsumingBlocks);
        assertEquals(endBlock, consumerSimulator.getStreamStatus().consumedBlocks());
    }

    @Test
    @DisplayName("Should validate 2ms slowdown after each block in a range")
    public void shouldValidateSlowdownAfterEachBlock() throws IOException, InterruptedException {
        // ===== Prepare environment =================================================================
        final long startBlock = 1L;
        final long endBlock = 10L;
        final long expectedSlowdownMillis = 2L;
        final Map<String, String> consumerConfiguration = Map.of(
                "blockStream.simulatorMode",
                "CONSUMER",
                "consumer.startBlockNumber",
                String.valueOf(startBlock),
                "consumer.endBlockNumber",
                String.valueOf(endBlock),
                "consumer.slowDownMilliseconds",
                String.valueOf(expectedSlowdownMillis),
                "consumer.slowDown",
                "true",
                "consumer.slowDownForBlockRange",
                "1-10");
        final BlockStreamSimulatorApp publisherSimulator = createBlockSimulator();
        final BlockStreamSimulatorApp consumerSimulator = createBlockSimulator(consumerConfiguration);

        simulatorAppsRef.add(publisherSimulator);
        simulatorAppsRef.add(consumerSimulator);

        // ===== Start publisher and make sure it's streaming =======================================
        final Future<?> publisherSimulatorThread = startSimulatorInstance(publisherSimulator);
        simulators.add(publisherSimulatorThread);

        boolean publisherReachedEndBlock = false;
        while (!publisherReachedEndBlock) {
            if (publisherSimulator.getStreamStatus().publishedBlocks() > endBlock) {
                publisherReachedEndBlock = true;
            }
        }

        // ===== Start consumer and validate slowdown ===============================================
        final Future<?> consumerSimulatorThread = startSimulatorInThread(consumerSimulator);
        simulators.add(consumerSimulatorThread);

        long previousBlockTime = System.currentTimeMillis();
        boolean slowdownValidated = true;

        for (long block = startBlock; block <= endBlock; block++) {
            while (consumerSimulator.getStreamStatus().consumedBlocks() < block) {
                Thread.sleep(1); // Wait for the block to be consumed
            }
            long currentBlockTime = System.currentTimeMillis();
            long timeDifference = currentBlockTime - previousBlockTime;

            if (timeDifference < expectedSlowdownMillis) {
                slowdownValidated = false;
                break;
            }
            previousBlockTime = currentBlockTime;
        }

        assertTrue(slowdownValidated, "The expected 2ms slowdown was not validated for all blocks.");
        assertEquals(endBlock, consumerSimulator.getStreamStatus().consumedBlocks());
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
