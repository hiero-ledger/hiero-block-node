// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.subscriber.positive;

import static org.hiero.block.suites.utils.BlockSimulatorUtils.createBlockSimulator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.hiero.block.simulator.BlockStreamSimulatorApp;
import org.hiero.block.simulator.config.data.StreamStatus;
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
        final Future<?> publisherSimulatorThread =
                startPublisherAndAwaitFirstResponse(publisherSimulator, WAIT_TIMEOUT);
        simulators.add(publisherSimulatorThread);

        // ===== Start consumer and try to request blocks ===========================================
        final Future<?> consumerSimulatorThread = startSimulatorInThread(consumerSimulator);
        simulators.add(consumerSimulatorThread);

        boolean isConsumingBlocks = false;
        long lastConsumedBlock = 0;
        int retries = 10;

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
    public void shouldSubscribeForHistoricalBlocks()
            throws IOException, InterruptedException, ExecutionException, TimeoutException {
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
        final Future<?> publisherSimulatorThread =
                startPublisherAndAwaitFirstResponse(publisherSimulator, WAIT_TIMEOUT);
        simulators.add(publisherSimulatorThread);
        awaitPublishedBlocks(publisherSimulator, endBlock + 1, WAIT_TIMEOUT);
        // ===== Start consumer and wait for the requested range to complete ========================
        final Future<?> consumerSimulatorThread = startSimulatorInThread(consumerSimulator);
        simulators.add(consumerSimulatorThread);
        // The consumer returns once the block node closes the stream after the last requested block.
        consumerSimulatorThread.get(WAIT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);

        final StreamStatus consumerStatus = consumerSimulator.getStreamStatus();
        assertEquals(endBlock, consumerStatus.consumedBlocks());
        assertTrue(
                consumerStatus.lastKnownConsumersStatuses().getLast().contains("SUCCESS"),
                "Stream should end with SUCCESS: " + consumerStatus.lastKnownConsumersStatuses());
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
                "consumer.slowDownType",
                "FIXED",
                "consumer.slowDownForBlockRange",
                "1-10");
        final BlockStreamSimulatorApp publisherSimulator = createBlockSimulator();
        final BlockStreamSimulatorApp consumerSimulator = createBlockSimulator(consumerConfiguration);

        simulatorAppsRef.add(publisherSimulator);
        simulatorAppsRef.add(consumerSimulator);

        // ===== Start publisher and make sure it's streaming =======================================
        final Future<?> publisherSimulatorThread =
                startPublisherAndAwaitFirstResponse(publisherSimulator, WAIT_TIMEOUT);
        simulators.add(publisherSimulatorThread);

        awaitPublishedBlocks(publisherSimulator, endBlock + 1, WAIT_TIMEOUT);

        // ===== Start consumer and validate slowdown ===============================================
        final Future<?> consumerSimulatorThread = startSimulatorInThread(consumerSimulator);
        simulators.add(consumerSimulatorThread);

        // Wait for the first block to be consumed before starting the clock. Block 1 has no
        // slowdown applied (the consumer's internal counter is 0 when block 1 arrives, which is
        // outside the configured range 1-10), so it arrives quickly and serves as a warm-up gate.
        while (consumerSimulator.getStreamStatus().consumedBlocks() < startBlock) {
            Thread.sleep(5);
        }
        final long startTime = System.currentTimeMillis();

        // Wait for blocks startBlock+1 through endBlock. Each of these 9 blocks triggers the 2ms
        // slowdown inside the consumer's gRPC onNext() thread, delaying the END_OF_BLOCK response
        // that increments the consumedBlocks counter.
        while (consumerSimulator.getStreamStatus().consumedBlocks() < endBlock) {
            Thread.sleep(5);
        }
        final long elapsed = System.currentTimeMillis() - startTime;

        // 9 inter-block intervals × 2ms each = 18ms minimum. Measuring total duration rather than
        // per-block intervals avoids a race where the test's poll thread observes two consecutive
        // block increments in the same wake-up cycle and records a near-zero interval for one of them.
        final long expectedMinimumMs = (endBlock - startBlock) * expectedSlowdownMillis;
        assertTrue(
                elapsed >= expectedMinimumMs,
                "Slowdown check failed: elapsed=%dms, expected>=%dms (range=%d-%d, slowdown=%dms)"
                        .formatted(elapsed, expectedMinimumMs, startBlock, endBlock, expectedSlowdownMillis));
        assertEquals(endBlock, consumerSimulator.getStreamStatus().consumedBlocks());
    }
}
