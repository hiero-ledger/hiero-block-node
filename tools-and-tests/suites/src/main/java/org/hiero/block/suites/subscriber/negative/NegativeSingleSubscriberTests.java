// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.subscriber.negative;

import static org.hiero.block.suites.utils.BlockSimulatorUtils.createBlockSimulator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
 * Test class for verifying correct behaviour of the application, when one subscriber makes invalid request for data.
 *
 * <p>Inherits from {@link BaseSuite} to reuse the container setup and teardown logic for the Block
 * Node.
 */
@DisplayName("Negative Single Subscriber Tests")
public class NegativeSingleSubscriberTests extends BaseSuite {
    private final List<Future<?>> simulators = new ArrayList<>();
    private final List<BlockStreamSimulatorApp> simulatorAppsRef = new ArrayList<>();

    /** Default constructor for the {@link NegativeSingleSubscriberTests} class */
    public NegativeSingleSubscriberTests() {}

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

    /**
     * Tests the behavior when a subscriber requests blocks in an invalid order (END block number is less than START block number).
     * Verifies that the system returns the appropriate error status and no blocks are consumed.
     *
     * @throws IOException if there's an I/O error during the test
     */
    @Test
    @DisplayName("Should return correct status when subscription request has invalid block order")
    public void shouldReturnCorrectStatusForInvalidBlocksOrderRequest() throws IOException {
        // ===== Prepare environment =================================================================
        final Map<String, String> consumerConfiguration = Map.of(
                "blockStream.simulatorMode",
                "CONSUMER",
                "consumer.startBlockNumber",
                "5",
                "consumer.endBlockNumber",
                "0");
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
        String consumerStatus = "";
        long consumedBlocks = -1L;
        while (consumerStatus.isEmpty()) {
            if (!consumerSimulator
                    .getStreamStatus()
                    .lastKnownConsumersStatuses()
                    .isEmpty()) {
                consumerStatus = consumerSimulator
                        .getStreamStatus()
                        .lastKnownConsumersStatuses()
                        .getLast();
                consumedBlocks = consumerSimulator.getStreamStatus().consumedBlocks();
            }
        }

        assertTrue(consumerStatus.contains("READ_STREAM_INVALID_END_BLOCK_NUMBER"));
        assertEquals(0L, consumedBlocks);
    }

    /**
     * Tests the behavior when a subscriber requests blocks with an invalid negative START block number.
     * Verifies that the system returns the appropriate error status and no blocks are consumed.
     *
     * @throws IOException if there's an I/O error during the test
     */
    @Test
    @DisplayName("Should return correct status when subscription request has invalid start block")
    public void shouldReturnCorrectStatusForIncorrectStartBlock() throws IOException {
        // ===== Prepare environment =================================================================
        final Map<String, String> consumerConfiguration = Map.of(
                "blockStream.simulatorMode",
                "CONSUMER",
                "consumer.startBlockNumber",
                "-2",
                "consumer.endBlockNumber",
                "0");
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
        String consumerStatus = "";
        long consumedBlocks = -1L;
        while (consumerStatus.isEmpty()) {
            if (!consumerSimulator
                    .getStreamStatus()
                    .lastKnownConsumersStatuses()
                    .isEmpty()) {
                consumerStatus = consumerSimulator
                        .getStreamStatus()
                        .lastKnownConsumersStatuses()
                        .getLast();
                consumedBlocks = consumerSimulator.getStreamStatus().consumedBlocks();
            }
        }

        assertTrue(consumerStatus.contains("READ_STREAM_INVALID_START_BLOCK_NUMBER"));
        assertEquals(0L, consumedBlocks);
    }

    /**
     * Tests the behavior when a subscriber requests blocks with an invalid negative END block number.
     * Verifies that the system returns the appropriate error status and no blocks are consumed.
     *
     * @throws IOException if there's an I/O error during the test
     */
    @Test
    @DisplayName("Should return correct status when subscription request has invalid end block")
    public void shouldReturnCorrectStatusForIncorrectEndBlock() throws IOException {
        // ===== Prepare environment =================================================================
        final Map<String, String> consumerConfiguration = Map.of(
                "blockStream.simulatorMode",
                "CONSUMER",
                "consumer.startBlockNumber",
                "-1",
                "consumer.endBlockNumber",
                "-2");
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
        String consumerStatus = "";
        long consumedBlocks = -1L;
        while (consumerStatus.isEmpty()) {
            if (!consumerSimulator
                    .getStreamStatus()
                    .lastKnownConsumersStatuses()
                    .isEmpty()) {
                consumerStatus = consumerSimulator
                        .getStreamStatus()
                        .lastKnownConsumersStatuses()
                        .getLast();
                consumedBlocks = consumerSimulator.getStreamStatus().consumedBlocks();
            }
        }

        assertTrue(consumerStatus.contains("READ_STREAM_INVALID_END_BLOCK_NUMBER"));
        assertEquals(0L, consumedBlocks);
    }

    @Test
    @DisplayName("Should fail 2ms slowdown validation after each block in a range")
    public void shouldFailSlowdownValidationAfterEachBlock() throws IOException, InterruptedException {
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
                "1-3");
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
            final long currentBlockTime = System.currentTimeMillis();
            final long timeDifference = currentBlockTime - previousBlockTime;

            if (timeDifference < expectedSlowdownMillis) {
                slowdownValidated = false;
                break;
            }
            previousBlockTime = currentBlockTime;
        }

        assertFalse(slowdownValidated, "Not expected 2ms slowdown was validated for all blocks.");
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
