// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.publisher.positive;

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
import org.hiero.block.api.protoc.BlockResponse;
import org.hiero.block.api.protoc.BlockResponse.Code;
import org.hiero.block.simulator.BlockStreamSimulatorApp;
import org.hiero.block.suites.BaseSuite;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Test class for verifying correct behaviour of the application, when more than one publisher is streaming data.
 *
 * <p>Inherits from {@link BaseSuite} to reuse the container setup and teardown logic for the Block
 * Node.
 */
@DisplayName("Positive Multiple Publishers Tests")
public class PositiveMultiplePublishersTests extends BaseSuite {

    private final List<Future<?>> simulators = new ArrayList<>();
    private final List<BlockStreamSimulatorApp> simulatorAppsRef = new ArrayList<>();

    /** Default constructor for the {@link PositiveMultiplePublishersTests} class. */
    public PositiveMultiplePublishersTests() {}

    @AfterEach
    void teardownEnvironment() {
        simulatorAppsRef.forEach(simulator -> {
            try {
                simulator.stop();
                while (simulator.isRunning()) {
                    Thread.sleep(100);
                }
            } catch (InterruptedException e) {
                // Do nothing, this is not mandatory, we try to shut down  cleaner and graceful
            }
        });
        simulators.forEach(simulator -> simulator.cancel(true));
        simulators.clear();
    }

    /**
     * Verifies that block data is taken from the faster publisher, when two publishers are streaming to the block-node.
     * The test asserts that the slower one receives skip block response.
     */
    @Test
    @DisplayName("Should switch to faster publisher when it catches up with current block number")
    @Timeout(30)
    public void shouldSwitchToFasterPublisherWhenCaughtUp() throws IOException, InterruptedException {
        // ===== Prepare environment =================================================================
        final Map<String, String> slowerSimulatorConfiguration = Map.of("blockStream.millisecondsPerBlock", "1000");
        final Map<String, String> fasterSimulatorConfiguration = Map.of("blockStream.millisecondsPerBlock", "100");

        final BlockStreamSimulatorApp slowerSimulator = createBlockSimulator(slowerSimulatorConfiguration);
        final BlockStreamSimulatorApp fasterSimulator = createBlockSimulator(fasterSimulatorConfiguration);
        simulatorAppsRef.add(slowerSimulator);
        simulatorAppsRef.add(fasterSimulator);

        String lastFasterSimulatorStatusBefore = null;
        String lastFasterSimulatorStatusAfter = null;

        boolean fasterSimulatorCaughtUp = false;

        long slowerSimulatorPublishedBlocksAfter = 0;
        long fasterSimulatorPublishedBlocksAfter = 0;

        // ===== Start slower simulator and make sure it's streaming ==================================
        final Future<?> slowerSimulatorThread = startSimulatorInstance(slowerSimulator);
        simulators.add(slowerSimulatorThread);
        // ===== Start faster simulator and make sure it's streaming ==================================
        final Future<?> fasterSimulatorThread = startSimulatorInThread(fasterSimulator);
        simulators.add(fasterSimulatorThread);
        while (lastFasterSimulatorStatusBefore == null) {
            if (!fasterSimulator
                    .getStreamStatus()
                    .lastKnownPublisherClientStatuses()
                    .isEmpty()) {
                lastFasterSimulatorStatusBefore = fasterSimulator
                        .getStreamStatus()
                        .lastKnownPublisherClientStatuses()
                        .getLast();
            }
        }
        assertTrue(lastFasterSimulatorStatusBefore.contains("block_already_exists"));

        // ===== Assert whether catching up to the slower will result in correct statutes =============

        long slowerSimulatorPublishedBlocksBefore =
                slowerSimulator.getStreamStatus().publishedBlocks();
        long fasterSimulatorPublishedBlocksBefore =
                fasterSimulator.getStreamStatus().publishedBlocks();

        while (!fasterSimulatorCaughtUp) {
            if (fasterSimulator.getStreamStatus().publishedBlocks()
                    > slowerSimulator.getStreamStatus().publishedBlocks()) {
                fasterSimulatorCaughtUp = true;
                lastFasterSimulatorStatusAfter = fasterSimulator
                        .getStreamStatus()
                        .lastKnownPublisherClientStatuses()
                        .getLast();
                slowerSimulatorPublishedBlocksAfter =
                        slowerSimulator.getStreamStatus().publishedBlocks();
                fasterSimulatorPublishedBlocksAfter =
                        fasterSimulator.getStreamStatus().publishedBlocks();
            }
            Thread.sleep(100); // not necessary, just to avoid not needed iterations
        }

        assertTrue(slowerSimulatorPublishedBlocksBefore > fasterSimulatorPublishedBlocksBefore);
        assertTrue(fasterSimulatorPublishedBlocksAfter > slowerSimulatorPublishedBlocksAfter);
        assertFalse(lastFasterSimulatorStatusAfter.contains("block_already_exists"));
    }

    /**
     * Verifies that the block-node correctly prioritizes publishers that are streaming current blocks
     * over publishers that are streaming future blocks. This test asserts that the block-node maintains
     * a consistent and chronological order of block processing, even when multiple publishers with
     * different block timings are connected simultaneously.
     */
    @Test
    @DisplayName("Should prefer publisher with current blocks over future blocks")
    @Timeout(30)
    public void shouldPreferCurrentBlockPublisher() throws IOException {
        // ===== Prepare environment =================================================================
        final Map<String, String> currentSimulatorConfiguration = Map.of("generator.startBlockNumber", "0");
        final Map<String, String> futureSimulatorConfiguration = Map.of("generator.startBlockNumber", "1000");

        final BlockStreamSimulatorApp currentSimulator = createBlockSimulator(currentSimulatorConfiguration);
        final BlockStreamSimulatorApp futureSimulator = createBlockSimulator(futureSimulatorConfiguration);
        simulatorAppsRef.add(currentSimulator);
        simulatorAppsRef.add(futureSimulator);

        // ===== Start current simulator and make sure it's streaming ==================================
        final Future<?> currentSimulatorThread = startSimulatorInstance(currentSimulator);
        simulators.add(currentSimulatorThread);

        // ===== Start future simulator and make sure it's streaming ===================================
        final Future<?> futureSimulatorThread = startSimulatorInstance(futureSimulator);
        simulators.add(futureSimulatorThread);

        // ===== Assert that we are persisting only the current blocks =================================
        final BlockResponse currentBlockResponse = getLatestBlock(false);
        final BlockResponse futureBlockResponse = getBlock(1000, false);

        assertNotNull(currentBlockResponse);
        assertNotNull(futureBlockResponse);
        assertEquals(Code.READ_BLOCK_SUCCESS, currentBlockResponse.getStatus());
        assertEquals(Code.READ_BLOCK_NOT_AVAILABLE, futureBlockResponse.getStatus());
        assertTrue(currentBlockResponse.getBlock().getItemsList().getFirst().hasBlockHeader());
    }

    /**
     * Verifies that block streaming continues from a new publisher after the primary publisher disconnects.
     * The test asserts that the block-node successfully switches to the new publisher and resumes block streaming
     * once the new publisher catches up to the current block number.
     */
    @Test
    @DisplayName("Should resume block streaming from new publisher after primary publisher disconnects")
    @Timeout(30)
    public void shouldResumeFromNewPublisherAfterPrimaryDisconnects() throws IOException, InterruptedException {
        // ===== Prepare and Start first simulator and make sure it's streaming ======================
        final Map<String, String> firstSimulatorConfiguration = Map.of("generator.startBlockNumber", "0");
        final BlockStreamSimulatorApp firstSimulator = createBlockSimulator(firstSimulatorConfiguration);
        final Future<?> firstSimulatorThread = startSimulatorInstance(firstSimulator);
        // ===== Stop simulator and assert ===========================================================]
        firstSimulator.stop();
        final String firstSimulatorLatestStatus = firstSimulator
                .getStreamStatus()
                .lastKnownPublisherClientStatuses()
                .getLast();
        final long firstSimulatorLatestPublishedBlockNumber =
                firstSimulator.getStreamStatus().publishedBlocks() - 1; // we subtract one since we started on 0
        firstSimulatorThread.cancel(true);

        final BlockResponse latestPublishedBlockBefore = getBlock(firstSimulatorLatestPublishedBlockNumber, false);
        final BlockResponse nextPublishedBlockBefore = getBlock(firstSimulatorLatestPublishedBlockNumber + 1, false);

        assertNotNull(firstSimulatorLatestStatus);
        assertTrue(firstSimulatorLatestStatus.contains(Long.toString(firstSimulatorLatestPublishedBlockNumber)));
        assertEquals(
                firstSimulatorLatestPublishedBlockNumber,
                latestPublishedBlockBefore
                        .getBlock()
                        .getItemsList()
                        .getFirst()
                        .getBlockHeader()
                        .getNumber());
        assertEquals(Code.READ_BLOCK_NOT_AVAILABLE, nextPublishedBlockBefore.getStatus());

        // ===== Prepare and Start second simulator and make sure it's streaming =====================
        final Map<String, String> secondSimulatorConfiguration =
                Map.of("generator.startBlockNumber", Long.toString(firstSimulatorLatestPublishedBlockNumber - 1));
        final BlockStreamSimulatorApp secondSimulator = createBlockSimulator(secondSimulatorConfiguration);
        final Future<?> secondSimulatorThread = startSimulatorInstance(secondSimulator);
        // ===== Assert that we are persisting blocks from the second simulator ======================
        secondSimulator.stop();
        final String secondSimulatorLatestStatus = secondSimulator
                .getStreamStatus()
                .lastKnownPublisherClientStatuses()
                .getLast();
        secondSimulatorThread.cancel(true);

        final BlockResponse latestPublishedBlockAfter = getLatestBlock(false);

        assertNotNull(secondSimulatorLatestStatus);
        assertNotNull(latestPublishedBlockAfter);

        final long latestBlockNodeBlockNumber = latestPublishedBlockAfter
                .getBlock()
                .getItemsList()
                .getFirst()
                .getBlockHeader()
                .getNumber();
        assertTrue(secondSimulatorLatestStatus.contains(Long.toString(latestBlockNodeBlockNumber)));
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
