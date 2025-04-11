// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.publisher.positive;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
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
                throw new RuntimeException(e);
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

        final int slowerSimulatorBlocksAhead = 6;

        String lastSlowerSimulatorStatusBefore = null;
        String lastFasterSimulatorStatusBefore = null;
        String lastFasterSimulatorStatusAfter = null;

        boolean fasterSimulatorCaughtUp = false;

        long slowerSimulatorPublishedBlocksAfter = 0;
        long fasterSimulatorPublishedBlocksAfter = 0;

        // ===== Start slower simulator and make sure it's streaming ==================================
        final Future<?> slowerSimulatorThread = startSimulatorInThread(slowerSimulator);
        simulators.add(slowerSimulatorThread);
        simulatorAppsRef.add(slowerSimulator);

        while (slowerSimulator
                        .getStreamStatus()
                        .lastKnownPublisherClientStatuses()
                        .size()
                < slowerSimulatorBlocksAhead) {
            if (!slowerSimulator
                    .getStreamStatus()
                    .lastKnownPublisherClientStatuses()
                    .isEmpty()) {
                lastSlowerSimulatorStatusBefore = slowerSimulator
                        .getStreamStatus()
                        .lastKnownPublisherClientStatuses()
                        .getLast();
            }
        }

        // ===== Start faster simulator and make sure it's streaming ==================================
        final Future<?> fasterSimulatorThread = startSimulatorInThread(fasterSimulator);
        simulators.add(fasterSimulatorThread);
        simulatorAppsRef.add(fasterSimulator);
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
        assertNotNull(lastSlowerSimulatorStatusBefore);
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
    public void shouldPreferCurrentBlockPublisher() {
        // Connect two simulators, one with future blocks, one with current. Block-node should keep receiving from the
        // current
    }

    /**
     * Verifies that block streaming continues from a new publisher after the primary publisher disconnects.
     * The test asserts that the block-node successfully switches to the new publisher and resumes block streaming
     * once the new publisher catches up to the current block number.
     */
    @Test
    @DisplayName("Should resume block streaming from new publisher after primary publisher disconnects")
    public void shouldResumeFromNewPublisherAfterPrimaryDisconnects() {
        // Connect two simualtors, get couple of blocks from the first one, stop it, start the second from begining,
        // block-node should start receiving when simulator catches up
    }
}
