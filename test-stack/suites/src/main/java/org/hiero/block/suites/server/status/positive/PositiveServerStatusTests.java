// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.server.status.positive;

import static org.hiero.block.suites.utils.BlockSimulatorUtils.createBlockSimulator;
import static org.hiero.block.suites.utils.ServerStatusUtils.requestServerStatus;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Future;
import org.hiero.block.api.protoc.ServerStatusResponse;
import org.hiero.block.simulator.BlockStreamSimulatorApp;
import org.hiero.block.suites.BaseSuite;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class PositiveServerStatusTests extends BaseSuite {
    private final List<Future<?>> simulators = new ArrayList<>();
    private final List<BlockStreamSimulatorApp> simulatorAppsRef = new ArrayList<>();

    /** Default constructor for the {@link PositiveServerStatusTests} class. */
    public PositiveServerStatusTests() {}

    @AfterEach
    void teardownEnvironment() {
        simulatorAppsRef.forEach(simulator -> {
            try {
                simulator.stop();
                while (simulator.isRunning()) {
                    Thread.sleep(100);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        simulators.forEach(simulator -> simulator.cancel(true));
        simulators.clear();
    }

    @Test
    @DisplayName("Should be able to request and receive server status")
    public void shouldSuccessfullyRequestServerStatus() throws IOException {
        // ===== Get Server Status before starting publisher ========================================
        final ServerStatusResponse serverStatusBefore = requestServerStatus(blockServiceStub);

        assertNotNull(serverStatusBefore);
        assertEquals(-1L, serverStatusBefore.getFirstAvailableBlock());
        assertEquals(-1L, serverStatusBefore.getLastAvailableBlock());

        final BlockStreamSimulatorApp publisherSimulator = createBlockSimulator();
        simulatorAppsRef.add(publisherSimulator);

        // ===== Start publisher and make sure it's streaming =======================================
        final Future<?> publisherSimulatorThread = startSimulatorInstance(publisherSimulator);
        simulators.add(publisherSimulatorThread);

        // ===== Get Server Status after and assert =================================================
        final ServerStatusResponse serverStatusAfter = requestServerStatus(blockServiceStub);

        assertNotNull(serverStatusAfter);
        assertTrue(serverStatusAfter.getLastAvailableBlock() > serverStatusBefore.getLastAvailableBlock());
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
