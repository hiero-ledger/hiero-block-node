// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.grpc.positive;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.concurrent.Future;
import org.hiero.block.simulator.BlockStreamSimulatorApp;
import org.hiero.block.simulator.config.data.StreamStatus;
import org.hiero.block.suites.BaseSuite;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Test class for verifying the positive scenarios for server availability, specifically related to
 * the gRPC server. This class contains tests to check that the gRPC server and the exposed endpoints are working as expected
 * and returning correct responses on valid requests.
 *
 * <p>Inherits from {@link BaseSuite} to reuse the container setup and teardown logic for the Block
 * Node.
 */
@DisplayName("Positive Endpoint Behaviour Tests")
public class PositiveEndpointBehaviourTests extends BaseSuite {

    private BlockStreamSimulatorApp blockStreamSimulatorApp;

    private Future<?> simulatorThread;

    @AfterEach
    void teardownEnvironment() {
        if (simulatorThread != null && !simulatorThread.isCancelled()) {
            simulatorThread.cancel(true);
        }
    }
    /** Default constructor for the {@link PositiveEndpointBehaviourTests} class. */
    public PositiveEndpointBehaviourTests() {}

    /**
     * Tests the {@code PublishBlockStream} gRPC endpoint by starting the Block Stream Simulator,
     * allowing it to publish blocks, and validating the following:
     *
     * <ul>
     *   <li>The number of published blocks is greater than zero.
     *   <li>The number of published blocks matches the size of the last known publisher statuses.
     *   <li>Each publisher status contains the word "acknowledgement" to confirm successful
     *       responses.
     * </ul>
     *
     * @throws IOException if there is an error starting or stopping the Block Stream Simulator.
     * @throws InterruptedException if the simulator thread is interrupted during execution.
     */
    @Test
    @DisplayName("Should successfully publish blocks through gRPC endpoint")
    void verifyPublishBlockStreamEndpoint() throws IOException, InterruptedException {
        blockStreamSimulatorApp = createBlockSimulator();
        simulatorThread = startSimulatorInThread(blockStreamSimulatorApp);
        Thread.sleep(5000);
        blockStreamSimulatorApp.stop();
        StreamStatus streamStatus = blockStreamSimulatorApp.getStreamStatus();
        assertTrue(streamStatus.publishedBlocks() > 0);
        // We just need to make sure that number of published blocks is equal or greater than the statuses. Statuses are
        // tracked in a queue to avoid unnecessary memory usage, therefore will always be less or equal to published.
        assertTrue(streamStatus.publishedBlocks()
                >= streamStatus.lastKnownPublisherClientStatuses().size());

        // Verify each status contains the word "acknowledgement"
        streamStatus
                .lastKnownPublisherClientStatuses()
                .forEach(status -> assertTrue(status.toLowerCase().contains("acknowledgement")));
    }
}
