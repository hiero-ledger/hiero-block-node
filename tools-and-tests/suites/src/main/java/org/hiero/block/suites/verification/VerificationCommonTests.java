// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.verification;

import static org.hiero.block.suites.utils.BlockSimulatorUtils.createBlockSimulator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Future;
import org.hiero.block.simulator.BlockStreamSimulatorApp;
import org.hiero.block.suites.BaseSuite;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Verification Common Tests")
public class VerificationCommonTests extends BaseSuite {

    // Simulator instance to be used for testing
    private BlockStreamSimulatorApp blockStreamSimulatorApp;

    // Thread to run the simulator
    private Future<?> simulatorThread;

    @AfterEach
    void teardownEnvironment() {
        if (simulatorThread != null && !simulatorThread.isCancelled()) {
            simulatorThread.cancel(true);
        }
    }

    @BeforeEach
    void publishSomeBlocks() throws IOException, InterruptedException {
        // Use the simulator to publish some blocks
        blockStreamSimulatorApp = createBlockSimulator();
        simulatorThread = startSimulatorInThread(blockStreamSimulatorApp);
        Thread.sleep(5000);
        blockStreamSimulatorApp.stop();
    }

    @Test
    @DisplayName("Verify that blocks were acknowledged - Happy Path for Verification is working.")
    void verifyBlocksAcknowledged() {
        final String consoleLog = blockStreamSimulatorApp
                .getStreamStatus()
                .lastKnownPublisherClientStatuses()
                .getFirst();
        // if simulator sends deterministic blocks we would know beforehand the Hash.
        // however getting a BlockAck with the block hash is a good enough for now
        assertEquals(true, consoleLog.contains("acknowledgement"), "acknowledgement not found in console log");
    }

    @Test
    @DisplayName(
            "Send some corrupted/malformed block hash - Verify response is a VerificationFailure with resend block.")
    void verifyMalformedBlockResponse() throws IOException, InterruptedException {
        // since it starts with 0, no need to add +1
        final long expectedNextBlockNumber =
                blockStreamSimulatorApp.getStreamStatus().publishedBlocks();

        // Simulate sending a malformed block
        var badHashSimulator = createBlockSimulator(Map.of(
                "generator.invalidBlockHash",
                "true",
                "generator.startBlockNumber",
                String.valueOf(expectedNextBlockNumber)));
        simulatorThread = startSimulatorInThread(badHashSimulator);
        Thread.sleep(2000);
        badHashSimulator.stop();

        // Check the status log for a bad_block_proof response indicating verification failure
        assertTrue(
                badHashSimulator.getStreamStatus().lastKnownPublisherClientStatuses().stream()
                        .anyMatch(status -> status.toLowerCase().contains("bad_block_proof")),
                "bad_block_proof not found in publisher statuses");
    }
}
