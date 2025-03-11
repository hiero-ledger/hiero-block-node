// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.persistence.positive;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.concurrent.Future;
import org.hiero.block.simulator.BlockStreamSimulatorApp;
import org.hiero.block.suites.BaseSuite;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;

/**
 * Test class for verifying the positive scenarios for data persistence.
 *
 * <p>Inherits from {@link BaseSuite} to reuse the container setup and teardown logic for the Block
 * Node.
 */
@DisplayName("Positive Data Persistence Tests")
public class PositiveDataPersistenceTests extends BaseSuite {
    // @todo(#371) - the default life/archive root path must be absolute starting from /opt
    private final String[] GET_BLOCKS_COMMAND = new String[] {"ls", "hashgraph/blocknode/data/live", "-1"};

    private Future<?> simulatorThread;

    /** Default constructor for the {@link PositiveDataPersistenceTests} class. */
    public PositiveDataPersistenceTests() {}

    @AfterEach
    void teardownEnvironment() {
        if (simulatorThread != null && !simulatorThread.isCancelled()) {
            simulatorThread.cancel(true);
        }
    }

    /**
     * Verifies that block data is saved in the correct directory by comparing the count of saved
     * blocks before and after running the simulator. The test asserts that the number of saved
     * blocks increases after the simulator runs.
     *
     * @throws IOException if an I/O error occurs during execution in the container
     * @throws InterruptedException if the thread is interrupted while sleeping or executing
     *     commands
     */
    @Test
    @Disabled("Needs simulator to be updated with blocks that pass verification @todo(502) @todo(175)")
    public void verifyBlockDataSavedInCorrectDirectory() throws InterruptedException, IOException {
        String savedBlocksFolderBefore = getContainerCommandResult(GET_BLOCKS_COMMAND);
        int savedBlocksCountBefore = getSavedBlocksCount(savedBlocksFolderBefore);

        final BlockStreamSimulatorApp blockStreamSimulatorApp = createBlockSimulator();
        simulatorThread = startSimulatorInThread(blockStreamSimulatorApp);
        Thread.sleep(5000);
        blockStreamSimulatorApp.stop();

        String savedBlocksFolderAfter = getContainerCommandResult(GET_BLOCKS_COMMAND);
        int savedBlocksCountAfter = getSavedBlocksCount(savedBlocksFolderAfter);

        assertTrue(savedBlocksFolderBefore.isEmpty());
        assertFalse(savedBlocksFolderAfter.isEmpty());
        assertTrue(savedBlocksCountAfter > savedBlocksCountBefore);
        assertTrue(blockStreamSimulatorApp.getStreamStatus().publishedBlocks() > 0);
    }

    private int getSavedBlocksCount(String blocksFolders) {
        String[] blocksArray = blocksFolders.split("\\n");
        return blocksArray.length;
    }

    private String getContainerCommandResult(String[] command) throws IOException, InterruptedException {
        Container.ExecResult result = blockNodeContainer.execInContainer(command);
        return result.getStdout();
    }
}
