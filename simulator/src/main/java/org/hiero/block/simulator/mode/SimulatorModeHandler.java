// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.mode;

import java.io.IOException;
import org.hiero.block.simulator.exception.BlockSimulatorParsingException;

/**
 * The {@code SimulatorModeHandler} interface defines the contract for implementing different
 * working modes of the Block Stream Simulator. Implementations of this interface handle
 * specific behaviors for starting the simulator and managing the streaming process,
 * depending on the selected mode.
 *
 * <p>Examples of working modes include:
 * <ul>
 *   <li>Consumer mode: The simulator consumes data from the block stream.</li>
 *   <li>Publisher Client mode: The simulator publishes data to the block stream.</li>
 *   <li>Publisher Server mode: The simulator receives blocks from client and sends back acknowledgments or errors.</li>
 * </ul>
 */
public interface SimulatorModeHandler {

    /**
     * Initializes the handler by setting up required resources and connections.
     * This method should be called before {@link #start()}.
     */
    void init();

    /**
     * Starts the simulator and initiates the streaming process according to the configured mode.
     * The behavior of this method depends on the specific working mode (consumer, publisher in client mode, or publisher in server mode).
     *
     * @throws BlockSimulatorParsingException if an error occurs while parsing blocks
     * @throws IOException if an I/O error occurs during block streaming
     * @throws InterruptedException if the thread running the simulator is interrupted
     */
    void start() throws BlockSimulatorParsingException, IOException, InterruptedException;

    /**
     * Gracefully stops the handler, cleaning up resources and terminating any active streams.
     *
     * @throws InterruptedException if the shutdown process is interrupted
     */
    void stop() throws InterruptedException;
}
