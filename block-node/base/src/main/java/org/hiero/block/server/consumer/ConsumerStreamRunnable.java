// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.consumer;

import static java.lang.System.Logger.Level.TRACE;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;

/**
 * The ConsumerStreamRunnable class is responsible for executing a continuous loop of calls to the StreamManager
 * to drive the consumer stream of block items to a downstream client.
 */
public class ConsumerStreamRunnable implements Runnable {

    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    private final StreamManager streamManager;

    /**
     * Constructs a ConsumerStreamRunnable.
     *
     * @param streamManager the stream manager
     */
    public ConsumerStreamRunnable(@NonNull final StreamManager streamManager) {
        this.streamManager = Objects.requireNonNull(streamManager);
    }

    /**
     * Executes the consumer stream.
     */
    @Override
    public void run() {
        LOGGER.log(TRACE, "Starting ConsumerStreamRunnable. Calling the StreamManager.");

        // Continue to loop until the stream manager
        // returns false
        // noinspection StatementWithEmptyBody
        while (streamManager.execute()) {}
    }
}
