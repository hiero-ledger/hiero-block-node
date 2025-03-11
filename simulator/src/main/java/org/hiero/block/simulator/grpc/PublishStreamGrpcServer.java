// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.grpc;

import java.util.List;

public interface PublishStreamGrpcServer {
    /**
     * Initialize, opens a gRPC channel and creates the needed services with the passed configuration.
     */
    void init();

    /**
     * Starts the gRPC server.
     */
    void start();

    /**
     * Gets the number of processed blocks.
     *
     * @return the number of published blocks
     */
    long getProcessedBlocks();

    /**
     * Gets the last known statuses.
     *
     * @return the last known statuses
     */
    List<String> getLastKnownStatuses();

    /**
     * Shutdowns the server.
     *
     * @throws InterruptedException if the thread is interrupted
     */
    void shutdown() throws InterruptedException;
}
