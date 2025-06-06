// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.grpc;

import java.util.List;

/**
 * Interface defining the contract for a gRPC client that consumes blocks from a stream.
 * This interface provides methods for initializing, managing, and monitoring block consumption.
 */
public interface ConsumerStreamGrpcClient {

    /**
     * Initializes the gRPC channel and creates the necessary stubs based on configuration.
     * This method must be called before any streaming operations can begin.
     */
    void init();

    /**
     * Requests a stream of blocks from the server within the specified range.
     *
     * @param startBlock The block number to start streaming from (inclusive)
     * @param endBlock The block number to end streaming at (inclusive). Use 0 for infinite streaming
     * @throws InterruptedException if the streaming process is interrupted
     */
    void requestBlocks(long startBlock, long endBlock) throws InterruptedException;

    /**
     * Requests a stream of blocks from the server. This method should be used when the range should be specified via
     * configuration.
     *
     * @throws InterruptedException if the streaming process is interrupted
     */
    void requestBlocks() throws InterruptedException;

    /**
     * Shutdown the channel and signals completion of the streaming process to the server.
     * This method should be called to gracefully terminate the stream.
     *
     * @throws InterruptedException if the completion process is interrupted
     */
    void completeStreaming() throws InterruptedException;

    /**
     * Retrieves the total number of blocks that have been consumed from the stream.
     *
     * @return the count of consumed blocks
     */
    long getConsumedBlocks();

    /**
     * Retrieves the most recent status messages received from the server.
     *
     * @return a list of status messages in chronological order
     */
    List<String> getLastKnownStatuses();
}
