// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.grpc;

import com.hedera.hapi.block.stream.protoc.Block;
import java.util.List;

/**
 * The PublishStreamGrpcClient interface provides the methods to stream the block and block item.
 */
public interface PublishStreamGrpcClient {
    /**
     * Initialize, opens a gRPC channel and creates the needed stubs with the passed configuration.
     */
    void init();

    /**
     * Streams the block.
     *
     * @param block the block to be streamed
     * @return true if the block is streamed successfully, false otherwise
     */
    boolean streamBlock(Block block);

    /**
     * Sends a onCompleted message to the server and waits for a short period of time to ensure the message is sent.
     *
     * @throws InterruptedException if the thread is interrupted
     */
    void completeStreaming() throws InterruptedException;

    /**
     * Gets the number of published blocks.
     *
     * @return the number of published blocks
     */
    long getPublishedBlocks();

    /**
     * Gets the last known statuses.
     *
     * @return the last known statuses
     */
    List<String> getLastKnownStatuses();

    /**
     * Shutdowns the channel.
     *
     * @throws InterruptedException if the thread is interrupted
     */
    void shutdown() throws InterruptedException;
}
