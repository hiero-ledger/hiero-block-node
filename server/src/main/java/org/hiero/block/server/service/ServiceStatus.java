// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.service;

import edu.umd.cs.findbugs.annotations.NonNull;
import io.helidon.webserver.WebServer;
import org.hiero.block.server.block.BlockInfo;

/**
 * The ServiceStatus interface defines the contract for checking the status of the service and
 * shutting down the web server.
 */
public interface ServiceStatus {

    /**
     * Checks if the service is running.
     *
     * @return true if the service is running, false otherwise
     */
    boolean isRunning();

    /**
     * Sets the running status of the service.
     *
     * @param className the name of the class stopping the service
     */
    void stopRunning(final String className);

    /**
     * Sets the web server instance.
     *
     * @param webServer the web server instance
     */
    void setWebServer(@NonNull final WebServer webServer);

    /**
     * Stops the service and web server. This method is called to shut down the service and the web
     * server in the event of an error or when the service needs to restart.
     *
     * @param className the name of the class stopping the service
     */
    void stopWebServer(final String className);

    /**
     * Gets the latest acked block number.
     *
     * @return the latest acked block number
     */
    BlockInfo getLatestAckedBlock();

    /**
     * Sets the latest acked block number.
     *
     * @param latestAckedBlockInfo the latest acked block number
     */
    void setLatestAckedBlock(BlockInfo latestAckedBlockInfo);

    /**
     * Gets the latest received block number, when ack is skipped it might be used instead of last acked block number.
     * Also, if persistence + verification is in progress, it might be used to check if the block is already received.
     *
     * @return the latest received block number
     */
    long getLatestReceivedBlockNumber();

    /**
     * Sets the latest received block number. should be set when a block_header is received and before the first batch is placed on the ring buffer.
     *
     * @param latestReceivedBlockNumber the latest received block number
     */
    void setLatestReceivedBlockNumber(long latestReceivedBlockNumber);
}
