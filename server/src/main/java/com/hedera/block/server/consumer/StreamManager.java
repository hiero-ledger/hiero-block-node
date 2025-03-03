// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.consumer;

/**
 * A StreamManager abstracts the handling of streaming data from a variety of sources
 * and with, potentially, different algorithms.
 */
public interface StreamManager {

    /**
     * This entrypoint into the StreamManager signals to send the next item in a stream.
     * That item may come from a live stream, an historic data source, etc.
     *
     * @return true if the stream manager should continue executing, false otherwise
     * @throws Exception if an error occurs
     */
    boolean execute() throws Exception;

    /**
     * This method is called when an exception is thrown during the execution of the stream manager.
     * Call it to handle cleaning up resources, logging, and sending a response code to the client.
     *
     * @param e the exception that was thrown
     */
    void handleException(Exception e);
}
