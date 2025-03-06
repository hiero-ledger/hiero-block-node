// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.consumer;

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
     */
    boolean execute();
}
