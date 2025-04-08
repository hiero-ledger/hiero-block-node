// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.config.types;

/** The MidBlockFailType enum defines the type of failure in the process of block streaming.
 * Failure will occur randomly between the header and the proof of the block. */
public enum MidBlockFailType {
    /**
     * The NONE value indicates no failure will be simulated during streaming.
     */
    NONE,
    /**
     * The ABRUPT value indicates that an abrupt disconnection will occur while streaming
     * (without closing the connection or sending an EndOfStream message).
     */
    ABRUPT,
    /**
     * The EOS value indicates that an EndOfStream message will be sent before the final item of the block.
     * Currently, onError is called, as the client is not able to send actual EOS message
     */
    EOS
}
