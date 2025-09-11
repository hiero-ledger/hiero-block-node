// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.config.types;

/**
 * Enum representing EndStream modes.
 */
public enum EndStreamMode {
    /**
     * Indicates that EndStream will not be sent.
     */
    NONE,
    /**
     * Indicates that RESET will be sent.
     * Occasionally resetting the stream increases stability and allows for routine network configuration changes.
     */
    RESET,

    /**
     * Indicates that TIMEOUT will be sent.
     * The delay between items was too long. The destination system did not timely acknowledge a block.
     */
    TIMEOUT,

    /**
     * Indicates that ERROR will be sent.
     * The Publisher encountered an error.
     */
    ERROR,

    /**
     * Indicates that TOO_FAR_BEHIND will be sent.
     * This is used to simulate when the block node is too far behind with blocks.
     */
    TOO_FAR_BEHIND
}
