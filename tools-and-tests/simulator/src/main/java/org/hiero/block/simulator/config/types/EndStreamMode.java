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
     * Indicates that TOO_FAR_BEHIND will be sent.
     * This is used to simulate when the block node is too far behind with blocks.
     */
    TOO_FAR_BEHIND
}
