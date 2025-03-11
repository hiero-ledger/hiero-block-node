// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.config.types;

/** The StreamingMode enum defines the different modes for streaming blocks. */
public enum StreamingMode {

    /** It will wait X Nanos between each block. */
    CONSTANT_RATE,

    /** It will attempt to send a block each X Millis. */
    MILLIS_PER_BLOCK;

    /**
     * Converts a string to a StreamingMode.
     *
     * @param mode the string to convert
     * @return the StreamingMode
     */
    public static StreamingMode fromString(String mode) {
        return switch (mode) {
            case "CONSTANT_RATE" -> CONSTANT_RATE;
            case "MILLIS_PER_BLOCK" -> MILLIS_PER_BLOCK;
            default -> throw new IllegalArgumentException("Invalid mode: " + mode);
        };
    }
}
