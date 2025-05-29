// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator;

/** The Constants class defines the constants for the block simulator. */
public final class Constants {
    /** The file extension for block files. */
    public static final String RECORD_EXTENSION = ".blk";

    /** postfix for gzip files */
    public static final String GZ_EXTENSION = ".gz";

    /**
     * Used for converting nanoseconds to milliseconds and vice versa
     */
    public static final int NANOS_PER_MILLI = 1_000_000;

    /** Constructor to prevent instantiation. this is only a utility class */
    private Constants() {}
}
