// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator;

import org.hiero.block.common.hasher.HashAlgorithm;

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

    /**
     * The algorithm the crafted blocks are hashed with: the leaf hashes, the subtree folds, the block root tree
     * and the all previous block hashes tree. It must match the algorithm of the Block Node under test
     * ({@code BlockHasher.HASH_ALGORITHM} in the block-verification module).
     */
    public static final HashAlgorithm BLOCK_HASH_ALGORITHM = HashAlgorithm.SHA2_256;

    /** Constructor to prevent instantiation. this is only a utility class */
    private Constants() {}
}
