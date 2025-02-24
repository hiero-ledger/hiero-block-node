// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.simulator.config.types;

/** The GenerationMode enum defines the modes of generation for the block stream. */
public enum GenerationMode {
    /** Reads Blocks from a Folder. */
    DIR,
    /** Generates Blocks from rules */
    CRAFT
}
