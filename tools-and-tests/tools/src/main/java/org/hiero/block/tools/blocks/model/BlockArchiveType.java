// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.model;

/**
 * Enum for disk archive types, for block stream files. Each block in the block stream is a separate file with
 * the extension ".blk"
 */
public enum BlockArchiveType {
    /** Directory structure of raw blocks, no combining/batching into archive files */
    INDIVIDUAL_FILES,
    /**
     * Combine N blocks into an uncompressed ZIP file. This reduces file system pressure while still supporting random
     * access.
     */
    UNCOMPRESSED_ZIP
}
