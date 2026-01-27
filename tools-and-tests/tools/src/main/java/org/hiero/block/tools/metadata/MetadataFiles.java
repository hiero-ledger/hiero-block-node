// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.metadata;

import java.nio.file.Path;

/**
 * Many of the command line tools need metadata from buckets or mirror node. The metadata files are:
 * <ul>
 *     <li>block_times.bin - binary mapping file from block times to block numbers.</li>
 *     <li>day_blocks.json - JSON metadata for each day of blocks from the mirror node</li>
 *     <li>listingsByDay - directory of binary bucket contents listing files</li>
 * </ul>
 */
public class MetadataFiles {
    /** Default path to metadata files */
    public static final Path METADATA_DIR = Path.of("metadata");
    /** Default path to block times file */
    public static final Path BLOCK_TIMES_FILE = METADATA_DIR.resolve("block_times.bin");
    /** Default path to day blocks file */
    public static final Path DAY_BLOCKS_FILE = METADATA_DIR.resolve("day_blocks.json");
    /** Default path to listingsByDay directory */
    public static final Path LISTINGS_DIR = METADATA_DIR.resolve("listingsByDay");
}
