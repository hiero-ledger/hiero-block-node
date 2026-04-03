// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.blockmessaging;

/**
 * Enum representing the source of a block in the block messaging system.
 * This is used to differentiate the origin of the block data.
 */
public enum BlockSource {
    UNKNOWN,
    PUBLISHER,
    BACKFILL,
    HISTORY
}
