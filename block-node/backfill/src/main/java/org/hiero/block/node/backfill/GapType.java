// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

/**
 * Classification of a detected gap for routing to the appropriate scheduler.
 */
public enum GapType {
    /**
     * Gap in historical blocks (older blocks that should already exist).
     * Processed by the historical scheduler with lower priority.
     */
    HISTORICAL,

    /**
     * Gap in live-tail blocks (recent blocks near the chain head).
     * Processed by the live-tail scheduler with higher priority.
     */
    LIVE_TAIL
}
