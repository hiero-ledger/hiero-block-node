// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hiero.block.node.backfill.client.BackfillSourceConfig;
import org.hiero.block.node.spi.historicalblocks.LongRange;

/**
 * Strategy interface for selecting the optimal node for fetching a block range.
 * Implementations can consider priority, health, latency, and availability.
 */
public interface NodeSelectionStrategy {

    /**
     * Selects the best node and starting block for fetching.
     *
     * @param startBlock the first block number to fetch
     * @param gapEnd inclusive end of the gap
     * @param availability map of node config to available ranges
     * @return Optional containing the selection, or empty if no suitable node
     */
    Optional<NodeSelection> select(
            long startBlock, long gapEnd, @NonNull Map<BackfillSourceConfig, List<LongRange>> availability);

    /**
     * Result of node selection containing the chosen node and start block.
     */
    record NodeSelection(BackfillSourceConfig nodeConfig, long startBlock) {}
}
