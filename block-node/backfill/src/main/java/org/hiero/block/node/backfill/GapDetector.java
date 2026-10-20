// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.hiero.block.api.BlockRange;
import org.hiero.block.node.spi.historicalblocks.BlockRanges;

/**
 * Utility to compute gaps in available block ranges.
 */
final class GapDetector {

    /**
     * Classification of a detected gap for routing to the appropriate scheduler.
     */
    public enum Type {
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

    /**
     * A detected gap with its classification for scheduling decisions.
     *
     * @param range the block range representing the gap
     * @param type the classification of this gap (historical or live-tail)
     */
    public record Gap(@NonNull BlockRange range, @NonNull Type type) {}

    /**
     * Detect missing contiguous ranges between {@code startBlock} and {@code endBlock}
     * across the supplied ranges.
     */
    @NonNull
    private List<BlockRange> findGaps(@NonNull Collection<BlockRange> availableRanges, long startBlock, long endBlock) {
        final List<BlockRange> gaps = new ArrayList<>();
        long previousEnd = startBlock - 1;

        for (BlockRange range : availableRanges) {
            if (range.rangeStart() > previousEnd + 1) {
                gaps.add(BlockRanges.of(previousEnd + 1, range.rangeStart() - 1));
            }
            previousEnd = range.rangeEnd();
        }

        // Add trailing gap if there are missing blocks after the last range
        // Don't add trailing gap when endBlock is Long.MAX_VALUE (unlimited backfill)
        if (previousEnd < endBlock && endBlock != Long.MAX_VALUE) {
            gaps.add(BlockRanges.of(previousEnd + 1, endBlock));
        }

        return gaps;
    }

    /**
     * Compute gaps and classify them as historical or live-tail using a boundary.
     * Gaps below or ending before the boundary are historical, after the boundary are live-tail,
     * and overlapping gaps are split.
     */
    @NonNull
    List<Gap> findTypedGaps(
            @NonNull Collection<BlockRange> availableRanges, long startBlock, long liveTailBoundary, long endCap) {
        List<BlockRange> baseGaps = findGaps(availableRanges, startBlock, endCap);
        List<Gap> typed = new ArrayList<>();
        for (BlockRange gap : baseGaps) {
            if (gap.rangeStart() > endCap) {
                continue;
            }
            long cappedEnd = Math.min(gap.rangeEnd(), endCap);
            if (cappedEnd < gap.rangeStart()) {
                continue;
            }
            if (cappedEnd <= liveTailBoundary) {
                typed.add(new Gap(BlockRanges.of(gap.rangeStart(), cappedEnd), Type.HISTORICAL));
            } else if (gap.rangeStart() > liveTailBoundary) {
                typed.add(new Gap(BlockRanges.of(gap.rangeStart(), cappedEnd), Type.LIVE_TAIL));
            } else {
                // split across boundary
                typed.add(new Gap(BlockRanges.of(gap.rangeStart(), liveTailBoundary), Type.HISTORICAL));
                typed.add(new Gap(BlockRanges.of(liveTailBoundary + 1, cappedEnd), Type.LIVE_TAIL));
            }
        }
        return typed;
    }
}
