// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.hiero.block.node.spi.historicalblocks.LongRange;

/**
 * Utility to compute gaps in available block ranges.
 */
final class GapDetector {

    /**
     * Detect missing contiguous ranges between {@code startBlock} and {@code endBlock}
     * across the supplied ranges.
     */
    @NonNull
    private List<LongRange> findGaps(@NonNull Collection<LongRange> availableRanges, long startBlock, long endBlock) {
        final List<LongRange> gaps = new ArrayList<>();
        long previousEnd = startBlock - 1;

        for (LongRange range : availableRanges) {
            if (range.start() > previousEnd + 1) {
                gaps.add(new LongRange(previousEnd + 1, range.start() - 1));
            }
            previousEnd = range.end();
        }

        // Add trailing gap if there are missing blocks after the last range
        // Don't add trailing gap when endBlock is Long.MAX_VALUE (unlimited backfill)
        if (previousEnd < endBlock && endBlock != Long.MAX_VALUE) {
            gaps.add(new LongRange(previousEnd + 1, endBlock));
        }

        return gaps;
    }

    /**
     * Compute gaps and classify them as historical or live-tail using a boundary.
     * Gaps below or ending before the boundary are historical, after the boundary are live-tail,
     * and overlapping gaps are split.
     */
    @NonNull
    List<TypedGap> findTypedGaps(
            @NonNull Collection<LongRange> availableRanges, long startBlock, long liveTailBoundary, long endCap) {
        List<LongRange> baseGaps = findGaps(availableRanges, startBlock, endCap);
        List<TypedGap> typed = new ArrayList<>();
        for (LongRange gap : baseGaps) {
            if (gap.start() > endCap) {
                continue;
            }
            long cappedEnd = Math.min(gap.end(), endCap);
            if (cappedEnd < gap.start()) {
                continue;
            }
            if (cappedEnd <= liveTailBoundary) {
                typed.add(new TypedGap(new LongRange(gap.start(), cappedEnd), GapType.HISTORICAL));
            } else if (gap.start() > liveTailBoundary) {
                typed.add(new TypedGap(new LongRange(gap.start(), cappedEnd), GapType.LIVE_TAIL));
            } else {
                // split across boundary
                typed.add(new TypedGap(new LongRange(gap.start(), liveTailBoundary), GapType.HISTORICAL));
                typed.add(new TypedGap(new LongRange(liveTailBoundary + 1, cappedEnd), GapType.LIVE_TAIL));
            }
        }
        return typed;
    }
}
