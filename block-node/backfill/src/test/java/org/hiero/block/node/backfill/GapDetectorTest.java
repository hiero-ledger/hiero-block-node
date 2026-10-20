// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.hiero.block.api.BlockRange;
import org.hiero.block.node.spi.historicalblocks.BlockRanges;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Unit tests for {@link GapDetector}.
 */
@Timeout(value = 5, unit = TimeUnit.SECONDS)
class GapDetectorTest {

    private final GapDetector detector = new GapDetector();

    @Test
    @DisplayName("detects gaps and classifies by type")
    void detectsAndClassifiesGaps() {
        // Gap between ranges - HISTORICAL
        assertGaps(
                List.of(BlockRanges.of(5, 9), BlockRanges.of(15, 20)),
                5,
                20,
                20,
                List.of(gap(10, 14, GapDetector.Type.HISTORICAL)));

        // Leading gap - HISTORICAL
        assertGaps(List.of(BlockRanges.of(3, 10)), 0, 10, 10, List.of(gap(0, 2, GapDetector.Type.HISTORICAL)));

        // No gaps
        assertGaps(List.of(BlockRanges.of(0, 10)), 0, 10, 10, Collections.emptyList());

        // Trailing gap - LIVE_TAIL (after boundary)
        assertGaps(List.of(BlockRanges.of(0, 5)), 0, 3, 10, List.of(gap(6, 10, GapDetector.Type.LIVE_TAIL)));

        // Gap split at boundary
        assertGaps(
                List.of(BlockRanges.of(0, 4), BlockRanges.of(15, 20)),
                0,
                10,
                20,
                List.of(gap(5, 10, GapDetector.Type.HISTORICAL), gap(11, 14, GapDetector.Type.LIVE_TAIL)));

        // Pure LIVE_TAIL gap (start > boundary)
        assertGaps(
                List.of(BlockRanges.of(0, 5), BlockRanges.of(15, 20)),
                0,
                5,
                20,
                List.of(gap(6, 14, GapDetector.Type.LIVE_TAIL)));

        // Pure HISTORICAL gap (end <= boundary)
        assertGaps(
                List.of(BlockRanges.of(0, 2), BlockRanges.of(8, 20)),
                0,
                10,
                20,
                List.of(gap(3, 7, GapDetector.Type.HISTORICAL)));

        // Empty ranges - split at boundary
        assertGaps(
                Collections.emptyList(),
                0,
                5,
                10,
                List.of(gap(0, 5, GapDetector.Type.HISTORICAL), gap(6, 10, GapDetector.Type.LIVE_TAIL)));

        // Single block gap
        assertGaps(
                List.of(BlockRanges.of(0, 4), BlockRanges.of(6, 10)),
                0,
                10,
                10,
                List.of(gap(5, 5, GapDetector.Type.HISTORICAL)));

        // Contiguous ranges - no gap
        assertGaps(List.of(BlockRanges.of(0, 5), BlockRanges.of(6, 10)), 0, 10, 10, Collections.emptyList());
    }

    @Test
    @DisplayName("respects end cap and Long.MAX_VALUE")
    void respectsEndCap() {
        // Caps gap at endCap
        var gaps = detector.findTypedGaps(List.of(BlockRanges.of(0, 4)), 0, 10, 7);
        assertEquals(1, gaps.size());
        assertEquals(BlockRanges.of(5, 7), gaps.getFirst().range());

        // No trailing gap for Long.MAX_VALUE
        gaps = detector.findTypedGaps(List.of(BlockRanges.of(0, 10)), 0, 5, Long.MAX_VALUE);
        assertTrue(gaps.isEmpty());

        // Skips gaps where start > endCap
        gaps = detector.findTypedGaps(List.of(BlockRanges.of(0, 5), BlockRanges.of(100, 200)), 0, 10, 3);
        assertTrue(gaps.isEmpty());
    }

    private void assertGaps(
            List<BlockRange> ranges, long start, long boundary, long endCap, List<GapDetector.Gap> expected) {
        assertEquals(expected, detector.findTypedGaps(ranges, start, boundary, endCap));
    }

    private GapDetector.Gap gap(long start, long end, GapDetector.Type type) {
        return new GapDetector.Gap(BlockRanges.of(start, end), type);
    }
}
