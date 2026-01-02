// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.hiero.block.node.spi.historicalblocks.LongRange;
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
    @DisplayName("should find gaps between non-contiguous ranges")
    void findsGapsBetweenRanges() {
        List<LongRange> ranges = List.of(new LongRange(5, 9), new LongRange(15, 20));

        // endCap=20 matches last range end, so no trailing gap is created
        List<GapDetector.Gap> gaps = detector.findTypedGaps(ranges, 5, 20, 20);

        assertEquals(1, gaps.size());
        assertEquals(new LongRange(10, 14), gaps.getFirst().range());
        assertEquals(GapDetector.Type.HISTORICAL, gaps.getFirst().type());
    }

    @Test
    @DisplayName("should find leading gap from start block")
    void findsGapFromStartBlock() {
        List<LongRange> ranges = List.of(new LongRange(3, 4));

        // endCap=4 matches last range end, so only leading gap is found
        List<GapDetector.Gap> gaps = detector.findTypedGaps(ranges, 0, 10, 4);

        assertEquals(1, gaps.size());
        assertEquals(new LongRange(0, 2), gaps.getFirst().range());
        assertEquals(GapDetector.Type.HISTORICAL, gaps.getFirst().type());
    }

    @Test
    @DisplayName("should split gap across live-tail boundary into historical and live-tail types")
    void splitsGapAcrossBoundary() {
        List<LongRange> ranges = List.of(new LongRange(0, 4), new LongRange(15, 20));

        // endCap=20 matches last range end; gap 5-14 is split at liveTailBoundary=10
        List<GapDetector.Gap> gaps = detector.findTypedGaps(ranges, 0, 10, 20);

        assertEquals(2, gaps.size());
        assertEquals(new GapDetector.Gap(new LongRange(5, 10), GapDetector.Type.HISTORICAL), gaps.get(0));
        assertEquals(new GapDetector.Gap(new LongRange(11, 14), GapDetector.Type.LIVE_TAIL), gaps.get(1));
    }

    @Test
    @DisplayName("should respect end cap and not extend gaps beyond it")
    void respectsEndCap() {
        List<LongRange> ranges = List.of(new LongRange(0, 4));

        List<GapDetector.Gap> gaps = detector.findTypedGaps(ranges, 0, 10, 7);

        assertEquals(1, gaps.size());
        assertEquals(new LongRange(5, 7), gaps.getFirst().range());
    }

    @Test
    @DisplayName("should return empty list when no gaps exist")
    void returnsEmptyWhenNoGaps() {
        List<LongRange> ranges = List.of(new LongRange(0, 10));

        // endCap=10 matches last range end; no gaps since range covers everything
        List<GapDetector.Gap> gaps = detector.findTypedGaps(ranges, 0, 10, 10);

        assertTrue(gaps.isEmpty());
    }
}
