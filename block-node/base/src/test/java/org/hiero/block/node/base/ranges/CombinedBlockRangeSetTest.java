// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.base.ranges;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.stream.Stream;
import org.hiero.block.node.spi.historicalblocks.BlockRangeSet;
import org.hiero.block.node.spi.historicalblocks.LongRange;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * A test class for {@link CombinedBlockRangeSet}.
 */
class CombinedBlockRangeSetTest {

    @Test
    @DisplayName("contains() returns true if any underlying set contains the block number")
    void testContainsBlock() {
        final BlockRangeSet set1 = mock(BlockRangeSet.class);
        final BlockRangeSet set2 = mock(BlockRangeSet.class);
        when(set1.contains(5L)).thenReturn(false);
        when(set2.contains(5L)).thenReturn(true);

        final CombinedBlockRangeSet combinedSet = new CombinedBlockRangeSet(set1, set2);
        assertTrue(combinedSet.contains(5L));

        verify(set1).contains(5L);
        verify(set2).contains(5L);
    }

    @Test
    @DisplayName("contains() returns false if no underlying set contains the block number")
    void testContainsBlockNotFound() {
        final BlockRangeSet set1 = mock(BlockRangeSet.class);
        final BlockRangeSet set2 = mock(BlockRangeSet.class);
        when(set1.contains(5L)).thenReturn(false);
        when(set2.contains(5L)).thenReturn(false);

        final CombinedBlockRangeSet combinedSet = new CombinedBlockRangeSet(set1, set2);
        assertFalse(combinedSet.contains(5L));

        verify(set1).contains(5L);
        verify(set2).contains(5L);
    }

    @Test
    @DisplayName("contains(start, end) returns true if any underlying set contains the range")
    void testContainsRange() {
        final BlockRangeSet set1 = mock(BlockRangeSet.class);
        final BlockRangeSet set2 = mock(BlockRangeSet.class);
        when(set1.contains(10L, 20L)).thenReturn(false);
        when(set2.contains(10L, 20L)).thenReturn(true);

        final CombinedBlockRangeSet combinedSet = new CombinedBlockRangeSet(set1, set2);
        assertTrue(combinedSet.contains(10L, 20L));
        assertFalse(combinedSet.contains(30L, 40L));

        verify(set1).contains(10L, 20L);
        verify(set2).contains(10L, 20L);
    }

    @Test
    @DisplayName("size() returns the sum of sizes of all underlying sets")
    void testSize() {
        final BlockRangeSet set1 = mock(BlockRangeSet.class);
        final BlockRangeSet set2 = mock(BlockRangeSet.class);
        when(set1.size()).thenReturn(5L);
        when(set2.size()).thenReturn(10L);

        final CombinedBlockRangeSet combinedSet = new CombinedBlockRangeSet(set1, set2);
        assertEquals(15L, combinedSet.size());

        verify(set1).size();
        verify(set2).size();
    }

    @Test
    @DisplayName("min() returns the smallest minimum value across all sets")
    void testMin() {
        final BlockRangeSet set1 = mock(BlockRangeSet.class);
        final BlockRangeSet set2 = mock(BlockRangeSet.class);
        when(set1.min()).thenReturn(10L);
        when(set2.min()).thenReturn(5L);

        final CombinedBlockRangeSet combinedSet = new CombinedBlockRangeSet(set1, set2);
        assertEquals(5L, combinedSet.min());

        verify(set1).min();
        verify(set2).min();
    }

    @Test
    @DisplayName("max() returns the largest maximum value across all sets")
    void testMax() {
        final BlockRangeSet set1 = mock(BlockRangeSet.class);
        final BlockRangeSet set2 = mock(BlockRangeSet.class);
        when(set1.max()).thenReturn(20L);
        when(set2.max()).thenReturn(25L);

        final CombinedBlockRangeSet combinedSet = new CombinedBlockRangeSet(set1, set2);
        assertEquals(25L, combinedSet.max());

        verify(set1).max();
        verify(set2).max();
    }

    @Test
    @DisplayName("stream() combines streams from all underlying sets")
    void testStream() {
        final BlockRangeSet set1 = mock(BlockRangeSet.class);
        final BlockRangeSet set2 = mock(BlockRangeSet.class);
        final LongRange range1 = new LongRange(1L, 3L);
        final LongRange range2 = new LongRange(5L, 7L);
        when(set1.streamRanges()).thenReturn(Stream.of(range1));
        when(set2.streamRanges()).thenReturn(Stream.of(range2));

        final CombinedBlockRangeSet combinedSet = new CombinedBlockRangeSet(set1, set2);
        assertArrayEquals(
                new long[] {1L, 2L, 3L, 5L, 6L, 7L}, combinedSet.stream().toArray());

        verify(set1).streamRanges();
        verify(set2).streamRanges();
    }

    @Test
    @DisplayName("streamRanges() combines ranges from all underlying sets")
    void testStreamRanges() {
        final BlockRangeSet set1 = mock(BlockRangeSet.class);
        final BlockRangeSet set2 = mock(BlockRangeSet.class);
        final LongRange range1 = new LongRange(1L, 3L);
        final LongRange range2 = new LongRange(10L, 20L);
        when(set1.streamRanges()).thenReturn(Stream.of(range1));
        when(set2.streamRanges()).thenReturn(Stream.of(range2));

        final CombinedBlockRangeSet combinedSet = new CombinedBlockRangeSet(set1, set2);
        assertArrayEquals(
                new LongRange[] {range1, range2}, combinedSet.streamRanges().toArray());

        verify(set1).streamRanges();
        verify(set2).streamRanges();
    }

    @Test
    @DisplayName("streamRanges() combines ranges from all underlying sets, merging if the touch")
    void testStreamRangesMerged() {
        final BlockRangeSet set1 = mock(BlockRangeSet.class);
        final BlockRangeSet set2 = mock(BlockRangeSet.class);
        final LongRange range1 = new LongRange(1L, 3L);
        final LongRange range2 = new LongRange(4L, 20L);
        when(set1.streamRanges()).thenReturn(Stream.of(range1));
        when(set2.streamRanges()).thenReturn(Stream.of(range2));

        final CombinedBlockRangeSet combinedSet = new CombinedBlockRangeSet(set1, set2);
        assertArrayEquals(
                new LongRange[] {new LongRange(1L, 20L)},
                combinedSet.streamRanges().toArray());

        verify(set1).streamRanges();
        verify(set2).streamRanges();
    }
}
