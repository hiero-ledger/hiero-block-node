// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.historicalblocks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hiero.block.api.BlockRange;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@SuppressWarnings({"EqualsWithItself", "MisorderedAssertEqualsArguments"})
class BlockRangesTest {

    /**
     * Tests the creation of a BlockRange with various start and end values.
     */
    @Test
    @DisplayName("Constructor should create a BlockRange with specified start and end values")
    void testConstructor() {
        final BlockRange range1 = BlockRanges.of(1, 5);
        assertEquals(1, range1.rangeStart());
        assertEquals(5, range1.rangeEnd());

        final BlockRange range2 = BlockRanges.of(0, Long.MAX_VALUE - 1);
        assertEquals(0, range2.rangeStart());
        assertEquals(Long.MAX_VALUE - 1, range2.rangeEnd());

        final BlockRange range3 = BlockRanges.of(10, 10);
        assertEquals(10, range3.rangeStart());
        assertEquals(10, range3.rangeEnd());

        // Test validation failures
        assertThrows(IllegalArgumentException.class, () -> BlockRanges.of(-1, -1));
        assertThrows(IllegalArgumentException.class, () -> BlockRanges.of(-1, 5));
        assertThrows(IllegalArgumentException.class, () -> BlockRanges.of(5, -1));
        assertThrows(IllegalArgumentException.class, () -> BlockRanges.of(6, 5));
        assertThrows(IllegalArgumentException.class, () -> BlockRanges.of(0, Long.MAX_VALUE));
    }

    /**
     * Tests the size calculation of ranges.
     */
    @Test
    @DisplayName("size() should return correct number of elements in the range")
    void testSize() {
        assertEquals(5, BlockRanges.size(BlockRanges.of(1, 5)));
        assertEquals(1, BlockRanges.size(BlockRanges.of(10, 10)));
        assertEquals(10, BlockRanges.size(BlockRanges.of(0, 9)));
        assertEquals(Long.MAX_VALUE, BlockRanges.size(BlockRanges.of(0, Long.MAX_VALUE - 1)));
    }

    /**
     * Tests the contains method for single values.
     */
    @Test
    @DisplayName("contains(long) should correctly determine if a value is in the range")
    void testContainsValue() {
        final BlockRange range = BlockRanges.of(1, 5);

        // Test values inside range
        assertTrue(BlockRanges.contains(range, 1));
        assertTrue(BlockRanges.contains(range, 3));
        assertTrue(BlockRanges.contains(range, 5));

        // Test values outside range
        assertFalse(BlockRanges.contains(range, 0));
        assertFalse(BlockRanges.contains(range, 6));

        // Edge cases
        final BlockRange edgeRange = BlockRanges.of(0, Long.MAX_VALUE - 1);
        assertTrue(BlockRanges.contains(edgeRange, 0));
        assertTrue(BlockRanges.contains(edgeRange, 1000));
        assertTrue(BlockRanges.contains(edgeRange, Long.MAX_VALUE - 1));

        // Single value range
        final BlockRange singleValueRange = BlockRanges.of(42, 42);
        assertTrue(BlockRanges.contains(singleValueRange, 42));
        assertFalse(BlockRanges.contains(singleValueRange, 41));
        assertFalse(BlockRanges.contains(singleValueRange, 43));
    }

    /**
     * Tests the contains method for ranges specified by start and end values.
     */
    @Test
    @DisplayName("contains(long, long) should correctly determine if a range is contained within this range")
    void testContainsRange() {
        final BlockRange range = BlockRanges.of(1, 10);

        // Test ranges completely inside
        assertTrue(BlockRanges.contains(range, 2, 9));
        assertTrue(BlockRanges.contains(range, 1, 10)); // Exact match
        assertTrue(BlockRanges.contains(range, 5, 5)); // Single value

        // Test ranges extending outside
        assertFalse(BlockRanges.contains(range, 0, 5));
        assertFalse(BlockRanges.contains(range, 5, 11));
        assertFalse(BlockRanges.contains(range, 0, 11));

        // Test completely outside ranges
        assertFalse(BlockRanges.contains(range, 11, 15));
        assertFalse(BlockRanges.contains(range, 0, 0));
    }

    /**
     * Tests the overlaps method with various overlapping and non-overlapping ranges.
     */
    @Test
    @DisplayName("overlaps() should correctly identify when ranges overlap")
    void testOverlaps() {
        final BlockRange range = BlockRanges.of(5, 10);

        // Overlapping ranges
        assertTrue(BlockRanges.overlaps(range, BlockRanges.of(1, 6)));
        assertTrue(BlockRanges.overlaps(range, BlockRanges.of(9, 15)));
        assertTrue(BlockRanges.overlaps(range, BlockRanges.of(7, 8)));
        assertTrue(BlockRanges.overlaps(range, BlockRanges.of(3, 12)));
        assertTrue(BlockRanges.overlaps(range, BlockRanges.of(5, 10))); // Exact match

        // Edge case: overlapping at exactly one point
        assertTrue(BlockRanges.overlaps(range, BlockRanges.of(1, 5)));
        assertTrue(BlockRanges.overlaps(range, BlockRanges.of(10, 15)));

        // Non-overlapping ranges
        assertFalse(BlockRanges.overlaps(range, BlockRanges.of(1, 4)));
        assertFalse(BlockRanges.overlaps(range, BlockRanges.of(11, 15)));

        // Single value ranges
        final BlockRange singleValueRange = BlockRanges.of(3, 3);
        assertTrue(BlockRanges.overlaps(singleValueRange, BlockRanges.of(1, 5)));
        assertTrue(BlockRanges.overlaps(singleValueRange, BlockRanges.of(3, 3)));
        assertFalse(BlockRanges.overlaps(singleValueRange, BlockRanges.of(4, 10)));
    }

    /**
     * Tests the isAdjacent method to verify correct adjacency detection.
     */
    @Test
    @DisplayName("isAdjacent() should correctly identify when ranges are adjacent")
    void testIsAdjacent() {
        final BlockRange range = BlockRanges.of(5, 10);

        // Adjacent ranges
        assertTrue(BlockRanges.isAdjacent(range, BlockRanges.of(11, 15)));
        assertTrue(BlockRanges.isAdjacent(range, BlockRanges.of(1, 4)));

        // Test inverse adjacency
        final BlockRange otherRange = BlockRanges.of(15, 20);
        assertTrue(BlockRanges.isAdjacent(BlockRanges.of(11, 14), otherRange));

        // Non-adjacent ranges
        assertFalse(BlockRanges.isAdjacent(range, BlockRanges.of(12, 15))); // Gap
        assertFalse(BlockRanges.isAdjacent(range, BlockRanges.of(1, 3))); // Gap
        assertFalse(BlockRanges.isAdjacent(range, BlockRanges.of(1, 5))); // Overlapping
        assertFalse(BlockRanges.isAdjacent(range, BlockRanges.of(10, 15))); // Overlapping
        assertFalse(BlockRanges.isAdjacent(range, BlockRanges.of(3, 12))); // Overlapping

        // Test with single value ranges
        final BlockRange singleValueRange = BlockRanges.of(4, 4);
        assertTrue(BlockRanges.isAdjacent(singleValueRange, BlockRanges.of(5, 10)));
        assertTrue(BlockRanges.isAdjacent(BlockRanges.of(3, 3), singleValueRange));
        assertFalse(BlockRanges.isAdjacent(singleValueRange, BlockRanges.of(6, 10)));

        // Edge cases around boundaries
        assertTrue(BlockRanges.isAdjacent(BlockRanges.of(0, 0), BlockRanges.of(1, 10)));
        assertTrue(BlockRanges.isAdjacent(
                BlockRanges.of(Long.MAX_VALUE - 10, Long.MAX_VALUE - 2),
                BlockRanges.of(Long.MAX_VALUE - 1, Long.MAX_VALUE - 1)));
    }

    /**
     * Tests the merge method to ensure ranges are correctly combined.
     */
    @Test
    @DisplayName("merge() should combine two ranges correctly")
    void testMerge() {
        final BlockRange range = BlockRanges.of(5, 10);

        // Merge with overlapping ranges
        BlockRange merged = BlockRanges.merge(range, BlockRanges.of(8, 15));
        assertEquals(5, merged.rangeStart());
        assertEquals(15, merged.rangeEnd());

        merged = BlockRanges.merge(range, BlockRanges.of(1, 7));
        assertEquals(1, merged.rangeStart());
        assertEquals(10, merged.rangeEnd());

        // Merge with contained range
        merged = BlockRanges.merge(range, BlockRanges.of(6, 9));
        assertEquals(5, merged.rangeStart());
        assertEquals(10, merged.rangeEnd());

        // Merge with containing range
        merged = BlockRanges.merge(range, BlockRanges.of(1, 15));
        assertEquals(1, merged.rangeStart());
        assertEquals(15, merged.rangeEnd());

        // Merge with adjacent ranges
        merged = BlockRanges.merge(range, BlockRanges.of(11, 15));
        assertEquals(5, merged.rangeStart());
        assertEquals(15, merged.rangeEnd());

        merged = BlockRanges.merge(range, BlockRanges.of(1, 4));
        assertEquals(1, merged.rangeStart());
        assertEquals(10, merged.rangeEnd());

        // Merge with non-overlapping, non-adjacent ranges
        merged = BlockRanges.merge(range, BlockRanges.of(20, 25));
        assertEquals(5, merged.rangeStart());
        assertEquals(25, merged.rangeEnd());

        merged = BlockRanges.merge(range, BlockRanges.of(1, 2));
        assertEquals(1, merged.rangeStart());
        assertEquals(10, merged.rangeEnd());

        // Edge cases
        final BlockRange edgeRange = BlockRanges.of(0, 100);
        merged = BlockRanges.merge(edgeRange, BlockRanges.of(50, Long.MAX_VALUE - 1));
        assertEquals(0, merged.rangeStart());
        assertEquals(Long.MAX_VALUE - 1, merged.rangeEnd());
    }

    /**
     * Tests the mergeContiguousRanges static method.
     */
    @Test
    @DisplayName("mergeContiguousRanges() should merge overlapping and adjacent ranges")
    void testMergeContiguousRanges() {
        // Empty list
        assertEquals(List.of(), BlockRanges.mergeContiguousRanges(List.of()));

        // Single range
        List<BlockRange> single = List.of(BlockRanges.of(5, 10));
        assertEquals(single, BlockRanges.mergeContiguousRanges(single));

        // Overlapping ranges
        List<BlockRange> overlapping = List.of(BlockRanges.of(0, 10), BlockRanges.of(5, 15));
        List<BlockRange> mergedOverlapping = BlockRanges.mergeContiguousRanges(overlapping);
        assertEquals(1, mergedOverlapping.size());
        assertEquals(BlockRanges.of(0, 15), mergedOverlapping.getFirst());

        // Adjacent ranges
        List<BlockRange> adjacent = List.of(BlockRanges.of(0, 10), BlockRanges.of(11, 20));
        List<BlockRange> mergedAdjacent = BlockRanges.mergeContiguousRanges(adjacent);
        assertEquals(1, mergedAdjacent.size());
        assertEquals(BlockRanges.of(0, 20), mergedAdjacent.getFirst());

        // Disjoint ranges (gap between them)
        List<BlockRange> disjoint = List.of(BlockRanges.of(0, 10), BlockRanges.of(50, 60));
        List<BlockRange> mergedDisjoint = BlockRanges.mergeContiguousRanges(disjoint);
        assertEquals(2, mergedDisjoint.size());
        assertEquals(BlockRanges.of(0, 10), mergedDisjoint.get(0));
        assertEquals(BlockRanges.of(50, 60), mergedDisjoint.get(1));

        // Multiple ranges, some overlapping, some adjacent, some disjoint
        List<BlockRange> mixed =
                List.of(BlockRanges.of(50, 60), BlockRanges.of(0, 10), BlockRanges.of(5, 15), BlockRanges.of(16, 20));
        List<BlockRange> mergedMixed = BlockRanges.mergeContiguousRanges(mixed);
        assertEquals(2, mergedMixed.size());
        assertEquals(BlockRanges.of(0, 20), mergedMixed.get(0));
        assertEquals(BlockRanges.of(50, 60), mergedMixed.get(1));
    }

    /**
     * Tests the stream method to ensure it produces the correct sequence of values.
     */
    @Test
    @DisplayName("stream() should generate all values in the range")
    void testStream() {
        // Test a small range
        final BlockRange smallRange = BlockRanges.of(5, 9);
        final List<Long> values = BlockRanges.stream(smallRange).boxed().collect(Collectors.toList());

        assertEquals(5, values.size());
        assertEquals(List.of(5L, 6L, 7L, 8L, 9L), values);

        // Test a range with just one value
        final BlockRange singleValueRange = BlockRanges.of(42, 42);
        final List<Long> singleValue =
                BlockRanges.stream(singleValueRange).boxed().toList();

        assertEquals(1, singleValue.size());
        assertEquals(42L, singleValue.getFirst());
    }

    /**
     * Provides test cases for the COMPARATOR.
     */
    private static Stream<Arguments> comparatorTestCases() {
        return Stream.of(
                // Equal ranges
                Arguments.of(BlockRanges.of(5, 10), BlockRanges.of(5, 10), 0),

                // First range starts before second
                Arguments.of(BlockRanges.of(1, 10), BlockRanges.of(5, 10), -1),

                // First range starts after second
                Arguments.of(BlockRanges.of(7, 10), BlockRanges.of(5, 10), 1),

                // Same start, first range ends before second
                Arguments.of(BlockRanges.of(5, 8), BlockRanges.of(5, 10), -1),

                // Same start, first range ends after second
                Arguments.of(BlockRanges.of(5, 12), BlockRanges.of(5, 10), 1),

                // Edge cases
                Arguments.of(BlockRanges.of(0, 1000), BlockRanges.of(1000, Long.MAX_VALUE - 1), -1),
                Arguments.of(BlockRanges.of(1000, Long.MAX_VALUE - 1), BlockRanges.of(0, 1000), 1));
    }

    /**
     * Tests the COMPARATOR with various range comparisons.
     */
    @ParameterizedTest
    @MethodSource("comparatorTestCases")
    @DisplayName("COMPARATOR should correctly order ranges")
    void testComparatorOrdering(BlockRange first, BlockRange second, int expectedResult) {
        if (expectedResult < 0) {
            assertTrue(BlockRanges.COMPARATOR.compare(first, second) < 0);
        } else if (expectedResult > 0) {
            assertTrue(BlockRanges.COMPARATOR.compare(first, second) > 0);
        } else {
            assertEquals(0, BlockRanges.COMPARATOR.compare(first, second));
        }
    }

    /**
     * Tests the equals and hashCode methods.
     */
    @SuppressWarnings("AssertBetweenInconvertibleTypes")
    @Test
    @DisplayName("equals() and hashCode() should follow the contract")
    void testEqualsAndHashCode() {
        final BlockRange range1 = BlockRanges.of(5, 10);
        final BlockRange range2 = BlockRanges.of(5, 10);
        final BlockRange range3 = BlockRanges.of(1, 10);
        final BlockRange range4 = BlockRanges.of(5, 15);

        // Test reflexivity
        assertEquals(range1, range1);

        // Test symmetry
        assertEquals(range1, range2);
        assertEquals(range2, range1);

        // Test transitivity
        assertNotEquals(range1, range3);
        assertNotEquals(range1, range4);

        // Test with null
        assertNotEquals(range1, null);

        // Test with different object type
        assertNotEquals(range1, "not a range");

        // Test hashCode consistency
        assertEquals(range1.hashCode(), range2.hashCode());
        assertNotEquals(range1.hashCode(), range3.hashCode());
    }

    /**
     * Tests behavior with range values at the extremes of the valid range.
     */
    @Test
    @DisplayName("Boundary conditions should be handled correctly")
    void testBoundaryConditions() {
        final BlockRange extremeRange = BlockRanges.of(0, Long.MAX_VALUE - 1);

        // Size calculation
        assertEquals(Long.MAX_VALUE, BlockRanges.size(extremeRange));

        // Contains values at extremes
        assertTrue(BlockRanges.contains(extremeRange, 0));
        assertTrue(BlockRanges.contains(extremeRange, 1000));
        assertTrue(BlockRanges.contains(extremeRange, Long.MAX_VALUE - 1));
        assertFalse(BlockRanges.contains(extremeRange, Long.MAX_VALUE));

        // Test with a range at 0
        final BlockRange minRange = BlockRanges.of(0, 10);
        assertEquals(11, BlockRanges.size(minRange));
        assertTrue(BlockRanges.contains(minRange, 0));
        assertFalse(BlockRanges.contains(minRange, -1));

        // Test with a range at MAX_VALUE-1
        final BlockRange maxRange = BlockRanges.of(Long.MAX_VALUE - 10, Long.MAX_VALUE - 1);
        assertEquals(10, BlockRanges.size(maxRange));
        assertTrue(BlockRanges.contains(maxRange, Long.MAX_VALUE - 1));
        assertFalse(BlockRanges.contains(maxRange, Long.MAX_VALUE));
    }

    /// Tests merging more than two lists in one pass.
    @Test
    @DisplayName("merge(List) should merge k ascending lists, coalescing overlapping and adjacent ranges")
    void testMergeKLists() {
        assertEquals(List.of(), BlockRanges.merge(List.of()));
        assertEquals(List.of(), BlockRanges.merge(List.of(List.of(), List.of(), List.of())));

        final List<BlockRange> single = List.of(BlockRanges.of(0, 5), BlockRanges.of(10, 12));
        assertEquals(single, BlockRanges.merge(List.of(single)));

        final List<BlockRange> a = List.of(BlockRanges.of(0, 9), BlockRanges.of(100, 109));
        final List<BlockRange> b = List.of(BlockRanges.of(10, 19), BlockRanges.of(50, 59));
        final List<BlockRange> c = List.of(BlockRanges.of(5, 7), BlockRanges.of(108, 120), BlockRanges.of(200, 200));
        assertEquals(
                List.of(
                        BlockRanges.of(0, 19),
                        BlockRanges.of(50, 59),
                        BlockRanges.of(100, 120),
                        BlockRanges.of(200, 200)),
                BlockRanges.merge(List.of(a, b, c)));
        // order of the input lists must not matter
        assertEquals(BlockRanges.merge(List.of(a, b, c)), BlockRanges.merge(List.of(c, a, b)));
        assertEquals(BlockRanges.merge(List.of(a, b, c)), BlockRanges.merge(List.of(c, b, a)));
    }

    /// Tests that the two list convenience overload agrees with the k-way merge.
    @Test
    @DisplayName("merge(List, List) should merge two ascending lists")
    void testMergeTwoLists() {
        final List<BlockRange> stored = List.of(BlockRanges.of(0, 4), BlockRanges.of(20, 30));
        final List<BlockRange> available = List.of(BlockRanges.of(5, 9), BlockRanges.of(25, 40));
        assertEquals(List.of(BlockRanges.of(0, 9), BlockRanges.of(20, 40)), BlockRanges.merge(stored, available));
        assertEquals(stored, BlockRanges.merge(stored, List.of()));
        assertEquals(stored, BlockRanges.merge(List.of(), stored));
    }

    /// Compares the k-way merge against a brute force set of block numbers on random input.
    @Test
    @DisplayName("merge(List) should match a brute force union on random input")
    void testMergeKListsRandom() {
        final Random random = new Random(42);
        for (int round = 0; round < 200; round++) {
            final int k = random.nextInt(5);
            final List<List<BlockRange>> lists = new ArrayList<>();
            final BitSet expected = new BitSet();
            for (int i = 0; i < k; i++) {
                final List<BlockRange> list = new ArrayList<>();
                long cursor = random.nextInt(10);
                final int count = random.nextInt(6);
                for (int r = 0; r < count; r++) {
                    final long start = cursor;
                    final long end = start + random.nextInt(8);
                    list.add(BlockRanges.of(start, end));
                    expected.set((int) start, (int) end + 1);
                    cursor = end + 2 + random.nextInt(8);
                }
                lists.add(list);
            }
            final BitSet actual = new BitSet();
            BlockRange previous = null;
            for (final BlockRange range : BlockRanges.merge(lists)) {
                if (previous != null) {
                    assertTrue(range.rangeStart() > previous.rangeEnd() + 1, "ranges must be coalesced");
                }
                actual.set((int) range.rangeStart(), (int) range.rangeEnd() + 1);
                previous = range;
            }
            assertEquals(expected, actual);
        }
    }
}
