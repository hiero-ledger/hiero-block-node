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
class BlockRangeHelpersTest {

    /**
     * Tests the creation of a BlockRange with various start and end values.
     */
    @Test
    @DisplayName("Constructor should create a BlockRange with specified start and end values")
    void testConstructor() {
        final BlockRange range1 = BlockRangeSet.of(1, 5);
        assertEquals(1, range1.rangeStart());
        assertEquals(5, range1.rangeEnd());

        final BlockRange range2 = BlockRangeSet.of(0, Long.MAX_VALUE - 1);
        assertEquals(0, range2.rangeStart());
        assertEquals(Long.MAX_VALUE - 1, range2.rangeEnd());

        final BlockRange range3 = BlockRangeSet.of(10, 10);
        assertEquals(10, range3.rangeStart());
        assertEquals(10, range3.rangeEnd());

        // Test validation failures
        assertThrows(IllegalArgumentException.class, () -> BlockRangeSet.of(-1, -1));
        assertThrows(IllegalArgumentException.class, () -> BlockRangeSet.of(-1, 5));
        assertThrows(IllegalArgumentException.class, () -> BlockRangeSet.of(5, -1));
        assertThrows(IllegalArgumentException.class, () -> BlockRangeSet.of(6, 5));
        assertThrows(IllegalArgumentException.class, () -> BlockRangeSet.of(0, Long.MAX_VALUE));
    }

    /**
     * Tests the size calculation of ranges.
     */
    @Test
    @DisplayName("size() should return correct number of elements in the range")
    void testSize() {
        assertEquals(5, BlockRangeSet.size(BlockRangeSet.of(1, 5)));
        assertEquals(1, BlockRangeSet.size(BlockRangeSet.of(10, 10)));
        assertEquals(10, BlockRangeSet.size(BlockRangeSet.of(0, 9)));
        assertEquals(Long.MAX_VALUE, BlockRangeSet.size(BlockRangeSet.of(0, Long.MAX_VALUE - 1)));
    }

    /**
     * Tests the contains method for single values.
     */
    @Test
    @DisplayName("contains(long) should correctly determine if a value is in the range")
    void testContainsValue() {
        final BlockRange range = BlockRangeSet.of(1, 5);

        // Test values inside range
        assertTrue(BlockRangeSet.contains(range, 1));
        assertTrue(BlockRangeSet.contains(range, 3));
        assertTrue(BlockRangeSet.contains(range, 5));

        // Test values outside range
        assertFalse(BlockRangeSet.contains(range, 0));
        assertFalse(BlockRangeSet.contains(range, 6));

        // Edge cases
        final BlockRange edgeRange = BlockRangeSet.of(0, Long.MAX_VALUE - 1);
        assertTrue(BlockRangeSet.contains(edgeRange, 0));
        assertTrue(BlockRangeSet.contains(edgeRange, 1000));
        assertTrue(BlockRangeSet.contains(edgeRange, Long.MAX_VALUE - 1));

        // Single value range
        final BlockRange singleValueRange = BlockRangeSet.of(42, 42);
        assertTrue(BlockRangeSet.contains(singleValueRange, 42));
        assertFalse(BlockRangeSet.contains(singleValueRange, 41));
        assertFalse(BlockRangeSet.contains(singleValueRange, 43));
    }

    /**
     * Tests the contains method for ranges specified by start and end values.
     */
    @Test
    @DisplayName("contains(long, long) should correctly determine if a range is contained within this range")
    void testContainsRange() {
        final BlockRange range = BlockRangeSet.of(1, 10);

        // Test ranges completely inside
        assertTrue(BlockRangeSet.contains(range, 2, 9));
        assertTrue(BlockRangeSet.contains(range, 1, 10)); // Exact match
        assertTrue(BlockRangeSet.contains(range, 5, 5)); // Single value

        // Test ranges extending outside
        assertFalse(BlockRangeSet.contains(range, 0, 5));
        assertFalse(BlockRangeSet.contains(range, 5, 11));
        assertFalse(BlockRangeSet.contains(range, 0, 11));

        // Test completely outside ranges
        assertFalse(BlockRangeSet.contains(range, 11, 15));
        assertFalse(BlockRangeSet.contains(range, 0, 0));
    }

    /**
     * Tests the overlaps method with various overlapping and non-overlapping ranges.
     */
    @Test
    @DisplayName("overlaps() should correctly identify when ranges overlap")
    void testOverlaps() {
        final BlockRange range = BlockRangeSet.of(5, 10);

        // Overlapping ranges
        assertTrue(BlockRangeSet.overlaps(range, BlockRangeSet.of(1, 6)));
        assertTrue(BlockRangeSet.overlaps(range, BlockRangeSet.of(9, 15)));
        assertTrue(BlockRangeSet.overlaps(range, BlockRangeSet.of(7, 8)));
        assertTrue(BlockRangeSet.overlaps(range, BlockRangeSet.of(3, 12)));
        assertTrue(BlockRangeSet.overlaps(range, BlockRangeSet.of(5, 10))); // Exact match

        // Edge case: overlapping at exactly one point
        assertTrue(BlockRangeSet.overlaps(range, BlockRangeSet.of(1, 5)));
        assertTrue(BlockRangeSet.overlaps(range, BlockRangeSet.of(10, 15)));

        // Non-overlapping ranges
        assertFalse(BlockRangeSet.overlaps(range, BlockRangeSet.of(1, 4)));
        assertFalse(BlockRangeSet.overlaps(range, BlockRangeSet.of(11, 15)));

        // Single value ranges
        final BlockRange singleValueRange = BlockRangeSet.of(3, 3);
        assertTrue(BlockRangeSet.overlaps(singleValueRange, BlockRangeSet.of(1, 5)));
        assertTrue(BlockRangeSet.overlaps(singleValueRange, BlockRangeSet.of(3, 3)));
        assertFalse(BlockRangeSet.overlaps(singleValueRange, BlockRangeSet.of(4, 10)));
    }

    /**
     * Tests the isAdjacent method to verify correct adjacency detection.
     */
    @Test
    @DisplayName("isAdjacent() should correctly identify when ranges are adjacent")
    void testIsAdjacent() {
        final BlockRange range = BlockRangeSet.of(5, 10);

        // Adjacent ranges
        assertTrue(BlockRangeSet.isAdjacent(range, BlockRangeSet.of(11, 15)));
        assertTrue(BlockRangeSet.isAdjacent(range, BlockRangeSet.of(1, 4)));

        // Test inverse adjacency
        final BlockRange otherRange = BlockRangeSet.of(15, 20);
        assertTrue(BlockRangeSet.isAdjacent(BlockRangeSet.of(11, 14), otherRange));

        // Non-adjacent ranges
        assertFalse(BlockRangeSet.isAdjacent(range, BlockRangeSet.of(12, 15))); // Gap
        assertFalse(BlockRangeSet.isAdjacent(range, BlockRangeSet.of(1, 3))); // Gap
        assertFalse(BlockRangeSet.isAdjacent(range, BlockRangeSet.of(1, 5))); // Overlapping
        assertFalse(BlockRangeSet.isAdjacent(range, BlockRangeSet.of(10, 15))); // Overlapping
        assertFalse(BlockRangeSet.isAdjacent(range, BlockRangeSet.of(3, 12))); // Overlapping

        // Test with single value ranges
        final BlockRange singleValueRange = BlockRangeSet.of(4, 4);
        assertTrue(BlockRangeSet.isAdjacent(singleValueRange, BlockRangeSet.of(5, 10)));
        assertTrue(BlockRangeSet.isAdjacent(BlockRangeSet.of(3, 3), singleValueRange));
        assertFalse(BlockRangeSet.isAdjacent(singleValueRange, BlockRangeSet.of(6, 10)));

        // Edge cases around boundaries
        assertTrue(BlockRangeSet.isAdjacent(BlockRangeSet.of(0, 0), BlockRangeSet.of(1, 10)));
        assertTrue(BlockRangeSet.isAdjacent(
                BlockRangeSet.of(Long.MAX_VALUE - 10, Long.MAX_VALUE - 2),
                BlockRangeSet.of(Long.MAX_VALUE - 1, Long.MAX_VALUE - 1)));
    }

    /**
     * Tests the mergeContiguousRanges static method.
     */
    @Test
    @DisplayName("mergeContiguousRanges() should merge overlapping and adjacent ranges")
    void testMergeContiguousRanges() {
        // Empty list
        assertEquals(List.of(), BlockRangeSet.mergeContiguousRanges(List.of()));

        // Single range
        List<BlockRange> single = List.of(BlockRangeSet.of(5, 10));
        assertEquals(single, BlockRangeSet.mergeContiguousRanges(single));

        // Overlapping ranges
        List<BlockRange> overlapping = List.of(BlockRangeSet.of(0, 10), BlockRangeSet.of(5, 15));
        List<BlockRange> mergedOverlapping = BlockRangeSet.mergeContiguousRanges(overlapping);
        assertEquals(1, mergedOverlapping.size());
        assertEquals(BlockRangeSet.of(0, 15), mergedOverlapping.getFirst());

        // Adjacent ranges
        List<BlockRange> adjacent = List.of(BlockRangeSet.of(0, 10), BlockRangeSet.of(11, 20));
        List<BlockRange> mergedAdjacent = BlockRangeSet.mergeContiguousRanges(adjacent);
        assertEquals(1, mergedAdjacent.size());
        assertEquals(BlockRangeSet.of(0, 20), mergedAdjacent.getFirst());

        // Disjoint ranges (gap between them)
        List<BlockRange> disjoint = List.of(BlockRangeSet.of(0, 10), BlockRangeSet.of(50, 60));
        List<BlockRange> mergedDisjoint = BlockRangeSet.mergeContiguousRanges(disjoint);
        assertEquals(2, mergedDisjoint.size());
        assertEquals(BlockRangeSet.of(0, 10), mergedDisjoint.get(0));
        assertEquals(BlockRangeSet.of(50, 60), mergedDisjoint.get(1));

        // Multiple ranges, some overlapping, some adjacent, some disjoint
        List<BlockRange> mixed = List.of(
                BlockRangeSet.of(50, 60), BlockRangeSet.of(0, 10), BlockRangeSet.of(5, 15), BlockRangeSet.of(16, 20));
        List<BlockRange> mergedMixed = BlockRangeSet.mergeContiguousRanges(mixed);
        assertEquals(2, mergedMixed.size());
        assertEquals(BlockRangeSet.of(0, 20), mergedMixed.get(0));
        assertEquals(BlockRangeSet.of(50, 60), mergedMixed.get(1));
    }

    /**
     * Tests the stream method to ensure it produces the correct sequence of values.
     */
    @Test
    @DisplayName("stream() should generate all values in the range")
    void testStream() {
        // Test a small range
        final BlockRange smallRange = BlockRangeSet.of(5, 9);
        final List<Long> values = BlockRangeSet.stream(smallRange).boxed().collect(Collectors.toList());

        assertEquals(5, values.size());
        assertEquals(List.of(5L, 6L, 7L, 8L, 9L), values);

        // Test a range with just one value
        final BlockRange singleValueRange = BlockRangeSet.of(42, 42);
        final List<Long> singleValue =
                BlockRangeSet.stream(singleValueRange).boxed().toList();

        assertEquals(1, singleValue.size());
        assertEquals(42L, singleValue.getFirst());
    }

    /**
     * Provides test cases for the COMPARATOR.
     */
    private static Stream<Arguments> comparatorTestCases() {
        return Stream.of(
                // Equal ranges
                Arguments.of(BlockRangeSet.of(5, 10), BlockRangeSet.of(5, 10), 0),

                // First range starts before second
                Arguments.of(BlockRangeSet.of(1, 10), BlockRangeSet.of(5, 10), -1),

                // First range starts after second
                Arguments.of(BlockRangeSet.of(7, 10), BlockRangeSet.of(5, 10), 1),

                // Same start, first range ends before second
                Arguments.of(BlockRangeSet.of(5, 8), BlockRangeSet.of(5, 10), -1),

                // Same start, first range ends after second
                Arguments.of(BlockRangeSet.of(5, 12), BlockRangeSet.of(5, 10), 1),

                // Edge cases
                Arguments.of(BlockRangeSet.of(0, 1000), BlockRangeSet.of(1000, Long.MAX_VALUE - 1), -1),
                Arguments.of(BlockRangeSet.of(1000, Long.MAX_VALUE - 1), BlockRangeSet.of(0, 1000), 1));
    }

    /**
     * Tests the COMPARATOR with various range comparisons.
     */
    @ParameterizedTest
    @MethodSource("comparatorTestCases")
    @DisplayName("COMPARATOR should correctly order ranges")
    void testComparatorOrdering(BlockRange first, BlockRange second, int expectedResult) {
        if (expectedResult < 0) {
            assertTrue(BlockRangeSet.COMPARATOR.compare(first, second) < 0);
        } else if (expectedResult > 0) {
            assertTrue(BlockRangeSet.COMPARATOR.compare(first, second) > 0);
        } else {
            assertEquals(0, BlockRangeSet.COMPARATOR.compare(first, second));
        }
    }

    /**
     * Tests the equals and hashCode methods.
     */
    @SuppressWarnings("AssertBetweenInconvertibleTypes")
    @Test
    @DisplayName("equals() and hashCode() should follow the contract")
    void testEqualsAndHashCode() {
        final BlockRange range1 = BlockRangeSet.of(5, 10);
        final BlockRange range2 = BlockRangeSet.of(5, 10);
        final BlockRange range3 = BlockRangeSet.of(1, 10);
        final BlockRange range4 = BlockRangeSet.of(5, 15);

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
        final BlockRange extremeRange = BlockRangeSet.of(0, Long.MAX_VALUE - 1);

        // Size calculation
        assertEquals(Long.MAX_VALUE, BlockRangeSet.size(extremeRange));

        // Contains values at extremes
        assertTrue(BlockRangeSet.contains(extremeRange, 0));
        assertTrue(BlockRangeSet.contains(extremeRange, 1000));
        assertTrue(BlockRangeSet.contains(extremeRange, Long.MAX_VALUE - 1));
        assertFalse(BlockRangeSet.contains(extremeRange, Long.MAX_VALUE));

        // Test with a range at 0
        final BlockRange minRange = BlockRangeSet.of(0, 10);
        assertEquals(11, BlockRangeSet.size(minRange));
        assertTrue(BlockRangeSet.contains(minRange, 0));
        assertFalse(BlockRangeSet.contains(minRange, -1));

        // Test with a range at MAX_VALUE-1
        final BlockRange maxRange = BlockRangeSet.of(Long.MAX_VALUE - 10, Long.MAX_VALUE - 1);
        assertEquals(10, BlockRangeSet.size(maxRange));
        assertTrue(BlockRangeSet.contains(maxRange, Long.MAX_VALUE - 1));
        assertFalse(BlockRangeSet.contains(maxRange, Long.MAX_VALUE));
    }

    /// Tests merging more than two lists in one pass.
    @Test
    @DisplayName("merge(List) should merge k ascending lists, coalescing overlapping and adjacent ranges")
    void testMergeKLists() {
        assertEquals(List.of(), BlockRangeSet.merge(List.of()));
        assertEquals(List.of(), BlockRangeSet.merge(List.of(List.of(), List.of(), List.of())));

        final List<BlockRange> single = List.of(BlockRangeSet.of(0, 5), BlockRangeSet.of(10, 12));
        assertEquals(single, BlockRangeSet.merge(List.of(single)));

        final List<BlockRange> a = List.of(BlockRangeSet.of(0, 9), BlockRangeSet.of(100, 109));
        final List<BlockRange> b = List.of(BlockRangeSet.of(10, 19), BlockRangeSet.of(50, 59));
        final List<BlockRange> c =
                List.of(BlockRangeSet.of(5, 7), BlockRangeSet.of(108, 120), BlockRangeSet.of(200, 200));
        assertEquals(
                List.of(
                        BlockRangeSet.of(0, 19),
                        BlockRangeSet.of(50, 59),
                        BlockRangeSet.of(100, 120),
                        BlockRangeSet.of(200, 200)),
                BlockRangeSet.merge(List.of(a, b, c)));
        // order of the input lists must not matter
        assertEquals(BlockRangeSet.merge(List.of(a, b, c)), BlockRangeSet.merge(List.of(c, a, b)));
        assertEquals(BlockRangeSet.merge(List.of(a, b, c)), BlockRangeSet.merge(List.of(c, b, a)));
    }

    /// Tests that the two list convenience overload agrees with the k-way merge.
    @Test
    @DisplayName("merge(List, List) should merge two ascending lists")
    void testMergeTwoLists() {
        final List<BlockRange> stored = List.of(BlockRangeSet.of(0, 4), BlockRangeSet.of(20, 30));
        final List<BlockRange> available = List.of(BlockRangeSet.of(5, 9), BlockRangeSet.of(25, 40));
        assertEquals(List.of(BlockRangeSet.of(0, 9), BlockRangeSet.of(20, 40)), BlockRangeSet.merge(stored, available));
        assertEquals(stored, BlockRangeSet.merge(stored, List.of()));
        assertEquals(stored, BlockRangeSet.merge(List.of(), stored));
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
                    list.add(BlockRangeSet.of(start, end));
                    expected.set((int) start, (int) end + 1);
                    cursor = end + 2 + random.nextInt(8);
                }
                lists.add(list);
            }
            final BitSet actual = new BitSet();
            BlockRange previous = null;
            for (final BlockRange range : BlockRangeSet.merge(lists)) {
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
