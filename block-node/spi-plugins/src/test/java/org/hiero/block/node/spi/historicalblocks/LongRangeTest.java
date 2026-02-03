// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.historicalblocks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@SuppressWarnings({"EqualsWithItself", "MisorderedAssertEqualsArguments"})
class LongRangeTest {

    /**
     * Tests the creation of a LongRange with various start and end values.
     */
    @Test
    @DisplayName("Constructor should create a LongRange with specified start and end values")
    void testConstructor() {
        final LongRange range1 = new LongRange(1, 5);
        assertEquals(1, range1.start());
        assertEquals(5, range1.end());

        final LongRange range2 = new LongRange(0, Long.MAX_VALUE - 1);
        assertEquals(0, range2.start());
        assertEquals(Long.MAX_VALUE - 1, range2.end());

        final LongRange range3 = new LongRange(10, 10);
        assertEquals(10, range3.start());
        assertEquals(10, range3.end());

        // Test validation failures
        assertThrows(IllegalArgumentException.class, () -> new LongRange(-1, -1));
        assertThrows(IllegalArgumentException.class, () -> new LongRange(-1, 5));
        assertThrows(IllegalArgumentException.class, () -> new LongRange(5, -1));
        assertThrows(IllegalArgumentException.class, () -> new LongRange(6, 5));
        assertThrows(IllegalArgumentException.class, () -> new LongRange(0, Long.MAX_VALUE));
    }

    /**
     * Tests the size calculation of ranges.
     */
    @Test
    @DisplayName("size() should return correct number of elements in the range")
    void testSize() {
        assertEquals(5, new LongRange(1, 5).size());
        assertEquals(1, new LongRange(10, 10).size());
        assertEquals(10, new LongRange(0, 9).size());
        assertEquals(Long.MAX_VALUE, new LongRange(0, Long.MAX_VALUE - 1).size());
    }

    /**
     * Tests the contains method for single values.
     */
    @Test
    @DisplayName("contains(long) should correctly determine if a value is in the range")
    void testContainsValue() {
        final LongRange range = new LongRange(1, 5);

        // Test values inside range
        assertTrue(range.contains(1));
        assertTrue(range.contains(3));
        assertTrue(range.contains(5));

        // Test values outside range
        assertFalse(range.contains(0));
        assertFalse(range.contains(6));

        // Edge cases
        final LongRange edgeRange = new LongRange(0, Long.MAX_VALUE - 1);
        assertTrue(edgeRange.contains(0));
        assertTrue(edgeRange.contains(1000));
        assertTrue(edgeRange.contains(Long.MAX_VALUE - 1));

        // Single value range
        final LongRange singleValueRange = new LongRange(42, 42);
        assertTrue(singleValueRange.contains(42));
        assertFalse(singleValueRange.contains(41));
        assertFalse(singleValueRange.contains(43));
    }

    /**
     * Tests the contains method for ranges specified by start and end values.
     */
    @Test
    @DisplayName("contains(long, long) should correctly determine if a range is contained within this range")
    void testContainsRange() {
        final LongRange range = new LongRange(1, 10);

        // Test ranges completely inside
        assertTrue(range.contains(2, 9));
        assertTrue(range.contains(1, 10)); // Exact match
        assertTrue(range.contains(5, 5)); // Single value

        // Test ranges extending outside
        assertFalse(range.contains(0, 5));
        assertFalse(range.contains(5, 11));
        assertFalse(range.contains(0, 11));

        // Test completely outside ranges
        assertFalse(range.contains(11, 15));
        assertFalse(range.contains(0, 0));
    }

    /**
     * Tests the overlaps method with various overlapping and non-overlapping ranges.
     */
    @Test
    @DisplayName("overlaps() should correctly identify when ranges overlap")
    void testOverlaps() {
        final LongRange range = new LongRange(5, 10);

        // Overlapping ranges
        assertTrue(range.overlaps(new LongRange(1, 6)));
        assertTrue(range.overlaps(new LongRange(9, 15)));
        assertTrue(range.overlaps(new LongRange(7, 8)));
        assertTrue(range.overlaps(new LongRange(3, 12)));
        assertTrue(range.overlaps(new LongRange(5, 10))); // Exact match

        // Edge case: overlapping at exactly one point
        assertTrue(range.overlaps(new LongRange(1, 5)));
        assertTrue(range.overlaps(new LongRange(10, 15)));

        // Non-overlapping ranges
        assertFalse(range.overlaps(new LongRange(1, 4)));
        assertFalse(range.overlaps(new LongRange(11, 15)));

        // Single value ranges
        final LongRange singleValueRange = new LongRange(3, 3);
        assertTrue(singleValueRange.overlaps(new LongRange(1, 5)));
        assertTrue(singleValueRange.overlaps(new LongRange(3, 3)));
        assertFalse(singleValueRange.overlaps(new LongRange(4, 10)));
    }

    /**
     * Tests the isAdjacent method to verify correct adjacency detection.
     */
    @Test
    @DisplayName("isAdjacent() should correctly identify when ranges are adjacent")
    void testIsAdjacent() {
        final LongRange range = new LongRange(5, 10);

        // Adjacent ranges
        assertTrue(range.isAdjacent(new LongRange(11, 15)));
        assertTrue(range.isAdjacent(new LongRange(1, 4)));

        // Test inverse adjacency
        final LongRange otherRange = new LongRange(15, 20);
        assertTrue(new LongRange(11, 14).isAdjacent(otherRange));

        // Non-adjacent ranges
        assertFalse(range.isAdjacent(new LongRange(12, 15))); // Gap
        assertFalse(range.isAdjacent(new LongRange(1, 3))); // Gap
        assertFalse(range.isAdjacent(new LongRange(1, 5))); // Overlapping
        assertFalse(range.isAdjacent(new LongRange(10, 15))); // Overlapping
        assertFalse(range.isAdjacent(new LongRange(3, 12))); // Overlapping

        // Test with single value ranges
        final LongRange singleValueRange = new LongRange(4, 4);
        assertTrue(singleValueRange.isAdjacent(new LongRange(5, 10)));
        assertTrue(new LongRange(3, 3).isAdjacent(singleValueRange));
        assertFalse(singleValueRange.isAdjacent(new LongRange(6, 10)));

        // Edge cases around boundaries
        assertTrue(new LongRange(0, 0).isAdjacent(new LongRange(1, 10)));
        assertTrue(new LongRange(Long.MAX_VALUE - 10, Long.MAX_VALUE - 2)
                .isAdjacent(new LongRange(Long.MAX_VALUE - 1, Long.MAX_VALUE - 1)));
    }

    /**
     * Tests the merge method to ensure ranges are correctly combined.
     */
    @Test
    @DisplayName("merge() should combine two ranges correctly")
    void testMerge() {
        final LongRange range = new LongRange(5, 10);

        // Merge with overlapping ranges
        LongRange merged = range.merge(new LongRange(8, 15));
        assertEquals(5, merged.start());
        assertEquals(15, merged.end());

        merged = range.merge(new LongRange(1, 7));
        assertEquals(1, merged.start());
        assertEquals(10, merged.end());

        // Merge with contained range
        merged = range.merge(new LongRange(6, 9));
        assertEquals(5, merged.start());
        assertEquals(10, merged.end());

        // Merge with containing range
        merged = range.merge(new LongRange(1, 15));
        assertEquals(1, merged.start());
        assertEquals(15, merged.end());

        // Merge with adjacent ranges
        merged = range.merge(new LongRange(11, 15));
        assertEquals(5, merged.start());
        assertEquals(15, merged.end());

        merged = range.merge(new LongRange(1, 4));
        assertEquals(1, merged.start());
        assertEquals(10, merged.end());

        // Merge with non-overlapping, non-adjacent ranges
        merged = range.merge(new LongRange(20, 25));
        assertEquals(5, merged.start());
        assertEquals(25, merged.end());

        merged = range.merge(new LongRange(1, 2));
        assertEquals(1, merged.start());
        assertEquals(10, merged.end());

        // Edge cases
        final LongRange edgeRange = new LongRange(0, 100);
        merged = edgeRange.merge(new LongRange(50, Long.MAX_VALUE - 1));
        assertEquals(0, merged.start());
        assertEquals(Long.MAX_VALUE - 1, merged.end());
    }

    /**
     * Tests the mergeContiguousRanges static method.
     */
    @Test
    @DisplayName("mergeContiguousRanges() should merge overlapping and adjacent ranges")
    void testMergeContiguousRanges() {
        // Empty list
        assertEquals(List.of(), LongRange.mergeContiguousRanges(List.of()));

        // Single range
        List<LongRange> single = List.of(new LongRange(5, 10));
        assertEquals(single, LongRange.mergeContiguousRanges(single));

        // Overlapping ranges
        List<LongRange> overlapping = List.of(new LongRange(0, 10), new LongRange(5, 15));
        List<LongRange> mergedOverlapping = LongRange.mergeContiguousRanges(overlapping);
        assertEquals(1, mergedOverlapping.size());
        assertEquals(new LongRange(0, 15), mergedOverlapping.getFirst());

        // Adjacent ranges
        List<LongRange> adjacent = List.of(new LongRange(0, 10), new LongRange(11, 20));
        List<LongRange> mergedAdjacent = LongRange.mergeContiguousRanges(adjacent);
        assertEquals(1, mergedAdjacent.size());
        assertEquals(new LongRange(0, 20), mergedAdjacent.getFirst());

        // Disjoint ranges (gap between them)
        List<LongRange> disjoint = List.of(new LongRange(0, 10), new LongRange(50, 60));
        List<LongRange> mergedDisjoint = LongRange.mergeContiguousRanges(disjoint);
        assertEquals(2, mergedDisjoint.size());
        assertEquals(new LongRange(0, 10), mergedDisjoint.get(0));
        assertEquals(new LongRange(50, 60), mergedDisjoint.get(1));

        // Multiple ranges, some overlapping, some adjacent, some disjoint
        List<LongRange> mixed =
                List.of(new LongRange(50, 60), new LongRange(0, 10), new LongRange(5, 15), new LongRange(16, 20));
        List<LongRange> mergedMixed = LongRange.mergeContiguousRanges(mixed);
        assertEquals(2, mergedMixed.size());
        assertEquals(new LongRange(0, 20), mergedMixed.get(0));
        assertEquals(new LongRange(50, 60), mergedMixed.get(1));
    }

    /**
     * Tests the stream method to ensure it produces the correct sequence of values.
     */
    @Test
    @DisplayName("stream() should generate all values in the range")
    void testStream() {
        // Test a small range
        final LongRange smallRange = new LongRange(5, 9);
        final List<Long> values = smallRange.stream().boxed().collect(Collectors.toList());

        assertEquals(5, values.size());
        assertEquals(List.of(5L, 6L, 7L, 8L, 9L), values);

        // Test a range with just one value
        final LongRange singleValueRange = new LongRange(42, 42);
        final List<Long> singleValue = singleValueRange.stream().boxed().toList();

        assertEquals(1, singleValue.size());
        assertEquals(42L, singleValue.getFirst());
    }

    /**
     * Provides test cases for compareTo method.
     */
    private static Stream<Arguments> compareToTestCases() {
        return Stream.of(
                // Equal ranges
                Arguments.of(new LongRange(5, 10), new LongRange(5, 10), 0),

                // First range starts before second
                Arguments.of(new LongRange(1, 10), new LongRange(5, 10), -1),

                // First range starts after second
                Arguments.of(new LongRange(7, 10), new LongRange(5, 10), 1),

                // Same start, first range ends before second
                Arguments.of(new LongRange(5, 8), new LongRange(5, 10), -1),

                // Same start, first range ends after second
                Arguments.of(new LongRange(5, 12), new LongRange(5, 10), 1),

                // Edge cases
                Arguments.of(new LongRange(0, 1000), new LongRange(1000, Long.MAX_VALUE - 1), -1),
                Arguments.of(new LongRange(1000, Long.MAX_VALUE - 1), new LongRange(0, 1000), 1));
    }

    /**
     * Tests the compareTo method with various range comparisons.
     */
    @ParameterizedTest
    @MethodSource("compareToTestCases")
    @DisplayName("compareTo() should correctly compare ranges")
    void testCompareTo(LongRange first, LongRange second, int expectedResult) {
        if (expectedResult < 0) {
            assertTrue(first.compareTo(second) < 0);
        } else if (expectedResult > 0) {
            assertTrue(first.compareTo(second) > 0);
        } else {
            assertEquals(0, first.compareTo(second));
        }
    }

    /**
     * Tests the equals and hashCode methods.
     */
    @SuppressWarnings("AssertBetweenInconvertibleTypes")
    @Test
    @DisplayName("equals() and hashCode() should follow the contract")
    void testEqualsAndHashCode() {
        final LongRange range1 = new LongRange(5, 10);
        final LongRange range2 = new LongRange(5, 10);
        final LongRange range3 = new LongRange(1, 10);
        final LongRange range4 = new LongRange(5, 15);

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
     * Tests the COMPARATOR constant for consistent comparison.
     */
    @Test
    @DisplayName("COMPARATOR should compare ranges consistently with compareTo")
    void testComparator() {
        final LongRange range1 = new LongRange(5, 10);
        final LongRange range2 = new LongRange(5, 10);
        final LongRange range3 = new LongRange(1, 10);
        final LongRange range4 = new LongRange(5, 15);

        assertEquals(0, LongRange.COMPARATOR.compare(range1, range2));
        assertEquals(range1.compareTo(range3), Integer.signum(LongRange.COMPARATOR.compare(range1, range3)));
        assertEquals(range1.compareTo(range4), Integer.signum(LongRange.COMPARATOR.compare(range1, range4)));
        assertEquals(range3.compareTo(range4), Integer.signum(LongRange.COMPARATOR.compare(range3, range4)));
    }

    /**
     * Tests behavior with range values at the extremes of the valid range.
     */
    @Test
    @DisplayName("Boundary conditions should be handled correctly")
    void testBoundaryConditions() {
        final LongRange extremeRange = new LongRange(0, Long.MAX_VALUE - 1);

        // Size calculation
        assertEquals(Long.MAX_VALUE, extremeRange.size());

        // Contains values at extremes
        assertTrue(extremeRange.contains(0));
        assertTrue(extremeRange.contains(1000));
        assertTrue(extremeRange.contains(Long.MAX_VALUE - 1));
        assertFalse(extremeRange.contains(Long.MAX_VALUE));

        // Test with a range at 0
        final LongRange minRange = new LongRange(0, 10);
        assertEquals(11, minRange.size());
        assertTrue(minRange.contains(0));
        assertFalse(minRange.contains(-1));

        // Test with a range at MAX_VALUE-1
        final LongRange maxRange = new LongRange(Long.MAX_VALUE - 10, Long.MAX_VALUE - 1);
        assertEquals(10, maxRange.size());
        assertTrue(maxRange.contains(Long.MAX_VALUE - 1));
        assertFalse(maxRange.contains(Long.MAX_VALUE));
    }
}
