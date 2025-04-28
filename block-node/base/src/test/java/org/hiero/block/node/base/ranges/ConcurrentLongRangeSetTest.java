// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.base.ranges;

import static org.hiero.block.node.spi.BlockNodePlugin.UNKNOWN_BLOCK_NUMBER;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.List;
import org.hiero.block.node.spi.historicalblocks.LongRange;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the {@link ConcurrentLongRangeSet} class.
 */
class ConcurrentLongRangeSetTest {

    /**
     * Tests the creation of an empty ConcurrentLongRangeSet.
     */
    @Test
    @DisplayName("Constructor should create an empty ConcurrentLongRangeSet")
    void testConstructorEmpty() {
        final ConcurrentLongRangeSet set = new ConcurrentLongRangeSet();
        assertEquals(0, set.size());
        assertEquals(0, set.rangeCount());
    }

    /**
     * Tests the creation of a ConcurrentLongRangeSet with a single range.
     */
    @Test
    @DisplayName("Constructor should create a ConcurrentLongRangeSet with a single range")
    void testConstructorSingleRange() {
        final ConcurrentLongRangeSet set = new ConcurrentLongRangeSet(1, 5);
        assertEquals(5, set.size());
        assertEquals(1, set.rangeCount());
        assertTrue(set.contains(1));
        assertTrue(set.contains(5));
        assertFalse(set.contains(0));
        assertFalse(set.contains(6));
    }

    /**
     * Tests the creation of a ConcurrentLongRangeSet with multiple ranges.
     */
    @Test
    @DisplayName("Constructor should create a ConcurrentLongRangeSet with multiple ranges")
    void testConstructorMultipleRanges() {
        final LongRange range1 = new LongRange(1, 5);
        final LongRange range2 = new LongRange(10, 15);
        final ConcurrentLongRangeSet set = new ConcurrentLongRangeSet(range1, range2);
        assertEquals(11, set.size());
        assertEquals(2, set.rangeCount());
        assertTrue(set.contains(1));
        assertTrue(set.contains(5));
        assertTrue(set.contains(10));
        assertTrue(set.contains(15));
        assertFalse(set.contains(6));
        assertFalse(set.contains(9));
    }

    /**
     * Tests the add method for a single value.
     */
    @Test
    @DisplayName("add(long) should add a single value to the set")
    void testAddSingleValue() {
        final ConcurrentLongRangeSet set = new ConcurrentLongRangeSet();
        set.add(5);
        assertTrue(set.contains(5));
        assertEquals(1, set.size());
        assertEquals(1, set.rangeCount());
    }

    /**
     * Tests the add method for a range of values.
     */
    @Test
    @DisplayName("add(long, long) should add a range of values to the set")
    void testAddRange() {
        final ConcurrentLongRangeSet set = new ConcurrentLongRangeSet();
        set.add(1, 5);
        assertTrue(set.contains(1));
        assertTrue(set.contains(5));
        assertEquals(5, set.size());
        assertEquals(1, set.rangeCount());
    }

    /**
     * Tests the remove method for a single value.
     */
    @Test
    @DisplayName("remove(long) should remove a single value from the set")
    void testRemoveSingleValue() {
        final ConcurrentLongRangeSet set = new ConcurrentLongRangeSet(1, 5);
        set.remove(3);
        assertFalse(set.contains(3));
        assertEquals(4, set.size());
        assertEquals(2, set.rangeCount());
    }

    /**
     * Tests the remove method for a range of values.
     */
    @Test
    @DisplayName("remove(long, long) should remove a range of values from the set")
    void testRemoveRange() {
        final ConcurrentLongRangeSet set = new ConcurrentLongRangeSet(1, 10);
        set.remove(3, 7);
        assertFalse(set.contains(3));
        assertFalse(set.contains(7));
        assertEquals(5, set.size());
        assertEquals(2, set.rangeCount());
    }

    /**
     * Tests the size method.
     */
    @Test
    @DisplayName("size() should return the correct number of values in the set")
    void testSize() {
        final ConcurrentLongRangeSet set = new ConcurrentLongRangeSet(1, 5);
        assertEquals(5, set.size());
        set.add(10, 15);
        assertEquals(11, set.size());
        set.remove(3, 4);
        assertEquals(9, set.size());
    }

    /**
     * Tests the rangeCount method.
     */
    @Test
    @DisplayName("rangeCount() should return the correct number of ranges in the set")
    void testRangeCount() {
        final ConcurrentLongRangeSet set = new ConcurrentLongRangeSet(1, 5);
        assertEquals(1, set.rangeCount());
        set.add(10, 15);
        assertEquals(2, set.rangeCount());
        set.remove(3, 4);
        assertEquals(3, set.rangeCount());
    }

    /**
     * Tests the contains method for a single value.
     */
    @Test
    @DisplayName("contains(long) should correctly determine if a value is in the set")
    void testContainsSingleValue() {
        final ConcurrentLongRangeSet set = new ConcurrentLongRangeSet(1, 5);
        assertTrue(set.contains(1));
        assertTrue(set.contains(5));
        assertFalse(set.contains(0));
        assertFalse(set.contains(6));
    }

    /**
     * Tests the contains method for a range of values.
     */
    @Test
    @DisplayName("contains(long, long) should correctly determine if a range is in the set")
    void testContainsRange() {
        final ConcurrentLongRangeSet set = new ConcurrentLongRangeSet(1, 10);
        assertTrue(set.contains(1, 5));
        assertTrue(set.contains(5, 10));
        assertFalse(set.contains(0, 5));
        assertFalse(set.contains(5, 11));
    }

    /**
     * Tests the stream method.
     */
    @Test
    @DisplayName("stream() should generate all values in the set")
    void testStream() {
        final ConcurrentLongRangeSet set = new ConcurrentLongRangeSet(1, 5);
        set.add(10, 15);
        final long[] values = set.stream().toArray();
        assertArrayEquals(new long[] {1, 2, 3, 4, 5, 10, 11, 12, 13, 14, 15}, values);
    }

    /**
     * Tests the streamRanges method.
     */
    @Test
    @DisplayName("streamRanges() should generate all ranges in the set")
    void testStreamRanges() {
        final ConcurrentLongRangeSet set = new ConcurrentLongRangeSet(1, 5);
        set.add(10, 15);
        final LongRange[] ranges = set.streamRanges().toArray(LongRange[]::new);
        assertArrayEquals(new LongRange[] {new LongRange(1, 5), new LongRange(10, 15)}, ranges);
    }

    /**
     * Tests the mergeRange method with non-overlapping ranges.
     */
    @Test
    @DisplayName("mergeRange() should correctly handle non-overlapping ranges")
    void testMergeRangeNonOverlapping() {
        final ConcurrentLongRangeSet set = new ConcurrentLongRangeSet();

        // Merge into empty list
        List<LongRange> result = set.mergeRange(Collections.emptyList(), new LongRange(5, 10));
        assertEquals(1, result.size());
        assertEquals(new LongRange(5, 10), result.getFirst());

        // Merge before existing range
        List<LongRange> existing = List.of(new LongRange(20, 30));
        result = set.mergeRange(existing, new LongRange(5, 10));
        assertEquals(2, result.size());
        assertEquals(new LongRange(5, 10), result.get(0));
        assertEquals(new LongRange(20, 30), result.get(1));

        // Merge after existing range
        existing = List.of(new LongRange(5, 10));
        result = set.mergeRange(existing, new LongRange(20, 30));
        assertEquals(2, result.size());
        assertEquals(new LongRange(5, 10), result.get(0));
        assertEquals(new LongRange(20, 30), result.get(1));

        // Merge between existing ranges
        existing = List.of(new LongRange(5, 10), new LongRange(20, 30));
        result = set.mergeRange(existing, new LongRange(15, 17));
        assertEquals(3, result.size());
        assertEquals(new LongRange(5, 10), result.get(0));
        assertEquals(new LongRange(15, 17), result.get(1));
        assertEquals(new LongRange(20, 30), result.get(2));
    }

    /**
     * Tests the mergeRange method with overlapping ranges.
     */
    @Test
    @DisplayName("mergeRange() should correctly merge overlapping ranges")
    void testMergeRangeOverlapping() {
        final ConcurrentLongRangeSet set = new ConcurrentLongRangeSet();

        // Merge with partial overlap at beginning
        List<LongRange> existing = List.of(new LongRange(5, 10));
        List<LongRange> result = set.mergeRange(existing, new LongRange(3, 7));
        assertEquals(1, result.size());
        assertEquals(new LongRange(3, 10), result.getFirst());

        // Merge with partial overlap at end
        existing = List.of(new LongRange(5, 10));
        result = set.mergeRange(existing, new LongRange(8, 15));
        assertEquals(1, result.size());
        assertEquals(new LongRange(5, 15), result.getFirst());

        // Merge completely contained
        existing = List.of(new LongRange(5, 15));
        result = set.mergeRange(existing, new LongRange(7, 12));
        assertEquals(1, result.size());
        assertEquals(new LongRange(5, 15), result.getFirst());

        // Merge completely containing
        existing = List.of(new LongRange(7, 12));
        result = set.mergeRange(existing, new LongRange(5, 15));
        assertEquals(1, result.size());
        assertEquals(new LongRange(5, 15), result.getFirst());

        // Merge overlapping multiple ranges
        existing = List.of(new LongRange(5, 10), new LongRange(15, 20), new LongRange(25, 30));
        result = set.mergeRange(existing, new LongRange(8, 27));
        assertEquals(1, result.size());
        assertEquals(new LongRange(5, 30), result.getFirst());
    }

    /**
     * Tests the mergeRange method with adjacent ranges.
     */
    @Test
    @DisplayName("mergeRange() should correctly merge adjacent ranges")
    void testMergeRangeAdjacent() {
        final ConcurrentLongRangeSet set = new ConcurrentLongRangeSet();

        // Merge adjacent at end
        List<LongRange> existing = List.of(new LongRange(5, 10));
        List<LongRange> result = set.mergeRange(existing, new LongRange(11, 15));
        assertEquals(1, result.size());
        assertEquals(new LongRange(5, 15), result.getFirst());

        // Merge adjacent at beginning
        existing = List.of(new LongRange(5, 10));
        result = set.mergeRange(existing, new LongRange(1, 4));
        assertEquals(1, result.size());
        assertEquals(new LongRange(1, 10), result.getFirst());

        // Merge adjacent between multiple ranges
        existing = List.of(new LongRange(1, 5), new LongRange(11, 15));
        result = set.mergeRange(existing, new LongRange(6, 10));
        assertEquals(1, result.size());
        assertEquals(new LongRange(1, 15), result.getFirst());
    }

    /**
     * Tests adding a range that's already contained in the set.
     */
    @Test
    @DisplayName("add() should not modify the set when adding an already contained range")
    void testAddContainedRange() {
        final ConcurrentLongRangeSet set = new ConcurrentLongRangeSet(1, 10);

        // Add a completely contained range
        set.add(3, 7);
        assertEquals(1, set.rangeCount());
        assertEquals(10, set.size());

        // Add an identical range
        set.add(1, 10);
        assertEquals(1, set.rangeCount());
        assertEquals(10, set.size());
    }

    /**
     * Tests the add(LongRange) method.
     */
    @Test
    @DisplayName("add(LongRange) should add a range to the set")
    void testAddLongRange() {
        final ConcurrentLongRangeSet set = new ConcurrentLongRangeSet();

        set.add(new LongRange(5, 10));
        assertTrue(set.contains(5, 10));
        assertEquals(6, set.size());
        assertEquals(1, set.rangeCount());

        set.add(new LongRange(15, 20));
        assertTrue(set.contains(15, 20));
        assertEquals(12, set.size());
        assertEquals(2, set.rangeCount());

        // Add an overlapping range
        set.add(new LongRange(8, 17));
        assertTrue(set.contains(8, 17));
        assertEquals(16, set.size());
        assertEquals(1, set.rangeCount());
    }

    /**
     * Tests the addAll(LongRange...) method.
     */
    @Test
    @DisplayName("addAll(LongRange...) should add multiple ranges to the set")
    void testAddAllVarArgs() {
        final ConcurrentLongRangeSet set = new ConcurrentLongRangeSet();

        set.addAll(new LongRange(5, 10), new LongRange(15, 20));
        assertTrue(set.contains(5, 10));
        assertTrue(set.contains(15, 20));
        assertEquals(12, set.size());
        assertEquals(2, set.rangeCount());

        // Add overlapping ranges
        set.addAll(new LongRange(8, 17), new LongRange(25, 30));
        assertTrue(set.contains(5, 20));
        assertTrue(set.contains(25, 30));
        assertEquals(22, set.size());
        assertEquals(2, set.rangeCount());
    }

    /**
     * Tests the addAll(Collection<LongRange>...) method.
     */
    @Test
    @DisplayName("addAll(Collection<LongRange>...) should add multiple collections of ranges")
    void testAddAllCollections() {
        final ConcurrentLongRangeSet set = new ConcurrentLongRangeSet();

        List<LongRange> collection1 = List.of(new LongRange(5, 10), new LongRange(15, 20));
        List<LongRange> collection2 = List.of(new LongRange(25, 30), new LongRange(35, 40));

        set.addAll(collection1, collection2);

        assertTrue(set.contains(5, 10));
        assertTrue(set.contains(15, 20));
        assertTrue(set.contains(25, 30));
        assertTrue(set.contains(35, 40));
        assertEquals(24, set.size());
        assertEquals(4, set.rangeCount());
    }

    /**
     * Tests the addAll(ConcurrentLongRangeSet) method.
     */
    @Test
    @DisplayName("addAll(ConcurrentLongRangeSet) should add all ranges from another set")
    void testAddAllSet() {
        final ConcurrentLongRangeSet set1 = new ConcurrentLongRangeSet(5, 10);
        final ConcurrentLongRangeSet set2 = new ConcurrentLongRangeSet(15, 20);

        set1.addAll(set2);

        assertTrue(set1.contains(5, 10));
        assertTrue(set1.contains(15, 20));
        assertEquals(12, set1.size());
        assertEquals(2, set1.rangeCount());

        // Adding a set with overlapping ranges
        final ConcurrentLongRangeSet set3 = new ConcurrentLongRangeSet();
        set3.add(8, 17);

        set1.addAll(set3);
        assertTrue(set1.contains(5, 20));
        assertEquals(16, set1.size());
        assertEquals(1, set1.rangeCount());
    }

    /**
     * Tests the contains(LongRange) method.
     */
    @Test
    @DisplayName("contains(LongRange) should correctly determine if a range is in the set")
    void testContainsLongRange() {
        final ConcurrentLongRangeSet set = new ConcurrentLongRangeSet(1, 10);

        assertTrue(set.contains(new LongRange(1, 10)));
        assertTrue(set.contains(new LongRange(3, 7)));
        assertFalse(set.contains(new LongRange(0, 5)));
        assertFalse(set.contains(new LongRange(5, 15)));
    }

    /**
     * Tests the remove(LongRange) method.
     */
    @Test
    @DisplayName("remove(LongRange) should remove a range from the set")
    void testRemoveLongRange() {
        final ConcurrentLongRangeSet set = new ConcurrentLongRangeSet(1, 10);

        set.remove(new LongRange(3, 7));
        assertFalse(set.contains(5));
        assertTrue(set.contains(1));
        assertTrue(set.contains(10));
        assertEquals(5, set.size());
        assertEquals(2, set.rangeCount());
    }

    /**
     * Tests the removeAll(LongRange...) method.
     */
    @Test
    @DisplayName("removeAll(LongRange...) should remove multiple ranges from the set")
    void testRemoveAllVarArgs() {
        final ConcurrentLongRangeSet set = new ConcurrentLongRangeSet(1, 20);

        set.removeAll(new LongRange(3, 7), new LongRange(12, 15));
        assertFalse(set.contains(5));
        assertFalse(set.contains(14));
        assertTrue(set.contains(1));
        assertTrue(set.contains(10));
        assertTrue(set.contains(20));
        assertEquals(11, set.size());
        assertEquals(3, set.rangeCount());
    }

    /**
     * Tests the removeAll(Collection<LongRange>...) method.
     */
    @Test
    @DisplayName("removeAll(Collection<LongRange>...) should remove multiple collections of ranges")
    void testRemoveAllCollections() {
        final ConcurrentLongRangeSet set = new ConcurrentLongRangeSet(1, 30);

        List<LongRange> collection1 = List.of(new LongRange(3, 7), new LongRange(12, 15));
        List<LongRange> collection2 = List.of(new LongRange(20, 25));

        set.removeAll(collection1, collection2);

        assertFalse(set.contains(5));
        assertFalse(set.contains(14));
        assertFalse(set.contains(22));
        assertTrue(set.contains(1));
        assertTrue(set.contains(10));
        assertTrue(set.contains(30));
        assertEquals(15, set.size());
        assertEquals(4, set.rangeCount());
    }

    /**
     * Tests the removeAll(ConcurrentLongRangeSet) method.
     */
    @Test
    @DisplayName("removeAll(ConcurrentLongRangeSet) should remove all ranges from the set")
    void testRemoveAllSet() {
        final ConcurrentLongRangeSet set1 = new ConcurrentLongRangeSet(1, 30);
        final ConcurrentLongRangeSet set2 = new ConcurrentLongRangeSet();
        set2.add(3, 7);
        set2.add(12, 15);
        set2.add(20, 25);

        set1.removeAll(set2);

        assertFalse(set1.contains(5));
        assertFalse(set1.contains(14));
        assertFalse(set1.contains(22));
        assertTrue(set1.contains(1));
        assertTrue(set1.contains(10));
        assertTrue(set1.contains(30));
        assertEquals(15, set1.size());
        assertEquals(4, set1.rangeCount());
    }

    /**
     * Tests boundary conditions with values at the extremes of the valid range.
     */
    @Test
    @DisplayName("Boundary conditions should be handled correctly")
    void testBoundaryConditions() {
        final ConcurrentLongRangeSet set = new ConcurrentLongRangeSet();

        // Test with values at 0
        set.add(0, 10);
        assertTrue(set.contains(0));
        assertEquals(11, set.size());

        // Test with values at Long.MAX_VALUE-1
        set.add(Long.MAX_VALUE - 10, Long.MAX_VALUE - 1);
        assertTrue(set.contains(Long.MAX_VALUE - 1));
        assertEquals(21, set.size());

        // Remove a value at boundary
        set.remove(0);
        assertFalse(set.contains(0));
        assertTrue(set.contains(1));

        set.remove(Long.MAX_VALUE - 1);
        assertFalse(set.contains(Long.MAX_VALUE - 1));
        assertTrue(set.contains(Long.MAX_VALUE - 2));
    }

    /**
     * Tests operations with empty sets.
     */
    @Test
    @DisplayName("Operations with empty sets should behave correctly")
    void testEmptySetOperations() {
        final ConcurrentLongRangeSet emptySet = new ConcurrentLongRangeSet();
        final ConcurrentLongRangeSet set = new ConcurrentLongRangeSet(1, 5);

        // Adding empty set to a set
        set.addAll(emptySet);
        assertEquals(5, set.size());
        assertEquals(1, set.rangeCount());

        // Adding a set to empty set
        emptySet.addAll(set);
        assertEquals(5, emptySet.size());
        assertEquals(1, emptySet.rangeCount());

        // Removing empty set from a set
        set.removeAll(new ConcurrentLongRangeSet());
        assertEquals(5, set.size());

        // Removing a set from empty set
        final ConcurrentLongRangeSet anotherEmptySet = new ConcurrentLongRangeSet();
        anotherEmptySet.removeAll(set);
        assertEquals(0, anotherEmptySet.size());
        assertEquals(0, anotherEmptySet.rangeCount());

        // Adding and removing from empty set
        final ConcurrentLongRangeSet testSet = new ConcurrentLongRangeSet();
        testSet.add(5, 10);
        testSet.remove(5, 10);
        assertEquals(0, testSet.size());
        assertEquals(0, testSet.rangeCount());
    }

    /**
     * Tests operations with single-value ranges.
     */
    @Test
    @DisplayName("Operations with single-value ranges should behave correctly")
    void testSingleValueRanges() {
        final ConcurrentLongRangeSet set = new ConcurrentLongRangeSet();

        // Add single values
        set.add(5);
        set.add(7);
        set.add(9);
        assertEquals(3, set.size());
        assertEquals(3, set.rangeCount());

        // Add a value adjacent to existing one
        set.add(6);
        assertEquals(4, set.size());
        assertEquals(2, set.rangeCount());

        // Add a value that connects two ranges
        set.add(8);
        assertEquals(5, set.size());
        assertEquals(1, set.rangeCount());
        assertTrue(set.contains(5, 9));

        // Remove a single value from middle
        set.remove(7);
        assertEquals(4, set.size());
        assertEquals(2, set.rangeCount());
        assertTrue(set.contains(5, 6));
        assertTrue(set.contains(8, 9));

        // Remove single value ranges
        set.remove(5);
        set.remove(9);
        assertEquals(2, set.size());
        assertEquals(2, set.rangeCount());
    }

    /**
     * Tests complex scenarios with multiple operations.
     */
    @Test
    @DisplayName("Complex scenarios with multiple operations should work correctly")
    void testComplexScenarios() {
        final ConcurrentLongRangeSet set = new ConcurrentLongRangeSet();

        // Build a complex set
        set.add(1, 10);
        set.add(20, 30);
        set.add(40, 50);
        assertEquals(32, set.size());
        assertEquals(3, set.rangeCount());

        // Connect ranges
        set.add(11, 19);
        set.add(31, 39);
        assertEquals(50, set.size());
        assertEquals(1, set.rangeCount());

        // Create gaps
        set.remove(15, 25);
        set.remove(35, 45);
        assertEquals(28, set.size());
        assertEquals(3, set.rangeCount());

        // Add a range that spans multiple gaps
        set.add(10, 46);
        assertEquals(50, set.size());
        assertEquals(1, set.rangeCount());

        // Remove from both ends
        set.remove(1, 5);
        set.remove(45, 50);
        assertEquals(39, set.size());
        assertEquals(1, set.rangeCount());

        // Add back as separate ranges
        set.add(1, 3);
        set.add(47, 50);
        assertEquals(46, set.size());
        assertEquals(3, set.rangeCount());
    }

    /**
     * Tests the min() method.
     */
    @Test
    @DisplayName("min() should return the minimum value in the set")
    void testMin() {
        final ConcurrentLongRangeSet set = new ConcurrentLongRangeSet(10, 20);
        assertEquals(10, set.min());

        set.add(5, 8);
        assertEquals(5, set.min());

        set.remove(5, 10);
        assertEquals(11, set.min());
    }

    /**
     * Tests the max() method.
     */
    @Test
    @DisplayName("max() should return the maximum value in the set")
    void testMax() {
        final ConcurrentLongRangeSet set = new ConcurrentLongRangeSet(10, 20);
        assertEquals(20, set.max());

        set.add(25, 30);
        assertEquals(30, set.max());

        set.remove(20, 30);
        assertEquals(19, set.max());
    }

    /**
     * Tests min() and max() on an empty set.
     */
    @Test
    @DisplayName("min() and max() should throw IllegalStateException for an empty set")
    void testMinMaxEmptySet() {
        final ConcurrentLongRangeSet set = new ConcurrentLongRangeSet();
        assertEquals(UNKNOWN_BLOCK_NUMBER, set.min());
        assertEquals(UNKNOWN_BLOCK_NUMBER, set.max());
    }
}
