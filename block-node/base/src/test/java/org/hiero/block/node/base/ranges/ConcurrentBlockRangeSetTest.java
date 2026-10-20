// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.base.ranges;

import static org.hiero.block.node.spi.BlockNodePlugin.UNKNOWN_BLOCK_NUMBER;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.List;
import org.hiero.block.api.BlockRange;
import org.hiero.block.node.spi.historicalblocks.BlockRanges;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the {@link ConcurrentBlockRangeSet} class.
 */
class ConcurrentBlockRangeSetTest {

    /**
     * Tests the creation of an empty ConcurrentBlockRangeSet.
     */
    @Test
    @DisplayName("Constructor should create an empty ConcurrentBlockRangeSet")
    void testConstructorEmpty() {
        final ConcurrentBlockRangeSet set = new ConcurrentBlockRangeSet();
        assertEquals(0, set.size());
        assertEquals(0, set.rangeCount());
    }

    /**
     * Tests the creation of a ConcurrentBlockRangeSet with a single range.
     */
    @Test
    @DisplayName("Constructor should create a ConcurrentBlockRangeSet with a single range")
    void testConstructorSingleRange() {
        final ConcurrentBlockRangeSet set = new ConcurrentBlockRangeSet(1, 5);
        assertEquals(5, set.size());
        assertEquals(1, set.rangeCount());
        assertTrue(set.contains(1));
        assertTrue(set.contains(5));
        assertFalse(set.contains(0));
        assertFalse(set.contains(6));
    }

    /**
     * Tests the creation of a ConcurrentBlockRangeSet with multiple ranges.
     */
    @Test
    @DisplayName("Constructor should create a ConcurrentBlockRangeSet with multiple ranges")
    void testConstructorMultipleRanges() {
        final BlockRange range1 = BlockRanges.of(1, 5);
        final BlockRange range2 = BlockRanges.of(10, 15);
        final ConcurrentBlockRangeSet set = new ConcurrentBlockRangeSet(range1, range2);
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
        final ConcurrentBlockRangeSet set = new ConcurrentBlockRangeSet();
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
        final ConcurrentBlockRangeSet set = new ConcurrentBlockRangeSet();
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
        final ConcurrentBlockRangeSet set = new ConcurrentBlockRangeSet(1, 5);
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
        final ConcurrentBlockRangeSet set = new ConcurrentBlockRangeSet(1, 10);
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
        final ConcurrentBlockRangeSet set = new ConcurrentBlockRangeSet(1, 5);
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
        final ConcurrentBlockRangeSet set = new ConcurrentBlockRangeSet(1, 5);
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
        final ConcurrentBlockRangeSet set = new ConcurrentBlockRangeSet(1, 5);
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
        final ConcurrentBlockRangeSet set = new ConcurrentBlockRangeSet(1, 10);
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
        final ConcurrentBlockRangeSet set = new ConcurrentBlockRangeSet(1, 5);
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
        final ConcurrentBlockRangeSet set = new ConcurrentBlockRangeSet(1, 5);
        set.add(10, 15);
        final BlockRange[] ranges = set.streamRanges().toArray(BlockRange[]::new);
        assertArrayEquals(new BlockRange[] {BlockRanges.of(1, 5), BlockRanges.of(10, 15)}, ranges);
    }

    /**
     * Tests the mergeRange method with non-overlapping ranges.
     */
    @Test
    @DisplayName("mergeRange() should correctly handle non-overlapping ranges")
    void testMergeRangeNonOverlapping() {
        final ConcurrentBlockRangeSet set = new ConcurrentBlockRangeSet();

        // Merge into empty list
        List<BlockRange> result = set.mergeRange(Collections.emptyList(), BlockRanges.of(5, 10));
        assertEquals(1, result.size());
        assertEquals(BlockRanges.of(5, 10), result.getFirst());

        // Merge before existing range
        List<BlockRange> existing = List.of(BlockRanges.of(20, 30));
        result = set.mergeRange(existing, BlockRanges.of(5, 10));
        assertEquals(2, result.size());
        assertEquals(BlockRanges.of(5, 10), result.get(0));
        assertEquals(BlockRanges.of(20, 30), result.get(1));

        // Merge after existing range
        existing = List.of(BlockRanges.of(5, 10));
        result = set.mergeRange(existing, BlockRanges.of(20, 30));
        assertEquals(2, result.size());
        assertEquals(BlockRanges.of(5, 10), result.get(0));
        assertEquals(BlockRanges.of(20, 30), result.get(1));

        // Merge between existing ranges
        existing = List.of(BlockRanges.of(5, 10), BlockRanges.of(20, 30));
        result = set.mergeRange(existing, BlockRanges.of(15, 17));
        assertEquals(3, result.size());
        assertEquals(BlockRanges.of(5, 10), result.get(0));
        assertEquals(BlockRanges.of(15, 17), result.get(1));
        assertEquals(BlockRanges.of(20, 30), result.get(2));
    }

    /**
     * Tests the mergeRange method with overlapping ranges.
     */
    @Test
    @DisplayName("mergeRange() should correctly merge overlapping ranges")
    void testMergeRangeOverlapping() {
        final ConcurrentBlockRangeSet set = new ConcurrentBlockRangeSet();

        // Merge with partial overlap at beginning
        List<BlockRange> existing = List.of(BlockRanges.of(5, 10));
        List<BlockRange> result = set.mergeRange(existing, BlockRanges.of(3, 7));
        assertEquals(1, result.size());
        assertEquals(BlockRanges.of(3, 10), result.getFirst());

        // Merge with partial overlap at end
        existing = List.of(BlockRanges.of(5, 10));
        result = set.mergeRange(existing, BlockRanges.of(8, 15));
        assertEquals(1, result.size());
        assertEquals(BlockRanges.of(5, 15), result.getFirst());

        // Merge completely contained
        existing = List.of(BlockRanges.of(5, 15));
        result = set.mergeRange(existing, BlockRanges.of(7, 12));
        assertEquals(1, result.size());
        assertEquals(BlockRanges.of(5, 15), result.getFirst());

        // Merge completely containing
        existing = List.of(BlockRanges.of(7, 12));
        result = set.mergeRange(existing, BlockRanges.of(5, 15));
        assertEquals(1, result.size());
        assertEquals(BlockRanges.of(5, 15), result.getFirst());

        // Merge overlapping multiple ranges
        existing = List.of(BlockRanges.of(5, 10), BlockRanges.of(15, 20), BlockRanges.of(25, 30));
        result = set.mergeRange(existing, BlockRanges.of(8, 27));
        assertEquals(1, result.size());
        assertEquals(BlockRanges.of(5, 30), result.getFirst());
    }

    /**
     * Tests the mergeRange method with adjacent ranges.
     */
    @Test
    @DisplayName("mergeRange() should correctly merge adjacent ranges")
    void testMergeRangeAdjacent() {
        final ConcurrentBlockRangeSet set = new ConcurrentBlockRangeSet();

        // Merge adjacent at end
        List<BlockRange> existing = List.of(BlockRanges.of(5, 10));
        List<BlockRange> result = set.mergeRange(existing, BlockRanges.of(11, 15));
        assertEquals(1, result.size());
        assertEquals(BlockRanges.of(5, 15), result.getFirst());

        // Merge adjacent at beginning
        existing = List.of(BlockRanges.of(5, 10));
        result = set.mergeRange(existing, BlockRanges.of(1, 4));
        assertEquals(1, result.size());
        assertEquals(BlockRanges.of(1, 10), result.getFirst());

        // Merge adjacent between multiple ranges
        existing = List.of(BlockRanges.of(1, 5), BlockRanges.of(11, 15));
        result = set.mergeRange(existing, BlockRanges.of(6, 10));
        assertEquals(1, result.size());
        assertEquals(BlockRanges.of(1, 15), result.getFirst());
    }

    /**
     * Tests adding a range that's already contained in the set.
     */
    @Test
    @DisplayName("add() should not modify the set when adding an already contained range")
    void testAddContainedRange() {
        final ConcurrentBlockRangeSet set = new ConcurrentBlockRangeSet(1, 10);

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
     * Tests the add(BlockRange) method.
     */
    @Test
    @DisplayName("add(BlockRange) should add a range to the set")
    void testAddLongRange() {
        final ConcurrentBlockRangeSet set = new ConcurrentBlockRangeSet();

        set.add(BlockRanges.of(5, 10));
        assertTrue(set.contains(5, 10));
        assertEquals(6, set.size());
        assertEquals(1, set.rangeCount());

        set.add(BlockRanges.of(15, 20));
        assertTrue(set.contains(15, 20));
        assertEquals(12, set.size());
        assertEquals(2, set.rangeCount());

        // Add an overlapping range
        set.add(BlockRanges.of(8, 17));
        assertTrue(set.contains(8, 17));
        assertEquals(16, set.size());
        assertEquals(1, set.rangeCount());
    }

    /**
     * Tests the addAll(BlockRange...) method.
     */
    @Test
    @DisplayName("addAll(BlockRange...) should add multiple ranges to the set")
    void testAddAllVarArgs() {
        final ConcurrentBlockRangeSet set = new ConcurrentBlockRangeSet();

        set.addAll(BlockRanges.of(5, 10), BlockRanges.of(15, 20));
        assertTrue(set.contains(5, 10));
        assertTrue(set.contains(15, 20));
        assertEquals(12, set.size());
        assertEquals(2, set.rangeCount());

        // Add overlapping ranges
        set.addAll(BlockRanges.of(8, 17), BlockRanges.of(25, 30));
        assertTrue(set.contains(5, 20));
        assertTrue(set.contains(25, 30));
        assertEquals(22, set.size());
        assertEquals(2, set.rangeCount());
    }

    /**
     * Tests the addAll(Collection<BlockRange>...) method.
     */
    @Test
    @DisplayName("addAll(Collection<BlockRange>...) should add multiple collections of ranges")
    void testAddAllCollections() {
        final ConcurrentBlockRangeSet set = new ConcurrentBlockRangeSet();

        List<BlockRange> collection1 = List.of(BlockRanges.of(5, 10), BlockRanges.of(15, 20));
        List<BlockRange> collection2 = List.of(BlockRanges.of(25, 30), BlockRanges.of(35, 40));

        set.addAll(collection1, collection2);

        assertTrue(set.contains(5, 10));
        assertTrue(set.contains(15, 20));
        assertTrue(set.contains(25, 30));
        assertTrue(set.contains(35, 40));
        assertEquals(24, set.size());
        assertEquals(4, set.rangeCount());
    }

    /**
     * Tests the addAll(ConcurrentBlockRangeSet) method.
     */
    @Test
    @DisplayName("addAll(ConcurrentBlockRangeSet) should add all ranges from another set")
    void testAddAllSet() {
        final ConcurrentBlockRangeSet set1 = new ConcurrentBlockRangeSet(5, 10);
        final ConcurrentBlockRangeSet set2 = new ConcurrentBlockRangeSet(15, 20);

        set1.addAll(set2);

        assertTrue(set1.contains(5, 10));
        assertTrue(set1.contains(15, 20));
        assertEquals(12, set1.size());
        assertEquals(2, set1.rangeCount());

        // Adding a set with overlapping ranges
        final ConcurrentBlockRangeSet set3 = new ConcurrentBlockRangeSet();
        set3.add(8, 17);

        set1.addAll(set3);
        assertTrue(set1.contains(5, 20));
        assertEquals(16, set1.size());
        assertEquals(1, set1.rangeCount());
    }

    /**
     * Tests the contains(BlockRange) method.
     */
    @Test
    @DisplayName("contains(BlockRange) should correctly determine if a range is in the set")
    void testContainsLongRange() {
        final ConcurrentBlockRangeSet set = new ConcurrentBlockRangeSet(1, 10);

        assertTrue(set.contains(BlockRanges.of(1, 10)));
        assertTrue(set.contains(BlockRanges.of(3, 7)));
        assertFalse(set.contains(BlockRanges.of(0, 5)));
        assertFalse(set.contains(BlockRanges.of(5, 15)));
    }

    /**
     * Tests the remove(BlockRange) method.
     */
    @Test
    @DisplayName("remove(BlockRange) should remove a range from the set")
    void testRemoveLongRange() {
        final ConcurrentBlockRangeSet set = new ConcurrentBlockRangeSet(1, 10);

        set.remove(BlockRanges.of(3, 7));
        assertFalse(set.contains(5));
        assertTrue(set.contains(1));
        assertTrue(set.contains(10));
        assertEquals(5, set.size());
        assertEquals(2, set.rangeCount());
    }

    /**
     * Tests the removeAll(BlockRange...) method.
     */
    @Test
    @DisplayName("removeAll(BlockRange...) should remove multiple ranges from the set")
    void testRemoveAllVarArgs() {
        final ConcurrentBlockRangeSet set = new ConcurrentBlockRangeSet(1, 20);

        set.removeAll(BlockRanges.of(3, 7), BlockRanges.of(12, 15));
        assertFalse(set.contains(5));
        assertFalse(set.contains(14));
        assertTrue(set.contains(1));
        assertTrue(set.contains(10));
        assertTrue(set.contains(20));
        assertEquals(11, set.size());
        assertEquals(3, set.rangeCount());
    }

    /**
     * Tests the removeAll(Collection<BlockRange>...) method.
     */
    @Test
    @DisplayName("removeAll(Collection<BlockRange>...) should remove multiple collections of ranges")
    void testRemoveAllCollections() {
        final ConcurrentBlockRangeSet set = new ConcurrentBlockRangeSet(1, 30);

        List<BlockRange> collection1 = List.of(BlockRanges.of(3, 7), BlockRanges.of(12, 15));
        List<BlockRange> collection2 = List.of(BlockRanges.of(20, 25));

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
     * Tests the removeAll(ConcurrentBlockRangeSet) method.
     */
    @Test
    @DisplayName("removeAll(ConcurrentBlockRangeSet) should remove all ranges from the set")
    void testRemoveAllSet() {
        final ConcurrentBlockRangeSet set1 = new ConcurrentBlockRangeSet(1, 30);
        final ConcurrentBlockRangeSet set2 = new ConcurrentBlockRangeSet();
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
        final ConcurrentBlockRangeSet set = new ConcurrentBlockRangeSet();

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
        final ConcurrentBlockRangeSet emptySet = new ConcurrentBlockRangeSet();
        final ConcurrentBlockRangeSet set = new ConcurrentBlockRangeSet(1, 5);

        // Adding empty set to a set
        set.addAll(emptySet);
        assertEquals(5, set.size());
        assertEquals(1, set.rangeCount());

        // Adding a set to empty set
        emptySet.addAll(set);
        assertEquals(5, emptySet.size());
        assertEquals(1, emptySet.rangeCount());

        // Removing empty set from a set
        set.removeAll(new ConcurrentBlockRangeSet());
        assertEquals(5, set.size());

        // Removing a set from empty set
        final ConcurrentBlockRangeSet anotherEmptySet = new ConcurrentBlockRangeSet();
        anotherEmptySet.removeAll(set);
        assertEquals(0, anotherEmptySet.size());
        assertEquals(0, anotherEmptySet.rangeCount());

        // Adding and removing from empty set
        final ConcurrentBlockRangeSet testSet = new ConcurrentBlockRangeSet();
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
        final ConcurrentBlockRangeSet set = new ConcurrentBlockRangeSet();

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
        final ConcurrentBlockRangeSet set = new ConcurrentBlockRangeSet();

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
        final ConcurrentBlockRangeSet set = new ConcurrentBlockRangeSet(10, 20);
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
        final ConcurrentBlockRangeSet set = new ConcurrentBlockRangeSet(10, 20);
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
        final ConcurrentBlockRangeSet set = new ConcurrentBlockRangeSet();
        assertEquals(UNKNOWN_BLOCK_NUMBER, set.min());
        assertEquals(UNKNOWN_BLOCK_NUMBER, set.max());
    }

    /**
     * Tests that the varargs constructor merges unordered, overlapping and adjacent ranges.
     */
    @Test
    @DisplayName("constructor should sort and merge unordered overlapping and adjacent ranges")
    void testConstructorMergesUnorderedRanges() {
        final ConcurrentBlockRangeSet set = new ConcurrentBlockRangeSet(
                BlockRanges.of(50, 60),
                BlockRanges.of(11, 20),
                BlockRanges.of(0, 10),
                BlockRanges.of(55, 70),
                BlockRanges.of(100, 100));
        assertEquals(List.of(BlockRanges.of(0, 20), BlockRanges.of(50, 70), BlockRanges.of(100, 100)), set.toList());
    }

    /**
     * Tests that bulk adds from unordered collections and other sets merge correctly.
     */
    @Test
    @DisplayName("addAll variants should merge unordered input with existing ranges")
    void testAddAllMergesUnorderedInput() {
        final ConcurrentBlockRangeSet set = new ConcurrentBlockRangeSet(BlockRanges.of(10, 20), BlockRanges.of(40, 50));
        set.addAll(List.of(BlockRanges.of(60, 70), BlockRanges.of(21, 30), BlockRanges.of(0, 5)));
        assertEquals(
                List.of(BlockRanges.of(0, 5), BlockRanges.of(10, 30), BlockRanges.of(40, 50), BlockRanges.of(60, 70)),
                set.toList());

        set.addAll(new ConcurrentBlockRangeSet(BlockRanges.of(6, 9), BlockRanges.of(31, 39)));
        assertEquals(List.of(BlockRanges.of(0, 50), BlockRanges.of(60, 70)), set.toList());

        // adding nothing, or ranges already contained, leaves the set unchanged
        set.addAll(new ConcurrentBlockRangeSet());
        set.addAll(BlockRanges.of(5, 10), BlockRanges.of(62, 65));
        assertEquals(List.of(BlockRanges.of(0, 50), BlockRanges.of(60, 70)), set.toList());
    }
}
