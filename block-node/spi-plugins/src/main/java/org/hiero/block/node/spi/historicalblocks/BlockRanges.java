// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.historicalblocks;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.LongStream;
import org.hiero.block.api.BlockRange;

/// Utility methods for working with [BlockRange], the protobuf type used to represent a contiguous, inclusive
/// range of block numbers end to end.
///
/// A [BlockRange] is a generated record and cannot validate itself, so ranges should be created with
/// [#of(long, long)] wherever the values are not already known to be valid. Valid ranges have a start and end
/// between 0 and `Long.MAX_VALUE - 1` inclusive, with start less than or equal to end. This ensures that [#size]
/// can represent any valid range with a `long` without risk of overflow.
public final class BlockRanges {
    /// Comparator for comparing [BlockRange] objects by their start and end values.
    public static final Comparator<BlockRange> COMPARATOR =
            Comparator.comparingLong(BlockRange::rangeStart).thenComparingLong(BlockRange::rangeEnd);

    private BlockRanges() {
        throw new UnsupportedOperationException("Utility Class");
    }

    /// Creates a validated [BlockRange].
    ///
    /// @param start the first block number of the range (inclusive), between 0 and `Long.MAX_VALUE - 1`
    /// @param end the last block number of the range (inclusive), between 0 and `Long.MAX_VALUE - 1`
    /// @return the new range
    /// @throws IllegalArgumentException if start or end is negative or greater than `Long.MAX_VALUE - 1`, or if
    ///     start is greater than end
    @NonNull
    public static BlockRange of(final long start, final long end) {
        if (start < 0) {
            throw new IllegalArgumentException("BlockRange start: %d must not be negative".formatted(start));
        }
        if (end < 0) {
            throw new IllegalArgumentException("BlockRange end: %d must not be negative".formatted(end));
        }
        if (end > Long.MAX_VALUE - 1) {
            throw new IllegalArgumentException(
                    "BlockRange end: %d must not be greater than Long.MAX_VALUE-1".formatted(end));
        }
        if (start > end) {
            throw new IllegalArgumentException(
                    "BlockRange start: %d must not be greater than end: %d".formatted(start, end));
        }
        return new BlockRange(start, end);
    }

    /// Checks if the range contains a specific block number.
    ///
    /// @param range the range to check
    /// @param value the block number to look for
    /// @return true if the range contains the block number, false otherwise
    public static boolean contains(@NonNull final BlockRange range, final long value) {
        return value >= range.rangeStart() && value <= range.rangeEnd();
    }

    /// Checks if the range contains another range specified by start and end values.
    ///
    /// @param range the range to check
    /// @param start the first block number of the range to look for
    /// @param end the last block number of the range to look for
    /// @return true if the range contains the specified range, false otherwise
    public static boolean contains(@NonNull final BlockRange range, final long start, final long end) {
        if (start > end) {
            // if the caller supplies invalid values, responsibility falls on them, we must return a response that
            // makes sense based on input, in this case false, as no range can start after it ends
            return false;
        }
        return start >= range.rangeStart() && end <= range.rangeEnd();
    }

    /// Gets the number of block numbers in the range, computed as end - start + 1.
    ///
    /// @param range the range to measure
    /// @return the size of the range
    public static long size(@NonNull final BlockRange range) {
        return range.rangeEnd() - range.rangeStart() + 1;
    }

    /// Checks if two ranges overlap.
    ///
    /// @param a the first range
    /// @param b the second range
    /// @return true if the ranges overlap, false otherwise
    public static boolean overlaps(@NonNull final BlockRange a, @NonNull final BlockRange b) {
        return !(a.rangeEnd() < b.rangeStart() || a.rangeStart() > b.rangeEnd());
    }

    /// Checks if two ranges are adjacent, i.e. one starts immediately after the other ends.
    ///
    /// @param a the first range
    /// @param b the second range
    /// @return true if the ranges are adjacent, false otherwise
    public static boolean isAdjacent(@NonNull final BlockRange a, @NonNull final BlockRange b) {
        return a.rangeEnd() + 1 == b.rangeStart() || b.rangeEnd() + 1 == a.rangeStart();
    }

    /// Merges two ranges into the smallest range covering both.
    ///
    /// @param a the first range
    /// @param b the second range
    /// @return a new range covering both
    @NonNull
    public static BlockRange merge(@NonNull final BlockRange a, @NonNull final BlockRange b) {
        return new BlockRange(Math.min(a.rangeStart(), b.rangeStart()), Math.max(a.rangeEnd(), b.rangeEnd()));
    }

    /// Merges a list of ranges into a minimal list of non-overlapping ranges. Overlapping or adjacent ranges are
    /// combined into single ranges. Unlike [#merge(List)], the input may be unordered; it is sorted first.
    ///
    /// @param ranges the ranges to merge, in any order
    /// @return a new list, in ascending order, with contiguous and overlapping ranges merged
    @NonNull
    public static List<BlockRange> mergeContiguousRanges(@NonNull final List<BlockRange> ranges) {
        if (ranges.size() <= 1) {
            return ranges;
        }
        final List<BlockRange> sorted = new ArrayList<>(ranges);
        sorted.sort(COMPARATOR);
        return merge(List.of(sorted));
    }

    /// Merges two ascending lists of ranges into a single ascending list. Ranges that overlap or are adjacent
    /// are combined.
    ///
    /// @param a the first list; ascending and non-overlapping
    /// @param b the second list; ascending and non-overlapping
    /// @return a new immutable list in ascending order
    /// @see #merge(List)
    @NonNull
    public static List<BlockRange> merge(@NonNull final List<BlockRange> a, @NonNull final List<BlockRange> b) {
        return merge(List.of(a, b));
    }

    /// Merges any number of ascending lists of ranges into a single ascending list in one pass. Ranges that
    /// overlap or are adjacent, within a list or across lists, are combined.
    ///
    /// Each pass takes the remaining range with the lowest start from among the heads of the lists, so the cost
    /// is O(N * k) for N ranges in k lists. Lists are expected to be few (one per block provider), which makes the
    /// linear scan of the heads cheaper than a priority queue.
    ///
    /// @param sortedLists the lists to merge; each ascending and non-overlapping. May be empty, as may any list.
    /// @return a new immutable list in ascending order
    @NonNull
    public static List<BlockRange> merge(@NonNull final List<List<BlockRange>> sortedLists) {
        final int[] cursors = new int[sortedLists.size()];
        final List<BlockRange> merged = new ArrayList<>();
        long start = 0;
        long end = 0;
        boolean open = false;
        int pick = pickNext(sortedLists, cursors);
        while (pick >= 0) {
            final BlockRange next = sortedLists.get(pick).get(cursors[pick]++);
            if (open && next.rangeStart() <= end + 1) {
                end = Math.max(end, next.rangeEnd());
            } else {
                if (open) {
                    merged.add(new BlockRange(start, end));
                }
                start = next.rangeStart();
                end = next.rangeEnd();
                open = true;
            }
            pick = pickNext(sortedLists, cursors);
        }
        if (open) {
            merged.add(new BlockRange(start, end));
        }
        return List.copyOf(merged);
    }

    /// Finds the list whose next unconsumed range starts lowest.
    ///
    /// @return the index of that list, or -1 if every list is exhausted
    private static int pickNext(final List<List<BlockRange>> sortedLists, final int[] cursors) {
        int pick = -1;
        long lowestStart = Long.MAX_VALUE;
        for (int i = 0; i < cursors.length; i++) {
            final List<BlockRange> list = sortedLists.get(i);
            if (cursors[i] < list.size()) {
                final long candidate = list.get(cursors[i]).rangeStart();
                if (pick < 0 || candidate < lowestStart) {
                    pick = i;
                    lowestStart = candidate;
                }
            }
        }
        return pick;
    }

    /// Creates a stream of the block numbers within the range.
    ///
    /// @param range the range to stream
    /// @return a `LongStream` of the block numbers in the range
    @NonNull
    public static LongStream stream(@NonNull final BlockRange range) {
        return LongStream.rangeClosed(range.rangeStart(), range.rangeEnd());
    }

    /// Returns a string representation of the range in the format "start->end".
    ///
    /// @param range the range to format
    /// @return the string representation
    @NonNull
    public static String toString(@NonNull final BlockRange range) {
        return range.rangeStart() + "->" + range.rangeEnd();
    }
}
