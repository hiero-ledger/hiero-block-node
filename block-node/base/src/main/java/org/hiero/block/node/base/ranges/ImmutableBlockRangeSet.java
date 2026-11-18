// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.base.ranges;

import static org.hiero.block.node.spi.BlockNodePlugin.UNKNOWN_BLOCK_NUMBER;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.hiero.block.api.BlockRange;
import org.hiero.block.node.spi.historicalblocks.BlockRangeSet;

/// A [BlockRangeSet] that never changes after it is created, so it can be shared between threads and handed
/// out without copying.
///
/// It is built once from a list of ranges that is already in ascending order and does not overlap. Ranges
/// that touch (one ends where the next begins minus one) are merged, so a range query that spans them is
/// answered correctly. Lookups use a binary search over the range starts, and `size`, `min` and `max` are
/// precomputed.
public final class ImmutableBlockRangeSet implements BlockRangeSet {
    /// The ranges, in ascending order and not overlapping.
    private final List<BlockRange> ranges;
    /// The start of each range, parallel to `ranges`, for binary search.
    private final long[] starts;
    /// The total number of block numbers in all the ranges.
    private final long size;

    /// Creates a set from ranges in ascending order that do not overlap.
    ///
    /// @param ranges the ranges, in ascending order and not overlapping; copied, with touching ranges merged
    public ImmutableBlockRangeSet(@NonNull final List<BlockRange> ranges) {
        this.ranges = mergeTouching(ranges);
        this.starts = this.ranges.stream().mapToLong(BlockRange::rangeStart).toArray();
        this.size = this.ranges.stream()
                .mapToLong(r -> r.rangeEnd() - r.rangeStart() + 1)
                .sum();
    }

    /// Merges ranges that touch. Returns an unmodifiable copy; when nothing needs merging this is
    /// `List.copyOf`, which does not copy a list that is already unmodifiable.
    private static List<BlockRange> mergeTouching(final List<BlockRange> input) {
        boolean touching = false;
        for (int i = 1; i < input.size(); i++) {
            if (input.get(i).rangeStart() <= input.get(i - 1).rangeEnd() + 1) {
                touching = true;
                break;
            }
        }
        if (!touching) {
            return List.copyOf(input);
        }
        final List<BlockRange> merged = new ArrayList<>(input.size());
        long start = input.getFirst().rangeStart();
        long end = input.getFirst().rangeEnd();
        for (int i = 1; i < input.size(); i++) {
            final BlockRange next = input.get(i);
            if (next.rangeStart() <= end + 1) {
                end = Math.max(end, next.rangeEnd());
            } else {
                merged.add(new BlockRange(start, end));
                start = next.rangeStart();
                end = next.rangeEnd();
            }
        }
        merged.add(new BlockRange(start, end));
        return List.copyOf(merged);
    }

    /// The index of the range that contains the block number, or -1 if none does.
    private int indexOfRangeContaining(final long blockNumber) {
        final int found = Arrays.binarySearch(starts, blockNumber);
        // an exact hit is the start of a range; otherwise the candidate is the range before the insertion point
        final int candidate = found >= 0 ? found : -found - 2;
        return candidate >= 0 && blockNumber <= ranges.get(candidate).rangeEnd() ? candidate : -1;
    }

    /// {@inheritDoc}
    @Override
    public boolean contains(final long blockNumber) {
        return indexOfRangeContaining(blockNumber) >= 0;
    }

    /// {@inheritDoc}
    @Override
    public boolean contains(final long start, final long end) {
        if (start > end) {
            return false;
        }
        final int index = indexOfRangeContaining(start);
        return index >= 0 && end <= ranges.get(index).rangeEnd();
    }

    /// {@inheritDoc}
    @Override
    public long size() {
        return size;
    }

    /// {@inheritDoc}
    @Override
    public long min() {
        return ranges.isEmpty() ? UNKNOWN_BLOCK_NUMBER : ranges.getFirst().rangeStart();
    }

    /// {@inheritDoc}
    @Override
    public long max() {
        return ranges.isEmpty() ? UNKNOWN_BLOCK_NUMBER : ranges.getLast().rangeEnd();
    }

    /// {@inheritDoc}
    @Override
    public LongStream stream() {
        return ranges.stream().flatMapToLong(r -> LongStream.rangeClosed(r.rangeStart(), r.rangeEnd()));
    }

    /// {@inheritDoc}
    @Override
    public Stream<BlockRange> streamRanges() {
        return ranges.stream();
    }

    /// {@inheritDoc}
    ///
    /// Returns the list this set holds, which is already unmodifiable, without copying.
    @Override
    public List<BlockRange> toList() {
        return ranges;
    }

    /// {@inheritDoc}
    @Override
    public String toString() {
        return "ImmutableBlockRangeSet " + ranges;
    }
}
