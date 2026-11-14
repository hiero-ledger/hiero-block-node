// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.base.ranges;

import static org.hiero.block.node.spi.BlockNodePlugin.UNKNOWN_BLOCK_NUMBER;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Arrays;
import java.util.List;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.hiero.block.api.BlockRange;
import org.hiero.block.node.spi.historicalblocks.BlockRangeSet;

/// A [BlockRangeSet] that never changes after it is created, so it can be shared between threads and handed
/// out without copying.
///
/// It is built once from a list of ranges that is already in ascending order and does not overlap. Lookups
/// use a binary search over the range starts, and `size`, `min` and `max` are precomputed.
public final class ImmutableBlockRangeSet implements BlockRangeSet {
    /// The ranges, in ascending order and not overlapping.
    private final List<BlockRange> ranges;
    /// The start of each range, parallel to `ranges`, for binary search.
    private final long[] starts;
    /// The total number of block numbers in all the ranges.
    private final long size;

    /// Creates a set from ranges in ascending order that do not overlap.
    ///
    /// @param ranges the ranges, in ascending order and not overlapping; copied
    public ImmutableBlockRangeSet(@NonNull final List<BlockRange> ranges) {
        this.ranges = List.copyOf(ranges);
        this.starts = this.ranges.stream().mapToLong(BlockRange::rangeStart).toArray();
        this.size = this.ranges.stream()
                .mapToLong(r -> r.rangeEnd() - r.rangeStart() + 1)
                .sum();
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
        return "ImmutableBlockRangeSet" + ranges;
    }
}
