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
import org.hiero.block.node.spi.historicalblocks.BlockRanges;

/**
 * A class that combines multiple {@link BlockRangeSet} instances into a single set. The combining is done live via
 * reference so as the underlying sets change, this set will reflect those changes.
 */
@SuppressWarnings("ClassCanBeRecord")
public class CombinedBlockRangeSet implements BlockRangeSet {
    /**
     * The block range sets to combine. This array is fixed at creation time and its contents does not change but each
     * set referenced can have changing contents.
     */
    public final BlockRangeSet[] blockRangeSets;

    /**
     * Creates a new CombinedBlockRangeSet with the specified block range sets.
     *
     * @param blockRangeSets the block range sets to combine
     */
    public CombinedBlockRangeSet(BlockRangeSet... blockRangeSets) {
        this.blockRangeSets = blockRangeSets;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean contains(long blockNumber) {
        for (BlockRangeSet set : blockRangeSets) {
            if (set.contains(blockNumber)) {
                return true;
            }
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean contains(long start, long end) {
        if (start > end) {
            return false;
        }
        // the merged ranges are coalesced, so a contiguous run must be inside one of them
        return mergedRanges().stream().anyMatch(range -> BlockRanges.contains(range, start, end));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long size() {
        long totalSize = 0;
        for (BlockRangeSet set : blockRangeSets) {
            totalSize += set.size();
        }
        return totalSize;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long min() {
        return Arrays.stream(blockRangeSets)
                .mapToLong(BlockRangeSet::min)
                .filter(min -> min != UNKNOWN_BLOCK_NUMBER)
                .min()
                .orElse(UNKNOWN_BLOCK_NUMBER);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long max() {
        return Arrays.stream(blockRangeSets)
                .mapToLong(BlockRangeSet::max)
                .filter(max -> max != UNKNOWN_BLOCK_NUMBER)
                .max()
                .orElse(UNKNOWN_BLOCK_NUMBER);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LongStream stream() {
        return mergedRanges().stream().flatMapToLong(BlockRanges::stream);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stream<BlockRange> streamRanges() {
        return mergedRanges().stream();
    }

    /// Merges the ranges of every set in one linear pass. Each set yields its ranges in ascending order.
    @NonNull
    private List<BlockRange> mergedRanges() {
        return BlockRanges.merge(Arrays.stream(blockRangeSets)
                .map(set -> set.streamRanges().toList())
                .toList());
    }
}
