// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.base.ranges;

import static org.hiero.block.node.spi.BlockNodePlugin.UNKNOWN_BLOCK_NUMBER;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.hiero.block.node.spi.historicalblocks.BlockRangeSet;
import org.hiero.block.node.spi.historicalblocks.LongRange;

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
        for (BlockRangeSet set : blockRangeSets) {
            if (set.contains(start, end)) {
                return true;
            }
        }
        return false;
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
        final ConcurrentLongRangeSet combinedSet = new ConcurrentLongRangeSet();
        Arrays.stream(blockRangeSets)
                .flatMap(BlockRangeSet::streamRanges)
                .collect(Collectors.toSet())
                .forEach(combinedSet::add);
        return combinedSet.stream();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stream<LongRange> streamRanges() {
        return new ConcurrentLongRangeSet(Arrays.stream(blockRangeSets)
                        .flatMap(BlockRangeSet::streamRanges)
                        .toArray(LongRange[]::new))
                .streamRanges();
    }
}
