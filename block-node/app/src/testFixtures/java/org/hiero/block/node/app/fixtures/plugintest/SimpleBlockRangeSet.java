// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.plugintest;

import static org.hiero.block.node.spi.BlockNodePlugin.UNKNOWN_BLOCK_NUMBER;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.hiero.block.node.spi.historicalblocks.BlockRangeSet;
import org.hiero.block.node.spi.historicalblocks.LongRange;

/**
 * A simple implementation of {@link BlockRangeSet} that uses a {@link ConcurrentSkipListSet} to store the block numbers.
 */
public class SimpleBlockRangeSet implements BlockRangeSet {
    /** The set of block numbers. */
    private final ConcurrentSkipListSet<Long> blockNumbers = new ConcurrentSkipListSet<>();

    /**
     * Add a block number to the set.
     *
     * @param blockNumber the block number to add
     */
    public void add(long blockNumber) {
        blockNumbers.add(blockNumber);
    }

    /**
     * Add a range of block numbers to the set.
     *
     * @param start of the range to add (inclusive)
     * @param end of the range to add (inclusive)
     */
    public void add(final long start, final long end) {
        for (long i = start; i <= end; i++) {
            blockNumbers.add(i);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean contains(long blockNumber) {
        return blockNumbers.contains(blockNumber);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean contains(long start, long end) {
        for (long i = start; i <= end; i++) {
            if (!blockNumbers.contains(i)) {
                return false;
            }
        }
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long size() {
        return blockNumbers.size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long min() {
        return blockNumbers.isEmpty() ? UNKNOWN_BLOCK_NUMBER : blockNumbers.first();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long max() {
        return blockNumbers.isEmpty() ? UNKNOWN_BLOCK_NUMBER : blockNumbers.last();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LongStream stream() {
        return blockNumbers.stream().mapToLong(Long::longValue);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stream<LongRange> streamRanges() {
        if (blockNumbers.isEmpty()) {
            return Stream.empty();
        }

        List<LongRange> ranges = new ArrayList<>();

        Iterator<Long> it = blockNumbers.iterator();
        long start = it.next(); // first element
        long prev = start;

        while (it.hasNext()) {
            long cur = it.next();
            if (cur != prev + 1) {
                // break in continuity → close out the previous range
                ranges.add(new LongRange(start, prev));
                start = cur;
            }
            prev = cur;
        }

        // finally, add the last open range [start … prev]
        ranges.add(new LongRange(start, prev));

        return ranges.stream();
    }
}
