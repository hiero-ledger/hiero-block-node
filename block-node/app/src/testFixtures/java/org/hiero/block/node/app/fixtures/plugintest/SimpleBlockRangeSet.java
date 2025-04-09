// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.plugintest;

import static org.hiero.block.node.spi.BlockNodePlugin.UNKNOWN_BLOCK_NUMBER;

import java.util.ArrayList;
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
        return blockNumbers.isEmpty() ? 0 : blockNumbers.first();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long max() {
        return blockNumbers.isEmpty() ? 0 : blockNumbers.last();
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
        } else {
            final List<LongRange> ranges = new ArrayList<>();
            long start = UNKNOWN_BLOCK_NUMBER;
            long current = UNKNOWN_BLOCK_NUMBER;

            for (long i : blockNumbers) {
                if (start == UNKNOWN_BLOCK_NUMBER) {
                    start = i;
                } else if (i != current + 1) {
                    ranges.add(new LongRange(start, current));
                    start = i;
                }
                current = i;
            }
            if (start != UNKNOWN_BLOCK_NUMBER && current != UNKNOWN_BLOCK_NUMBER) {
                ranges.clear();
            } else if (ranges.getLast().end() != current) {
                // extra range for last items
                ranges.add(new LongRange(start, start));
            }
            return ranges.stream();
        }
    }
}
