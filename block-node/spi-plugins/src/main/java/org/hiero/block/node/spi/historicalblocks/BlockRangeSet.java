// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.historicalblocks;

import static org.hiero.block.node.spi.BlockNodePlugin.UNKNOWN_BLOCK_NUMBER;

import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * Interface representing a set of long block numbers stored as contiguous ranges. It provides methods to add, remove,
 * and query ranges or individual block numbers.
 */
public interface BlockRangeSet {
    /**
     * An empty block range set. This is a singleton instance that represents an empty set of block numbers.
     */
    BlockRangeSet EMPTY = new BlockRangeSet() {

        @Override
        public boolean contains(long blockNumber) {
            return false;
        }

        @Override
        public boolean contains(long start, long end) {
            return false;
        }

        @Override
        public long size() {
            return 0;
        }

        @Override
        public long min() {
            return UNKNOWN_BLOCK_NUMBER;
        }

        @Override
        public long max() {
            return UNKNOWN_BLOCK_NUMBER;
        }

        @Override
        public LongStream stream() {
            return LongStream.empty();
        }

        @Override
        public Stream<LongRange> streamRanges() {
            return Stream.empty();
        }
    };

    /**
     * Checks if the set contains a specific block number.
     *
     * @param blockNumber the block number to check
     * @return true if the block number is contained in the set, false otherwise
     */
    boolean contains(long blockNumber);

    /**
     * Checks if the set contains a specific range of block numbers.
     *
     * @param start the start block number of the range (inclusive)
     * @param end the end block number of the range (inclusive)
     * @return true if the range is contained in the set, false otherwise
     */
    boolean contains(long start, long end);

    /**
     * Returns the total number of block numbers in the set.
     *
     * @return the total number of block numbers
     */
    long size();

    /**
     * Returns the minimum block number in the set.
     *
     * @return the minimum block number, or UNKNOWN_BLOCK_NUMBER if the set is empty
     */
    long min();

    /**
     * Returns the maximum block number in the set.
     *
     * @return the maximum block number, or UNKNOWN_BLOCK_NUMBER if the set is empty
     */
    long max();

    /**
     * Returns a stream of all block numbers in the set.
     *
     * @return a stream of block numbers
     */
    LongStream stream();

    /**
     * Returns a stream of all block number ranges in the set.
     *
     * @return a stream of block number ranges
     */
    Stream<LongRange> streamRanges();
}
