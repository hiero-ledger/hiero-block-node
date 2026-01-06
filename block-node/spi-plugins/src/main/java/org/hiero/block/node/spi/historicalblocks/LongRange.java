// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.historicalblocks;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Comparator;
import java.util.stream.LongStream;

/**
 * Contiguous range of long values, inclusive of start and end.
 * Valid ranges must have start and end values between 0 and Long.MAX_VALUE-1
 * inclusive, with start less than or equal to end. This ensures that the size()
 * method can correctly represent any valid range with a long value without risk
 * of overflow.
 */
public record LongRange(long start, long end) implements Comparable<LongRange> {
    /** Comparator for comparing LongRange objects by their start and end values. */
    public static final Comparator<LongRange> COMPARATOR =
            Comparator.comparingLong(LongRange::start).thenComparingLong(LongRange::end);

    /**
     * Creates a new LongRange with the specified start and end values.
     *
     * @param start the start value of the range (inclusive), must be between 0
     * and Long.MAX_VALUE-1
     * @param end the end value of the range (inclusive), must be between 0 and
     * Long.MAX_VALUE-1
     * @throws IllegalArgumentException if start or end is negative or greater
     * than Long.MAX_VALUE-1, or if start is greater than end
     */
    public LongRange {
        if (start < 0) {
            throw new IllegalArgumentException("LongRange start: %d must not be negative".formatted(start));
        }
        if (end < 0) {
            throw new IllegalArgumentException("LongRange end: %d must not be negative".formatted(end));
        }
        if (end > Long.MAX_VALUE - 1) {
            throw new IllegalArgumentException(
                    "LongRange end: %d must not be greater than Long.MAX_VALUE-1".formatted(end));
        }
        if (start > end) {
            throw new IllegalArgumentException(
                    "LongRange start: %d must not be greater than end: %d".formatted(start, end));
        }
    }

    /**
     * Checks if the range contains a specific value.
     *
     * @param value the value to check
     * @return true if the range contains the value, false otherwise
     */
    public boolean contains(final long value) {
        return value >= start && value <= end;
    }

    /**
     * Checks if the range contains another range specified by start and end
     * values.
     *
     * @param start the start value of the range to check
     * @param end the end value of the range to check
     * @return true if the range contains the specified range, false otherwise
     */
    public boolean contains(final long start, final long end) {
        if (start > end) {
            // if the caller supplies invalid values, responsibility falls on
            // them, we must return a response that makes sense based on input,
            // in this case false, as no range can start after it ends
            return false;
        } else {
            return start >= this.start && end <= this.end;
        }
    }

    /**
     * Gets the size of the range.
     *
     * @return the size of the range (number of elements),
     * computed as end - start + 1
     */
    public long size() {
        return end - start + 1;
    }

    /**
     * Checks if the range overlaps with another range.
     *
     * @param other the other range to check
     * @return true if the ranges overlap, false otherwise
     */
    public boolean overlaps(final LongRange other) {
        return !(end < other.start() || start > other.end());
    }

    /**
     * Checks if the range is adjacent to another range.
     *
     * @param other the other range to check
     * @return true if the ranges are adjacent, false otherwise
     */
    public boolean isAdjacent(LongRange other) {
        return end + 1 == other.start() || other.end() + 1 == start;
    }

    /**
     * Merges the range with another range.
     *
     * @param other the other range to merge with
     * @return a new ImmutableLongRange representing the merged range
     */
    public LongRange merge(final LongRange other) {
        return new LongRange(Math.min(start, other.start()), Math.max(end, other.end()));
    }

    /**
     * Creates a stream of long values within the range.
     *
     * @return a LongStream of values within the range
     */
    public LongStream stream() {
        return LongStream.rangeClosed(start, end);
    }

    /**
     * Compares this range to another range.
     *
     * @param o the other range to compare to
     * @return a negative integer, zero, or a positive integer as this range is
     * less than, equal to, or greater than the specified range
     */
    @Override
    public int compareTo(@NonNull final LongRange o) {
        return COMPARATOR.compare(this, o);
    }

    /**
     * Returns a string representation of the range.
     *
     * @return a string representation of the range in the format "start->end"
     */
    @NonNull
    @Override
    public String toString() {
        return start + "->" + end;
    }
}
