// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.base.ranges;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * Collection that represents a set of long values. This is represented internally as a set of contiguous ranges of
 * long values. This will be very efficient for storing large sets of long values that have large contiguous areas.
 * This is useful for storing large sets of block numbers values that are used in a block stream.
 * <p>
 * This class is thread-safe and can be used in a multithreaded environment. It uses a lock-free algorithm to provide
 * a high level of performance.
 * <p>
 * Note: All ranges and total range in this set must comply with LongRange constraints: values between 0 and
 * Long.MAX_VALUE-1 inclusive.
 */
@SuppressWarnings("unused")
public class ConcurrentLongRangeSet {
    /**
     * The main data for this set. It is an atomic reference to an immutable {@link List}.All other changes mean the
     * atomic reference's value will be replaced with a new immutable list. This means that all changes to the
     * list are atomic and thread-safe. This is always sorted and non-overlapping.
     */
    private final AtomicReference<List<LongRange>> ranges = new AtomicReference<>(Collections.emptyList());

    /**
     * Constructs a LongRangeSet with no ranges.
     */
    public ConcurrentLongRangeSet() {}

    /**
     * Constructs a LongRangeSet with single range.
     *
     * @param start the start value (inclusive) must be between 0 and Long.MAX_VALUE-1
     * @param end the end value (inclusive) must be between 0 and Long.MAX_VALUE-1
     * @throws IllegalArgumentException if the range is invalid
     */
    public ConcurrentLongRangeSet(long start, long end) {
        ranges.set(List.of(new LongRange(start, end)));
    }

    /**
     * Constructs a LongRangeSet with multiple ranges.
     *
     * @param longRanges the ranges to include in this set
     * @throws NullPointerException if longRanges is null
     */
    public ConcurrentLongRangeSet(LongRange... longRanges) {
        ranges.set(List.of(longRanges));
    }

    /**
     * Checks if this LongRangeSet contains a specific long value.
     *
     * @param value the long value to check
     * @return true if the value is contained in any of the ranges, false otherwise
     */
    public boolean contains(long value) {
        if (value < 0 || value > Long.MAX_VALUE - 1) {
            return false;
        }

        final List<LongRange> ranges = this.ranges.get();
        int low = 0;
        int high = ranges.size() - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            LongRange range = ranges.get(mid);
            if (range.contains(value)) {
                return true;
            } else if (range.start() > value) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return false;
    }

    /**
     * Checks if this LongRangeSet contains a specific range.
     *
     * @param start the first value in range
     * @param end the last value in range
     * @return true if the range is contained in any of the ranges, false otherwise
     */
    public boolean contains(long start, long end) {
        if (start < 0 || end > Long.MAX_VALUE - 1 || start > end) {
            return false;
        }

        final List<LongRange> ranges = this.ranges.get();
        int low = 0;
        int high = ranges.size() - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            LongRange range = ranges.get(mid);
            if (range.contains(start, end)) {
                return true;
            } else if (range.start() > end) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return false;
    }

    /**
     * Checks if this LongRangeSet contains a specific range.
     *
     * @param range the range to check
     * @return true if the range is contained in any of the ranges, false otherwise
     */
    public boolean contains(LongRange range) {
        return contains(range.start(), range.end());
    }

    /**
     * Adds a new value to the set. This will append to the ends of any existing ranges if one matches otherwise it
     * will add a new range. If the value is already in the set, no change will be made.
     *
     * @param value the single value to add
     * @throws IllegalArgumentException if the value is negative or greater than Long.MAX_VALUE-1
     */
    public void add(long value) {
        add(value, value);
    }

    /**
     * Adds a new range to the set. This will append to the ends of any existing ranges if one matches otherwise it
     * will add a new range. Only new values that are not in the set will be added.
     *
     * @param start the first value in range
     * @param end the last value in range
     * @throws IllegalArgumentException if the range is invalid
     */
    public void add(long start, long end) {
        // LongRange constructor will validate the range
        LongRange newRange = new LongRange(start, end);

        while (true) {
            List<LongRange> currentRanges = ranges.get();
            List<LongRange> newRanges = mergeRange(currentRanges, newRange);

            // If ranges didn't change (already contained all values), no need to update
            if (newRanges == currentRanges) {
                return;
            }

            // Try to update the reference atomically
            if (ranges.compareAndSet(currentRanges, newRanges)) {
                return;
            }
            // If CAS fails, retry with current ranges
        }
    }

    /**
     * Adds a new range to the set. This will append to the ends of any existing ranges if one matches otherwise it
     * will add a new range. Only new values that are not in the set will be added.
     *
     * @param range the range to add
     */
    public void add(LongRange range) {
        add(range.start(), range.end());
    }

    /**
     * Adds multiple new ranges to the set. This will expand any existing ranges if they match otherwise it will add one
     * or more new ranges. Only new values that are not in the set will be added.
     *
     * @param ranges the ranges to add
     */
    public void addAll(LongRange... ranges) {
        for (LongRange range : ranges) {
            add(range);
        }
    }

    /**
     * Adds multiple new ranges to the set. This will expand any existing ranges if they match otherwise it will add one
     * or more new ranges. Only new values that are not in the set will be added.
     *
     * @param ranges the ranges to add
     */
    @SafeVarargs
    public final void addAll(Collection<LongRange>... ranges) {
        for (Collection<LongRange> rangeCollection : ranges) {
            for (LongRange range : rangeCollection) {
                add(range);
            }
        }
    }

    /**
     * Adds multiple new ranges to the set. This will expand any existing ranges if they match otherwise it will add one
     * or more new ranges. Only new values that are not in the set will be added.
     *
     * @param ranges the ranges to add
     */
    public void addAll(ConcurrentLongRangeSet ranges) {
        List<LongRange> otherRanges = ranges.ranges.get();
        for (LongRange range : otherRanges) {
            add(range);
        }
    }

    /**
     * Removes a value from the set. This will remove the value from any existing ranges. If the value is not in the set,
     * no change will be made.
     *
     * @param value the single value to remove
     */
    public void remove(long value) {
        remove(value, value);
    }

    /**
     * Removes a range from the set. This will remove the range from any existing ranges. If the range is not in the set,
     * no change will be made.
     *
     * @param start the first value in range
     * @param end the last value in range
     */
    public void remove(long start, long end) {
        if (start > end) {
            throw new IllegalArgumentException("Range start must be less than or equal to end");
        }

        LongRange rangeToRemove = new LongRange(start, end);

        while (true) {
            List<LongRange> currentRanges = ranges.get();
            List<LongRange> newRanges = removeRange(currentRanges, rangeToRemove);

            // If ranges didn't change, no need to update
            if (newRanges == currentRanges) {
                return;
            }

            // Try to update the reference atomically
            if (ranges.compareAndSet(currentRanges, newRanges)) {
                return;
            }
            // If CAS fails, retry with current ranges
        }
    }

    /**
     * Removes a range from the set. This will remove the range from any existing ranges. If the range is not in the set,
     * no change will be made.
     *
     * @param range the range to remove
     */
    public void remove(LongRange range) {
        remove(range.start(), range.end());
    }

    /**
     * Removes multiple ranges from the set. This will remove the ranges from any existing ranges. If the ranges are not
     * in the set, no change will be made.
     *
     * @param ranges the ranges to remove
     */
    public void removeAll(LongRange... ranges) {
        for (LongRange range : ranges) {
            remove(range);
        }
    }

    /**
     * Removes multiple ranges from the set. This will remove the ranges from any existing ranges. If the ranges are not
     * in the set, no change will be made.
     *
     * @param ranges the ranges to remove
     */
    @SafeVarargs
    public final void removeAll(Collection<LongRange>... ranges) {
        for (Collection<LongRange> rangeCollection : ranges) {
            for (LongRange range : rangeCollection) {
                remove(range);
            }
        }
    }

    /**
     * Removes multiple ranges from the set. This will remove the ranges from any existing ranges. If the ranges are not
     * in the set, no change will be made.
     *
     * @param ranges the ranges to remove
     */
    public void removeAll(ConcurrentLongRangeSet ranges) {
        List<LongRange> otherRanges = ranges.ranges.get();
        for (LongRange range : otherRanges) {
            remove(range);
        }
    }

    /**
     * Helper method that merges a new range into the current list of ranges.
     * Returns a new list if changes were made, otherwise returns the original list.
     *
     * @param currentRanges the current list of ranges
     * @param newRange the new range to merge
     * @return a new list with the merged range, or the original list if no change
     */
    List<LongRange> mergeRange(List<LongRange> currentRanges, LongRange newRange) { // package for tests
        if (currentRanges.isEmpty()) {
            return List.of(newRange); // Just add the new range if the list is empty
        }

        // Find potentially affected ranges
        int size = currentRanges.size();
        int insertPosition = 0;

        // Find potential insertion point using binary search
        int low = 0;
        int high = size - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            LongRange range = currentRanges.get(mid);

            if (range.end() < newRange.start() - 1) {
                // Range is completely before newRange
                low = mid + 1;
                insertPosition = low;
            } else if (range.start() > newRange.end() + 1) {
                // Range is completely after newRange
                high = mid - 1;
            } else {
                // Range overlaps or is adjacent to newRange
                // We need to scan for all affected ranges
                insertPosition = mid;
                break;
            }
        }

        // Find the first and last affected ranges (overlapping or adjacent)
        int firstAffected = insertPosition;
        while (firstAffected > 0
                && (currentRanges.get(firstAffected - 1).overlaps(newRange)
                        || currentRanges.get(firstAffected - 1).isAdjacent(newRange))) {
            firstAffected--;
        }

        int lastAffected = insertPosition;
        // Ensure we don't go out of bounds
        while (lastAffected < size - 1
                && (currentRanges.get(lastAffected + 1).overlaps(newRange)
                        || currentRanges.get(lastAffected + 1).isAdjacent(newRange))) {
            lastAffected++;
        }

        // Check if insertPosition is valid and we need to merge with any existing range
        // Fix: Check that firstAffected is in bounds and either overlaps or is adjacent to newRange
        if (firstAffected < size
                && (currentRanges.get(firstAffected).overlaps(newRange)
                        || currentRanges.get(firstAffected).isAdjacent(newRange))) {

            // Calculate the merged range
            long mergedStart = Math.min(currentRanges.get(firstAffected).start(), newRange.start());
            long mergedEnd = Math.max(currentRanges.get(lastAffected).end(), newRange.end());
            LongRange mergedRange = new LongRange(mergedStart, mergedEnd);

            // Create a new list with the merged range
            List<LongRange> result = new ArrayList<>(size - (lastAffected - firstAffected));
            result.addAll(currentRanges.subList(0, firstAffected));
            result.add(mergedRange);
            result.addAll(currentRanges.subList(lastAffected + 1, size));
            return Collections.unmodifiableList(result);
        } else {
            // No overlap/adjacent, just insert the new range
            List<LongRange> result = new ArrayList<>(size + 1);
            result.addAll(currentRanges.subList(0, insertPosition));
            result.add(newRange);
            result.addAll(currentRanges.subList(insertPosition, size));
            return Collections.unmodifiableList(result);
        }
    }

    /**
     * Helper method that removes a range from the current list of ranges.
     * Returns a new list if changes were made, otherwise returns the original list.
     *
     * @param currentRanges the current list of ranges
     * @param rangeToRemove the range to remove
     * @return a new list with the range removed, or the original list if no change
     */
    private List<LongRange> removeRange(List<LongRange> currentRanges, LongRange rangeToRemove) {
        if (currentRanges.isEmpty()) {
            return currentRanges; // Nothing to remove from an empty list
        }

        int size = currentRanges.size();

        // Find the first and last affected ranges using binary search
        int firstAffectedIndex = -1;
        int lastAffectedIndex = -1;

        // Find the first range that might be affected
        int low = 0;
        int high = size - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            LongRange range = currentRanges.get(mid);

            if (range.end() < rangeToRemove.start()) {
                // Range ends before removal range starts
                low = mid + 1;
            } else if (range.start() > rangeToRemove.end()) {
                // Range starts after removal range ends
                high = mid - 1;
            } else {
                // Range overlaps with removal range
                // Search for the first affected range
                high = mid - 1;
                firstAffectedIndex = mid;
            }
        }

        // No overlaps found
        if (firstAffectedIndex == -1) {
            return currentRanges;
        }

        // Find the last affected range
        low = firstAffectedIndex;
        high = size - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            LongRange range = currentRanges.get(mid);

            if (range.start() > rangeToRemove.end()) {
                // Range starts after removal range ends
                high = mid - 1;
            } else {
                // Range might overlap with removal range
                if (range.overlaps(rangeToRemove)) {
                    lastAffectedIndex = mid;
                }
                low = mid + 1;
            }
        }

        // Build new list with affected ranges modified/removed
        List<LongRange> result = new ArrayList<>(size + 2); // At most 2 extra ranges (from splitting)

        // Add ranges before the affected area
        result.addAll(currentRanges.subList(0, firstAffectedIndex));

        // Process affected ranges
        for (int i = firstAffectedIndex; i <= lastAffectedIndex; i++) {
            LongRange currentRange = currentRanges.get(i);

            // Check if there's a part of the range that should remain before the removal range
            if (currentRange.start() < rangeToRemove.start()) {
                result.add(new LongRange(currentRange.start(), rangeToRemove.start() - 1));
            }

            // Check if there's a part of the range that should remain after the removal range
            if (currentRange.end() > rangeToRemove.end()) {
                result.add(new LongRange(rangeToRemove.end() + 1, currentRange.end()));
            }
        }

        // Add ranges after the affected area
        result.addAll(currentRanges.subList(lastAffectedIndex + 1, size));

        return result.isEmpty() ? Collections.emptyList() : Collections.unmodifiableList(result);
    }

    /**
     * Returns the number of long values in the set.
     *
     * @return the number of long values in the set
     */
    public long size() {
        return ranges.get().stream().mapToLong(LongRange::size).sum();
    }

    /**
     * Returns the number of ranges in the set.
     *
     * @return the number of ranges in the set
     */
    public int rangeCount() {
        return ranges.get().size();
    }

    /**
     * Returns a stream of long values in the set.
     *
     * @return a stream of long values in the set
     */
    public LongStream stream() {
        return ranges.get().stream().flatMapToLong(LongRange::stream);
    }

    /**
     * Returns a stream of ranges in the set.
     *
     * @return a stream of ranges in the set
     */
    public Stream<LongRange> streamRanges() {
        return ranges.get().stream();
    }
}
