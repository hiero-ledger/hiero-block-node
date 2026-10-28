// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.base.ranges;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.hiero.block.api.BlockRange;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.historicalblocks.BlockRangeSet;
import org.hiero.block.node.spi.historicalblocks.BlockRanges;

/**
 * Collection that represents a set of long values. This is represented internally as a set of contiguous ranges of
 * long values. This will be very efficient for storing large sets of long values that have large contiguous areas.
 * This is useful for storing large sets of block numbers values that are used in a block stream.
 * <p>
 * This class is thread-safe and can be used in a multithreaded environment. It uses a lock-free algorithm to provide
 * a high level of performance.
 * <p>
 * Note: All ranges and total range in this set must comply with BlockRange constraints: values between 0 and
 * Long.MAX_VALUE-1 inclusive.
 */
@SuppressWarnings("unused")
public class ConcurrentBlockRangeSet implements BlockRangeSet {
    /**
     * The main data for this set. It is an atomic reference to an immutable {@link List}.All other changes mean the
     * atomic reference's value will be replaced with a new immutable list. This means that all changes to the
     * list are atomic and thread-safe. This is always sorted and non-overlapping.
     */
    private final AtomicReference<List<BlockRange>> ranges = new AtomicReference<>(Collections.emptyList());

    /**
     * Constructs a ConcurrentBlockRangeSet with no ranges.
     */
    public ConcurrentBlockRangeSet() {}

    /**
     * Constructs a ConcurrentBlockRangeSet with single range.
     *
     * @param start the start value (inclusive) must be between 0 and Long.MAX_VALUE-1
     * @param end the end value (inclusive) must be between 0 and Long.MAX_VALUE-1
     * @throws IllegalArgumentException if the range is invalid
     */
    public ConcurrentBlockRangeSet(long start, long end) {
        ranges.set(List.of(BlockRanges.of(start, end)));
    }

    /**
     * Constructs a ConcurrentBlockRangeSet with multiple ranges. If there is more than one range they will be merged into a single
     * set of ranges with no overlaps or duplication.
     *
     * @param longRanges the ranges to include in this set
     * @throws NullPointerException if longRanges is null
     */
    public ConcurrentBlockRangeSet(BlockRange... longRanges) {
        this.ranges.set(
                longRanges.length == 0
                        ? Collections.emptyList()
                        : BlockRanges.mergeContiguousRanges(List.of(longRanges)));
    }

    /**
     * Checks if this ConcurrentBlockRangeSet contains a specific long value.
     *
     * @param value the long value to check
     * @return true if the value is contained in any of the ranges, false otherwise
     */
    @Override
    public boolean contains(long value) {
        if (value < 0 || value > Long.MAX_VALUE - 1) {
            return false;
        }

        final List<BlockRange> ranges = this.ranges.get();
        int low = 0;
        int high = ranges.size() - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            BlockRange range = ranges.get(mid);
            if (BlockRanges.contains(range, value)) {
                return true;
            } else if (range.rangeStart() > value) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return false;
    }

    /**
     * Checks if this ConcurrentBlockRangeSet contains a specific range.
     *
     * @param start the first value in range
     * @param end the last value in range
     * @return true if the range is contained in any of the ranges, false otherwise
     */
    @Override
    public boolean contains(long start, long end) {
        if (start < 0 || end > Long.MAX_VALUE - 1 || start > end) {
            return false;
        }

        final List<BlockRange> ranges = this.ranges.get();
        int low = 0;
        int high = ranges.size() - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            BlockRange range = ranges.get(mid);
            if (BlockRanges.contains(range, start, end)) {
                return true;
            } else if (range.rangeStart() > end) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return false;
    }

    /**
     * Checks if this ConcurrentBlockRangeSet contains a specific range.
     *
     * @param range the range to check
     * @return true if the range is contained in any of the ranges, false otherwise
     */
    public boolean contains(BlockRange range) {
        return contains(range.rangeStart(), range.rangeEnd());
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
        // BlockRange constructor will validate the range
        add(BlockRanges.of(start, end));
    }

    /**
     * Adds a new range to the set. This will append to the ends of any existing ranges if one matches otherwise it
     * will add a new range. Only new values that are not in the set will be added.
     *
     * @param range the range to add
     */
    public void add(BlockRange range) {
        while (true) {
            List<BlockRange> currentRanges = ranges.get();
            List<BlockRange> newRanges = mergeRange(currentRanges, range);

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
     * Adds multiple new ranges to the set. This will expand any existing ranges if they match otherwise it will add one
     * or more new ranges. Only new values that are not in the set will be added.
     *
     * @param ranges the ranges to add
     */
    public void addAll(BlockRange... ranges) {
        addMerged(BlockRanges.mergeContiguousRanges(List.of(ranges)));
    }

    /**
     * Adds all the ranges in the supplied collections to this set in a single linear merge.
     *
     * @param ranges the collections of ranges to add, each in any order
     */
    @SafeVarargs
    public final void addAll(Collection<BlockRange>... ranges) {
        final List<BlockRange> all = new ArrayList<>();
        for (Collection<BlockRange> rangeCollection : ranges) {
            all.addAll(rangeCollection);
        }
        addMerged(BlockRanges.mergeContiguousRanges(all));
    }

    /**
     * Adds all the ranges in another set to this set in a single linear merge.
     *
     * @param ranges the set whose ranges to add
     */
    public void addAll(ConcurrentBlockRangeSet ranges) {
        addMerged(ranges.ranges.get());
    }

    /**
     * Adds all the ranges in another set to this set in a single linear merge.
     *
     * @param ranges the set whose ranges to add
     */
    public void addAll(BlockRangeSet ranges) {
        addMerged(BlockRanges.mergeContiguousRanges(ranges.streamRanges().toList()));
    }

    /**
     * Merges ranges that are already ascending and non-overlapping into this set, retrying if another thread
     * changed the set in the meantime. O(n + m) for n existing and m incoming ranges.
     */
    private void addMerged(final List<BlockRange> incoming) {
        if (incoming.isEmpty()) {
            return;
        }
        while (true) {
            final List<BlockRange> currentRanges = ranges.get();
            final List<BlockRange> newRanges = BlockRanges.merge(currentRanges, incoming);
            if (newRanges.equals(currentRanges) || ranges.compareAndSet(currentRanges, newRanges)) {
                return;
            }
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
        remove(BlockRanges.of(start, end));
    }

    /**
     * Removes a range from the set. This will remove the range from any existing ranges. If the range is not in the set,
     * no change will be made.
     *
     * @param range the range to remove
     */
    public void remove(BlockRange range) {
        while (true) {
            List<BlockRange> currentRanges = ranges.get();
            List<BlockRange> newRanges = removeRange(currentRanges, range);

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
     * Removes multiple ranges from the set. This will remove the ranges from any existing ranges. If the ranges are not
     * in the set, no change will be made.
     *
     * @param ranges the ranges to remove
     */
    public void removeAll(BlockRange... ranges) {
        for (BlockRange range : ranges) {
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
    public final void removeAll(Collection<BlockRange>... ranges) {
        for (Collection<BlockRange> rangeCollection : ranges) {
            for (BlockRange range : rangeCollection) {
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
    public void removeAll(ConcurrentBlockRangeSet ranges) {
        List<BlockRange> otherRanges = ranges.ranges.get();
        for (BlockRange range : otherRanges) {
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
    List<BlockRange> mergeRange(List<BlockRange> currentRanges, BlockRange newRange) { // package for tests
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
            BlockRange range = currentRanges.get(mid);

            if (range.rangeEnd() < newRange.rangeStart() - 1) {
                // Range is completely before newRange
                low = mid + 1;
                insertPosition = low;
            } else if (range.rangeStart() > newRange.rangeEnd() + 1) {
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
                && (BlockRanges.overlaps(currentRanges.get(firstAffected - 1), newRange)
                        || BlockRanges.isAdjacent(currentRanges.get(firstAffected - 1), newRange))) {
            firstAffected--;
        }

        int lastAffected = insertPosition;
        // Ensure we don't go out of bounds
        while (lastAffected < size - 1
                && (BlockRanges.overlaps(currentRanges.get(lastAffected + 1), newRange)
                        || BlockRanges.isAdjacent(currentRanges.get(lastAffected + 1), newRange))) {
            lastAffected++;
        }

        // Check if insertPosition is valid, and we need to merge with any existing range
        // Fix: Check that firstAffected is in bounds and either overlaps or is adjacent to newRange
        if (firstAffected < size
                && (BlockRanges.overlaps(currentRanges.get(firstAffected), newRange)
                        || BlockRanges.isAdjacent(currentRanges.get(firstAffected), newRange))) {

            // Calculate the merged range
            long mergedStart = Math.min(currentRanges.get(firstAffected).rangeStart(), newRange.rangeStart());
            long mergedEnd = Math.max(currentRanges.get(lastAffected).rangeEnd(), newRange.rangeEnd());
            BlockRange mergedRange = BlockRanges.of(mergedStart, mergedEnd);

            // Create a new list with the merged range
            List<BlockRange> result = new ArrayList<>(size - (lastAffected - firstAffected));
            result.addAll(currentRanges.subList(0, firstAffected));
            result.add(mergedRange);
            result.addAll(currentRanges.subList(lastAffected + 1, size));
            return Collections.unmodifiableList(result);
        } else {
            // No overlap/adjacent, just insert the new range
            List<BlockRange> result = new ArrayList<>(size + 1);
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
    private List<BlockRange> removeRange(List<BlockRange> currentRanges, BlockRange rangeToRemove) {
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
            BlockRange range = currentRanges.get(mid);

            if (range.rangeEnd() < rangeToRemove.rangeStart()) {
                // Range ends before removal range starts
                low = mid + 1;
            } else if (range.rangeStart() > rangeToRemove.rangeEnd()) {
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
            BlockRange range = currentRanges.get(mid);

            if (range.rangeStart() > rangeToRemove.rangeEnd()) {
                // Range starts after removal range ends
                high = mid - 1;
            } else {
                // Range might overlap with removal range
                if (BlockRanges.overlaps(range, rangeToRemove)) {
                    lastAffectedIndex = mid;
                }
                low = mid + 1;
            }
        }

        // Build new list with affected ranges modified/removed
        List<BlockRange> result = new ArrayList<>(size + 2); // At most 2 extra ranges (from splitting)

        // Add ranges before the affected area
        result.addAll(currentRanges.subList(0, firstAffectedIndex));

        // Process affected ranges
        for (int i = firstAffectedIndex; i <= lastAffectedIndex; i++) {
            BlockRange currentRange = currentRanges.get(i);

            // Check if there's a part of the range that should remain before the removal range
            if (currentRange.rangeStart() < rangeToRemove.rangeStart()) {
                result.add(BlockRanges.of(currentRange.rangeStart(), rangeToRemove.rangeStart() - 1));
            }

            // Check if there's a part of the range that should remain after the removal range
            if (currentRange.rangeEnd() > rangeToRemove.rangeEnd()) {
                result.add(BlockRanges.of(rangeToRemove.rangeEnd() + 1, currentRange.rangeEnd()));
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
    @Override
    public long size() {
        return ranges.get().stream().mapToLong(BlockRanges::size).sum();
    }

    /**
     * Returns the minimum value in the set.
     *
     * @return the minimum value, or BlockNodePlugin.UNKNOWN_BLOCK_NUMBER if the set is empty
     */
    @Override
    public long min() {
        final List<BlockRange> currentRanges = ranges.get();
        if (currentRanges.isEmpty()) {
            return BlockNodePlugin.UNKNOWN_BLOCK_NUMBER;
        }
        return currentRanges.getFirst().rangeStart();
    }

    /**
     * Returns the maximum value in the set.
     *
     * @return the maximum value, or BlockNodePlugin.UNKNOWN_BLOCK_NUMBER if the set is empty
     */
    @Override
    public long max() {
        final List<BlockRange> currentRanges = ranges.get();
        if (currentRanges.isEmpty()) {
            return BlockNodePlugin.UNKNOWN_BLOCK_NUMBER;
        }
        return currentRanges.getLast().rangeEnd();
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
    @Override
    public LongStream stream() {
        return ranges.get().stream().flatMapToLong(BlockRanges::stream);
    }

    /**
     * Returns a stream of ranges in the set.
     *
     * @return a stream of ranges in the set
     */
    @Override
    public Stream<BlockRange> streamRanges() {
        return ranges.get().stream();
    }

    /**
     * Returns the ranges in the set as an unmodifiable list, without the overhead of streaming.
     *
     * @return an unmodifiable, ascending, non-overlapping list of the ranges in the set
     */
    public List<BlockRange> toList() {
        return Collections.unmodifiableList(ranges.get());
    }

    /**
     * Returns a string representation of the set.
     *
     * @return a string representation of the set in the format: "ConcurrentBlockRangeSet{start->end, start->end, ...}"
     */
    @Override
    public String toString() {
        return "ConcurrentBlockRangeSet{"
                + ranges.get().stream().map(BlockRanges::toString).collect(Collectors.joining(", "))
                + '}';
    }
}
