// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.state;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.hiero.block.api.BlockRange;
import org.hiero.block.node.base.ranges.ConcurrentLongRangeSet;
import org.hiero.block.node.spi.historicalblocks.BlockRangeSet;
import org.hiero.block.node.spi.historicalblocks.LongRange;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/// Microbenchmark of the per-call cost of `toBlockRange` plus `mergeRanges`, the work done on every
/// `updateAvailableBlocks()` call (see issue #3803). It compares the current implementation with the
/// previous one, [#legacyMergeRanges]. It prints timings and asserts nothing, so it is disabled unless
/// the environment variable `RUN_BENCHMARKS` is `true`:
///
/// ```
/// RUN_BENCHMARKS=true ./gradlew :facility-app-state:cleanTest :facility-app-state:test \
///     --tests '*MergeRangesBenchmarkTest' --no-build-cache -i
/// ```
@EnabledIfEnvironmentVariable(named = "RUN_BENCHMARKS", matches = "true")
class MergeRangesBenchmarkTest {
    /// The previous implementation is quadratic, so it is not run beyond this many ranges.
    private static final int MAX_LEGACY_RANGES = 100_000;

    @Test
    void measureSnapshotAndMergeCost() {
        for (final int ranges : new int[] {1, 10, 100, 1_000, 10_000, 100_000, 1_000_000}) {
            final List<LongRange> availableRanges = new ArrayList<>(ranges);
            for (int i = 0; i < ranges; i++) {
                availableRanges.add(new LongRange(i * 10L, i * 10L + 5));
            }
            final BlockRangeSet available = new FixedRanges(availableRanges);
            final BlockRangeSet stored = new FixedRanges(List.of(new LongRange(0, 3)));

            final double current = microsPerCall(ranges, () -> {
                ApplicationStateUtility.toBlockRange(available);
                ApplicationStateUtility.mergeRanges(stored, available);
            });
            final String legacy = ranges > MAX_LEGACY_RANGES
                    ? "skipped"
                    : "%.2f".formatted(microsPerCall(ranges, () -> {
                        ApplicationStateUtility.toBlockRange(available);
                        legacyMergeRanges(stored, available);
                    }));
            System.out.printf("MEASURE ranges=%d current us/call=%.2f legacy us/call=%s%n", ranges, current, legacy);
        }
    }

    /// Times `work`, using fewer iterations for the larger sets so the run stays short.
    private static double microsPerCall(final int ranges, final Runnable work) {
        final int iterations = ranges >= 100_000 ? 20 : ranges >= 10_000 ? 200 : 20_000;
        final int warmup = ranges >= 100_000 ? 5 : ranges >= 10_000 ? 50 : 5_000;
        for (int i = 0; i < warmup; i++) {
            work.run();
        }
        final long start = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            work.run();
        }
        return (System.nanoTime() - start) / 1000.0 / iterations;
    }

    /// The previous `mergeRanges`: re-adds every range of both sets into a new `ConcurrentLongRangeSet`,
    /// which copies its list on each add, so it is O(n<sup>2</sup>).
    private static List<BlockRange> legacyMergeRanges(final BlockRangeSet stored, final BlockRangeSet available) {
        final ConcurrentLongRangeSet combined = new ConcurrentLongRangeSet();
        combined.addAll(stored);
        combined.addAll(available);
        return combined.streamRanges()
                .map(longRange -> new BlockRange(longRange.start(), longRange.end()))
                .toList();
    }

    /// A read-only range set over a prebuilt list. `ConcurrentLongRangeSet` copies its list on every add, so
    /// building a set of a million ranges with it would dominate the run.
    private record FixedRanges(List<LongRange> ranges) implements BlockRangeSet {
        @Override
        public boolean contains(final long blockNumber) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean contains(final long start, final long end) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long size() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long min() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long max() {
            throw new UnsupportedOperationException();
        }

        @Override
        public LongStream stream() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Stream<LongRange> streamRanges() {
            return ranges.stream();
        }
    }
}
