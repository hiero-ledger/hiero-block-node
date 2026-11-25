// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.base.ranges;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import org.hiero.block.api.BlockRange;
import org.junit.jupiter.api.Test;

/// Unit tests for [ImmutableBlockRangeSet].
class ImmutableBlockRangeSetTest {
    private final ImmutableBlockRangeSet set =
            new ImmutableBlockRangeSet(List.of(new BlockRange(0, 9), new BlockRange(20, 29), new BlockRange(40, 40)));

    @Test
    void emptySetHasUnknownMinAndMax() {
        final ImmutableBlockRangeSet empty = new ImmutableBlockRangeSet(List.of());
        assertThat(empty.size()).isZero();
        assertThat(empty.min()).isEqualTo(-1L);
        assertThat(empty.max()).isEqualTo(-1L);
        assertThat(empty.contains(0)).isFalse();
        assertThat(empty.stream().count()).isZero();
    }

    @Test
    void minMaxAndSize() {
        assertThat(set.min()).isZero();
        assertThat(set.max()).isEqualTo(40L);
        assertThat(set.size()).isEqualTo(21L);
    }

    @Test
    void containsSingleBlocks() {
        for (final long in : new long[] {0, 5, 9, 20, 29, 40}) {
            assertThat(set.contains(in)).as("contains %d", in).isTrue();
        }
        for (final long out : new long[] {-1, 10, 19, 30, 39, 41, 1000}) {
            assertThat(set.contains(out)).as("contains %d", out).isFalse();
        }
    }

    @Test
    void containsRanges() {
        assertThat(set.contains(2, 7)).isTrue();
        assertThat(set.contains(0, 9)).isTrue();
        assertThat(set.contains(0, 10)).isFalse();
        assertThat(set.contains(5, 25)).isFalse();
        assertThat(set.contains(40, 40)).isTrue();
        assertThat(set.contains(9, 0)).isFalse();
        assertThat(set.contains(new BlockRange(20, 29))).isTrue();
    }

    @Test
    void streamsAndToListReflectTheRanges() {
        assertThat(set.stream().count()).isEqualTo(21L);
        assertThat(set.streamRanges().toList())
                .containsExactly(new BlockRange(0, 9), new BlockRange(20, 29), new BlockRange(40, 40));
        assertThat(set.toList()).isSameAs(set.toList());
    }

    @Test
    void touchingRangesAreMergedSoSpanningQueriesWork() {
        final ImmutableBlockRangeSet touching =
                new ImmutableBlockRangeSet(List.of(new BlockRange(0, 9), new BlockRange(10, 19)));
        assertThat(touching.contains(0, 15)).isTrue();
        assertThat(touching.contains(0, 19)).isTrue();
        assertThat(touching.contains(0, 20)).isFalse();
        assertThat(touching.toList()).containsExactly(new BlockRange(0, 19));
        assertThat(touching.size()).isEqualTo(20L);
    }

    @Test
    void rangesWithAGapAreNotMerged() {
        assertThat(set.contains(0, 20)).isFalse();
        assertThat(set.toList()).hasSize(3);
    }

    @Test
    void streamYieldsEveryBlockInOrder() {
        assertThat(set.stream().boxed().toList())
                .startsWith(0L, 1L, 2L)
                .contains(9L, 20L, 29L, 40L)
                .doesNotContain(10L, 19L, 30L)
                .hasSize(21);
    }

    @Test
    void toListIsUnmodifiable() {
        assertThatThrownBy(() -> set.toList().add(new BlockRange(50, 60)))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void toStringListsTheRanges() {
        assertThat(set.toString()).startsWith("ImmutableBlockRangeSet ").contains("0");
    }

    @Test
    void emptySetHasEmptyListAndStreams() {
        final ImmutableBlockRangeSet empty = new ImmutableBlockRangeSet(List.of());
        assertThat(empty.toList()).isEmpty();
        assertThat(empty.streamRanges().toList()).isEmpty();
        assertThat(empty.contains(0, 0)).isFalse();
    }

    @Test
    void singleBlockRange() {
        final ImmutableBlockRangeSet single = new ImmutableBlockRangeSet(List.of(new BlockRange(7, 7)));
        assertThat(single.size()).isEqualTo(1L);
        assertThat(single.min()).isEqualTo(7L);
        assertThat(single.max()).isEqualTo(7L);
        assertThat(single.contains(7)).isTrue();
        assertThat(single.contains(6)).isFalse();
        assertThat(single.contains(8)).isFalse();
        assertThat(single.stream().boxed().toList()).containsExactly(7L);
    }

    @Test
    void changingTheInputListAfterConstructionHasNoEffect() {
        final List<BlockRange> input = new ArrayList<>(List.of(new BlockRange(0, 9)));
        final ImmutableBlockRangeSet copy = new ImmutableBlockRangeSet(input);
        input.add(new BlockRange(20, 29));
        assertThat(copy.size()).isEqualTo(10L);
        assertThat(copy.max()).isEqualTo(9L);
        assertThat(copy.contains(25)).isFalse();
    }

    @Test
    void unmodifiableInputIsNotCopied() {
        final List<BlockRange> input = List.of(new BlockRange(0, 9), new BlockRange(20, 29));
        assertThat(new ImmutableBlockRangeSet(input).toList()).isSameAs(input);
    }

    @Test
    void manyTouchingRangesMergeIntoOne() {
        final ImmutableBlockRangeSet merged = new ImmutableBlockRangeSet(List.of(
                new BlockRange(0, 4),
                new BlockRange(5, 9),
                new BlockRange(10, 14),
                new BlockRange(30, 34),
                new BlockRange(35, 39)));
        assertThat(merged.toList()).containsExactly(new BlockRange(0, 14), new BlockRange(30, 39));
        assertThat(merged.size()).isEqualTo(25L);
        assertThat(merged.contains(0, 14)).isTrue();
        assertThat(merged.contains(30, 39)).isTrue();
        assertThat(merged.contains(14, 30)).isFalse();
    }

    @Test
    void overlappingRangesAreMerged() {
        final ImmutableBlockRangeSet merged =
                new ImmutableBlockRangeSet(List.of(new BlockRange(0, 10), new BlockRange(5, 15)));
        assertThat(merged.toList()).containsExactly(new BlockRange(0, 15));
        assertThat(merged.size()).isEqualTo(16L);
    }

    @Test
    void containsHandlesBoundaryQueries() {
        assertThat(set.contains(-5, 3)).isFalse();
        assertThat(set.contains(20, 29)).isTrue();
        assertThat(set.contains(20, 30)).isFalse();
        assertThat(set.contains(19, 20)).isFalse();
        assertThat(set.contains(41, 41)).isFalse();
        assertThat(set.contains(Long.MAX_VALUE)).isFalse();
    }

    @Test
    void streamRangesMatchesToList() {
        assertThat(set.streamRanges().toList()).isEqualTo(set.toList());
    }

    @Test
    void streamIsInAscendingOrder() {
        final List<Long> blocks = set.stream().boxed().toList();
        assertThat(blocks).isSorted();
        assertThat(blocks).hasSize((int) set.size());
    }

    @Test
    void largeBlockNumbers() {
        final long base = 5_000_000_000L;
        final ImmutableBlockRangeSet large = new ImmutableBlockRangeSet(
                List.of(new BlockRange(base, base + 99), new BlockRange(base + 200, base + 299)));
        assertThat(large.size()).isEqualTo(200L);
        assertThat(large.min()).isEqualTo(base);
        assertThat(large.max()).isEqualTo(base + 299);
        assertThat(large.contains(base + 150)).isFalse();
        assertThat(large.contains(base + 250)).isTrue();
    }
}
