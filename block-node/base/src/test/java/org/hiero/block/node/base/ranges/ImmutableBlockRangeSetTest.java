// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.base.ranges;

import static org.assertj.core.api.Assertions.assertThat;

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
}
