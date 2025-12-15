// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.historicalblocks;

import static org.hiero.block.node.spi.BlockNodePlugin.UNKNOWN_BLOCK_NUMBER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Test class for {@link BlockRangeSet}.
 */
class BlockRangeSetTest {

    @Test
    @DisplayName("Test EMPTY set contains no block numbers")
    void testEmptySetContains() {
        final BlockRangeSet emptySet = BlockRangeSet.EMPTY;

        assertFalse(emptySet.contains(1), "EMPTY set should not contain any block number");
        assertFalse(emptySet.contains(0, 10), "EMPTY set should not contain any range");
    }

    @Test
    @DisplayName("Test EMPTY set size is zero")
    void testEmptySetSize() {
        final BlockRangeSet emptySet = BlockRangeSet.EMPTY;

        assertEquals(0, emptySet.size(), "EMPTY set size should be zero");
    }

    @Test
    @DisplayName("Test EMPTY set min and max return UNKNOWN_BLOCK_NUMBER")
    void testEmptySetMinMax() {
        final BlockRangeSet emptySet = BlockRangeSet.EMPTY;

        assertEquals(UNKNOWN_BLOCK_NUMBER, emptySet.min(), "EMPTY set min should return UNKNOWN_BLOCK_NUMBER");
        assertEquals(UNKNOWN_BLOCK_NUMBER, emptySet.max(), "EMPTY set max should return UNKNOWN_BLOCK_NUMBER");
    }

    @Test
    @DisplayName("Test EMPTY set stream is empty")
    void testEmptySetStream() {
        final BlockRangeSet emptySet = BlockRangeSet.EMPTY;

        assertEquals(0, emptySet.stream().count(), "Stream from EMPTY set should be empty");
    }

    @Test
    @DisplayName("Test EMPTY set streamRanges is empty")
    void testEmptySetStreamRanges() {
        final BlockRangeSet emptySet = BlockRangeSet.EMPTY;

        assertEquals(0, emptySet.streamRanges().count(), "Stream of ranges from EMPTY set should be empty");
    }

    @Test
    @DisplayName("Test EMPTY set edge cases")
    void testEmptySetEdgeCases() {
        final BlockRangeSet emptySet = BlockRangeSet.EMPTY;

        assertFalse(emptySet.contains(Long.MIN_VALUE), "EMPTY set should not contain Long.MIN_VALUE");
        assertFalse(emptySet.contains(Long.MAX_VALUE), "EMPTY set should not contain Long.MAX_VALUE");
        assertFalse(emptySet.contains(Long.MIN_VALUE, Long.MAX_VALUE), "EMPTY set should not contain full range");
    }
}
