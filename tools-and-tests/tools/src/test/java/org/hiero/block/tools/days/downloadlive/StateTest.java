// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.downloadlive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link State}.
 */
@DisplayName("State Tests")
public class StateTest {

    @Test
    @DisplayName("Should create State with day key and block number")
    void testConstructor() {
        String dayKey = "2025-12-01";
        long lastSeenBlock = 12345L;
        long lastSeenTimestmap = 1494L;

        State state = new State(dayKey, lastSeenBlock, lastSeenTimestmap);

        assertEquals(dayKey, state.getDayKey());
        assertEquals(lastSeenBlock, state.getLastSeenBlock());
        assertEquals(lastSeenTimestmap, state.getLastSeenTimestamp());
    }

    @Test
    @DisplayName("Should handle zero block number")
    void testZeroBlockNumber() {
        String dayKey = "2019-09-13";
        long lastSeenBlock = 0L;
        long lastSeenTimestmap = 0L;

        State state = new State(dayKey, lastSeenBlock, lastSeenTimestmap);

        assertEquals(0L, state.getLastSeenBlock());
        assertEquals(0L, state.getLastSeenTimestamp());
    }

    @Test
    @DisplayName("Should handle large block numbers")
    void testLargeBlockNumber() {
        String dayKey = "2025-12-31";
        long lastSeenBlock = Long.MAX_VALUE;
        long lastSeenTimestmap = 1494L;

        State state = new State(dayKey, lastSeenBlock, lastSeenTimestmap);

        assertEquals(Long.MAX_VALUE, state.getLastSeenBlock());
        assertEquals(lastSeenTimestmap, state.getLastSeenTimestamp());
    }

    @Test
    @DisplayName("Should preserve day key format")
    void testDayKeyFormat() {
        String dayKey = "2025-12-01";
        long lastSeenBlock = 100L;

        State state = new State(dayKey, lastSeenBlock, 100L);

        assertEquals("2025-12-01", state.getDayKey());
        assertTrue(state.getDayKey().matches("\\d{4}-\\d{2}-\\d{2}"));
    }

    @Test
    @DisplayName("Should handle negative block number for error scenarios")
    void testNegativeBlockNumber() {
        String dayKey = "2025-12-01";
        long lastSeenBlock = -1L;
        long lastSeenTimestmap = -1L;

        State state = new State(dayKey, lastSeenBlock, lastSeenTimestmap);

        assertEquals(-1L, state.getLastSeenBlock());
        assertEquals(-1L, state.getLastSeenTimestamp());
    }

    @Test
    @DisplayName("Should be immutable")
    void testImmutability() {
        String dayKey = "2025-12-01";
        long lastSeenBlock = 12345L;
        long lastSeenTimestmap = 1494L;
        State state = new State(dayKey, lastSeenBlock, lastSeenTimestmap);

        String dayKey1 = state.getDayKey();
        String dayKey2 = state.getDayKey();
        long block1 = state.getLastSeenBlock();
        long block2 = state.getLastSeenBlock();

        assertSame(dayKey1, dayKey2);
        assertEquals(block1, block2);
    }
}
