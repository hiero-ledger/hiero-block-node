// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link WrappedBlockIndex}.
 *
 * <p>Verifies the block structure positions for wrapped blocks:
 * <pre>
 * [0] BLOCK_HEADER
 * [1] RECORD_FILE
 * [2] STATE_CHANGES (insert point for genesis amendments)
 * [N-1] BLOCK_FOOTER (shifts when STATE_CHANGES inserted)
 * [N] BLOCK_PROOF (shifts when STATE_CHANGES inserted)
 * </pre>
 */
class WrappedBlockIndexTest {

    @Test
    @DisplayName("BLOCK_HEADER is at index 0")
    void testBlockHeaderIndex() {
        assertEquals(0, WrappedBlockIndex.BLOCK_HEADER.index());
    }

    @Test
    @DisplayName("RECORD_FILE is at index 1")
    void testRecordFileIndex() {
        assertEquals(1, WrappedBlockIndex.RECORD_FILE.index());
    }

    @Test
    @DisplayName("STATE_CHANGES insert point is at index 2")
    void testStateChangesIndex() {
        assertEquals(2, WrappedBlockIndex.STATE_CHANGES.index());
    }

    @Test
    @DisplayName("Block structure order is BLOCK_HEADER < RECORD_FILE < STATE_CHANGES")
    void testBlockStructureOrder() {
        assertTrue(
                WrappedBlockIndex.BLOCK_HEADER.index() < WrappedBlockIndex.RECORD_FILE.index(),
                "BLOCK_HEADER should come before RECORD_FILE");
        assertTrue(
                WrappedBlockIndex.RECORD_FILE.index() < WrappedBlockIndex.STATE_CHANGES.index(),
                "RECORD_FILE should come before STATE_CHANGES");
    }

    @Test
    @DisplayName("All enum values have valid indices")
    void testAllEnumValuesHaveValidIndices() {
        for (WrappedBlockIndex index : WrappedBlockIndex.values()) {
            assertTrue(index.index() >= 0, "Index should be non-negative for " + index);
        }
    }

    @Test
    @DisplayName("Enum has exactly 3 values")
    void testEnumValueCount() {
        assertEquals(
                3,
                WrappedBlockIndex.values().length,
                "WrappedBlockIndex should have 3 values: BLOCK_HEADER, RECORD_FILE, STATE_CHANGES");
    }
}
