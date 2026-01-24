// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

/**
 * Defines the index positions of items in a wrapped block structure.
 *
 * <p>A wrapped block has the following structure:
 * <pre>
 * [0] BLOCK_HEADER
 * [1] RECORD_FILE
 * [2] STATE_CHANGES (insert point for amendments, optional)
 * [N-1] BLOCK_FOOTER (signals end of hashed content)
 * [N] BLOCK_PROOF
 * </pre>
 *
 * <p>When amendments (STATE_CHANGES) are inserted at index 2, BLOCK_FOOTER and
 * BLOCK_PROOF shift to higher indices.
 */
public enum WrappedBlockIndex {
    /** Block header - always at index 0 */
    BLOCK_HEADER(0),
    /** Record file item - always at index 1 */
    RECORD_FILE(1),
    /** Insert point for STATE_CHANGES amendments - index 2 before BLOCK_FOOTER */
    STATE_CHANGES(2);

    private final int index;

    WrappedBlockIndex(int index) {
        this.index = index;
    }

    /**
     * Returns the index position in the block item list.
     *
     * @return the index
     */
    public int index() {
        return index;
    }
}
