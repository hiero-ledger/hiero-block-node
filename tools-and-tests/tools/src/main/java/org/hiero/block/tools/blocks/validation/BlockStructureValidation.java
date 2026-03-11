// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

import java.util.List;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.tools.records.model.parsed.ValidationException;

/**
 * Validates that block items appear in the correct order with no unexpected items.
 *
 * <p>The only valid structure for a wrapped block is:
 * <ol>
 *   <li>Exactly one {@code BlockHeader}
 *   <li>Zero or more {@code StateChanges} (genesis amendments for block 0)
 *   <li>Exactly one {@code RecordFile}
 *   <li>Exactly one {@code BlockFooter}
 *   <li>One or more {@code BlockProof} items
 * </ol>
 *
 * <p>This is a stateless validation — no cross-block state is needed.
 */
public final class BlockStructureValidation implements BlockValidation {

    @Override
    public String name() {
        return "Block Structure";
    }

    @Override
    public String description() {
        return "Enforces strict item ordering: BlockHeader, StateChanges*, RecordFile, BlockFooter, BlockProof+";
    }

    @Override
    public boolean requiresGenesisStart() {
        return false;
    }

    @Override
    public void validate(final BlockUnparsed block, final long blockNumber) throws ValidationException {
        final List<BlockItemUnparsed> items = block.blockItems();
        final int size = items.size();
        int index = 0;

        // 1. Exactly one BlockHeader
        if (index >= size || !items.get(index).hasBlockHeader()) {
            throw new ValidationException("Block: " + blockNumber + " - First item must be a BlockHeader, found "
                    + describeItem(items, index));
        }
        index++;
        if (index < size && items.get(index).hasBlockHeader()) {
            throw new ValidationException("Block: " + blockNumber + " - Multiple BlockHeaders found at index " + index);
        }

        // 2. Zero or more StateChanges
        while (index < size && items.get(index).hasStateChanges()) {
            index++;
        }

        // 3. Exactly one RecordFile
        if (index >= size || !items.get(index).hasRecordFile()) {
            throw new ValidationException("Block: " + blockNumber
                    + " - Expected RecordFile after BlockHeader/StateChanges at index " + index + ", found "
                    + describeItem(items, index));
        }
        index++;
        if (index < size && items.get(index).hasRecordFile()) {
            throw new ValidationException(
                    "Block: " + blockNumber + " - Multiple RecordFile items found at index " + index);
        }

        // 4. Exactly one BlockFooter
        if (index >= size || !items.get(index).hasBlockFooter()) {
            throw new ValidationException("Block: " + blockNumber
                    + " - Expected BlockFooter after RecordFile at index " + index + ", found "
                    + describeItem(items, index));
        }
        index++;
        if (index < size && items.get(index).hasBlockFooter()) {
            throw new ValidationException(
                    "Block: " + blockNumber + " - Multiple BlockFooter items found at index " + index);
        }

        // 5. One or more BlockProof items
        if (index >= size || !items.get(index).hasBlockProof()) {
            throw new ValidationException("Block: " + blockNumber
                    + " - Expected BlockProof after BlockFooter at index " + index + ", found "
                    + describeItem(items, index));
        }
        while (index < size && items.get(index).hasBlockProof()) {
            index++;
        }

        // Should be at the end
        if (index < size) {
            throw new ValidationException("Block: " + blockNumber + " - Unexpected " + describeItem(items, index)
                    + " at index " + index + " after BlockProof(s)");
        }
    }

    /**
     * Returns a human-readable description of the block item at the given index for use in error messages.
     *
     * @param items the list of block items
     * @param index the index of the item to describe
     * @return a short string identifying the item type
     */
    private static String describeItem(final List<BlockItemUnparsed> items, final int index) {
        if (index >= items.size()) {
            return "end of block";
        }
        final BlockItemUnparsed item = items.get(index);
        if (item.hasBlockHeader()) return "BlockHeader";
        if (item.hasRecordFile()) return "RecordFile";
        if (item.hasBlockFooter()) return "BlockFooter";
        if (item.hasBlockProof()) return "BlockProof";
        if (item.hasStateChanges()) return "StateChanges";
        return item.item().kind().name();
    }
}
