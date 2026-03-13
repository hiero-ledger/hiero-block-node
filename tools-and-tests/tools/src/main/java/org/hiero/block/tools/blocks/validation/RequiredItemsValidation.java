// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

import java.util.List;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.tools.records.model.parsed.ValidationException;

/**
 * Validates that all minimum required items are present in a wrapped block.
 *
 * <p>Every wrapped block must contain at least one of each: {@code BlockHeader},
 * {@code RecordFile}, {@code BlockFooter}, and {@code BlockProof}. This is a stateless
 * validation — no cross-block state is needed.
 */
public final class RequiredItemsValidation implements BlockValidation {

    @Override
    public String name() {
        return "Required Items";
    }

    @Override
    public String description() {
        return "Checks that every block contains at least one BlockHeader, RecordFile, BlockFooter, and BlockProof";
    }

    @Override
    public boolean requiresGenesisStart() {
        return false;
    }

    @Override
    public void validate(final BlockUnparsed block, final long blockNumber) throws ValidationException {
        final List<BlockItemUnparsed> items = block.blockItems();
        if (items.isEmpty()) {
            throw new ValidationException("Block: " + blockNumber + " - Block has no items");
        }
        boolean hasHeader = false;
        boolean hasRecordFile = false;
        boolean hasFooter = false;
        boolean hasProof = false;
        for (final BlockItemUnparsed item : items) {
            if (item.hasBlockHeader()) hasHeader = true;
            if (item.hasRecordFile()) hasRecordFile = true;
            if (item.hasBlockFooter()) hasFooter = true;
            if (item.hasBlockProof()) hasProof = true;
        }
        if (!hasHeader) {
            throw new ValidationException("Block: " + blockNumber + " - Missing required BlockHeader");
        }
        if (!hasRecordFile) {
            throw new ValidationException("Block: " + blockNumber + " - Missing required RecordFile");
        }
        if (!hasFooter) {
            throw new ValidationException("Block: " + blockNumber + " - Missing required BlockFooter");
        }
        if (!hasProof) {
            throw new ValidationException("Block: " + blockNumber + " - Missing required BlockProof");
        }
    }
}
