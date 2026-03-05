// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

import static org.hiero.block.tools.blocks.model.hashing.HashingUtils.EMPTY_TREE_HASH;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.RecordFileItem;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;
import org.hiero.block.tools.records.model.parsed.ValidationException;
import org.junit.jupiter.api.Test;

/** Tests for {@link HbarSupplyValidation}. */
class HbarSupplyValidationTest {

    private static final BlockItem HEADER_ITEM = BlockItem.newBuilder()
            .blockHeader(BlockHeader.newBuilder().number(0).build())
            .build();
    private static final BlockItem RECORD_FILE_ITEM =
            BlockItem.newBuilder().recordFile(RecordFileItem.DEFAULT).build();
    private static final BlockItem FOOTER_ITEM = BlockItem.newBuilder()
            .blockFooter(com.hedera.hapi.block.stream.output.BlockFooter.newBuilder()
                    .previousBlockRootHash(Bytes.wrap(EMPTY_TREE_HASH))
                    .rootHashOfAllBlockHashesTree(Bytes.wrap(EMPTY_TREE_HASH))
                    .startOfBlockStateRootHash(Bytes.wrap(EMPTY_TREE_HASH))
                    .build())
            .build();
    private static final BlockItem PROOF_ITEM =
            BlockItem.newBuilder().blockProof(BlockProof.DEFAULT).build();
    private static final Block VALID_BLOCK = new Block(List.of(HEADER_ITEM, RECORD_FILE_ITEM, FOOTER_ITEM, PROOF_ITEM));

    @Test
    void emptyBlock_zeroSupply_throwsValidationException() {
        // With zero initial state and no transfers, total HBAR = 0, which != 50B.
        // The block with no state changes produces zero delta, so validation should fail.
        HbarSupplyValidation validation = new HbarSupplyValidation();
        // Build a block with only a header + footer + proof (no RecordFile or StateChanges)
        Block blockNoRecordFile = new Block(List.of(HEADER_ITEM, FOOTER_ITEM, PROOF_ITEM));
        try {
            validation.validate(blockNoRecordFile, 0);
            // Should have thrown
            assertTrue(false, "Should have thrown ValidationException for zero supply");
        } catch (ValidationException e) {
            assertTrue(e.getMessage().contains("HBAR supply mismatch"));
        }
    }

    @Test
    void getAccounts_returnsNonNull() {
        HbarSupplyValidation validation = new HbarSupplyValidation();
        assertNotNull(validation.getAccounts());
    }

    @Test
    void requiresGenesisStart() {
        HbarSupplyValidation validation = new HbarSupplyValidation();
        assertTrue(validation.requiresGenesisStart());
    }

    @Test
    void name_returnsExpected() {
        HbarSupplyValidation validation = new HbarSupplyValidation();
        assertTrue(validation.name().contains("HBAR"));
    }

    @Test
    void mergeRecordStreamItems_emptyAmendments_returnsOriginal() {
        // Test that mergeRecordStreamItems returns original list when amendments are empty
        assertDoesNotThrow(() -> {
            List<?> result = HbarSupplyValidation.mergeRecordStreamItems(List.of(), List.of());
            assertTrue(result.isEmpty());
        });
    }
}
