// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

import static org.hiero.block.tools.blocks.model.hashing.HashingUtils.EMPTY_TREE_HASH;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

/** Tests for {@link RequiredItemsValidation}. */
class RequiredItemsValidationTest {

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

    private final RequiredItemsValidation validation = new RequiredItemsValidation();

    @Test
    void validBlock_passes() {
        assertDoesNotThrow(() -> validation.validate(VALID_BLOCK, 0));
    }

    @Test
    void emptyBlock_fails() {
        Block emptyBlock = new Block(List.of());
        ValidationException ex = assertThrows(ValidationException.class, () -> validation.validate(emptyBlock, 0));
        assertTrue(ex.getMessage().contains("no items"));
    }

    @Test
    void missingHeader_fails() {
        Block block = new Block(List.of(RECORD_FILE_ITEM, FOOTER_ITEM, PROOF_ITEM));
        ValidationException ex = assertThrows(ValidationException.class, () -> validation.validate(block, 0));
        assertTrue(ex.getMessage().contains("BlockHeader"));
    }

    @Test
    void missingRecordFile_fails() {
        Block block = new Block(List.of(HEADER_ITEM, FOOTER_ITEM, PROOF_ITEM));
        ValidationException ex = assertThrows(ValidationException.class, () -> validation.validate(block, 0));
        assertTrue(ex.getMessage().contains("RecordFile"));
    }

    @Test
    void missingFooter_fails() {
        Block block = new Block(List.of(HEADER_ITEM, RECORD_FILE_ITEM, PROOF_ITEM));
        ValidationException ex = assertThrows(ValidationException.class, () -> validation.validate(block, 0));
        assertTrue(ex.getMessage().contains("BlockFooter"));
    }

    @Test
    void missingProof_fails() {
        Block block = new Block(List.of(HEADER_ITEM, RECORD_FILE_ITEM, FOOTER_ITEM));
        ValidationException ex = assertThrows(ValidationException.class, () -> validation.validate(block, 0));
        assertTrue(ex.getMessage().contains("BlockProof"));
    }

    @Test
    void name_returnsExpected() {
        assertTrue(validation.name().contains("Required"));
    }

    @Test
    void doesNotRequireGenesisStart() {
        assertDoesNotThrow(() -> {
            assert !validation.requiresGenesisStart();
        });
    }
}
