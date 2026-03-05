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
import com.hedera.hapi.block.stream.output.StateChanges;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;
import org.hiero.block.tools.records.model.parsed.ValidationException;
import org.junit.jupiter.api.Test;

/** Tests for {@link BlockStructureValidation}. */
class BlockStructureValidationTest {

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
    private static final BlockItem STATE_CHANGES_ITEM = BlockItem.newBuilder()
            .stateChanges(StateChanges.newBuilder()
                    .consensusTimestamp(Timestamp.newBuilder().seconds(1L).build())
                    .build())
            .build();
    private static final Block VALID_BLOCK = new Block(List.of(HEADER_ITEM, RECORD_FILE_ITEM, FOOTER_ITEM, PROOF_ITEM));

    private final BlockStructureValidation validation = new BlockStructureValidation();

    @Test
    void validBlock_passes() {
        assertDoesNotThrow(() -> validation.validate(VALID_BLOCK, 0));
    }

    @Test
    void withStateChanges_passes() {
        Block block = new Block(List.of(
                HEADER_ITEM, STATE_CHANGES_ITEM, STATE_CHANGES_ITEM, RECORD_FILE_ITEM, FOOTER_ITEM, PROOF_ITEM));
        assertDoesNotThrow(() -> validation.validate(block, 0));
    }

    @Test
    void multipleProofs_passes() {
        Block block = new Block(List.of(HEADER_ITEM, RECORD_FILE_ITEM, FOOTER_ITEM, PROOF_ITEM, PROOF_ITEM));
        assertDoesNotThrow(() -> validation.validate(block, 0));
    }

    @Test
    void withStateChangesAndMultipleProofs_passes() {
        Block block = new Block(List.of(
                HEADER_ITEM, STATE_CHANGES_ITEM, RECORD_FILE_ITEM, FOOTER_ITEM, PROOF_ITEM, PROOF_ITEM, PROOF_ITEM));
        assertDoesNotThrow(() -> validation.validate(block, 0));
    }

    @Test
    void duplicateHeader_fails() {
        Block block = new Block(List.of(HEADER_ITEM, HEADER_ITEM, RECORD_FILE_ITEM, FOOTER_ITEM, PROOF_ITEM));
        ValidationException ex = assertThrows(ValidationException.class, () -> validation.validate(block, 0));
        assertTrue(ex.getMessage().contains("Multiple BlockHeaders"));
    }

    @Test
    void duplicateRecordFile_fails() {
        Block block = new Block(List.of(HEADER_ITEM, RECORD_FILE_ITEM, RECORD_FILE_ITEM, FOOTER_ITEM, PROOF_ITEM));
        ValidationException ex = assertThrows(ValidationException.class, () -> validation.validate(block, 0));
        assertTrue(ex.getMessage().contains("Multiple RecordFile"));
    }

    @Test
    void duplicateFooter_fails() {
        Block block = new Block(List.of(HEADER_ITEM, RECORD_FILE_ITEM, FOOTER_ITEM, FOOTER_ITEM, PROOF_ITEM));
        ValidationException ex = assertThrows(ValidationException.class, () -> validation.validate(block, 0));
        assertTrue(ex.getMessage().contains("Multiple BlockFooter"));
    }

    @Test
    void stateChangesAfterRecordFile_fails() {
        Block block = new Block(List.of(HEADER_ITEM, RECORD_FILE_ITEM, STATE_CHANGES_ITEM, FOOTER_ITEM, PROOF_ITEM));
        ValidationException ex = assertThrows(ValidationException.class, () -> validation.validate(block, 0));
        assertTrue(ex.getMessage().contains("Expected BlockFooter"));
    }

    @Test
    void headerNotFirst_fails() {
        Block block = new Block(List.of(RECORD_FILE_ITEM, HEADER_ITEM, FOOTER_ITEM, PROOF_ITEM));
        ValidationException ex = assertThrows(ValidationException.class, () -> validation.validate(block, 0));
        assertTrue(ex.getMessage().contains("First item must be a BlockHeader"));
    }

    @Test
    void unexpectedItemAfterProofs_fails() {
        Block block = new Block(List.of(HEADER_ITEM, RECORD_FILE_ITEM, FOOTER_ITEM, PROOF_ITEM, RECORD_FILE_ITEM));
        ValidationException ex = assertThrows(ValidationException.class, () -> validation.validate(block, 0));
        assertTrue(ex.getMessage().contains("Unexpected"));
    }

    @Test
    void missingProof_fails() {
        Block block = new Block(List.of(HEADER_ITEM, RECORD_FILE_ITEM, FOOTER_ITEM));
        ValidationException ex = assertThrows(ValidationException.class, () -> validation.validate(block, 0));
        assertTrue(ex.getMessage().contains("Expected BlockProof"));
    }
}
