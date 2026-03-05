// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.RecordFileItem;
import com.hedera.hapi.block.stream.SignedRecordFileProof;
import com.hedera.hapi.block.stream.TssSignedBlockProof;
import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;
import org.hiero.block.tools.records.model.parsed.ValidationException;
import org.junit.jupiter.api.Test;

/** Tests for {@link SignatureValidation}. */
class SignatureValidationTest {

    private static final BlockItem HEADER_ITEM = BlockItem.newBuilder()
            .blockHeader(BlockHeader.newBuilder()
                    .number(0)
                    .blockTimestamp(Timestamp.newBuilder().seconds(1L).build())
                    .build())
            .build();
    private static final BlockItem RECORD_FILE_ITEM =
            BlockItem.newBuilder().recordFile(RecordFileItem.DEFAULT).build();
    private static final BlockItem FOOTER_ITEM =
            BlockItem.newBuilder().blockFooter(BlockFooter.DEFAULT).build();

    @Test
    void noBlockProof_throwsValidationException() {
        // Block with no proof item at all
        Block block = new Block(List.of(HEADER_ITEM, RECORD_FILE_ITEM, FOOTER_ITEM));
        SignatureValidation validation = new SignatureValidation(null);
        ValidationException ex = assertThrows(ValidationException.class, () -> validation.validate(block, 0));
        assertTrue(ex.getMessage().contains("No BlockProof found"));
    }

    @Test
    void emptyTssSignature_throwsValidationException() {
        // TSS proof with empty signature
        BlockItem proofItem = BlockItem.newBuilder()
                .blockProof(BlockProof.newBuilder()
                        .signedBlockProof(TssSignedBlockProof.newBuilder()
                                .blockSignature(Bytes.EMPTY)
                                .build())
                        .build())
                .build();
        Block block = new Block(List.of(HEADER_ITEM, RECORD_FILE_ITEM, FOOTER_ITEM, proofItem));
        SignatureValidation validation = new SignatureValidation(null);
        ValidationException ex = assertThrows(ValidationException.class, () -> validation.validate(block, 42));
        assertTrue(ex.getMessage().contains("Empty TSS block signature"));
    }

    @Test
    void nonEmptyTssSignature_passes() {
        // TSS proof with non-empty signature
        BlockItem proofItem = BlockItem.newBuilder()
                .blockProof(BlockProof.newBuilder()
                        .signedBlockProof(TssSignedBlockProof.newBuilder()
                                .blockSignature(Bytes.wrap(new byte[] {1, 2, 3}))
                                .build())
                        .build())
                .build();
        Block block = new Block(List.of(HEADER_ITEM, RECORD_FILE_ITEM, FOOTER_ITEM, proofItem));
        SignatureValidation validation = new SignatureValidation(null);
        assertDoesNotThrow(() -> validation.validate(block, 42));
    }

    @Test
    void unknownProofType_throwsValidationException() {
        // BlockProof with no proof set
        BlockItem proofItem =
                BlockItem.newBuilder().blockProof(BlockProof.DEFAULT).build();
        Block block = new Block(List.of(HEADER_ITEM, RECORD_FILE_ITEM, FOOTER_ITEM, proofItem));
        SignatureValidation validation = new SignatureValidation(null);
        ValidationException ex = assertThrows(ValidationException.class, () -> validation.validate(block, 0));
        assertTrue(ex.getMessage().contains("Unknown proof type"));
    }

    @Test
    void emptyRecordFileSignatures_throwsValidationException() {
        // SignedRecordFileProof with empty signature list
        BlockItem proofItem = BlockItem.newBuilder()
                .blockProof(BlockProof.newBuilder()
                        .signedRecordFileProof(
                                SignedRecordFileProof.newBuilder().build())
                        .build())
                .build();
        Block block = new Block(List.of(HEADER_ITEM, RECORD_FILE_ITEM, FOOTER_ITEM, proofItem));
        SignatureValidation validation = new SignatureValidation(null);
        ValidationException ex = assertThrows(ValidationException.class, () -> validation.validate(block, 0));
        assertTrue(ex.getMessage().contains("No signatures"));
    }

    @Test
    void doesNotRequireGenesisStart() {
        SignatureValidation validation = new SignatureValidation(null);
        assertFalse(validation.requiresGenesisStart());
    }
}
