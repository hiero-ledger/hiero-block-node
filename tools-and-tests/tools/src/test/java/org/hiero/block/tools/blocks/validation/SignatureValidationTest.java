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
import com.hedera.hapi.block.stream.RecordFileSignature;
import com.hedera.hapi.block.stream.SignedRecordFileProof;
import com.hedera.hapi.block.stream.TssSignedBlockProof;
import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.tools.blocks.TestBlockFactory;
import org.hiero.block.tools.days.model.AddressBookRegistry;
import org.hiero.block.tools.records.model.parsed.ValidationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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

    private static BlockUnparsed toUnparsed(Block block) {
        try {
            return BlockUnparsed.PROTOBUF.parse(Block.PROTOBUF.toBytes(block));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void noBlockProof_throwsValidationException() {
        // Block with no proof item at all
        Block block = new Block(List.of(HEADER_ITEM, RECORD_FILE_ITEM, FOOTER_ITEM));
        SignatureValidation validation = new SignatureValidation(null);
        ValidationException ex =
                assertThrows(ValidationException.class, () -> validation.validate(toUnparsed(block), 0));
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
        ValidationException ex =
                assertThrows(ValidationException.class, () -> validation.validate(toUnparsed(block), 42));
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
        assertDoesNotThrow(() -> validation.validate(toUnparsed(block), 42));
    }

    @Test
    void unknownProofType_throwsValidationException() {
        // BlockProof with no proof set
        BlockItem proofItem =
                BlockItem.newBuilder().blockProof(BlockProof.DEFAULT).build();
        Block block = new Block(List.of(HEADER_ITEM, RECORD_FILE_ITEM, FOOTER_ITEM, proofItem));
        SignatureValidation validation = new SignatureValidation(null);
        ValidationException ex =
                assertThrows(ValidationException.class, () -> validation.validate(toUnparsed(block), 0));
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
        ValidationException ex =
                assertThrows(ValidationException.class, () -> validation.validate(toUnparsed(block), 0));
        assertTrue(ex.getMessage().contains("No signatures"));
    }

    @Test
    void doesNotRequireGenesisStart() {
        SignatureValidation validation = new SignatureValidation(null);
        assertFalse(validation.requiresGenesisStart());
    }

    @Test
    void duplicateSignerEntries_countedOnceForThreshold(@TempDir Path tempDir) throws Exception {
        // Create a valid chain (5 nodes, threshold = 2)
        List<Block> chain = TestBlockFactory.createValidChain(1);
        Block block = chain.getFirst();
        // Replace signatures with 3 copies of node 0's signature
        List<BlockItem> items = new ArrayList<>();
        for (BlockItem item : block.items()) {
            if (item.hasBlockProof()) {
                SignedRecordFileProof orig = item.blockProofOrThrow().signedRecordFileProofOrThrow();
                RecordFileSignature firstSig = orig.recordFileSignatures().getFirst();
                // 3 copies of node 0
                List<RecordFileSignature> duplicatedSigs = List.of(firstSig, firstSig, firstSig);
                SignedRecordFileProof newProof = SignedRecordFileProof.newBuilder()
                        .version(orig.version())
                        .recordFileSignatures(duplicatedSigs)
                        .build();
                items.add(BlockItem.newBuilder()
                        .blockProof(BlockProof.newBuilder()
                                .signedRecordFileProof(newProof)
                                .build())
                        .build());
            } else {
                items.add(item);
            }
        }
        Block blockWithDuplicateSigs = new Block(items);
        // Write address book history and create registry
        TestBlockFactory.writeAddressBookHistory(tempDir);
        AddressBookRegistry registry = new AddressBookRegistry(tempDir.resolve("addressBookHistory.json"));
        SignatureValidation validation = new SignatureValidation(registry);
        // Only 1 unique node signed — below threshold of 2
        ValidationException ex = assertThrows(
                ValidationException.class, () -> validation.validate(toUnparsed(blockWithDuplicateSigs), 0));
        assertTrue(ex.getMessage().contains("Insufficient valid signatures"));
    }

    @Test
    void unknownSignerMixedWithValid_thresholdStillMet(@TempDir Path tempDir) throws Exception {
        // Create a valid chain (5 nodes, threshold = 2)
        List<Block> chain = TestBlockFactory.createValidChain(1);
        Block block = chain.getFirst();
        // Keep only 2 valid signatures and add one from an unknown node (nodeId=99)
        List<BlockItem> items = new ArrayList<>();
        for (BlockItem item : block.items()) {
            if (item.hasBlockProof()) {
                SignedRecordFileProof orig = item.blockProofOrThrow().signedRecordFileProofOrThrow();
                List<RecordFileSignature> twoValid =
                        new ArrayList<>(orig.recordFileSignatures().subList(0, 2));
                // Add unknown node signature (bogus bytes)
                twoValid.add(RecordFileSignature.newBuilder()
                        .nodeId(99)
                        .signaturesBytes(Bytes.wrap(new byte[] {1, 2, 3}))
                        .build());
                SignedRecordFileProof newProof = SignedRecordFileProof.newBuilder()
                        .version(orig.version())
                        .recordFileSignatures(twoValid)
                        .build();
                items.add(BlockItem.newBuilder()
                        .blockProof(BlockProof.newBuilder()
                                .signedRecordFileProof(newProof)
                                .build())
                        .build());
            } else {
                items.add(item);
            }
        }
        Block blockWithUnknownSigner = new Block(items);
        // Write address book and create registry
        TestBlockFactory.writeAddressBookHistory(tempDir);
        AddressBookRegistry registry = new AddressBookRegistry(tempDir.resolve("addressBookHistory.json"));
        SignatureValidation validation = new SignatureValidation(registry);
        // 2 valid nodes >= threshold of 2 → should pass
        assertDoesNotThrow(() -> validation.validate(toUnparsed(blockWithUnknownSigner), 0));
    }
}
