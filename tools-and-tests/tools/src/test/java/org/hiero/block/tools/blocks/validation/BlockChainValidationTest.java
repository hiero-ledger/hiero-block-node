// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

import static org.hiero.block.tools.blocks.model.hashing.HashingUtils.EMPTY_TREE_HASH;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.RecordFileItem;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.nio.file.Path;
import java.util.List;
import org.hiero.block.tools.blocks.TestBlockFactory;
import org.hiero.block.tools.records.model.parsed.ValidationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Tests for {@link BlockChainValidation}. */
class BlockChainValidationTest {

    private static final BlockItem HEADER_ITEM = BlockItem.newBuilder()
            .blockHeader(BlockHeader.newBuilder()
                    .number(0)
                    .blockTimestamp(Timestamp.newBuilder().seconds(1L).build())
                    .build())
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
    void firstBlock_withEmptyTreeHash_passes() {
        BlockChainValidation validation = new BlockChainValidation();
        // First block: previousBlockHash is null, footer has EMPTY_TREE_HASH → passes genesis check
        assertDoesNotThrow(() -> validation.validate(VALID_BLOCK, 0));
    }

    @Test
    void firstBlock_withWrongHash_fails() {
        BlockChainValidation validation = new BlockChainValidation();
        // Create a block with a non-empty-tree previousBlockRootHash
        byte[] wrongHash = new byte[48];
        wrongHash[0] = (byte) 0xFF;
        BlockItem wrongFooter = BlockItem.newBuilder()
                .blockFooter(com.hedera.hapi.block.stream.output.BlockFooter.newBuilder()
                        .previousBlockRootHash(Bytes.wrap(wrongHash))
                        .rootHashOfAllBlockHashesTree(Bytes.wrap(EMPTY_TREE_HASH))
                        .startOfBlockStateRootHash(Bytes.wrap(EMPTY_TREE_HASH))
                        .build())
                .build();
        Block badBlock = new Block(List.of(HEADER_ITEM, RECORD_FILE_ITEM, wrongFooter, PROOF_ITEM));
        ValidationException ex = assertThrows(ValidationException.class, () -> validation.validate(badBlock, 0));
        assertTrue(ex.getMessage().contains("First block should have empty-tree previous hash"));
    }

    @Test
    void firstBlock_commitStoresHash() throws ValidationException {
        BlockChainValidation validation = new BlockChainValidation();
        assertNull(validation.getPreviousBlockHash());
        validation.validate(VALID_BLOCK, 0);
        assertNotNull(validation.getStagedBlockHash());
        validation.commitState(VALID_BLOCK, 0);
        assertNotNull(validation.getPreviousBlockHash());
    }

    @Test
    void secondBlock_mismatchedHash_fails() throws ValidationException {
        BlockChainValidation validation = new BlockChainValidation();
        // Commit first block
        validation.validate(VALID_BLOCK, 0);
        validation.commitState(VALID_BLOCK, 0);
        // The footer in VALID_BLOCK has EMPTY_TREE_HASH as previousBlockRootHash,
        // but the committed hash from block 0 won't be EMPTY_TREE_HASH
        ValidationException ex = assertThrows(ValidationException.class, () -> validation.validate(VALID_BLOCK, 1));
        assertTrue(ex.getMessage().contains("previous block hash mismatch"));
    }

    @Test
    void doesNotRequireGenesisStart() {
        BlockChainValidation validation = new BlockChainValidation();
        assertFalse(validation.requiresGenesisStart());
    }

    @Test
    void saveLoadRoundTrip_preservesHash(@TempDir Path tempDir) throws Exception {
        // Validate + commit block 0 using TestBlockFactory chain
        List<Block> chain = TestBlockFactory.createValidChain(2);
        BlockChainValidation validation = new BlockChainValidation();
        validation.validate(chain.getFirst(), 0);
        validation.commitState(chain.getFirst(), 0);
        // Save state
        validation.save(tempDir);
        // Load into new instance
        BlockChainValidation restored = new BlockChainValidation();
        restored.load(tempDir);
        // Validate block 1 — should pass because the restored hash matches
        assertDoesNotThrow(() -> restored.validate(chain.get(1), 1));
    }

    @Test
    void missingFooter_throwsValidationException() {
        BlockChainValidation validation = new BlockChainValidation();
        Block noFooter = new Block(List.of(HEADER_ITEM, RECORD_FILE_ITEM, PROOF_ITEM));
        ValidationException ex = assertThrows(ValidationException.class, () -> validation.validate(noFooter, 0));
        assertTrue(ex.getMessage().contains("Block footer"));
    }
}
