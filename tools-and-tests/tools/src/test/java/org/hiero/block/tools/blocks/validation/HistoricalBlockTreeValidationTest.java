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
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;
import org.hiero.block.tools.records.model.parsed.ValidationException;
import org.junit.jupiter.api.Test;

/** Tests for {@link HistoricalBlockTreeValidation}. */
class HistoricalBlockTreeValidationTest {

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
    void firstBlock_matchesEmptyTree() {
        BlockChainValidation chain = new BlockChainValidation();
        HistoricalBlockTreeValidation validation = new HistoricalBlockTreeValidation(chain);
        // First block: the streaming hasher has no leaves, so root = EMPTY_TREE_HASH
        // The FOOTER_ITEM has EMPTY_TREE_HASH as rootHashOfAllBlockHashesTree
        assertDoesNotThrow(() -> {
            chain.validate(VALID_BLOCK, 0);
            validation.validate(VALID_BLOCK, 0);
        });
    }

    @Test
    void afterCommittingOneBlock_treeRootMismatch_fails() throws ValidationException {
        BlockChainValidation chain = new BlockChainValidation();
        HistoricalBlockTreeValidation validation = new HistoricalBlockTreeValidation(chain);
        // Process first block
        chain.validate(VALID_BLOCK, 0);
        validation.validate(VALID_BLOCK, 0);
        validation.commitState(VALID_BLOCK, 0);
        chain.commitState(VALID_BLOCK, 0);
        // After committing block 0, the streaming hasher has block 0's hash,
        // so root != EMPTY_TREE_HASH. VALID_BLOCK's footer still has EMPTY_TREE_HASH,
        // so tree validation should fail.
        ValidationException ex = assertThrows(ValidationException.class, () -> validation.validate(VALID_BLOCK, 1));
        assertTrue(ex.getMessage().contains("Historical block tree root hash mismatch"));
    }

    @Test
    void requiresGenesisStart() {
        BlockChainValidation chain = new BlockChainValidation();
        HistoricalBlockTreeValidation validation = new HistoricalBlockTreeValidation(chain);
        assertTrue(validation.requiresGenesisStart());
    }
}
