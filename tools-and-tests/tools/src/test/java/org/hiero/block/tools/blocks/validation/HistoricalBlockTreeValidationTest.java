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
import java.nio.file.Path;
import java.util.List;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.tools.blocks.TestBlockFactory;
import org.hiero.block.tools.records.model.parsed.ValidationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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

    private static BlockUnparsed toUnparsed(Block block) {
        try {
            return BlockUnparsed.PROTOBUF.parse(Block.PROTOBUF.toBytes(block));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void firstBlock_matchesEmptyTree() {
        BlockChainValidation chain = new BlockChainValidation();
        HistoricalBlockTreeValidation validation = new HistoricalBlockTreeValidation(chain);
        // First block: the streaming hasher has no leaves, so root = EMPTY_TREE_HASH
        // The FOOTER_ITEM has EMPTY_TREE_HASH as rootHashOfAllBlockHashesTree
        assertDoesNotThrow(() -> {
            chain.validate(toUnparsed(VALID_BLOCK), 0);
            validation.validate(toUnparsed(VALID_BLOCK), 0);
        });
    }

    @Test
    void afterCommittingOneBlock_treeRootMismatch_fails() throws ValidationException {
        BlockChainValidation chain = new BlockChainValidation();
        HistoricalBlockTreeValidation validation = new HistoricalBlockTreeValidation(chain);
        // Process first block
        chain.validate(toUnparsed(VALID_BLOCK), 0);
        validation.validate(toUnparsed(VALID_BLOCK), 0);
        validation.commitState(toUnparsed(VALID_BLOCK), 0);
        chain.commitState(toUnparsed(VALID_BLOCK), 0);
        // After committing block 0, the streaming hasher has block 0's hash,
        // so root != EMPTY_TREE_HASH. VALID_BLOCK's footer still has EMPTY_TREE_HASH,
        // so tree validation should fail.
        ValidationException ex =
                assertThrows(ValidationException.class, () -> validation.validate(toUnparsed(VALID_BLOCK), 1));
        assertTrue(ex.getMessage().contains("Historical block tree root hash mismatch"));
    }

    @Test
    void requiresGenesisStart() {
        BlockChainValidation chain = new BlockChainValidation();
        HistoricalBlockTreeValidation validation = new HistoricalBlockTreeValidation(chain);
        assertTrue(validation.requiresGenesisStart());
    }

    @Test
    void saveLoadRoundTrip_preservesStreamingHasher(@TempDir Path tempDir) throws Exception {
        // Create a 2-block valid chain
        List<Block> chain = TestBlockFactory.createValidChain(2);
        // Process block 0 through chain + tree
        BlockChainValidation chainValidation = new BlockChainValidation();
        HistoricalBlockTreeValidation treeValidation = new HistoricalBlockTreeValidation(chainValidation);
        chainValidation.validate(toUnparsed(chain.getFirst()), 0);
        treeValidation.validate(toUnparsed(chain.getFirst()), 0);
        treeValidation.commitState(toUnparsed(chain.getFirst()), 0);
        chainValidation.commitState(toUnparsed(chain.getFirst()), 0);
        // Save tree state
        treeValidation.save(tempDir);
        chainValidation.save(tempDir);
        // Create new instances and load
        BlockChainValidation restoredChain = new BlockChainValidation();
        restoredChain.load(tempDir);
        HistoricalBlockTreeValidation restoredTree = new HistoricalBlockTreeValidation(restoredChain);
        restoredTree.load(tempDir);
        // Validate block 1 — should pass because the streaming hasher was correctly restored
        restoredChain.validate(toUnparsed(chain.get(1)), 1);
        assertDoesNotThrow(() -> restoredTree.validate(toUnparsed(chain.get(1)), 1));
    }
}
