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
import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.nio.file.Path;
import java.util.List;
import org.hiero.block.tools.blocks.model.hashing.InMemoryTreeHasher;
import org.hiero.block.tools.records.model.parsed.ValidationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Tests for {@link CompleteMerkleTreeValidation}. */
class CompleteMerkleTreeValidationTest {

    private static final BlockItem HEADER_ITEM = BlockItem.newBuilder()
            .blockHeader(BlockHeader.newBuilder()
                    .number(0)
                    .blockTimestamp(Timestamp.newBuilder().seconds(1L).build())
                    .build())
            .build();
    private static final BlockItem RECORD_FILE_ITEM =
            BlockItem.newBuilder().recordFile(RecordFileItem.DEFAULT).build();
    private static final BlockItem FOOTER_ITEM = BlockItem.newBuilder()
            .blockFooter(BlockFooter.newBuilder()
                    .previousBlockRootHash(Bytes.wrap(EMPTY_TREE_HASH))
                    .rootHashOfAllBlockHashesTree(Bytes.wrap(EMPTY_TREE_HASH))
                    .startOfBlockStateRootHash(Bytes.wrap(EMPTY_TREE_HASH))
                    .build())
            .build();
    private static final BlockItem PROOF_ITEM =
            BlockItem.newBuilder().blockProof(BlockProof.DEFAULT).build();
    private static final Block VALID_BLOCK = new Block(List.of(HEADER_ITEM, RECORD_FILE_ITEM, FOOTER_ITEM, PROOF_ITEM));

    @Test
    void commitState_populatesHasher_and_finalize_passes(@TempDir Path tempDir) throws Exception {
        BlockChainValidation chain = new BlockChainValidation();

        // Process block 0
        chain.validate(VALID_BLOCK, 0);
        chain.commitState(VALID_BLOCK, 0);
        byte[] blockHash = chain.getPreviousBlockHash();

        // Build the expected InMemoryTreeHasher and save to file
        InMemoryTreeHasher expected = new InMemoryTreeHasher();
        expected.addNodeByHash(blockHash);
        Path stateFile = tempDir.resolve("completeMerkleTree.bin");
        expected.save(stateFile);

        // Create validation — commitState should populate the internal hasher
        CompleteMerkleTreeValidation validation = new CompleteMerkleTreeValidation(stateFile, chain);
        // Re-validate so chain has stagedBlockHash available for getPreviousBlockHash
        chain.setPreviousBlockHash(null);
        chain.validate(VALID_BLOCK, 0);
        chain.commitState(VALID_BLOCK, 0);
        validation.commitState(VALID_BLOCK, 0);

        // finalize should pass because internal hasher matches file
        assertDoesNotThrow(() -> validation.finalize(1, 0));
    }

    @Test
    void mismatchedLeafCount_fails(@TempDir Path tempDir) throws Exception {
        BlockChainValidation chain = new BlockChainValidation();
        chain.validate(VALID_BLOCK, 0);
        chain.commitState(VALID_BLOCK, 0);

        // Save a hasher with 1 leaf
        InMemoryTreeHasher hasher = new InMemoryTreeHasher();
        hasher.addNodeByHash(chain.getPreviousBlockHash());
        Path stateFile = tempDir.resolve("completeMerkleTree.bin");
        hasher.save(stateFile);

        // Create validation but don't commitState (internal hasher has 0 leaves)
        CompleteMerkleTreeValidation validation = new CompleteMerkleTreeValidation(stateFile, chain);

        // finalize with totalBlocksValidated=1 but internal hasher has 0 leaves
        // The file has 1 leaf but we pass totalBlocksValidated=0
        ValidationException ex = assertThrows(ValidationException.class, () -> validation.finalize(0, 0));
        assertTrue(ex.getMessage().contains("leaf count"));
    }

    @Test
    void mismatchedRootHash_fails(@TempDir Path tempDir) throws Exception {
        BlockChainValidation chain = new BlockChainValidation();
        chain.validate(VALID_BLOCK, 0);
        chain.commitState(VALID_BLOCK, 0);

        // Save a hasher with a different hash
        InMemoryTreeHasher differentHasher = new InMemoryTreeHasher();
        differentHasher.addNodeByHash(new byte[48]); // zero hash, different from block hash
        Path stateFile = tempDir.resolve("completeMerkleTree.bin");
        differentHasher.save(stateFile);

        // Validation's internal hasher gets the real block hash via commitState
        CompleteMerkleTreeValidation validation = new CompleteMerkleTreeValidation(stateFile, chain);
        chain.setPreviousBlockHash(null);
        chain.validate(VALID_BLOCK, 0);
        chain.commitState(VALID_BLOCK, 0);
        validation.commitState(VALID_BLOCK, 0);

        ValidationException ex = assertThrows(ValidationException.class, () -> validation.finalize(1, 0));
        assertTrue(ex.getMessage().contains("root hash mismatch"));
    }

    @Test
    void missingFile_fails(@TempDir Path tempDir) {
        BlockChainValidation chain = new BlockChainValidation();
        Path nonExistentFile = tempDir.resolve("completeMerkleTree.bin");
        CompleteMerkleTreeValidation validation = new CompleteMerkleTreeValidation(nonExistentFile, chain);

        ValidationException ex = assertThrows(ValidationException.class, () -> validation.finalize(0, -1));
        assertTrue(ex.getMessage().contains("not found"));
    }

    @Test
    void requiresGenesisStart() {
        BlockChainValidation chain = new BlockChainValidation();
        CompleteMerkleTreeValidation validation = new CompleteMerkleTreeValidation(Path.of("dummy"), chain);
        assertTrue(validation.requiresGenesisStart());
    }

    @Test
    void saveAndLoad_preservesState(@TempDir Path tempDir) throws Exception {
        BlockChainValidation chain = new BlockChainValidation();
        chain.validate(VALID_BLOCK, 0);
        chain.commitState(VALID_BLOCK, 0);

        // Create and populate a validation
        CompleteMerkleTreeValidation validation = new CompleteMerkleTreeValidation(Path.of("dummy"), chain);
        validation.commitState(VALID_BLOCK, 0);

        // Save state
        validation.save(tempDir);

        // Create a fresh validation and load state
        CompleteMerkleTreeValidation loaded = new CompleteMerkleTreeValidation(Path.of("dummy"), chain);
        loaded.load(tempDir);

        // Build the expected file to match the loaded state
        InMemoryTreeHasher expected = new InMemoryTreeHasher();
        expected.addNodeByHash(chain.getPreviousBlockHash());
        Path stateFile = tempDir.resolve("completeMerkleTree.bin");
        expected.save(stateFile);

        CompleteMerkleTreeValidation withFile = new CompleteMerkleTreeValidation(stateFile, chain);
        withFile.load(tempDir);
        assertDoesNotThrow(() -> withFile.finalize(1, 0));
    }
}
