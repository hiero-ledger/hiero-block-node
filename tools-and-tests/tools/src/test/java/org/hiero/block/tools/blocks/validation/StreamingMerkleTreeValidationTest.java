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
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.tools.blocks.model.hashing.StreamingHasher;
import org.hiero.block.tools.records.model.parsed.ValidationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Tests for {@link StreamingMerkleTreeValidation}. */
class StreamingMerkleTreeValidationTest {

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

    private static BlockUnparsed toUnparsed(Block block) {
        try {
            return BlockUnparsed.PROTOBUF.parse(Block.PROTOBUF.toBytes(block));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void matchingStreamingHasher_passes(@TempDir Path tempDir) throws Exception {
        // Build a fresh streaming hasher and save it to a file
        BlockChainValidation chain = new BlockChainValidation();
        HistoricalBlockTreeValidation tree = new HistoricalBlockTreeValidation(chain);

        chain.validate(toUnparsed(VALID_BLOCK), 0);
        tree.validate(toUnparsed(VALID_BLOCK), 0);
        tree.commitState(toUnparsed(VALID_BLOCK), 0);
        chain.commitState(toUnparsed(VALID_BLOCK), 0);

        // Save the streaming hasher state to a file
        Path stateFile = tempDir.resolve("streamingMerkleTree.bin");
        tree.getStreamingHasher().save(stateFile);

        // The validation should pass since the file matches the hasher
        StreamingMerkleTreeValidation validation = new StreamingMerkleTreeValidation(stateFile, tree);
        assertDoesNotThrow(() -> validation.finalize(1, 0));
    }

    @Test
    void mismatchedLeafCount_fails(@TempDir Path tempDir) throws Exception {
        BlockChainValidation chain = new BlockChainValidation();
        HistoricalBlockTreeValidation tree = new HistoricalBlockTreeValidation(chain);

        chain.validate(toUnparsed(VALID_BLOCK), 0);
        tree.validate(toUnparsed(VALID_BLOCK), 0);
        tree.commitState(toUnparsed(VALID_BLOCK), 0);
        chain.commitState(toUnparsed(VALID_BLOCK), 0);

        // Save the current state (1 leaf)
        Path stateFile = tempDir.resolve("streamingMerkleTree.bin");
        tree.getStreamingHasher().save(stateFile);

        // Call finalize with wrong totalBlocksValidated (2 instead of 1)
        StreamingMerkleTreeValidation validation = new StreamingMerkleTreeValidation(stateFile, tree);
        ValidationException ex = assertThrows(ValidationException.class, () -> validation.finalize(2, 0));
        assertTrue(ex.getMessage().contains("leaf count"));
    }

    @Test
    void mismatchedRootHash_fails(@TempDir Path tempDir) throws Exception {
        BlockChainValidation chain = new BlockChainValidation();
        HistoricalBlockTreeValidation tree = new HistoricalBlockTreeValidation(chain);

        chain.validate(toUnparsed(VALID_BLOCK), 0);
        tree.validate(toUnparsed(VALID_BLOCK), 0);
        tree.commitState(toUnparsed(VALID_BLOCK), 0);
        chain.commitState(toUnparsed(VALID_BLOCK), 0);

        // Save a different streaming hasher (with a different leaf) to the file
        StreamingHasher differentHasher = new StreamingHasher();
        differentHasher.addNodeByHash(new byte[48]); // different hash
        Path stateFile = tempDir.resolve("streamingMerkleTree.bin");
        differentHasher.save(stateFile);

        // finalize should fail due to root hash mismatch
        StreamingMerkleTreeValidation validation = new StreamingMerkleTreeValidation(stateFile, tree);
        ValidationException ex = assertThrows(ValidationException.class, () -> validation.finalize(1, 0));
        assertTrue(ex.getMessage().contains("root hash mismatch"));
    }

    @Test
    void missingFile_fails(@TempDir Path tempDir) {
        BlockChainValidation chain = new BlockChainValidation();
        HistoricalBlockTreeValidation tree = new HistoricalBlockTreeValidation(chain);
        Path nonExistentFile = tempDir.resolve("streamingMerkleTree.bin");

        StreamingMerkleTreeValidation validation = new StreamingMerkleTreeValidation(nonExistentFile, tree);
        ValidationException ex = assertThrows(ValidationException.class, () -> validation.finalize(0, -1));
        assertTrue(ex.getMessage().contains("not found"));
    }

    @Test
    void requiresGenesisStart() {
        BlockChainValidation chain = new BlockChainValidation();
        HistoricalBlockTreeValidation tree = new HistoricalBlockTreeValidation(chain);
        StreamingMerkleTreeValidation validation = new StreamingMerkleTreeValidation(Path.of("dummy"), tree);
        assertTrue(validation.requiresGenesisStart());
    }
}
