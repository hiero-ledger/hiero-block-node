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
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.hiero.block.tools.blocks.model.hashing.StreamingHasher;
import org.hiero.block.tools.records.model.parsed.ValidationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Tests for {@link JumpstartValidation}. */
class JumpstartValidationTest {

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

    /**
     * Writes a jumpstart.bin file matching the given streaming hasher state.
     */
    private static void writeJumpstartFile(Path path, long blockNum, byte[] blockHash, StreamingHasher hasher)
            throws Exception {
        List<byte[]> hashes = hasher.intermediateHashingState();
        try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(path))) {
            out.writeLong(blockNum);
            out.write(blockHash);
            out.writeLong(hasher.leafCount());
            out.writeInt(hashes.size());
            for (byte[] h : hashes) {
                out.write(h);
            }
        }
    }

    @Test
    void matchingJumpstart_passes(@TempDir Path tempDir) throws Exception {
        BlockChainValidation chain = new BlockChainValidation();
        HistoricalBlockTreeValidation tree = new HistoricalBlockTreeValidation(chain);

        chain.validate(VALID_BLOCK, 0);
        tree.validate(VALID_BLOCK, 0);
        byte[] blockHash = chain.getStagedBlockHash();
        tree.commitState(VALID_BLOCK, 0);
        chain.commitState(VALID_BLOCK, 0);

        Path jumpstartFile = tempDir.resolve("jumpstart.bin");
        writeJumpstartFile(jumpstartFile, 0, blockHash, tree.getStreamingHasher());

        JumpstartValidation validation = new JumpstartValidation(jumpstartFile, tree, null);
        assertDoesNotThrow(() -> validation.finalize(1, 0));
    }

    @Test
    void wrongBlockNumber_fails(@TempDir Path tempDir) throws Exception {
        BlockChainValidation chain = new BlockChainValidation();
        HistoricalBlockTreeValidation tree = new HistoricalBlockTreeValidation(chain);

        chain.validate(VALID_BLOCK, 0);
        tree.validate(VALID_BLOCK, 0);
        byte[] blockHash = chain.getStagedBlockHash();
        tree.commitState(VALID_BLOCK, 0);
        chain.commitState(VALID_BLOCK, 0);

        Path jumpstartFile = tempDir.resolve("jumpstart.bin");
        // Write block number 99 instead of 0
        writeJumpstartFile(jumpstartFile, 99, blockHash, tree.getStreamingHasher());

        JumpstartValidation validation = new JumpstartValidation(jumpstartFile, tree, null);
        ValidationException ex = assertThrows(ValidationException.class, () -> validation.finalize(1, 0));
        assertTrue(ex.getMessage().contains("block number"));
    }

    @Test
    void wrongLeafCount_fails(@TempDir Path tempDir) throws Exception {
        BlockChainValidation chain = new BlockChainValidation();
        HistoricalBlockTreeValidation tree = new HistoricalBlockTreeValidation(chain);

        chain.validate(VALID_BLOCK, 0);
        tree.validate(VALID_BLOCK, 0);
        byte[] blockHash = chain.getStagedBlockHash();
        tree.commitState(VALID_BLOCK, 0);
        chain.commitState(VALID_BLOCK, 0);

        // Write a jumpstart file with wrong leaf count
        Path jumpstartFile = tempDir.resolve("jumpstart.bin");
        List<byte[]> hashes = tree.getStreamingHasher().intermediateHashingState();
        try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(jumpstartFile))) {
            out.writeLong(0); // correct block number
            out.write(blockHash);
            out.writeLong(999); // wrong leaf count
            out.writeInt(hashes.size());
            for (byte[] h : hashes) {
                out.write(h);
            }
        }

        JumpstartValidation validation = new JumpstartValidation(jumpstartFile, tree, null);
        ValidationException ex = assertThrows(ValidationException.class, () -> validation.finalize(1, 0));
        assertTrue(ex.getMessage().contains("leaf count"));
    }

    @Test
    void wrongRootHash_fails(@TempDir Path tempDir) throws Exception {
        BlockChainValidation chain = new BlockChainValidation();
        HistoricalBlockTreeValidation tree = new HistoricalBlockTreeValidation(chain);

        chain.validate(VALID_BLOCK, 0);
        tree.validate(VALID_BLOCK, 0);
        byte[] blockHash = chain.getStagedBlockHash();
        tree.commitState(VALID_BLOCK, 0);
        chain.commitState(VALID_BLOCK, 0);

        // Write a jumpstart file with different intermediate hashes (creating wrong root)
        Path jumpstartFile = tempDir.resolve("jumpstart.bin");
        try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(jumpstartFile))) {
            out.writeLong(0);
            out.write(blockHash);
            out.writeLong(1); // correct leaf count
            out.writeInt(1);
            out.write(new byte[48]); // wrong hash
        }

        JumpstartValidation validation = new JumpstartValidation(jumpstartFile, tree, null);
        ValidationException ex = assertThrows(ValidationException.class, () -> validation.finalize(1, 0));
        assertTrue(ex.getMessage().contains("root mismatch"));
    }

    @Test
    void missingFile_fails(@TempDir Path tempDir) {
        BlockChainValidation chain = new BlockChainValidation();
        HistoricalBlockTreeValidation tree = new HistoricalBlockTreeValidation(chain);
        Path nonExistentFile = tempDir.resolve("jumpstart.bin");

        JumpstartValidation validation = new JumpstartValidation(nonExistentFile, tree, null);
        ValidationException ex = assertThrows(ValidationException.class, () -> validation.finalize(0, -1));
        assertTrue(ex.getMessage().contains("not found"));
    }

    @Test
    void requiresGenesisStart() {
        BlockChainValidation chain = new BlockChainValidation();
        HistoricalBlockTreeValidation tree = new HistoricalBlockTreeValidation(chain);
        JumpstartValidation validation = new JumpstartValidation(Path.of("dummy"), tree, null);
        assertTrue(validation.requiresGenesisStart());
    }
}
