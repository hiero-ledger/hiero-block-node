// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

import static org.hiero.block.tools.blocks.model.hashing.HashingUtils.EMPTY_TREE_HASH;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.List;
import org.hiero.block.tools.blocks.model.hashing.BlockStreamBlockHashRegistry;
import org.hiero.block.tools.records.model.parsed.ValidationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Tests for {@link HashRegistryValidation}. */
class HashRegistryValidationTest {

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
    void hashMatch_passes(@TempDir Path tempDir) throws Exception {
        // First compute the block hash using BlockChainValidation
        BlockChainValidation chainValidation = new BlockChainValidation();
        chainValidation.validate(VALID_BLOCK, 0);
        byte[] blockHash = chainValidation.getStagedBlockHash();

        // Write that hash to a registry file
        Path registryPath = tempDir.resolve("blockStreamBlockHashes.bin");
        try (RandomAccessFile raf = new RandomAccessFile(registryPath.toFile(), "rw")) {
            raf.write(blockHash);
        }

        BlockStreamBlockHashRegistry registry = new BlockStreamBlockHashRegistry(registryPath);
        HashRegistryValidation validation = new HashRegistryValidation(registry, chainValidation);
        assertDoesNotThrow(() -> validation.validate(VALID_BLOCK, 0));
        validation.close();
    }

    @Test
    void hashMismatch_fails(@TempDir Path tempDir) throws Exception {
        BlockChainValidation chainValidation = new BlockChainValidation();
        chainValidation.validate(VALID_BLOCK, 0);

        // Write a different (wrong) hash to the registry
        Path registryPath = tempDir.resolve("blockStreamBlockHashes.bin");
        byte[] wrongHash = new byte[48];
        wrongHash[0] = (byte) 0xFF;
        try (RandomAccessFile raf = new RandomAccessFile(registryPath.toFile(), "rw")) {
            raf.write(wrongHash);
        }

        BlockStreamBlockHashRegistry registry = new BlockStreamBlockHashRegistry(registryPath);
        HashRegistryValidation validation = new HashRegistryValidation(registry, chainValidation);
        ValidationException ex = assertThrows(ValidationException.class, () -> validation.validate(VALID_BLOCK, 0));
        assertTrue(ex.getMessage().contains("Hash mismatch"));
        validation.close();
    }

    @Test
    void doesNotRequireGenesisStart(@TempDir Path tempDir) throws IOException {
        Path registryPath = tempDir.resolve("blockStreamBlockHashes.bin");
        try (RandomAccessFile raf = new RandomAccessFile(registryPath.toFile(), "rw")) {
            raf.write(new byte[48]); // dummy
        }
        BlockStreamBlockHashRegistry registry = new BlockStreamBlockHashRegistry(registryPath);
        HashRegistryValidation validation = new HashRegistryValidation(registry, new BlockChainValidation());
        assertFalse(validation.requiresGenesisStart());
        validation.close();
    }
}
