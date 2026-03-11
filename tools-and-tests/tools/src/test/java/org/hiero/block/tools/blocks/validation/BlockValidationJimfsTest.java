// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

import static org.hiero.block.tools.blocks.model.hashing.HashingUtils.EMPTY_TREE_HASH;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.RecordFileItem;
import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.tools.blocks.model.hashing.InMemoryTreeHasher;
import org.hiero.block.tools.blocks.model.hashing.StreamingHasher;
import org.hiero.block.tools.records.model.parsed.ValidationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for validation classes using jimfs in-memory file system.
 *
 * <p>Verifies that save/load and finalize operations work correctly on non-default file systems,
 * complementing the existing {@code @TempDir}-based tests.
 */
class BlockValidationJimfsTest {

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

    private FileSystem fs;
    private Path root;

    @BeforeEach
    void setUp() throws IOException {
        fs = Jimfs.newFileSystem(Configuration.unix());
        root = fs.getPath("/validation");
        Files.createDirectories(root);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (fs != null) {
            fs.close();
        }
    }

    // ── CompleteMerkleTreeValidation on jimfs ──

    @Test
    void completeMerkleTree_saveAndLoad_onJimfs() throws Exception {
        BlockChainValidation chain = new BlockChainValidation();
        chain.validate(toUnparsed(VALID_BLOCK), 0);
        chain.commitState(toUnparsed(VALID_BLOCK), 0);

        CompleteMerkleTreeValidation validation = new CompleteMerkleTreeValidation(fs.getPath("/dummy"), chain);
        validation.commitState(toUnparsed(VALID_BLOCK), 0);

        // Save to jimfs
        validation.save(root);

        // Load into fresh instance
        CompleteMerkleTreeValidation loaded = new CompleteMerkleTreeValidation(fs.getPath("/dummy"), chain);
        loaded.load(root);

        // Build expected file for finalize comparison
        InMemoryTreeHasher expected = new InMemoryTreeHasher();
        expected.addNodeByHash(chain.getPreviousBlockHash());
        Path stateFile = root.resolve("completeMerkleTree.bin");
        expected.save(stateFile);

        CompleteMerkleTreeValidation withFile = new CompleteMerkleTreeValidation(stateFile, chain);
        withFile.load(root);
        assertDoesNotThrow(() -> withFile.finalize(1, 0));
    }

    @Test
    void completeMerkleTree_missingFile_onJimfs() {
        BlockChainValidation chain = new BlockChainValidation();
        Path nonExistent = root.resolve("completeMerkleTree.bin");
        CompleteMerkleTreeValidation validation = new CompleteMerkleTreeValidation(nonExistent, chain);

        ValidationException ex = assertThrows(ValidationException.class, () -> validation.finalize(0, -1));
        assertTrue(ex.getMessage().contains("not found"));
    }

    // ── StreamingMerkleTreeValidation on jimfs ──

    @Test
    void streamingMerkleTree_matchingHasher_onJimfs() throws Exception {
        BlockChainValidation chain = new BlockChainValidation();
        HistoricalBlockTreeValidation treeValidation = new HistoricalBlockTreeValidation(chain);

        // Must validate chain first so tree validation can capture the staged hash
        chain.validate(toUnparsed(VALID_BLOCK), 0);
        treeValidation.validate(toUnparsed(VALID_BLOCK), 0);
        chain.commitState(toUnparsed(VALID_BLOCK), 0);
        treeValidation.commitState(toUnparsed(VALID_BLOCK), 0);

        // Save the streaming hasher state to a file on jimfs
        StreamingHasher hasher = treeValidation.getStreamingHasher();
        Path stateFile = root.resolve("streamingMerkleTree.bin");
        hasher.save(stateFile);

        // Validate finalize passes
        StreamingMerkleTreeValidation validation = new StreamingMerkleTreeValidation(stateFile, treeValidation);

        assertDoesNotThrow(() -> validation.finalize(1, 0));
    }

    @Test
    void streamingMerkleTree_missingFile_onJimfs() {
        BlockChainValidation chain = new BlockChainValidation();
        HistoricalBlockTreeValidation treeValidation = new HistoricalBlockTreeValidation(chain);
        Path nonExistent = root.resolve("streamingMerkleTree.bin");

        StreamingMerkleTreeValidation validation = new StreamingMerkleTreeValidation(nonExistent, treeValidation);
        ValidationException ex = assertThrows(ValidationException.class, () -> validation.finalize(0, -1));
        assertTrue(ex.getMessage().contains("not found"));
    }

    // ── JumpstartValidation on jimfs ──

    @Test
    void jumpstart_matchingData_onJimfs() throws Exception {
        BlockChainValidation chain = new BlockChainValidation();
        HistoricalBlockTreeValidation treeValidation = new HistoricalBlockTreeValidation(chain);

        chain.validate(toUnparsed(VALID_BLOCK), 0);
        treeValidation.validate(toUnparsed(VALID_BLOCK), 0);
        chain.commitState(toUnparsed(VALID_BLOCK), 0);
        treeValidation.commitState(toUnparsed(VALID_BLOCK), 0);
        byte[] blockHash = chain.getPreviousBlockHash();

        StreamingHasher hasher = treeValidation.getStreamingHasher();

        // Write jumpstart.bin on jimfs in the expected format
        Path jumpstartFile = root.resolve("jumpstart.bin");
        writeJumpstartFile(jumpstartFile, 0, blockHash, hasher);

        JumpstartValidation validation = new JumpstartValidation(jumpstartFile, treeValidation, null);
        assertDoesNotThrow(() -> validation.finalize(1, 0));
    }

    @Test
    void jumpstart_wrongBlockNumber_onJimfs() throws Exception {
        BlockChainValidation chain = new BlockChainValidation();
        HistoricalBlockTreeValidation treeValidation = new HistoricalBlockTreeValidation(chain);

        chain.validate(toUnparsed(VALID_BLOCK), 0);
        treeValidation.validate(toUnparsed(VALID_BLOCK), 0);
        chain.commitState(toUnparsed(VALID_BLOCK), 0);
        treeValidation.commitState(toUnparsed(VALID_BLOCK), 0);
        byte[] blockHash = chain.getPreviousBlockHash();

        StreamingHasher hasher = treeValidation.getStreamingHasher();

        // Write with wrong block number
        Path jumpstartFile = root.resolve("jumpstart.bin");
        writeJumpstartFile(jumpstartFile, 99, blockHash, hasher);

        JumpstartValidation validation = new JumpstartValidation(jumpstartFile, treeValidation, null);
        ValidationException ex = assertThrows(ValidationException.class, () -> validation.finalize(1, 0));
        assertTrue(ex.getMessage().contains("block number"));
    }

    @Test
    void jumpstart_missingFile_onJimfs() {
        BlockChainValidation chain = new BlockChainValidation();
        HistoricalBlockTreeValidation treeValidation = new HistoricalBlockTreeValidation(chain);
        Path nonExistent = root.resolve("jumpstart.bin");

        JumpstartValidation validation = new JumpstartValidation(nonExistent, treeValidation, null);
        ValidationException ex = assertThrows(ValidationException.class, () -> validation.finalize(0, -1));
        assertTrue(ex.getMessage().contains("not found"));
    }

    // ── InMemoryTreeHasher save/load on jimfs ──

    @Test
    void inMemoryTreeHasher_saveAndLoad_onJimfs() throws Exception {
        InMemoryTreeHasher original = new InMemoryTreeHasher();
        byte[] hash1 = new byte[48];
        hash1[0] = 1;
        byte[] hash2 = new byte[48];
        hash2[0] = 2;
        original.addNodeByHash(hash1);
        original.addNodeByHash(hash2);

        Path file = root.resolve("hasher.bin");
        original.save(file);

        InMemoryTreeHasher loaded = new InMemoryTreeHasher();
        loaded.load(file);

        assertEquals(original.leafCount(), loaded.leafCount());
        assertEquals(Bytes.wrap(original.computeRootHash()), Bytes.wrap(loaded.computeRootHash()));
    }

    // ── StreamingHasher save/load on jimfs ──

    @Test
    void streamingHasher_saveAndLoad_onJimfs() throws Exception {
        StreamingHasher original = new StreamingHasher();
        byte[] hash = new byte[48];
        hash[0] = 42;
        original.addNodeByHash(hash);

        Path file = root.resolve("streaming.bin");
        original.save(file);

        StreamingHasher loaded = new StreamingHasher();
        loaded.load(file);

        assertEquals(original.leafCount(), loaded.leafCount());
        assertEquals(Bytes.wrap(original.computeRootHash()), Bytes.wrap(loaded.computeRootHash()));
    }

    /**
     * Write a jumpstart file in the expected binary format.
     *
     * @param path output path
     * @param blockNumber the block number
     * @param blockHash 48-byte block hash
     * @param hasher the streaming hasher whose state to write
     */
    private static void writeJumpstartFile(Path path, long blockNumber, byte[] blockHash, StreamingHasher hasher)
            throws IOException {
        List<byte[]> state = hasher.intermediateHashingState();
        try (OutputStream fos = Files.newOutputStream(path);
                DataOutputStream dos = new DataOutputStream(fos)) {
            dos.writeLong(blockNumber);
            dos.write(blockHash);
            dos.writeLong(hasher.leafCount());
            dos.writeInt(state.size());
            for (byte[] h : state) {
                dos.write(h);
            }
        }
    }
}
