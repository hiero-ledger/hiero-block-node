// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.tools.blocks.model.hashing.BlockStreamBlockHashRegistry;
import org.hiero.block.tools.blocks.model.hashing.InMemoryTreeHasher;
import org.hiero.block.tools.records.model.parsed.ValidationException;

/**
 * Validates the {@code completeMerkleTree.bin} state file by comparing it against a
 * freshly-built {@link InMemoryTreeHasher} that is populated during block validation.
 *
 * <p>During each block's commit phase, the block hash from {@link BlockChainValidation} is
 * fed into an internal {@link InMemoryTreeHasher}. After all blocks are processed, the
 * {@link #finalize(long, long)} method compares the fresh hasher against the saved state file.
 */
public final class CompleteMerkleTreeValidation implements BlockValidation {

    /** Path to the completeMerkleTree.bin file. */
    private final Path completeMerkleTreePath;

    /** The chain validation that provides computed block hashes. */
    private final BlockChainValidation chainValidation;

    /** The in-memory tree hasher built from validated blocks. */
    private final InMemoryTreeHasher freshInMemoryHasher = new InMemoryTreeHasher();

    /**
     * Creates a new complete merkle tree validation.
     *
     * @param completeMerkleTreePath path to the completeMerkleTree.bin file
     * @param chainValidation the chain validation that provides block hashes
     */
    public CompleteMerkleTreeValidation(final Path completeMerkleTreePath, final BlockChainValidation chainValidation) {
        this.completeMerkleTreePath = completeMerkleTreePath;
        this.chainValidation = chainValidation;
    }

    @Override
    public String name() {
        return "Complete Merkle Tree";
    }

    @Override
    public String description() {
        return "Verifies completeMerkleTree.bin matches the freshly-computed in-memory merkle tree";
    }

    @Override
    public boolean requiresGenesisStart() {
        return true;
    }

    @Override
    public void validate(final BlockUnparsed block, final long blockNumber) throws ValidationException {
        // No per-block validation — state file is checked in finalize()
    }

    @Override
    public void commitState(final BlockUnparsed block, final long blockNumber) {
        freshInMemoryHasher.addNodeByHash(chainValidation.getPreviousBlockHash());
    }

    @Override
    public void finalize(final long totalBlocksValidated, final long lastBlockNumber) throws ValidationException {
        if (!Files.exists(completeMerkleTreePath)) {
            throw new ValidationException("completeMerkleTree.bin not found: " + completeMerkleTreePath);
        }
        InMemoryTreeHasher loadedHasher = new InMemoryTreeHasher();
        try {
            loadedHasher.load(completeMerkleTreePath);
        } catch (Exception e) {
            throw new ValidationException("Failed to load completeMerkleTree.bin: " + e.getMessage());
        }
        if (loadedHasher.leafCount() != totalBlocksValidated) {
            throw new ValidationException("completeMerkleTree.bin leaf count " + loadedHasher.leafCount()
                    + " != expected " + totalBlocksValidated);
        }
        if (!Arrays.equals(loadedHasher.computeRootHash(), freshInMemoryHasher.computeRootHash())) {
            throw new ValidationException("completeMerkleTree.bin root hash mismatch");
        }
    }

    /**
     * Replays block hashes from the registry into the in-memory hasher. Used during checkpoint
     * resume to rebuild the hasher state for blocks that were already validated.
     *
     * @param registry the block hash registry to replay from
     * @param firstBlock the first block number to replay (inclusive)
     * @param lastBlock the last block number to replay (inclusive)
     */
    public void replayFromRegistry(
            final BlockStreamBlockHashRegistry registry, final long firstBlock, final long lastBlock) {
        for (long bn = firstBlock; bn <= lastBlock; bn++) {
            freshInMemoryHasher.addNodeByHash(registry.getBlockHash(bn));
        }
    }

    private static final String SAVE_FILE_NAME = "completeMerkleTreeValidation.bin";

    @Override
    public void save(final Path directory) throws IOException {
        try {
            freshInMemoryHasher.save(directory.resolve(SAVE_FILE_NAME));
        } catch (Exception e) {
            throw new IOException("Failed to save in-memory hasher", e);
        }
    }

    @Override
    public void load(final Path directory) throws IOException {
        Path file = directory.resolve(SAVE_FILE_NAME);
        if (!Files.exists(file)) return;
        try {
            freshInMemoryHasher.load(file);
        } catch (Exception e) {
            throw new IOException("Failed to load in-memory hasher", e);
        }
    }
}
