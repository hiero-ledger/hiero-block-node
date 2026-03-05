// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

import com.hedera.hapi.block.stream.Block;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.hiero.block.tools.blocks.model.hashing.StreamingHasher;
import org.hiero.block.tools.records.model.parsed.ValidationException;

/**
 * Validates the {@code streamingMerkleTree.bin} state file by comparing it against the
 * freshly-built streaming hasher from {@link HistoricalBlockTreeValidation}.
 *
 * <p>This validation only performs work in {@link #finalize(long, long)} because the state
 * file comparison only makes sense after all blocks have been processed.
 */
public final class StreamingMerkleTreeValidation implements BlockValidation {

    /** Path to the streamingMerkleTree.bin file. */
    private final Path streamingMerkleTreePath;

    /** The tree validation that owns the freshly-built streaming hasher. */
    private final HistoricalBlockTreeValidation treeValidation;

    /**
     * Creates a new streaming merkle tree validation.
     *
     * @param streamingMerkleTreePath path to the streamingMerkleTree.bin file
     * @param treeValidation the historical block tree validation providing the fresh streaming hasher
     */
    public StreamingMerkleTreeValidation(
            final Path streamingMerkleTreePath, final HistoricalBlockTreeValidation treeValidation) {
        this.streamingMerkleTreePath = streamingMerkleTreePath;
        this.treeValidation = treeValidation;
    }

    @Override
    public String name() {
        return "Streaming Merkle Tree";
    }

    @Override
    public String description() {
        return "Verifies streamingMerkleTree.bin matches the freshly-computed streaming merkle tree";
    }

    @Override
    public boolean requiresGenesisStart() {
        return true;
    }

    @Override
    public void validate(final Block block, final long blockNumber) throws ValidationException {
        // No per-block validation — state file is checked in finalize()
    }

    @Override
    public void finalize(final long totalBlocksValidated, final long lastBlockNumber) throws ValidationException {
        if (!Files.exists(streamingMerkleTreePath)) {
            throw new ValidationException("streamingMerkleTree.bin not found: " + streamingMerkleTreePath);
        }
        StreamingHasher freshHasher = treeValidation.getStreamingHasher();
        StreamingHasher loadedHasher = new StreamingHasher();
        try {
            loadedHasher.load(streamingMerkleTreePath);
        } catch (Exception e) {
            throw new ValidationException("Failed to load streamingMerkleTree.bin: " + e.getMessage());
        }
        if (loadedHasher.leafCount() != totalBlocksValidated) {
            throw new ValidationException("streamingMerkleTree.bin leaf count " + loadedHasher.leafCount()
                    + " != expected " + totalBlocksValidated);
        }
        if (!Arrays.equals(loadedHasher.computeRootHash(), freshHasher.computeRootHash())) {
            throw new ValidationException("streamingMerkleTree.bin root hash mismatch");
        }
    }
}
