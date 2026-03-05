// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

import static org.hiero.block.tools.utils.PrettyPrint.simpleHash;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import org.hiero.block.tools.blocks.model.hashing.StreamingHasher;
import org.hiero.block.tools.records.model.parsed.ValidationException;

/**
 * Validates the all-blocks merkle tree root hash stored in each block's footer.
 *
 * <p>Maintains a {@link StreamingHasher} that tracks the running merkle tree of all block
 * hashes. Before adding the current block's hash, the hasher's root must match the
 * {@code rootHashOfAllBlockHashesTree} field in the footer.
 *
 * <p>This validation requires starting from block 0 because the full block hash history
 * is needed to compute the correct merkle tree root.
 *
 * <p>This validation depends on {@link BlockChainValidation} to provide the block hash
 * (to avoid computing it twice).
 */
public final class HistoricalBlockTreeValidation implements BlockValidation {

    /** The streaming hasher tracking the all-blocks merkle tree. */
    private final StreamingHasher streamingHasher = new StreamingHasher();

    /** The chain validation that provides the block hash. */
    private final BlockChainValidation chainValidation;

    /** Block hash captured during validate() for use in commitState(). */
    private byte[] capturedBlockHash;

    /**
     * Creates a new validation that uses the given chain validation to obtain block hashes.
     *
     * @param chainValidation the chain validation that computes block hashes
     */
    public HistoricalBlockTreeValidation(final BlockChainValidation chainValidation) {
        this.chainValidation = chainValidation;
    }

    @Override
    public String name() {
        return "Historical Block Tree";
    }

    @Override
    public String description() {
        return "Verifies the rootHashOfAllBlockHashesTree in the footer matches the streaming merkle tree";
    }

    @Override
    public boolean requiresGenesisStart() {
        return true;
    }

    @Override
    public void validate(final Block block, final long blockNumber) throws ValidationException {
        final byte[] expectedHash = streamingHasher.computeRootHash();
        final var footer = block.items().stream()
                .filter(BlockItem::hasBlockFooter)
                .findFirst()
                .orElseThrow(() -> new ValidationException(
                        "Block: " + blockNumber + "Block footer with historical block tree root not found"))
                .blockFooterOrThrow();
        final var treeRootBytes = footer.rootHashOfAllBlockHashesTree();
        final byte[] readHash = treeRootBytes != null ? treeRootBytes.toByteArray() : null;
        if (!Arrays.equals(expectedHash, readHash)) {
            throw new ValidationException("Block: " + blockNumber + " - Historical block tree root hash mismatch. "
                    + "expectedHash= " + (expectedHash != null ? simpleHash(expectedHash) : "null")
                    + " readHash= " + (readHash != null ? simpleHash(readHash) : "null"));
        }
        // Capture the block hash now (before chain validation's commitState nulls it)
        capturedBlockHash = chainValidation.getStagedBlockHash();
    }

    @Override
    public void commitState(final Block block, final long blockNumber) {
        streamingHasher.addNodeByHash(capturedBlockHash);
        capturedBlockHash = null;
    }

    /**
     * Returns the streaming hasher for external access (e.g. for state file validation).
     *
     * @return the streaming hasher
     */
    public StreamingHasher getStreamingHasher() {
        return streamingHasher;
    }

    private static final String SAVE_FILE_NAME = "historicalTreeValidation.bin";

    @Override
    public void save(final Path directory) throws IOException {
        try {
            streamingHasher.save(directory.resolve(SAVE_FILE_NAME));
        } catch (Exception e) {
            throw new IOException("Failed to save streaming hasher", e);
        }
    }

    @Override
    public void load(final Path directory) throws IOException {
        Path file = directory.resolve(SAVE_FILE_NAME);
        if (!java.nio.file.Files.exists(file)) return;
        try {
            streamingHasher.load(file);
        } catch (Exception e) {
            throw new IOException("Failed to load streaming hasher", e);
        }
    }
}
