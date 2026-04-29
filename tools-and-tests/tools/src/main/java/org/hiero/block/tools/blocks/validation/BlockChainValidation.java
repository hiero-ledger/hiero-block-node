// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

import static org.hiero.block.tools.blocks.model.hashing.BlockStreamBlockHasher.hashBlock;
import static org.hiero.block.tools.blocks.model.hashing.HashingUtils.EMPTY_TREE_HASH;
import static org.hiero.block.tools.utils.PrettyPrint.simpleHash;

import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.pbj.runtime.ParseException;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.tools.blocks.HasherStateFiles;
import org.hiero.block.tools.blocks.validation.ParallelBlockPreprocessor.PreprocessedData;
import org.hiero.block.tools.records.model.parsed.ValidationException;
import org.jspecify.annotations.Nullable;

/**
 * Validates the blockchain chain by comparing the previous block hash stored in each block's
 * footer with the hash of the preceding block.
 *
 * <p>Internal state: the hash of the most recently committed block. On the first block
 * validated (when the previous hash is unknown), the chain check is skipped.
 */
public final class BlockChainValidation implements BlockValidation {

    /** The hash of the most recently committed block, or null before the first block. */
    private byte @Nullable [] previousBlockHash;

    /** The block hash computed during validate(), staged for commitState(). */
    private byte[] stagedBlockHash;

    @Override
    public String name() {
        return "Block Chain";
    }

    @Override
    public String description() {
        return "Verifies the previousBlockRootHash in the footer matches the hash of the prior block";
    }

    @Override
    public boolean requiresGenesisStart() {
        return false;
    }

    @Override
    public void validate(final BlockUnparsed block, final long blockNumber) throws ValidationException {
        validate(block, blockNumber, (byte[]) null);
    }

    @Override
    public void validate(final BlockUnparsed block, final long blockNumber, final PreprocessedData preprocessed)
            throws ValidationException {
        validate(block, blockNumber, preprocessed != null ? preprocessed.blockHash() : null);
    }

    /**
     * Validates the block chain with an optional pre-computed block hash.
     *
     * @param block the shallow-parsed block
     * @param blockNumber the block number
     * @param preComputedHash pre-computed hash from parallel preprocessing, or null to compute here
     * @throws ValidationException if the chain is broken
     */
    public void validate(final BlockUnparsed block, final long blockNumber, final byte @Nullable [] preComputedHash)
            throws ValidationException {
        // Find the footer and selectively parse it
        BlockFooter footer = null;
        try {
            for (final BlockItemUnparsed item : block.blockItems()) {
                if (item.hasBlockFooter()) {
                    footer = BlockFooter.PROTOBUF.parse(item.blockFooterOrThrow());
                    break;
                }
            }
        } catch (ParseException e) {
            throw new ValidationException(
                    "Block: " + blockNumber + " - Failed to parse BlockFooter: " + e.getMessage());
        }
        if (footer == null) {
            throw new ValidationException(
                    "Block: " + blockNumber + " - Block footer with previousBlockRootHash not found");
        }
        final byte[] readHash = footer.previousBlockRootHash().toByteArray();
        if (previousBlockHash != null) {
            if (!Arrays.equals(previousBlockHash, readHash)) {
                throw new ValidationException("Block: " + blockNumber
                        + " - Blockchain is not valid: previous block hash mismatch. " + "previousBlockHash= "
                        + simpleHash(previousBlockHash) + " readHash= " + simpleHash(readHash));
            }
        } else {
            // First block: previousBlockRootHash must be the empty-tree hash or empty (testnet genesis)
            if (readHash.length != 0 && !Arrays.equals(readHash, EMPTY_TREE_HASH)) {
                throw new ValidationException("Block: " + blockNumber
                        + " - First block should have empty-tree previous hash. readHash= " + simpleHash(readHash));
            }
        }
        // Stage the block hash for commit — use pre-computed hash if available
        stagedBlockHash = (preComputedHash != null) ? preComputedHash : hashBlock(block);
    }

    @Override
    public void commitState(final BlockUnparsed block, final long blockNumber) {
        previousBlockHash = stagedBlockHash;
        stagedBlockHash = null;
    }

    /**
     * Returns the hash of the most recently committed block. This can be used by other
     * validations that need the block hash (e.g. {@link HistoricalBlockTreeValidation}).
     *
     * @return the previous block hash, or null if no block has been committed yet
     */
    public byte @Nullable [] getPreviousBlockHash() {
        return previousBlockHash;
    }

    /**
     * Returns the block hash that was computed during the most recent {@link #validate} call.
     * This is available between {@code validate()} and {@code commitState()} and can be used
     * by other validations to avoid recomputing the hash.
     *
     * @return the staged block hash
     */
    public byte[] getStagedBlockHash() {
        return stagedBlockHash;
    }

    /**
     * Sets the previous block hash directly, used when restoring from a checkpoint.
     *
     * @param hash the hash of the last validated block, or null to reset
     */
    public void setPreviousBlockHash(byte @Nullable [] hash) {
        this.previousBlockHash = hash;
    }

    private static final String SAVE_FILE_NAME = "chainValidation.bin";

    @Override
    public void save(final Path directory) throws IOException {
        if (previousBlockHash == null) return;
        Path file = directory.resolve(SAVE_FILE_NAME);
        try {
            HasherStateFiles.saveAtomically(file, path -> {
                try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(path))) {
                    out.writeInt(previousBlockHash.length);
                    out.write(previousBlockHash);
                }
            });
        } catch (Exception e) {
            throw new IOException("Failed to save chain validation state", e);
        }
    }

    @Override
    public void load(final Path directory) throws IOException {
        Path file = directory.resolve(SAVE_FILE_NAME);
        HasherStateFiles.loadWithFallback(file, path -> {
            try (DataInputStream in = new DataInputStream(Files.newInputStream(path))) {
                int len = in.readInt();
                previousBlockHash = new byte[len];
                in.readFully(previousBlockHash);
            }
        });
    }
}
