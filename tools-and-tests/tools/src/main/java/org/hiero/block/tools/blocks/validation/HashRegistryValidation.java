// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

import static org.hiero.block.tools.utils.PrettyPrint.simpleHash;

import java.util.Arrays;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.tools.blocks.model.hashing.BlockStreamBlockHashRegistry;
import org.hiero.block.tools.records.model.parsed.ValidationException;

/**
 * Validates that each block's computed hash matches the hash stored in the
 * {@code blockStreamBlockHashes.bin} registry file.
 *
 * <p>This validation depends on {@link BlockChainValidation} to provide the computed block
 * hash (to avoid recomputing it). The registry is an externally-opened
 * {@link BlockStreamBlockHashRegistry} whose lifecycle is managed by the caller.
 */
public final class HashRegistryValidation implements BlockValidation {

    /** The hash registry to check against. */
    private final BlockStreamBlockHashRegistry registry;

    /** The chain validation that provides the computed block hash. */
    private final BlockChainValidation chainValidation;

    /**
     * Creates a new hash registry validation.
     *
     * @param registry the block stream block hash registry
     * @param chainValidation the chain validation that computes block hashes
     */
    public HashRegistryValidation(
            final BlockStreamBlockHashRegistry registry, final BlockChainValidation chainValidation) {
        this.registry = registry;
        this.chainValidation = chainValidation;
    }

    @Override
    public String name() {
        return "Hash Registry";
    }

    @Override
    public String description() {
        return "Verifies per-block hash matches blockStreamBlockHashes.bin registry";
    }

    @Override
    public boolean requiresGenesisStart() {
        return false;
    }

    @Override
    public void validate(final BlockUnparsed block, final long blockNumber) throws ValidationException {
        final byte[] computedHash = chainValidation.getStagedBlockHash();
        final byte[] storedHash = registry.getBlockHash(blockNumber);
        if (!Arrays.equals(computedHash, storedHash)) {
            throw new ValidationException("Block: " + blockNumber
                    + " - Hash mismatch in blockStreamBlockHashes.bin. computed= " + simpleHash(computedHash)
                    + " stored= " + simpleHash(storedHash));
        }
    }

    @Override
    public void finalize(final long totalBlocksValidated, final long lastBlockNumber) throws ValidationException {
        long highestStored = registry.highestBlockNumberStored();
        if (highestStored != lastBlockNumber) {
            throw new ValidationException("blockStreamBlockHashes.bin highest stored block " + highestStored
                    + " != expected " + lastBlockNumber);
        }
    }

    @Override
    public void close() {
        try {
            registry.close();
        } catch (Exception ignored) {
            // best-effort close
        }
    }
}
