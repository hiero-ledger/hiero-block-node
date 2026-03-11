// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

import java.io.DataInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.tools.blocks.model.hashing.BlockStreamBlockHashRegistry;
import org.hiero.block.tools.blocks.model.hashing.StreamingHasher;
import org.hiero.block.tools.records.model.parsed.ValidationException;
import org.jspecify.annotations.Nullable;

/**
 * Validates the {@code jumpstart.bin} state file by checking its contents against the
 * freshly-built streaming hasher and optional hash registry.
 *
 * <p>The jumpstart file contains:
 * <ol>
 *   <li>Block number (long)</li>
 *   <li>Block hash (48 bytes)</li>
 *   <li>Leaf count (long)</li>
 *   <li>Hash count (int) followed by that many 48-byte hashes (streaming hasher state)</li>
 * </ol>
 *
 * <p>This validation only performs work in {@link #finalize(long, long)} because the state
 * file comparison only makes sense after all blocks have been processed.
 */
public final class JumpstartValidation implements BlockValidation {

    /** Path to the jumpstart.bin file. */
    private final Path jumpstartPath;

    /** The tree validation that owns the freshly-built streaming hasher. */
    private final HistoricalBlockTreeValidation treeValidation;

    /** Optional hash registry for block hash comparison. */
    private final @Nullable BlockStreamBlockHashRegistry registry;

    /**
     * Creates a new jumpstart validation.
     *
     * @param jumpstartPath path to the jumpstart.bin file
     * @param treeValidation the historical block tree validation providing the fresh streaming hasher
     * @param registry optional block hash registry for hash comparison (may be null)
     */
    public JumpstartValidation(
            final Path jumpstartPath,
            final HistoricalBlockTreeValidation treeValidation,
            final @Nullable BlockStreamBlockHashRegistry registry) {
        this.jumpstartPath = jumpstartPath;
        this.treeValidation = treeValidation;
        this.registry = registry;
    }

    @Override
    public String name() {
        return "Jumpstart";
    }

    @Override
    public String description() {
        return "Verifies jumpstart.bin matches the freshly-computed streaming merkle tree and block hashes";
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
    public void finalize(final long totalBlocksValidated, final long lastBlockNumber) throws ValidationException {
        if (!Files.exists(jumpstartPath)) {
            throw new ValidationException("jumpstart.bin not found: " + jumpstartPath);
        }
        StreamingHasher freshHasher = treeValidation.getStreamingHasher();
        try (DataInputStream din = new DataInputStream(Files.newInputStream(jumpstartPath))) {
            long jBlockNum = din.readLong();
            byte[] jHash = new byte[48];
            din.readFully(jHash);
            long jLeafCount = din.readLong();
            int jHashCount = din.readInt();
            List<byte[]> jHashes = new ArrayList<>();
            for (int i = 0; i < jHashCount; i++) {
                byte[] h = new byte[48];
                din.readFully(h);
                jHashes.add(h);
            }

            if (jBlockNum != lastBlockNumber) {
                throw new ValidationException(
                        "jumpstart.bin block number " + jBlockNum + " != expected " + lastBlockNumber);
            }
            if (registry != null) {
                byte[] registryHash = registry.getBlockHash(jBlockNum);
                if (!Arrays.equals(jHash, registryHash)) {
                    throw new ValidationException("jumpstart.bin block hash does not match registry");
                }
            }
            if (jLeafCount != freshHasher.leafCount()) {
                throw new ValidationException(
                        "jumpstart.bin leaf count " + jLeafCount + " != expected " + freshHasher.leafCount());
            }
            StreamingHasher jumpstartHasher = new StreamingHasher(jHashes);
            if (!Arrays.equals(jumpstartHasher.computeRootHash(), freshHasher.computeRootHash())) {
                throw new ValidationException("jumpstart.bin streaming tree root mismatch");
            }
        } catch (ValidationException e) {
            throw e;
        } catch (Exception e) {
            throw new ValidationException("Failed to read jumpstart.bin: " + e.getMessage());
        }
    }
}
