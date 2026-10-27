// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.tools.blocks.model.hashing.BlockStreamBlockHashRegistry;
import org.hiero.block.tools.blocks.model.hashing.StreamingHasher;
import org.hiero.block.tools.records.model.parsed.ValidationException;
import org.hiero.block.tools.utils.Sha256;
import org.hiero.block.tools.utils.Sha384;
import org.jspecify.annotations.Nullable;

/**
 * Validates the {@code jumpstart.bin} state file by checking its contents against the
 * freshly-built streaming hasher and optional hash registry.
 *
 * <p>The jumpstart file contains:
 * <ol>
 *   <li>Block number (long)</li>
 *   <li>Block hash (32 bytes)</li>
 *   <li>Consensus timestamp hash (32 bytes, SHA-256 leaf hash of the block's first consensus timestamp)</li>
 *   <li>Output items tree root hash (32 bytes, streaming merkle root of all output items)</li>
 *   <li>Leaf count (long)</li>
 *   <li>Hash count (int) followed by that many 32-byte hashes (streaming hasher state)</li>
 * </ol>
 *
 * <p>This validation only performs work in {@link #finalize(long, long)} because the state
 * file comparison only makes sense after all blocks have been processed.
 */
public final class JumpstartValidation implements BlockValidation {

    /** Offset of the leaf count in the SHA-256 layout: block number and three 32-byte hashes. */
    private static final int LEAF_COUNT_OFFSET = Long.BYTES + 3 * Sha256.SHA_256_HASH_SIZE;

    /** Offset of the leaf count in the legacy SHA-384 layout: block number and three 48-byte hashes. */
    private static final int LEGACY_LEAF_COUNT_OFFSET = Long.BYTES + 3 * Sha384.SHA_384_HASH_SIZE;

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
        try {
            final ByteBuffer buffer = ByteBuffer.wrap(Files.readAllBytes(jumpstartPath));
            final String layoutProblem = checkLayout(buffer);
            if (layoutProblem != null) {
                throw new ValidationException(layoutProblem);
            }
            long jBlockNum = buffer.getLong();
            byte[] jHash = new byte[Sha256.SHA_256_HASH_SIZE];
            buffer.get(jHash);
            byte[] jConsensusTimestampHash = new byte[Sha256.SHA_256_HASH_SIZE];
            buffer.get(jConsensusTimestampHash);
            byte[] jOutputItemsTreeRootHash = new byte[Sha256.SHA_256_HASH_SIZE];
            buffer.get(jOutputItemsTreeRootHash);
            long jLeafCount = buffer.getLong();
            int jHashCount = buffer.getInt();
            List<byte[]> jHashes = new ArrayList<>();
            for (int i = 0; i < jHashCount; i++) {
                byte[] h = new byte[Sha256.SHA_256_HASH_SIZE];
                buffer.get(h);
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

    /**
     * Checks that {@code buffer} has the SHA-256 jumpstart layout. Reads by absolute index, so the buffer's position
     * is left unchanged.
     *
     * @param buffer the whole content of jumpstart.bin
     * @return null if it does, otherwise why not
     */
    private static @Nullable String checkLayout(final ByteBuffer buffer) {
        String problem = null;
        if (!layoutMatches(buffer, LEAF_COUNT_OFFSET, Sha256.SHA_256_HASH_SIZE)) {
            if (layoutMatches(buffer, LEGACY_LEAF_COUNT_OFFSET, Sha384.SHA_384_HASH_SIZE)) {
                problem = "jumpstart.bin has the SHA-384 layout of an older CLI version. Re-wrap the blocks with this"
                        + " CLI version.";
            } else {
                problem = "jumpstart.bin is " + buffer.limit() + " bytes and does not match the jumpstart layout. The"
                        + " file is corrupt.";
            }
        }
        return problem;
    }

    /**
     * Checks the leaf count and hash count at {@code leafCountOffset} against each other and against the file size.
     *
     * @param buffer the whole content of jumpstart.bin
     * @param leafCountOffset offset of the leaf count for the layout being probed
     * @param hashSize size in bytes of each hash in the layout being probed
     * @return true if the hash count equals the number of set bits in the leaf count and the file has exactly that
     *     many hashes of {@code hashSize} bytes after the header
     */
    private static boolean layoutMatches(final ByteBuffer buffer, final int leafCountOffset, final int hashSize) {
        boolean matches = false;
        final int headerEnd = leafCountOffset + Long.BYTES + Integer.BYTES;
        if (buffer.limit() >= headerEnd) {
            final long leafCount = buffer.getLong(leafCountOffset);
            final int hashCount = buffer.getInt(leafCountOffset + Long.BYTES);
            matches = leafCount >= 0
                    && hashCount == Long.bitCount(leafCount)
                    && buffer.limit() == headerEnd + (long) hashCount * hashSize;
        }
        return matches;
    }
}
