// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.model;

import com.hedera.hapi.block.stream.Block;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Utility class for computing SHA-384 hashes of Block objects.
 *
 * <p>Computes the SHA-384 hash of the protobuf-serialized Block. This provides
 * a consistent hash that can be used for block identification and chain validation.
 *
 * <p>Note: For wrapped blocks, a more sophisticated hash computation using the
 * RecordStreamFile's running hash algorithm may be needed for full validation.
 * This implementation provides a simple protobuf hash suitable for basic validation.
 */
public final class BlockHashCalculator {

    /** Private constructor to prevent instantiation. */
    private BlockHashCalculator() {}

    /**
     * Compute the SHA-384 hash of a Block.
     *
     * @param block the Block to hash
     * @return the 48-byte SHA-384 hash of the block
     * @throws RuntimeException if hash computation fails
     */
    public static byte[] computeBlockHash(final Block block) {
        if (block == null) {
            throw new IllegalArgumentException("Block cannot be null");
        }

        try {
            final MessageDigest digest = MessageDigest.getInstance("SHA-384");
            final byte[] blockBytes = Block.PROTOBUF.toBytes(block).toByteArray();
            return digest.digest(blockBytes);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-384 algorithm not available", e);
        }
    }

    /**
     * Format a hash as a hex string for display.
     *
     * @param hash the hash bytes
     * @return hex string representation
     */
    public static String hashToHex(final byte[] hash) {
        if (hash == null) {
            return "null";
        }
        return Bytes.wrap(hash).toHex();
    }

    /**
     * Format a hash as a shortened hex string for display (first 8 chars).
     *
     * @param hash the hash bytes
     * @return shortened hex string
     */
    public static String shortHash(final byte[] hash) {
        if (hash == null) {
            return "null";
        }
        final String hex = hashToHex(hash);
        return hex.length() <= 8 ? hex : hex.substring(0, 8);
    }
}
