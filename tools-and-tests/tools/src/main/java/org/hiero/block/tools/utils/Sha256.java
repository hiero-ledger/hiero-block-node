// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.utils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Utility class for computing SHA-256 hashes.
 */
public class Sha256 {
    /** The size of an SHA-256 hash in bytes */
    public static final int SHA_256_HASH_SIZE = 32;

    /**
     * Compute the SHA-256 hash of the provided data.
     *
     * @param data the data to hash
     * @return the SHA-256 hash
     */
    public static byte[] hashSha256(byte[] data) {
        return sha256Digest().digest(data);
    }

    /**
     * Create and return a new MessageDigest instance for SHA-256.
     *
     * @return a MessageDigest for SHA-256
     */
    public static MessageDigest sha256Digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 algorithm not found", e);
        }
    }
}
