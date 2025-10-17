package org.hiero.block.tools.utils;

import java.security.MessageDigest;

/**
 * Utility class for computing SHA-384 hashes.
 */
public class Sha384 {
    /** The size of an SHA-384 hash in bytes */
    public static final int SHA_384_HASH_SIZE = 48;

    /**
     * Compute the SHA-384 hash of the provided data.
     *
     * @param data the data to hash
     * @return the SHA-384 hash
     */
    public static byte[] hashSha384(byte[] data) {
        return sha384Digest().digest(data);
    }

    /**
     * Create and return a new MessageDigest instance for SHA-384.
     *
     * @return a MessageDigest for SHA-384
     */
    public static MessageDigest sha384Digest() {
        try {
            return MessageDigest.getInstance("SHA-384");
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-384 algorithm not found", e);
        }
    }
}
