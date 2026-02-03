// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.DataInputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.HexFormat;
import org.hiero.block.tools.states.utils.Utils;

/**
 * A serializable SHA-384 hash with a hash-map seed, replacing {@code com.swirlds.platform.Hash}.
 *
 * @param hash the raw hash bytes
 * @param hashMapSeed the seed value used for hash map bucketing
 */
public record Hash(byte[] hash, int hashMapSeed) {
    /**
     * Deserializes a Hash from the given stream.
     *
     * @param dis the stream to read from
     * @return the deserialized Hash
     * @throws IOException if an I/O error occurs
     */
    public static Hash readHash(DataInputStream dis) throws IOException {
        int hashMapSeed = dis.readInt();
        byte[] hash = Utils.readByteArray(dis);
        return new Hash(hash, hashMapSeed);
    }

    /**
     * hash the given long
     */
    static void update(MessageDigest digest, long n) {
        for (int i = 0; i < Long.BYTES; i++) {
            digest.update((byte) (n & 0xFF));
            n >>= Byte.SIZE;
        }
    }

    /**
     * hash the given int
     */
    static void update(MessageDigest digest, int n) {
        for (int i = 0; i < Integer.BYTES; i++) {
            digest.update((byte) (n & 0xFF));
            n >>= Byte.SIZE;
        }
    }

    /**
     * Returns the hash bytes formatted as a lowercase hex string.
     *
     * @return the hash in hexadecimal format
     */
    public String hex() {
        return HexFormat.of().formatHex(hash);
    }

    /**
     * hash the given Instant
     */
    static void update(MessageDigest digest, Instant i) {
        // the instant class consists of only 2 parts, the seconds and the nanoseconds
        update(digest, i.getEpochSecond());
        update(digest, i.getNano());
    }

    @Override
    public String toString() {
        return "Hash{" + "hashMapSeed="
                + hashMapSeed + ", hash="
                + HexFormat.of().formatHex(hash) + '}';
    }
}
