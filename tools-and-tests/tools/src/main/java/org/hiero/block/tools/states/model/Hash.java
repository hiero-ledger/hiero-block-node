package org.hiero.block.tools.states.model;

import org.hiero.block.tools.states.utils.Utilities;
import java.io.DataInputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.HexFormat;

// com.swirlds.platform.Hash
public record Hash(
        byte[] hash,
        int hashMapSeed
) {
    public static Hash readHash(DataInputStream dis) throws IOException {
        int hashMapSeed = dis.readInt();
        byte[] hash = Utilities.readByteArray(dis);
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
        return "Hash{" +
                "hashMapSeed=" + hashMapSeed +
                ", hash=" + HexFormat.of().formatHex(hash) +
                '}';
    }
}
