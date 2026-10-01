// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.common.hasher;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * A hash algorithm the hashing utilities and streaming hashers of this package compute with.
 * <p>
 * Every utility and hasher in this package takes the algorithm to use as an explicit argument, so
 * the same code computes the block root tree with one algorithm and, where a data format requires
 * it, legacy payloads with another. Each constant carries the standard JCA message digest name, the
 * digest size in bytes and the precomputed empty tree hash, {@code hash(0x00)}, that an empty
 * Merkle subtree contributes to its parent.
 * <p>
 * The constant names mirror the names of the HAPI {@code BlockHashAlgorithm} enum, so a future
 * mapping from a block header value is a one to one name match.
 */
public enum HashAlgorithm {
    /** SHA2 with a 256 bit (32 byte) digest. */
    SHA2_256("SHA-256", 32),
    /** SHA2 with a 384 bit (48 byte) digest. */
    SHA2_384("SHA-384", 48);

    /** The standard JCA message digest name. */
    private final String jcaName;
    /** The size of a digest, in bytes. */
    private final int hashSize;
    /** The hash of an empty Merkle tree: {@code hash(0x00)}. */
    private final Bytes emptyTreeHash;

    HashAlgorithm(final String jcaName, final int hashSize) {
        this.jcaName = jcaName;
        this.hashSize = hashSize;
        this.emptyTreeHash = Bytes.wrap(newDigest().digest(new byte[] {0x00}));
    }

    /**
     * Returns the standard JCA message digest name of this algorithm, for example {@code "SHA-256"}.
     * @return the JCA name
     */
    public String jcaName() {
        return jcaName;
    }

    /**
     * Returns the size of a digest produced by this algorithm, in bytes.
     * @return the digest size in bytes
     */
    public int hashSize() {
        return hashSize;
    }

    /**
     * Returns the hash of an empty Merkle tree under this algorithm, the hash of a single zero
     * byte. An empty subtree contributes this value to its parent, matching the consensus node
     * convention introduced in HAPI v0.72.
     * @return the empty tree hash, {@link #hashSize()} bytes long
     */
    public Bytes emptyTreeHash() {
        return emptyTreeHash;
    }

    /**
     * Returns a fresh {@link MessageDigest} for this algorithm. Both algorithms ship with every
     * Java runtime the node supports, so a missing algorithm is a broken runtime and is reported
     * as an {@link IllegalStateException}.
     * @return a new message digest for this algorithm
     */
    public MessageDigest newDigest() {
        try {
            return MessageDigest.getInstance(jcaName);
        } catch (final NoSuchAlgorithmException fatal) {
            throw new IllegalStateException("Hash algorithm %s is not available".formatted(jcaName), fatal);
        }
    }
}
