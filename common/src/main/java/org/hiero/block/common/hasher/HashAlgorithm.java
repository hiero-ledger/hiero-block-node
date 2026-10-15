// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.common.hasher;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/// A hash algorithm with the conveniences the hashing code needs. Each constant carries the standard
/// JCA message digest name, the digest size in bytes and the precomputed empty tree hash,
/// `hash(0x00)`, that an empty Merkle subtree contributes to its parent, and provides a fresh
/// message digest on request.
///
/// The block hashing utilities and streaming hashers of this package always compute with
/// [#SHA2_256], the algorithm of the block root tree. [#SHA2_384] is the algorithm of the
/// record file signature payload of a wrapped record block, see
/// [HashingUtilities#computeV6SignedPayload].
///
/// The constant names mirror the names of the HAPI `BlockHashAlgorithm` enum, so a mapping
/// from a block header value is a one to one name match.
public enum HashAlgorithm {
    /// SHA2 with a 256 bit (32 byte) digest.
    SHA2_256("SHA-256", 32, "6e340b9cffb37a989ca544e6bb780a2c78901d3fb33738768511a30617afa01d"),
    /// SHA2 with a 384 bit (48 byte) digest.
    SHA2_384(
            "SHA-384",
            48,
            "bec021b4f368e3069134e012c2b4307083d3a9bdd206e24e5f0d86e13d6636655933ec2b413465966817a9c208a11717");

    /// The standard JCA message digest name.
    private final String jcaName;
    /// The size of a digest, in bytes.
    private final int hashSize;
    /// The hash of an empty Merkle tree: `hash(0x00)`.
    private final Bytes emptyTreeHash;

    /// Creates a constant. The empty tree hash is given as a precomputed literal because computing it
    /// here would require a message digest, and an enum constructor cannot throw the checked
    /// [NoSuchAlgorithmException] that obtaining one declares. Tests pin every literal to the
    /// digest of a single zero byte computed with [#newDigest()].
    ///
    /// @param jcaName the standard JCA message digest name
    /// @param hashSize the size of a digest, in bytes
    /// @param emptyTreeHashHex the hex encoded hash of a single zero byte
    HashAlgorithm(final String jcaName, final int hashSize, final String emptyTreeHashHex) {
        this.jcaName = jcaName;
        this.hashSize = hashSize;
        this.emptyTreeHash = Bytes.fromHex(emptyTreeHashHex);
    }

    /// Returns the standard JCA message digest name of this algorithm, for example `"SHA-256"`.
    ///
    /// @return the JCA name
    public String jcaName() {
        return jcaName;
    }

    /// Returns the size of a digest produced by this algorithm, in bytes.
    ///
    /// @return the digest size in bytes
    public int hashSize() {
        return hashSize;
    }

    /// Returns the hash of an empty Merkle tree under this algorithm, the hash of a single zero
    /// byte. An empty subtree contributes this value to its parent, matching the consensus node
    /// convention introduced in HAPI v0.72.
    ///
    /// @return the empty tree hash, [#hashSize()] bytes long
    public Bytes emptyTreeHash() {
        return emptyTreeHash;
    }

    /// Returns a fresh [MessageDigest] for this algorithm.
    ///
    /// @return a new message digest for this algorithm
    /// @throws NoSuchAlgorithmException if the Java runtime provides no implementation of this algorithm
    public MessageDigest newDigest() throws NoSuchAlgorithmException {
        return MessageDigest.getInstance(jcaName);
    }
}
