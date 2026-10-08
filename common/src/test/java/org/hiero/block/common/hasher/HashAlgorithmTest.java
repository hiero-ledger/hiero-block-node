// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.common.hasher;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/// Tests for the [HashAlgorithm] enum.
@DisplayName("Hash Algorithm Tests")
class HashAlgorithmTest {
    /// SHA-256 of a single zero byte, produced with `printf '\x00' | shasum -a 256`.
    private static final String SHA2_256_EMPTY_TREE_HASH =
            "6e340b9cffb37a989ca544e6bb780a2c78901d3fb33738768511a30617afa01d";
    /// SHA-384 of a single zero byte, produced with `printf '\x00' | shasum -a 384`.
    private static final String SHA2_384_EMPTY_TREE_HASH =
            "bec021b4f368e3069134e012c2b4307083d3a9bdd206e24e5f0d86e13d6636655933ec2b413465966817a9c208a11717";

    /// Tests for the digest properties of the constants.
    @Nested
    @DisplayName("Digest Tests")
    class DigestTests {
        /// This test aims to assert that the JCA name of every constant is the standard message
        /// digest name of the algorithm it stands for, so a digest obtained through the name is
        /// the algorithm the constant promises.
        @Test
        @DisplayName("jcaName() is the standard digest name")
        void testJcaNames() {
            assertThat(HashAlgorithm.SHA2_256.jcaName()).isEqualTo("SHA-256");
            assertThat(HashAlgorithm.SHA2_384.jcaName()).isEqualTo("SHA-384");
        }

        /// This test aims to assert that the digest size of every constant is the size of the
        /// digests the algorithm produces: 32 bytes for SHA-256 and 48 bytes for SHA-384.
        @Test
        @DisplayName("hashSize() is the digest size in bytes")
        void testHashSizes() {
            assertThat(HashAlgorithm.SHA2_256.hashSize()).isEqualTo(32);
            assertThat(HashAlgorithm.SHA2_384.hashSize()).isEqualTo(48);
        }

        /// This test aims to assert that the declared digest size of every constant matches the
        /// digest length the Java runtime reports for that algorithm, so the two can never drift
        /// apart.
        @ParameterizedTest
        @EnumSource(HashAlgorithm.class)
        @DisplayName("hashSize() matches the runtime digest length")
        void testHashSizeMatchesRuntimeDigestLength(final HashAlgorithm algorithm) throws NoSuchAlgorithmException {
            final MessageDigest runtimeDigest = MessageDigest.getInstance(algorithm.jcaName());
            assertThat(algorithm.hashSize()).isEqualTo(runtimeDigest.getDigestLength());
            assertThat(algorithm.newDigest().getDigestLength()).isEqualTo(algorithm.hashSize());
        }

        /// This test aims to assert that newDigest() returns a fresh digest of the right algorithm
        /// on every call, so callers never share digest state with each other.
        @ParameterizedTest
        @EnumSource(HashAlgorithm.class)
        @DisplayName("newDigest() returns a fresh digest every time")
        void testNewDigestReturnsFreshInstances(final HashAlgorithm algorithm) {
            final MessageDigest first = algorithm.newDigest();
            final MessageDigest second = algorithm.newDigest();
            assertThat(first).isNotSameAs(second);
            assertThat(first.getAlgorithm()).isEqualTo(algorithm.jcaName());
            assertThat(second.getAlgorithm()).isEqualTo(algorithm.jcaName());
        }
    }

    /// Tests for the empty tree hash of the constants.
    @Nested
    @DisplayName("Empty Tree Hash Tests")
    class EmptyTreeHashTests {
        /// This test aims to assert that the SHA-256 empty tree hash equals the externally
        /// computed SHA-256 digest of a single zero byte, pinning the wiring of the constant to a
        /// value produced outside the JVM.
        @Test
        @DisplayName("SHA2_256 empty tree hash matches the golden vector")
        void testSha256EmptyTreeHashGoldenVector() {
            assertThat(HashAlgorithm.SHA2_256.emptyTreeHash()).isEqualTo(Bytes.fromHex(SHA2_256_EMPTY_TREE_HASH));
        }

        /// This test aims to assert that the SHA-384 empty tree hash equals the externally
        /// computed SHA-384 digest of a single zero byte, which is also the empty tree hash the
        /// block root tree used before the move to SHA-256.
        @Test
        @DisplayName("SHA2_384 empty tree hash matches the golden vector")
        void testSha384EmptyTreeHashGoldenVector() {
            assertThat(HashAlgorithm.SHA2_384.emptyTreeHash()).isEqualTo(Bytes.fromHex(SHA2_384_EMPTY_TREE_HASH));
        }

        /// This test aims to assert that the empty tree hash of every constant is the digest of
        /// a single zero byte under that algorithm and has the digest size of the algorithm.
        @ParameterizedTest
        @EnumSource(HashAlgorithm.class)
        @DisplayName("emptyTreeHash() is the digest of a single zero byte")
        void testEmptyTreeHashIsDigestOfZeroByte(final HashAlgorithm algorithm) throws NoSuchAlgorithmException {
            final byte[] expected =
                    MessageDigest.getInstance(algorithm.jcaName()).digest(new byte[] {0x00});
            assertThat(algorithm.emptyTreeHash()).isEqualTo(Bytes.wrap(expected));
            assertThat(algorithm.emptyTreeHash().length()).isEqualTo(algorithm.hashSize());
        }
    }
}
