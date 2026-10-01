// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.common.hasher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/// Tests for the [NaiveStreamingTreeHasher] class. Expected roots are produced by a local
/// reference implementation of the two children node hash, independent of the production code.
/// Every test runs for every [HashAlgorithm].
@DisplayName("Naive Streaming Tree Hasher Tests")
class NaiveStreamingTreeHasherTest {
    /// Deterministic seed so failures are reproducible.
    private static final Random RANDOM = new Random(9138120L);

    /// Tests for the fold of leaves into the root.
    @Nested
    @DisplayName("Root Hash Tests")
    class RootHashTests {
        /// This test aims to assert that a hasher with no leaves returns the empty tree hash of
        /// its algorithm as root, so an empty subtree contributes a well defined value.
        @ParameterizedTest
        @EnumSource(HashAlgorithm.class)
        @DisplayName("rootHash() of an empty tree is the empty tree hash")
        void testEmptyTreeRoot(final HashAlgorithm algorithm) {
            final NaiveStreamingTreeHasher hasher = new NaiveStreamingTreeHasher(algorithm);
            assertThat(hasher.rootHash()).isEqualTo(algorithm.emptyTreeHash());
        }

        /// This test aims to assert that a tree with a single leaf has that leaf as root: the
        /// streaming fold has nothing to combine, so the leaf hash is returned unchanged.
        @ParameterizedTest
        @EnumSource(HashAlgorithm.class)
        @DisplayName("rootHash() of a single leaf is the leaf")
        void testSingleLeafRoot(final HashAlgorithm algorithm) {
            final byte[] leaf = randomHash(algorithm);
            final NaiveStreamingTreeHasher hasher = new NaiveStreamingTreeHasher(algorithm);
            hasher.addLeaf(ByteBuffer.wrap(leaf));
            assertThat(hasher.rootHash()).isEqualTo(Bytes.wrap(leaf));
        }

        /// This test aims to assert that two leaves are combined into a two children node hash
        /// with the 0x02 prefix under the hasher's algorithm.
        @ParameterizedTest
        @EnumSource(HashAlgorithm.class)
        @DisplayName("rootHash() of two leaves is their node hash")
        void testTwoLeavesRoot(final HashAlgorithm algorithm) {
            final byte[] first = randomHash(algorithm);
            final byte[] second = randomHash(algorithm);
            final NaiveStreamingTreeHasher hasher = new NaiveStreamingTreeHasher(algorithm);
            hasher.addLeaf(ByteBuffer.wrap(first));
            hasher.addLeaf(ByteBuffer.wrap(second));
            assertThat(hasher.rootHash()).isEqualTo(Bytes.wrap(refNode(algorithm, first, second)));
        }

        /// This test aims to assert that an odd number of leaves folds the open branches right
        /// to left: three leaves give node(node(l0, l1), l2).
        @ParameterizedTest
        @EnumSource(HashAlgorithm.class)
        @DisplayName("rootHash() of three leaves folds the open branch right to left")
        void testThreeLeavesRoot(final HashAlgorithm algorithm) {
            final byte[][] leaves = randomHashes(algorithm, 3);
            final NaiveStreamingTreeHasher hasher = new NaiveStreamingTreeHasher(algorithm);
            for (final byte[] leaf : leaves) {
                hasher.addLeaf(ByteBuffer.wrap(leaf));
            }
            final byte[] expected = refNode(algorithm, refNode(algorithm, leaves[0], leaves[1]), leaves[2]);
            assertThat(hasher.rootHash()).isEqualTo(Bytes.wrap(expected));
        }

        /// This test aims to assert that five leaves produce the root of the streaming fold
        /// node(node(node(l0, l1), node(l2, l3)), l4), the shape with two complete levels and one
        /// open branch.
        @ParameterizedTest
        @EnumSource(HashAlgorithm.class)
        @DisplayName("rootHash() of five leaves matches the reference fold")
        void testFiveLeavesRoot(final HashAlgorithm algorithm) {
            final byte[][] leaves = randomHashes(algorithm, 5);
            final NaiveStreamingTreeHasher hasher = new NaiveStreamingTreeHasher(algorithm);
            for (final byte[] leaf : leaves) {
                hasher.addLeaf(ByteBuffer.wrap(leaf));
            }
            final byte[] left = refNode(algorithm, leaves[0], leaves[1]);
            final byte[] right = refNode(algorithm, leaves[2], leaves[3]);
            final byte[] expected = refNode(algorithm, refNode(algorithm, left, right), leaves[4]);
            assertThat(hasher.rootHash()).isEqualTo(Bytes.wrap(expected));
        }

        /// This test aims to assert that the root hash always has the digest size of the
        /// hasher's algorithm, whatever the number of leaves.
        @ParameterizedTest
        @EnumSource(HashAlgorithm.class)
        @DisplayName("rootHash() has the digest size of the algorithm")
        void testRootHashSize(final HashAlgorithm algorithm) {
            for (int leafCount = 0; leafCount < 6; leafCount++) {
                final NaiveStreamingTreeHasher hasher = new NaiveStreamingTreeHasher(algorithm);
                for (final byte[] leaf : randomHashes(algorithm, leafCount)) {
                    hasher.addLeaf(ByteBuffer.wrap(leaf));
                }
                assertThat(hasher.rootHash().length()).isEqualTo(algorithm.hashSize());
            }
        }
    }

    /// Tests for the leaf buffer handling and the hasher state.
    @Nested
    @DisplayName("Leaf Buffer Tests")
    class LeafBufferTests {
        /// This test aims to assert that every addLeaf() call consumes exactly one digest from the
        /// buffer, so a packed buffer of several hashes yields one leaf per hash and the root
        /// equals the root of the same leaves added one by one.
        @ParameterizedTest
        @EnumSource(HashAlgorithm.class)
        @DisplayName("addLeaf() consumes exactly one digest per call")
        void testAddLeafConsumesOneDigest(final HashAlgorithm algorithm) {
            final byte[][] leaves = randomHashes(algorithm, 3);
            final ByteBuffer packed = ByteBuffer.allocate(3 * algorithm.hashSize());
            for (final byte[] leaf : leaves) {
                packed.put(leaf);
            }
            packed.flip();
            final NaiveStreamingTreeHasher fromPacked = new NaiveStreamingTreeHasher(algorithm);
            while (packed.hasRemaining()) {
                final int before = packed.remaining();
                fromPacked.addLeaf(packed);
                assertThat(before - packed.remaining()).isEqualTo(algorithm.hashSize());
            }
            final NaiveStreamingTreeHasher oneByOne = new NaiveStreamingTreeHasher(algorithm);
            for (final byte[] leaf : leaves) {
                oneByOne.addLeaf(ByteBuffer.wrap(leaf));
            }
            assertThat(fromPacked.rootHash()).isEqualTo(oneByOne.rootHash());
        }

        /// This test aims to assert that a buffer with fewer bytes than one digest is refused,
        /// because a leaf that is not a full digest cannot be part of the tree.
        @ParameterizedTest
        @EnumSource(HashAlgorithm.class)
        @DisplayName("addLeaf() refuses a buffer shorter than one digest")
        void testAddLeafRefusesShortBuffer(final HashAlgorithm algorithm) {
            final NaiveStreamingTreeHasher hasher = new NaiveStreamingTreeHasher(algorithm);
            final ByteBuffer shortBuffer = ByteBuffer.wrap(new byte[algorithm.hashSize() - 1]);
            assertThatThrownBy(() -> hasher.addLeaf(shortBuffer)).isInstanceOf(IllegalArgumentException.class);
        }

        /// This test aims to assert that no leaf is accepted once the root hash has been
        /// requested, so a finished tree cannot be changed afterwards.
        @ParameterizedTest
        @EnumSource(HashAlgorithm.class)
        @DisplayName("addLeaf() refuses leaves after rootHash()")
        void testAddLeafRefusedAfterRootHash(final HashAlgorithm algorithm) {
            final NaiveStreamingTreeHasher hasher = new NaiveStreamingTreeHasher(algorithm);
            hasher.addLeaf(ByteBuffer.wrap(randomHash(algorithm)));
            hasher.rootHash();
            final ByteBuffer anotherLeaf = ByteBuffer.wrap(randomHash(algorithm));
            assertThatThrownBy(() -> hasher.addLeaf(anotherLeaf)).isInstanceOf(IllegalStateException.class);
        }

        /// This test aims to assert that the hasher reports the algorithm it was constructed
        /// with, so callers can size their leaves and check consistency.
        @ParameterizedTest
        @EnumSource(HashAlgorithm.class)
        @DisplayName("algorithm() is the constructor argument")
        void testAlgorithmIsConstructorArgument(final HashAlgorithm algorithm) {
            assertThat(new NaiveStreamingTreeHasher(algorithm).algorithm()).isEqualTo(algorithm);
        }

        /// This test aims to assert that a hasher cannot be created without an algorithm.
        @Test
        @DisplayName("constructor refuses a null algorithm")
        void testNullAlgorithmRefused() {
            assertThatThrownBy(() -> new NaiveStreamingTreeHasher(null)).isInstanceOf(NullPointerException.class);
        }
    }

    private static byte[][] randomHashes(final HashAlgorithm algorithm, final int count) {
        final byte[][] hashes = new byte[count][];
        for (int i = 0; i < count; i++) {
            hashes[i] = randomHash(algorithm);
        }
        return hashes;
    }

    private static byte[] randomHash(final HashAlgorithm algorithm) {
        final byte[] hash = new byte[algorithm.hashSize()];
        RANDOM.nextBytes(hash);
        return hash;
    }

    /// Reference two children node hash with the 0x02 domain separation prefix.
    private static byte[] refNode(final HashAlgorithm algorithm, final byte[] left, final byte[] right) {
        try {
            final MessageDigest digest = MessageDigest.getInstance(jcaName(algorithm));
            digest.update(new byte[] {0x02});
            digest.update(left);
            return digest.digest(right);
        } catch (final NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /// The JCA name of the algorithm, spelled out here so the reference stays independent of
    /// the production enum.
    private static String jcaName(final HashAlgorithm algorithm) {
        return switch (algorithm) {
            case SHA2_256 -> "SHA-256";
            case SHA2_384 -> "SHA-384";
        };
    }
}
