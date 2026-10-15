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

/// Tests for the [NaiveStreamingTreeHasher] class. Expected roots are produced by a local
/// reference implementation of the two children node hash with the JDK SHA-256 digest,
/// independent of the production code.
@DisplayName("Naive Streaming Tree Hasher Tests")
class NaiveStreamingTreeHasherTest {
    /// Deterministic seed so failures are reproducible.
    private static final Random RANDOM = new Random(9138120L);
    /// The SHA-256 digest size, spelled out so the reference stays independent of the production enum.
    private static final int HASH_SIZE = 32;

    /// Tests for the fold of leaves into the root.
    @Nested
    @DisplayName("Root Hash Tests")
    class RootHashTests {
        /// This test aims to assert that a hasher with no leaves returns the SHA-256 empty tree
        /// hash, the digest of a single zero byte, as root, so an empty subtree contributes a well
        /// defined value.
        @Test
        @DisplayName("rootHash() of an empty tree is the empty tree hash")
        void testEmptyTreeRoot() throws NoSuchAlgorithmException {
            final NaiveStreamingTreeHasher hasher = new NaiveStreamingTreeHasher();
            assertThat(hasher.rootHash()).isEqualTo(Bytes.wrap(refDigest(new byte[] {0x00})));
        }

        /// This test aims to assert that a tree with a single leaf has that leaf as root: the
        /// streaming fold has nothing to combine, so the leaf hash is returned unchanged.
        @Test
        @DisplayName("rootHash() of a single leaf is the leaf")
        void testSingleLeafRoot() throws NoSuchAlgorithmException {
            final byte[] leaf = randomHash();
            final NaiveStreamingTreeHasher hasher = new NaiveStreamingTreeHasher();
            hasher.addLeaf(ByteBuffer.wrap(leaf));
            assertThat(hasher.rootHash()).isEqualTo(Bytes.wrap(leaf));
        }

        /// This test aims to assert that two leaves are combined into a two children node hash
        /// with the 0x02 prefix under SHA-256.
        @Test
        @DisplayName("rootHash() of two leaves is their node hash")
        void testTwoLeavesRoot() throws NoSuchAlgorithmException {
            final byte[] first = randomHash();
            final byte[] second = randomHash();
            final NaiveStreamingTreeHasher hasher = new NaiveStreamingTreeHasher();
            hasher.addLeaf(ByteBuffer.wrap(first));
            hasher.addLeaf(ByteBuffer.wrap(second));
            assertThat(hasher.rootHash()).isEqualTo(Bytes.wrap(refNode(first, second)));
        }

        /// This test aims to assert that an odd number of leaves folds the open branches right
        /// to left: three leaves give node(node(l0, l1), l2).
        @Test
        @DisplayName("rootHash() of three leaves folds the open branch right to left")
        void testThreeLeavesRoot() throws NoSuchAlgorithmException {
            final byte[][] leaves = randomHashes(3);
            final NaiveStreamingTreeHasher hasher = new NaiveStreamingTreeHasher();
            for (final byte[] leaf : leaves) {
                hasher.addLeaf(ByteBuffer.wrap(leaf));
            }
            final byte[] expected = refNode(refNode(leaves[0], leaves[1]), leaves[2]);
            assertThat(hasher.rootHash()).isEqualTo(Bytes.wrap(expected));
        }

        /// This test aims to assert that five leaves produce the root of the streaming fold
        /// node(node(node(l0, l1), node(l2, l3)), l4), the shape with two complete levels and one
        /// open branch.
        @Test
        @DisplayName("rootHash() of five leaves matches the reference fold")
        void testFiveLeavesRoot() throws NoSuchAlgorithmException {
            final byte[][] leaves = randomHashes(5);
            final NaiveStreamingTreeHasher hasher = new NaiveStreamingTreeHasher();
            for (final byte[] leaf : leaves) {
                hasher.addLeaf(ByteBuffer.wrap(leaf));
            }
            final byte[] left = refNode(leaves[0], leaves[1]);
            final byte[] right = refNode(leaves[2], leaves[3]);
            final byte[] expected = refNode(refNode(left, right), leaves[4]);
            assertThat(hasher.rootHash()).isEqualTo(Bytes.wrap(expected));
        }

        /// This test aims to assert that the root hash is always one SHA-256 digest, whatever the
        /// number of leaves.
        @Test
        @DisplayName("rootHash() is one SHA-256 digest")
        void testRootHashSize() throws NoSuchAlgorithmException {
            for (int leafCount = 0; leafCount < 6; leafCount++) {
                final NaiveStreamingTreeHasher hasher = new NaiveStreamingTreeHasher();
                for (final byte[] leaf : randomHashes(leafCount)) {
                    hasher.addLeaf(ByteBuffer.wrap(leaf));
                }
                assertThat(hasher.rootHash().length()).isEqualTo(HASH_SIZE);
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
        @Test
        @DisplayName("addLeaf() consumes exactly one digest per call")
        void testAddLeafConsumesOneDigest() throws NoSuchAlgorithmException {
            final byte[][] leaves = randomHashes(3);
            final ByteBuffer packed = ByteBuffer.allocate(3 * HASH_SIZE);
            for (final byte[] leaf : leaves) {
                packed.put(leaf);
            }
            packed.flip();
            final NaiveStreamingTreeHasher fromPacked = new NaiveStreamingTreeHasher();
            while (packed.hasRemaining()) {
                final int before = packed.remaining();
                fromPacked.addLeaf(packed);
                assertThat(before - packed.remaining()).isEqualTo(HASH_SIZE);
            }
            final NaiveStreamingTreeHasher oneByOne = new NaiveStreamingTreeHasher();
            for (final byte[] leaf : leaves) {
                oneByOne.addLeaf(ByteBuffer.wrap(leaf));
            }
            assertThat(fromPacked.rootHash()).isEqualTo(oneByOne.rootHash());
        }

        /// This test aims to assert that a buffer with fewer bytes than one digest is refused,
        /// because a leaf that is not a full digest cannot be part of the tree.
        @Test
        @DisplayName("addLeaf() refuses a buffer shorter than one digest")
        void testAddLeafRefusesShortBuffer() {
            final NaiveStreamingTreeHasher hasher = new NaiveStreamingTreeHasher();
            final ByteBuffer shortBuffer = ByteBuffer.wrap(new byte[HASH_SIZE - 1]);
            assertThatThrownBy(() -> hasher.addLeaf(shortBuffer)).isInstanceOf(IllegalArgumentException.class);
        }

        /// This test aims to assert that no leaf is accepted once the root hash has been
        /// requested, so a finished tree cannot be changed afterwards.
        @Test
        @DisplayName("addLeaf() refuses leaves after rootHash()")
        void testAddLeafRefusedAfterRootHash() throws NoSuchAlgorithmException {
            final NaiveStreamingTreeHasher hasher = new NaiveStreamingTreeHasher();
            hasher.addLeaf(ByteBuffer.wrap(randomHash()));
            hasher.rootHash();
            final ByteBuffer anotherLeaf = ByteBuffer.wrap(randomHash());
            assertThatThrownBy(() -> hasher.addLeaf(anotherLeaf)).isInstanceOf(IllegalStateException.class);
        }
    }

    private static byte[][] randomHashes(final int count) {
        final byte[][] hashes = new byte[count][];
        for (int i = 0; i < count; i++) {
            hashes[i] = randomHash();
        }
        return hashes;
    }

    private static byte[] randomHash() {
        final byte[] hash = new byte[HASH_SIZE];
        RANDOM.nextBytes(hash);
        return hash;
    }

    /// Reference two children node hash with the 0x02 domain separation prefix.
    private static byte[] refNode(final byte[] left, final byte[] right) throws NoSuchAlgorithmException {
        return refDigest(new byte[] {0x02}, left, right);
    }

    /// SHA-256 over the concatenation of the given parts, the JDK digest spelled out here so the
    /// reference stays independent of the production code.
    private static byte[] refDigest(final byte[]... parts) throws NoSuchAlgorithmException {
        final MessageDigest digest = MessageDigest.getInstance("SHA-256");
        for (final byte[] part : parts) {
            digest.update(part);
        }
        return digest.digest();
    }
}
