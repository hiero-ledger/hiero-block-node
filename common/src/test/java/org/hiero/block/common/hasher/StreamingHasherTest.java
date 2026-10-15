// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.common.hasher;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/// Tests for the [StreamingHasher] class, the hasher of the all previous block hashes tree.
/// Expected values are produced by a local reference implementation of the leaf and node hashes
/// with the JDK SHA-256 digest, independent of the production code.
@DisplayName("Streaming Hasher Tests")
class StreamingHasherTest {
    /// Deterministic seed so failures are reproducible.
    private static final Random RANDOM = new Random(4417291L);
    /// The SHA-256 digest size, spelled out so the reference stays independent of the production enum.
    private static final int HASH_SIZE = 32;

    /// Tests for the root hash computation.
    @Nested
    @DisplayName("Root Hash Tests")
    class RootHashTests {
        /// This test aims to assert that a hasher with no leaves computes the SHA-256 empty tree
        /// hash, the value the protocol defines for the all blocks tree of block 0.
        @Test
        @DisplayName("computeRootHash() of an empty tree is the empty tree hash")
        void testEmptyTreeRoot() throws NoSuchAlgorithmException {
            final StreamingHasher hasher = new StreamingHasher();
            assertThat(hasher.computeRootHash()).isEqualTo(refHash(new byte[] {0x00}));
        }

        /// This test aims to assert that a single leaf added as data is hashed with the 0x00 leaf
        /// prefix under SHA-256 and returned as the root.
        @Test
        @DisplayName("computeRootHash() of one leaf is the prefixed leaf hash")
        void testSingleLeafRoot() throws NoSuchAlgorithmException {
            final byte[] data = randomBytes(100);
            final StreamingHasher hasher = new StreamingHasher();
            hasher.addLeaf(data);
            assertThat(hasher.computeRootHash()).isEqualTo(refLeaf(data));
        }

        /// This test aims to assert that two nodes added by hash are combined with the 0x02 two
        /// children prefix without being hashed again, the way block root hashes enter the all
        /// blocks tree.
        @Test
        @DisplayName("computeRootHash() of two nodes added by hash is their node hash")
        void testTwoNodesByHashRoot() throws NoSuchAlgorithmException {
            final byte[] first = randomBytes(HASH_SIZE);
            final byte[] second = randomBytes(HASH_SIZE);
            final StreamingHasher hasher = new StreamingHasher();
            hasher.addNodeByHash(first);
            hasher.addNodeByHash(second);
            assertThat(hasher.computeRootHash()).isEqualTo(refNode(first, second));
        }

        /// This test aims to assert that three leaves fold the open branch right to left into
        /// node(node(leaf0, leaf1), leaf2), the same shape the streaming tree hasher produces.
        @Test
        @DisplayName("computeRootHash() of three leaves folds right to left")
        void testThreeLeavesRoot() throws NoSuchAlgorithmException {
            final byte[][] data = {randomBytes(10), randomBytes(20), randomBytes(30)};
            final StreamingHasher hasher = new StreamingHasher();
            for (final byte[] leaf : data) {
                hasher.addLeaf(leaf);
            }
            final byte[] expected = refNode(refNode(refLeaf(data[0]), refLeaf(data[1])), refLeaf(data[2]));
            assertThat(hasher.computeRootHash()).isEqualTo(expected);
        }

        /// This test aims to assert that the streaming hasher and the streaming tree hasher fold
        /// identically: adding the leaf hashes of the same data to a [NaiveStreamingTreeHasher]
        /// gives the root the streaming hasher computes from the data.
        @Test
        @DisplayName("computeRootHash() agrees with the streaming tree hasher")
        void testAgreesWithStreamingTreeHasher() throws NoSuchAlgorithmException {
            final StreamingHasher hasher = new StreamingHasher();
            final NaiveStreamingTreeHasher treeHasher = new NaiveStreamingTreeHasher();
            for (int i = 0; i < 7; i++) {
                final byte[] data = randomBytes(16 + i);
                hasher.addLeaf(data);
                treeHasher.addLeaf(ByteBuffer.wrap(refLeaf(data)));
            }
            assertThat(Bytes.wrap(hasher.computeRootHash())).isEqualTo(treeHasher.rootHash());
        }

        /// This test aims to assert that computing the root does not change the state: two calls
        /// give the same root, and a leaf added afterwards still changes the root.
        @Test
        @DisplayName("computeRootHash() leaves the state unchanged")
        void testComputeRootHashIsNonDestructive() throws NoSuchAlgorithmException {
            final StreamingHasher hasher = new StreamingHasher();
            hasher.addLeaf(randomBytes(12));
            hasher.addLeaf(randomBytes(12));
            final byte[] first = hasher.computeRootHash();
            final byte[] second = hasher.computeRootHash();
            assertThat(first).isEqualTo(second);
            hasher.addLeaf(randomBytes(12));
            assertThat(hasher.computeRootHash()).isNotEqualTo(first);
            assertThat(hasher.leafCount()).isEqualTo(3L);
        }
    }

    /// Tests for the hasher state and its resumption.
    @Nested
    @DisplayName("State Tests")
    class StateTests {
        /// This test aims to assert that a hasher resumed from the intermediate state and leaf
        /// count of another hasher continues exactly where that hasher was: both produce the same
        /// root and leaf count after the same further leaves.
        @Test
        @DisplayName("resumed hasher continues the interrupted computation")
        void testResumeFromIntermediateState() throws NoSuchAlgorithmException {
            final StreamingHasher original = new StreamingHasher();
            for (int i = 0; i < 3; i++) {
                original.addNodeByHash(randomBytes(HASH_SIZE));
            }
            final List<byte[]> savedState = new ArrayList<>(original.intermediateHashingState());
            final StreamingHasher resumed = new StreamingHasher(savedState, original.leafCount());
            final byte[] nextNode = randomBytes(HASH_SIZE);
            original.addNodeByHash(nextNode);
            resumed.addNodeByHash(nextNode);
            assertThat(resumed.computeRootHash()).isEqualTo(original.computeRootHash());
            assertThat(resumed.leafCount()).isEqualTo(original.leafCount());
        }

        /// This test aims to assert that the leaf count grows by one for every leaf added as
        /// data and for every node added by hash.
        @Test
        @DisplayName("leafCount() counts leaves and nodes")
        void testLeafCount() throws NoSuchAlgorithmException {
            final StreamingHasher hasher = new StreamingHasher();
            assertThat(hasher.leafCount()).isZero();
            hasher.addLeaf(randomBytes(8));
            assertThat(hasher.leafCount()).isEqualTo(1L);
            hasher.addNodeByHash(randomBytes(HASH_SIZE));
            assertThat(hasher.leafCount()).isEqualTo(2L);
        }
    }

    private static byte[] randomBytes(final int length) {
        final byte[] bytes = new byte[length];
        RANDOM.nextBytes(bytes);
        return bytes;
    }

    /// Reference leaf hash with the 0x00 domain separation prefix.
    private static byte[] refLeaf(final byte[] data) throws NoSuchAlgorithmException {
        return refHash(new byte[] {0x00}, data);
    }

    /// Reference two children node hash with the 0x02 domain separation prefix.
    private static byte[] refNode(final byte[] left, final byte[] right) throws NoSuchAlgorithmException {
        return refHash(new byte[] {0x02}, left, right);
    }

    /// SHA-256 over the concatenation of the given parts, the JDK digest spelled out here so the
    /// reference stays independent of the production code.
    private static byte[] refHash(final byte[]... parts) throws NoSuchAlgorithmException {
        final MessageDigest digest = MessageDigest.getInstance("SHA-256");
        for (final byte[] part : parts) {
            digest.update(part);
        }
        return digest.digest();
    }
}
