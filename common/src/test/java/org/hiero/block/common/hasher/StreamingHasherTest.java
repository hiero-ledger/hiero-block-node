// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.common.hasher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/// Tests for the [StreamingHasher] class, the hasher of the all previous block hashes tree.
/// Expected values are produced by a local reference implementation of the leaf and node hashes,
/// independent of the production code. Every test runs for every [HashAlgorithm].
@DisplayName("Streaming Hasher Tests")
class StreamingHasherTest {
    /// Deterministic seed so failures are reproducible.
    private static final Random RANDOM = new Random(4417291L);

    /// Tests for the root hash computation.
    @Nested
    @DisplayName("Root Hash Tests")
    class RootHashTests {
        /// This test aims to assert that a hasher with no leaves computes the empty tree hash of
        /// its algorithm, the value the protocol defines for the all blocks tree of block 0.
        @ParameterizedTest
        @EnumSource(HashAlgorithm.class)
        @DisplayName("computeRootHash() of an empty tree is the empty tree hash")
        void testEmptyTreeRoot(final HashAlgorithm algorithm) {
            final StreamingHasher hasher = new StreamingHasher(algorithm);
            assertThat(Bytes.wrap(hasher.computeRootHash())).isEqualTo(algorithm.emptyTreeHash());
        }

        /// This test aims to assert that a single leaf added as data is hashed with the 0x00 leaf
        /// prefix under the hasher's algorithm and returned as the root.
        @ParameterizedTest
        @EnumSource(HashAlgorithm.class)
        @DisplayName("computeRootHash() of one leaf is the prefixed leaf hash")
        void testSingleLeafRoot(final HashAlgorithm algorithm) {
            final byte[] data = randomBytes(100);
            final StreamingHasher hasher = new StreamingHasher(algorithm);
            hasher.addLeaf(data);
            assertThat(hasher.computeRootHash()).isEqualTo(refLeaf(algorithm, data));
        }

        /// This test aims to assert that two nodes added by hash are combined with the 0x02 two
        /// children prefix without being hashed again, the way block root hashes enter the all
        /// blocks tree.
        @ParameterizedTest
        @EnumSource(HashAlgorithm.class)
        @DisplayName("computeRootHash() of two nodes added by hash is their node hash")
        void testTwoNodesByHashRoot(final HashAlgorithm algorithm) {
            final byte[] first = randomBytes(algorithm.hashSize());
            final byte[] second = randomBytes(algorithm.hashSize());
            final StreamingHasher hasher = new StreamingHasher(algorithm);
            hasher.addNodeByHash(first);
            hasher.addNodeByHash(second);
            assertThat(hasher.computeRootHash()).isEqualTo(refNode(algorithm, first, second));
        }

        /// This test aims to assert that three leaves fold the open branch right to left into
        /// node(node(leaf0, leaf1), leaf2), the same shape the streaming tree hasher produces.
        @ParameterizedTest
        @EnumSource(HashAlgorithm.class)
        @DisplayName("computeRootHash() of three leaves folds right to left")
        void testThreeLeavesRoot(final HashAlgorithm algorithm) {
            final byte[][] data = {randomBytes(10), randomBytes(20), randomBytes(30)};
            final StreamingHasher hasher = new StreamingHasher(algorithm);
            for (final byte[] leaf : data) {
                hasher.addLeaf(leaf);
            }
            final byte[] expected = refNode(
                    algorithm,
                    refNode(algorithm, refLeaf(algorithm, data[0]), refLeaf(algorithm, data[1])),
                    refLeaf(algorithm, data[2]));
            assertThat(hasher.computeRootHash()).isEqualTo(expected);
        }

        /// This test aims to assert that the streaming hasher and the streaming tree hasher fold
        /// identically: adding the leaf hashes of the same data to a [NaiveStreamingTreeHasher]
        /// gives the root the streaming hasher computes from the data.
        @ParameterizedTest
        @EnumSource(HashAlgorithm.class)
        @DisplayName("computeRootHash() agrees with the streaming tree hasher")
        void testAgreesWithStreamingTreeHasher(final HashAlgorithm algorithm) {
            final StreamingHasher hasher = new StreamingHasher(algorithm);
            final NaiveStreamingTreeHasher treeHasher = new NaiveStreamingTreeHasher(algorithm);
            for (int i = 0; i < 7; i++) {
                final byte[] data = randomBytes(16 + i);
                hasher.addLeaf(data);
                treeHasher.addLeaf(ByteBuffer.wrap(refLeaf(algorithm, data)));
            }
            assertThat(Bytes.wrap(hasher.computeRootHash())).isEqualTo(treeHasher.rootHash());
        }

        /// This test aims to assert that computing the root does not change the state: two calls
        /// give the same root, and a leaf added afterwards still changes the root.
        @ParameterizedTest
        @EnumSource(HashAlgorithm.class)
        @DisplayName("computeRootHash() leaves the state unchanged")
        void testComputeRootHashIsNonDestructive(final HashAlgorithm algorithm) {
            final StreamingHasher hasher = new StreamingHasher(algorithm);
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
        @ParameterizedTest
        @EnumSource(HashAlgorithm.class)
        @DisplayName("resumed hasher continues the interrupted computation")
        void testResumeFromIntermediateState(final HashAlgorithm algorithm) {
            final StreamingHasher original = new StreamingHasher(algorithm);
            for (int i = 0; i < 3; i++) {
                original.addNodeByHash(randomBytes(algorithm.hashSize()));
            }
            final List<byte[]> savedState = new ArrayList<>(original.intermediateHashingState());
            final StreamingHasher resumed = new StreamingHasher(algorithm, savedState, original.leafCount());
            final byte[] nextNode = randomBytes(algorithm.hashSize());
            original.addNodeByHash(nextNode);
            resumed.addNodeByHash(nextNode);
            assertThat(resumed.computeRootHash()).isEqualTo(original.computeRootHash());
            assertThat(resumed.leafCount()).isEqualTo(original.leafCount());
            assertThat(resumed.algorithm()).isEqualTo(algorithm);
        }

        /// This test aims to assert that the leaf count grows by one for every leaf added as
        /// data and for every node added by hash.
        @ParameterizedTest
        @EnumSource(HashAlgorithm.class)
        @DisplayName("leafCount() counts leaves and nodes")
        void testLeafCount(final HashAlgorithm algorithm) {
            final StreamingHasher hasher = new StreamingHasher(algorithm);
            assertThat(hasher.leafCount()).isZero();
            hasher.addLeaf(randomBytes(8));
            assertThat(hasher.leafCount()).isEqualTo(1L);
            hasher.addNodeByHash(randomBytes(algorithm.hashSize()));
            assertThat(hasher.leafCount()).isEqualTo(2L);
        }

        /// This test aims to assert that the hasher reports the algorithm it was constructed with.
        @ParameterizedTest
        @EnumSource(HashAlgorithm.class)
        @DisplayName("algorithm() is the constructor argument")
        void testAlgorithmIsConstructorArgument(final HashAlgorithm algorithm) {
            assertThat(new StreamingHasher(algorithm).algorithm()).isEqualTo(algorithm);
        }

        /// This test aims to assert that a hasher cannot be created without an algorithm.
        @Test
        @DisplayName("constructor refuses a null algorithm")
        void testNullAlgorithmRefused() {
            assertThatThrownBy(() -> new StreamingHasher(null)).isInstanceOf(NullPointerException.class);
        }
    }

    private static byte[] randomBytes(final int length) {
        final byte[] bytes = new byte[length];
        RANDOM.nextBytes(bytes);
        return bytes;
    }

    /// Reference leaf hash with the 0x00 domain separation prefix.
    private static byte[] refLeaf(final HashAlgorithm algorithm, final byte[] data) {
        return refHash(algorithm, new byte[] {0x00}, data);
    }

    /// Reference two children node hash with the 0x02 domain separation prefix.
    private static byte[] refNode(final HashAlgorithm algorithm, final byte[] left, final byte[] right) {
        return refHash(algorithm, new byte[] {0x02}, left, right);
    }

    private static byte[] refHash(final HashAlgorithm algorithm, final byte[]... parts) {
        try {
            final MessageDigest digest = MessageDigest.getInstance(jcaName(algorithm));
            for (final byte[] part : parts) {
                digest.update(part);
            }
            return digest.digest();
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
