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

/// Tests both streaming hashers, [StreamingHasher] and [NaiveStreamingTreeHasher], against an
/// independent reference definition of the tree, for every leaf count up to a bound that crosses
/// several powers of two.
///
/// The streaming hashers fold leaves as they arrive and fold the open branches right to left at the
/// end. The reference instead defines the root recursively: the root of no leaves is
/// `hash(0x00)`, the root of one leaf is that leaf, and the root of `n > 1` leaves is
/// `hash(0x02 || root(first k) || root(remaining n - k))`, where `k` is the largest power of two
/// smaller than `n`. Both describe the same tree shape, so agreement for every leaf count shows the
/// fold is correct for every shape, not only for the small trees spelled out in the class tests.
/// The reference uses the JDK SHA-256 digest directly and none of the production code.
@DisplayName("Streaming Hashers Reference Tree Tests")
class StreamingHashersReferenceTreeTest {
    /// The largest leaf count checked, beyond 64 and 128 so trees with up to eight open branches are covered.
    private static final int MAX_LEAF_COUNT = 130;
    /// The leaf count of the tree resumed at every split point.
    private static final int RESUME_LEAF_COUNT = 70;
    /// The SHA-256 digest size, spelled out so the reference stays independent of the production enum.
    private static final int HASH_SIZE = 32;
    /// Deterministic seed so failures are reproducible.
    private static final Random RANDOM = new Random(7302215L);

    /// Tests for [StreamingHasher].
    @Nested
    @DisplayName("Streaming Hasher Tests")
    class StreamingHasherTests {
        /// This test aims to assert that leaves added as data give the reference root after every
        /// single addition, for every leaf count from 0 to the bound. Checking after each addition
        /// also shows that computing the root leaves the state unchanged.
        @Test
        @DisplayName("addLeaf() roots match the reference tree for every leaf count")
        void testLeavesMatchReferenceForEveryCount() throws NoSuchAlgorithmException {
            final StreamingHasher hasher = new StreamingHasher();
            final List<byte[]> leafHashes = new ArrayList<>();
            assertThat(hasher.computeRootHash()).as("leaf count 0").isEqualTo(refRoot(leafHashes));
            for (int count = 1; count <= MAX_LEAF_COUNT; count++) {
                final byte[] data = randomBytes(count % 50);
                hasher.addLeaf(data);
                leafHashes.add(refLeaf(data));
                assertThat(hasher.computeRootHash()).as("leaf count %d", count).isEqualTo(refRoot(leafHashes));
                assertThat(hasher.leafCount()).isEqualTo(count);
            }
        }

        /// This test aims to assert that nodes added by hash give the reference root after every
        /// single addition, for every leaf count from 0 to the bound, without being hashed again.
        @Test
        @DisplayName("addNodeByHash() roots match the reference tree for every leaf count")
        void testNodesMatchReferenceForEveryCount() throws NoSuchAlgorithmException {
            final StreamingHasher hasher = new StreamingHasher();
            final List<byte[]> nodes = new ArrayList<>();
            for (int count = 1; count <= MAX_LEAF_COUNT; count++) {
                final byte[] node = randomBytes(HASH_SIZE);
                hasher.addNodeByHash(node);
                nodes.add(node);
                assertThat(hasher.computeRootHash()).as("leaf count %d", count).isEqualTo(refRoot(nodes));
            }
        }

        /// This test aims to assert that a hasher resumed from a saved intermediate state and leaf
        /// count continues exactly where the original was, at every split point of the tree: after
        /// the remaining nodes it has the reference root and the full leaf count.
        @Test
        @DisplayName("resume at every split point gives the reference root")
        void testResumeAtEverySplitPoint() throws NoSuchAlgorithmException {
            final List<byte[]> nodes = new ArrayList<>();
            for (int i = 0; i < RESUME_LEAF_COUNT; i++) {
                nodes.add(randomBytes(HASH_SIZE));
            }
            final byte[] expectedRoot = refRoot(nodes);
            for (int split = 0; split <= RESUME_LEAF_COUNT; split++) {
                final StreamingHasher original = new StreamingHasher();
                for (int i = 0; i < split; i++) {
                    original.addNodeByHash(nodes.get(i));
                }
                final List<byte[]> savedState = new ArrayList<>();
                for (final byte[] hash : original.intermediateHashingState()) {
                    savedState.add(hash.clone());
                }
                final StreamingHasher resumed = new StreamingHasher(savedState, original.leafCount());
                for (int i = split; i < RESUME_LEAF_COUNT; i++) {
                    resumed.addNodeByHash(nodes.get(i));
                }
                assertThat(resumed.computeRootHash()).as("split at %d", split).isEqualTo(expectedRoot);
                assertThat(resumed.leafCount()).isEqualTo(RESUME_LEAF_COUNT);
            }
        }
    }

    /// Tests for [NaiveStreamingTreeHasher].
    @Nested
    @DisplayName("Naive Streaming Tree Hasher Tests")
    class NaiveStreamingTreeHasherTests {
        /// This test aims to assert that the streaming tree hasher gives the reference root for
        /// every leaf count from 0 to the bound, so every subtree root of the block root tree has
        /// the expected shape whatever the number of items in its category.
        @Test
        @DisplayName("rootHash() matches the reference tree for every leaf count")
        void testRootMatchesReferenceForEveryCount() throws NoSuchAlgorithmException {
            final List<byte[]> leaves = new ArrayList<>();
            for (int i = 0; i < MAX_LEAF_COUNT; i++) {
                leaves.add(randomBytes(HASH_SIZE));
            }
            for (int count = 0; count <= MAX_LEAF_COUNT; count++) {
                final NaiveStreamingTreeHasher hasher = new NaiveStreamingTreeHasher();
                for (int i = 0; i < count; i++) {
                    hasher.addLeaf(ByteBuffer.wrap(leaves.get(i)));
                }
                assertThat(hasher.rootHash())
                        .as("leaf count %d", count)
                        .isEqualTo(Bytes.wrap(refRoot(leaves.subList(0, count))));
            }
        }

        /// This test aims to assert that both streaming hashers fold identically: the same leaf
        /// hashes added to each give the same root for every leaf count from 0 to the bound.
        @Test
        @DisplayName("rootHash() agrees with the streaming hasher for every leaf count")
        void testAgreesWithStreamingHasherForEveryCount() throws NoSuchAlgorithmException {
            final List<byte[]> leaves = new ArrayList<>();
            for (int i = 0; i < MAX_LEAF_COUNT; i++) {
                leaves.add(randomBytes(HASH_SIZE));
            }
            for (int count = 0; count <= MAX_LEAF_COUNT; count++) {
                final NaiveStreamingTreeHasher treeHasher = new NaiveStreamingTreeHasher();
                final StreamingHasher streamingHasher = new StreamingHasher();
                for (int i = 0; i < count; i++) {
                    treeHasher.addLeaf(ByteBuffer.wrap(leaves.get(i)));
                    streamingHasher.addNodeByHash(leaves.get(i));
                }
                assertThat(treeHasher.rootHash())
                        .as("leaf count %d", count)
                        .isEqualTo(Bytes.wrap(streamingHasher.computeRootHash()));
            }
        }
    }

    /// The reference root of a tree of the given leaf hashes, defined recursively by splitting at
    /// the largest power of two smaller than the leaf count.
    private static byte[] refRoot(final List<byte[]> leafHashes) throws NoSuchAlgorithmException {
        final int count = leafHashes.size();
        final byte[] result;
        if (count == 0) {
            result = refHash(new byte[] {0x00});
        } else if (count == 1) {
            result = leafHashes.getFirst();
        } else {
            final int split = Integer.highestOneBit(count - 1);
            result = refHash(
                    new byte[] {0x02},
                    refRoot(leafHashes.subList(0, split)),
                    refRoot(leafHashes.subList(split, count)));
        }
        return result;
    }

    /// Reference leaf hash with the 0x00 domain separation prefix.
    private static byte[] refLeaf(final byte[] data) throws NoSuchAlgorithmException {
        return refHash(new byte[] {0x00}, data);
    }

    /// SHA-256 over the concatenation of the given parts.
    private static byte[] refHash(final byte[]... parts) throws NoSuchAlgorithmException {
        final MessageDigest digest = MessageDigest.getInstance("SHA-256");
        for (final byte[] part : parts) {
            digest.update(part);
        }
        return digest.digest();
    }

    private static byte[] randomBytes(final int length) {
        final byte[] bytes = new byte[length];
        RANDOM.nextBytes(bytes);
        return bytes;
    }
}
