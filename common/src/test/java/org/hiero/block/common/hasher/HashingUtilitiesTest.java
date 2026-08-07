// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.common.hasher;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.node.base.Timestamp;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/// Tests for the [HashingUtilities] class, focused on the extension subtree aware final block
/// hash computation. Expected values are produced by a local reference implementation of the
/// HIP-1424 block root tree ("Merkle Mountain Top"), independent of the production code, so the
/// two implementations must agree.
@DisplayName("Hashing Utilities Tests")
class HashingUtilitiesTest {
    private static final int HASH_SIZE = 48;
    /// Deterministic seed so failures are reproducible.
    private static final Random RANDOM = new Random(6431582L);
    /// Precomputed hash of an empty tree, matching [HashingUtilities#EMPTY_TREE_HASH].
    private static final byte[] REF_EMPTY_TREE_HASH = refLeaf(new byte[0]);

    /// Tests for [HashingUtilities#computeFinalBlockHash] with extension subtree roots.
    @Nested
    @DisplayName("Final Block Hash Extension Subtree Tests")
    class FinalBlockHashExtensionSubtreeTests {
        /// This test aims to assert that the legacy overload without extension subtree roots
        /// produces the exact same hash as the extension aware overload with no extension
        /// subtree present. Both must equal the reference implementation.
        @Test
        @DisplayName("computeFinalBlockHash() no extensions matches legacy overload")
        void testNoExtensionsMatchesLegacyOverload() {
            final FixedTreeInputs inputs = randomInputs();
            final Bytes legacy = HashingUtilities.computeFinalBlockHash(
                    inputs.timestamp(),
                    inputs.previousBlockHash(),
                    inputs.rootOfAllPreviousBlockHashes(),
                    inputs.startOfBlockStateRootHash(),
                    inputs.inputTreeHasher(),
                    inputs.outputTreeHasher(),
                    inputs.consensusHeaderHasher(),
                    inputs.stateChangesHasher(),
                    inputs.traceDataHasher());
            final Bytes extensionAware = computeWithExtensions(inputs, new byte[8][]);
            assertThat(extensionAware).isEqualTo(legacy);
            assertThat(extensionAware).isEqualTo(referenceRootHash(inputs, new byte[8][]));
        }

        /// This test aims to assert that every presence pattern of the eight extension
        /// subtree leaves produces the block root hash computed by the reference
        /// implementation of the HIP-1424 tree. Patterns are supplied as bitmasks where bit N
        /// marks Extension N as present.
        @ParameterizedTest
        @ValueSource(ints = {0b00000001, 0b00000010, 0b10000000, 0b00001001, 0b01100110, 0b11111111})
        @DisplayName("computeFinalBlockHash() extension presence patterns match reference")
        void testExtensionPresencePatternsMatchReference(final int presenceMask) {
            final FixedTreeInputs inputs = randomInputs();
            final byte[][] extensionRoots = new byte[8][];
            for (int i = 0; i < extensionRoots.length; i++) {
                if ((presenceMask & (1 << i)) != 0) {
                    extensionRoots[i] = randomHash();
                }
            }
            final Bytes actual = computeWithExtensions(inputs, extensionRoots);
            assertThat(actual).isEqualTo(referenceRootHash(inputs, extensionRoots));
        }

        /// This test aims to assert that a presence pattern with extension items produces a
        /// different root hash than the same inputs with no extension items, so extension
        /// items can never be dropped without changing the block hash.
        @Test
        @DisplayName("computeFinalBlockHash() extension presence changes the root hash")
        void testExtensionPresenceChangesRootHash() {
            final FixedTreeInputs inputs = randomInputs();
            final byte[][] noExtensions = new byte[8][];
            final byte[][] withExtension = new byte[8][];
            withExtension[0] = randomHash();
            final Bytes without = computeWithExtensions(inputs, noExtensions);
            final Bytes with = computeWithExtensions(inputs, withExtension);
            assertThat(with).isNotEqualTo(without);
        }

        /// Calls the extension aware [HashingUtilities#computeFinalBlockHash] overload,
        /// expanding the given roots (Extension 0 to Extension 7) into the individual
        /// extension subtree root parameters.
        private Bytes computeWithExtensions(final FixedTreeInputs inputs, final byte[][] extensionRoots) {
            return HashingUtilities.computeFinalBlockHash(
                    inputs.timestamp(),
                    inputs.previousBlockHash(),
                    inputs.rootOfAllPreviousBlockHashes(),
                    inputs.startOfBlockStateRootHash(),
                    inputs.inputTreeHasher(),
                    inputs.outputTreeHasher(),
                    inputs.consensusHeaderHasher(),
                    inputs.stateChangesHasher(),
                    inputs.traceDataHasher(),
                    extensionRoots[0],
                    extensionRoots[1],
                    extensionRoots[2],
                    extensionRoots[3],
                    extensionRoots[4],
                    extensionRoots[5],
                    extensionRoots[6],
                    extensionRoots[7]);
        }
    }

    /// Tests for the pre-defined side rightmost-scan behavior (issue #3377). Empty subtree
    /// hashers at trailing pre-defined leaf positions (2-7) must be dropped from the fold-up,
    /// not fed as EMPTY_TREE_HASH, so the pre-defined side has a variable shape driven by the
    /// rightmost non-empty leaf. Only positions 0-1 (previousBlockHash and rootHashOfAll-
    /// PreviousBlockHashes) are always populated by protocol; position 2 (state root) can be
    /// absent for WRB blocks.
    @Nested
    @DisplayName("Pre-Defined Side Rightmost Scan Tests")
    class PreDefinedRightmostScanTests {
        /// Presence bitmask over positions 2-7 (bit 0 = position 2, ..., bit 5 = position 7).
        /// Covers: none present (WRB with only prev-block and all-blocks), only position 2,
        /// only position 7, positions 2 and 7, interior gap with rightmost present, all present.
        @ParameterizedTest
        @ValueSource(ints = {0b000000, 0b000001, 0b100000, 0b100001, 0b100010, 0b111111})
        @DisplayName("computeFinalBlockHash() pre-defined presence patterns match reference")
        void testPreDefinedPresencePatternsMatchReference(final int presenceMask) {
            final FixedTreeInputs inputs = randomInputsWithPreDefinedPresence(presenceMask);
            final Bytes actual = HashingUtilities.computeFinalBlockHash(
                    inputs.timestamp(),
                    inputs.previousBlockHash(),
                    inputs.rootOfAllPreviousBlockHashes(),
                    inputs.startOfBlockStateRootHash(),
                    inputs.inputTreeHasher(),
                    inputs.outputTreeHasher(),
                    inputs.consensusHeaderHasher(),
                    inputs.stateChangesHasher(),
                    inputs.traceDataHasher());
            assertThat(actual).isEqualTo(referenceRootHash(inputs, new byte[8][]));
        }

        /// This test aims to assert that dropping the trailing empty pre-defined leaves does
        /// not collapse different presence patterns onto the same root hash: rightmost = 6 and
        /// rightmost = 7 with the same interior data must diverge.
        @Test
        @DisplayName("computeFinalBlockHash() trailing empty position changes the root hash")
        void testTrailingEmptyChangesRootHash() {
            final Bytes rightmostAtSix = HashingUtilities.computeFinalBlockHash(
                    fixedTimestamp(),
                    fixedHash(),
                    fixedHash(),
                    fixedHash(),
                    hasherWithLeaves(1),
                    hasherWithLeaves(1),
                    hasherWithLeaves(1),
                    hasherWithLeaves(1),
                    hasherWithLeaves(0));
            final Bytes rightmostAtSeven = HashingUtilities.computeFinalBlockHash(
                    fixedTimestamp(),
                    fixedHash(),
                    fixedHash(),
                    fixedHash(),
                    hasherWithLeaves(1),
                    hasherWithLeaves(1),
                    hasherWithLeaves(1),
                    hasherWithLeaves(1),
                    hasherWithLeaves(1));
            assertThat(rightmostAtSix).isNotEqualTo(rightmostAtSeven);
        }

        /// This test aims to assert the minimum 2-leaf case (only positions 0-1 populated,
        /// e.g. state-root-less WRB with no subtree content) hashes correctly against the
        /// reference: streaming hasher gives height 1, then 2 single-child promotions reach
        /// height 3.
        @Test
        @DisplayName("computeFinalBlockHash() only positions 0-1 populated matches reference")
        void testOnlyRequiredPositionsMatchesReference() {
            final FixedTreeInputs inputs = new FixedTreeInputs(
                    new Timestamp(1234567890L, 0),
                    Bytes.wrap(randomHash()),
                    Bytes.wrap(randomHash()),
                    Bytes.EMPTY,
                    hasherWithLeaves(0),
                    hasherWithLeaves(0),
                    hasherWithLeaves(0),
                    hasherWithLeaves(0),
                    hasherWithLeaves(0));
            final Bytes actual = HashingUtilities.computeFinalBlockHash(
                    inputs.timestamp(),
                    inputs.previousBlockHash(),
                    inputs.rootOfAllPreviousBlockHashes(),
                    inputs.startOfBlockStateRootHash(),
                    inputs.inputTreeHasher(),
                    inputs.outputTreeHasher(),
                    inputs.consensusHeaderHasher(),
                    inputs.stateChangesHasher(),
                    inputs.traceDataHasher());
            assertThat(actual).isEqualTo(referenceRootHash(inputs, new byte[8][]));
        }
    }

    /// The inputs to a final block hash computation.
    private record FixedTreeInputs(
            Timestamp timestamp,
            Bytes previousBlockHash,
            Bytes rootOfAllPreviousBlockHashes,
            Bytes startOfBlockStateRootHash,
            StreamingTreeHasher inputTreeHasher,
            StreamingTreeHasher outputTreeHasher,
            StreamingTreeHasher consensusHeaderHasher,
            StreamingTreeHasher stateChangesHasher,
            StreamingTreeHasher traceDataHasher) {}

    /// Builds deterministic pseudo random inputs: the block level values and the five category
    /// subtree hashers with varying leaf counts, including an empty one at position 3.
    private static FixedTreeInputs randomInputs() {
        return new FixedTreeInputs(
                new Timestamp(RANDOM.nextLong(0, Long.MAX_VALUE), RANDOM.nextInt(0, 1_000_000_000)),
                Bytes.wrap(randomHash()),
                Bytes.wrap(randomHash()),
                Bytes.wrap(randomHash()),
                hasherWithLeaves(1),
                hasherWithLeaves(3),
                hasherWithLeaves(0),
                hasherWithLeaves(4),
                hasherWithLeaves(2));
    }

    /// Builds inputs where pre-defined positions 2-7 are either empty or populated according to
    /// the given bitmask (bit 0 = position 2 state root, bit 1 = position 3 consensus headers,
    /// bit 2 = position 4 inputs, bit 3 = position 5 outputs, bit 4 = position 6 state changes,
    /// bit 5 = position 7 trace). Positions 0-1 are always populated with random hashes.
    private static FixedTreeInputs randomInputsWithPreDefinedPresence(final int presenceMask) {
        return new FixedTreeInputs(
                new Timestamp(RANDOM.nextLong(0, Long.MAX_VALUE), RANDOM.nextInt(0, 1_000_000_000)),
                Bytes.wrap(randomHash()),
                Bytes.wrap(randomHash()),
                (presenceMask & 0b000001) != 0 ? Bytes.wrap(randomHash()) : Bytes.EMPTY,
                hasherWithLeaves((presenceMask & 0b000100) != 0 ? 1 : 0),
                hasherWithLeaves((presenceMask & 0b001000) != 0 ? 1 : 0),
                hasherWithLeaves((presenceMask & 0b000010) != 0 ? 1 : 0),
                hasherWithLeaves((presenceMask & 0b010000) != 0 ? 1 : 0),
                hasherWithLeaves((presenceMask & 0b100000) != 0 ? 1 : 0));
    }

    private static StreamingTreeHasher hasherWithLeaves(final int leafCount) {
        final NaiveStreamingTreeHasher hasher = new NaiveStreamingTreeHasher();
        for (int i = 0; i < leafCount; i++) {
            hasher.addLeaf(ByteBuffer.wrap(randomHash()));
        }
        return hasher;
    }

    private static byte[] randomHash() {
        final byte[] hash = new byte[HASH_SIZE];
        RANDOM.nextBytes(hash);
        return hash;
    }

    private static Timestamp fixedTimestamp() {
        return new Timestamp(1234567890L, 0);
    }

    private static Bytes fixedHash() {
        final byte[] hash = new byte[HASH_SIZE];
        for (int i = 0; i < hash.length; i++) {
            hash[i] = (byte) i;
        }
        return Bytes.wrap(hash);
    }

    /// Reference implementation of the block root hash per HIP-1424 and issue #3377: the
    /// pre-defined side (positions 0-7) is a variable shape tree built by feeding only up to the
    /// rightmost non-empty leaf into the same fold-up algorithm used within each subtree hasher,
    /// then wrapping the result in single-child parents until it reaches height 3. Only positions
    /// 0-1 are always populated; position 2 (state root) and 3-7 can be absent. Interior empty
    /// leaves contribute EMPTY_TREE_HASH; trailing empties are dropped. The extension side
    /// (positions 8-15) is combined via combineOptional (single-child for lone present, dropped
    /// when both absent). The root combines the consensus timestamp leaf with the tree.
    private static Bytes referenceRootHash(final FixedTreeInputs inputs, final byte[][] extensionRoots) {
        final byte[] stateRoot = inputs.startOfBlockStateRootHash().length() == 0
                ? REF_EMPTY_TREE_HASH
                : inputs.startOfBlockStateRootHash().toByteArray();
        final byte[][] preDefinedLeaves = new byte[][] {
            inputs.previousBlockHash().toByteArray(),
            inputs.rootOfAllPreviousBlockHashes().toByteArray(),
            stateRoot,
            inputs.consensusHeaderHasher().rootHash().join().toByteArray(),
            inputs.inputTreeHasher().rootHash().join().toByteArray(),
            inputs.outputTreeHasher().rootHash().join().toByteArray(),
            inputs.stateChangesHasher().rootHash().join().toByteArray(),
            inputs.traceDataHasher().rootHash().join().toByteArray()
        };
        int rightmostIncluded = 1;
        for (int i = 2; i < preDefinedLeaves.length; i++) {
            if (!Arrays.equals(preDefinedLeaves[i], REF_EMPTY_TREE_HASH)) {
                rightmostIncluded = i;
            }
        }
        final List<byte[]> preDefinedFed = new ArrayList<>();
        for (int i = 0; i <= rightmostIncluded; i++) {
            preDefinedFed.add(preDefinedLeaves[i]);
        }
        byte[] leftHalf = refFoldUp(preDefinedFed);
        final int height = refHeightForLeafCount(preDefinedFed.size());
        for (int h = height; h < 3; h++) {
            leftHalf = refSingle(leftHalf);
        }
        final byte[] rightHalf = refCombineOptional(
                refCombineOptional(
                        refCombineOptional(extensionRoots[0], extensionRoots[1]),
                        refCombineOptional(extensionRoots[2], extensionRoots[3])),
                refCombineOptional(
                        refCombineOptional(extensionRoots[4], extensionRoots[5]),
                        refCombineOptional(extensionRoots[6], extensionRoots[7])));
        final byte[] mountainTop = rightHalf == null ? refSingle(leftHalf) : refNode(leftHalf, rightHalf);
        final byte[] timestampLeaf =
                refLeaf(Timestamp.PROTOBUF.toBytes(inputs.timestamp()).toByteArray());
        return Bytes.wrap(refNode(timestampLeaf, mountainTop));
    }

    /// Mirrors [NaiveStreamingTreeHasher]: pair-combine leaves at 0x02, promoting odd survivors
    /// to the next round until a single hash remains.
    private static byte[] refFoldUp(final List<byte[]> leaves) {
        List<byte[]> current = new ArrayList<>(leaves);
        while (current.size() > 1) {
            final List<byte[]> next = new ArrayList<>();
            for (int i = 0; i < current.size(); i += 2) {
                if (i + 1 < current.size()) {
                    next.add(refNode(current.get(i), current.get(i + 1)));
                } else {
                    next.add(current.get(i));
                }
            }
            current = next;
        }
        return current.get(0);
    }

    /// Streaming-hasher output tree height for a given leaf count (matches
    /// [NaiveStreamingTreeHasher] shape): 1 -> 0, 2 -> 1, 3-4 -> 2, 5-8 -> 3.
    private static int refHeightForLeafCount(final int leafCount) {
        int height = 0;
        int size = 1;
        while (size < leafCount) {
            size *= 2;
            height++;
        }
        return height;
    }

    private static byte[] refCombineOptional(final byte[] left, final byte[] right) {
        final byte[] node;
        if (left == null && right == null) {
            node = null;
        } else if (left == null) {
            node = refSingle(right);
        } else if (right == null) {
            node = refSingle(left);
        } else {
            node = refNode(left, right);
        }
        return node;
    }

    private static byte[] refLeaf(final byte[] data) {
        return refSha384(new byte[] {0x00}, data);
    }

    private static byte[] refSingle(final byte[] child) {
        return refSha384(new byte[] {0x01}, child);
    }

    private static byte[] refNode(final byte[] left, final byte[] right) {
        return refSha384(new byte[] {0x02}, left, right);
    }

    private static byte[] refSha384(final byte[]... parts) {
        try {
            final MessageDigest digest = MessageDigest.getInstance("SHA-384");
            for (final byte[] part : parts) {
                digest.update(part);
            }
            return digest.digest();
        } catch (final NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
