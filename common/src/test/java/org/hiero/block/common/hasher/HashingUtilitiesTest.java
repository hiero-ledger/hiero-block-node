// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.common.hasher;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.node.base.Timestamp;
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
        /// This test aims to assert that every presence pattern of the eight extension
        /// subtree leaves produces the block root hash computed by the reference
        /// implementation of the HIP-1424 tree. Patterns are supplied as bitmasks where bit N
        /// marks Extension N as present; absent slots contribute EMPTY_TREE_HASH.
        @ParameterizedTest
        @ValueSource(ints = {0b00000001, 0b00000010, 0b10000000, 0b00001001, 0b01100110, 0b11111111})
        @DisplayName("computeFinalBlockHash() extension presence patterns match reference")
        void testExtensionPresencePatternsMatchReference(final int presenceMask) {
            final FixedTreeInputs inputs = randomInputs();
            final StreamingTreeHasher[] extensionHashers = emptyExtensionHashers();
            for (int i = 0; i < extensionHashers.length; i++) {
                if ((presenceMask & (1 << i)) != 0) {
                    extensionHashers[i] = hasherWithRandomLeaf();
                }
            }
            final Bytes actual = computeFinalHash(inputs, extensionHashers);
            assertThat(actual).isEqualTo(referenceRootHash(inputs, extensionHashers));
        }

        /// This test aims to assert that a presence pattern with extension items produces a
        /// different root hash than the same inputs with no extension items, so extension
        /// items can never be dropped without changing the block hash.
        @Test
        @DisplayName("computeFinalBlockHash() extension presence changes the root hash")
        void testExtensionPresenceChangesRootHash() {
            final FixedTreeInputs inputs = randomInputs();
            final StreamingTreeHasher[] noExtensions = emptyExtensionHashers();
            final StreamingTreeHasher[] withExtension = emptyExtensionHashers();
            withExtension[0] = hasherWithRandomLeaf();
            final Bytes without = computeFinalHash(inputs, noExtensions);
            final Bytes with = computeFinalHash(inputs, withExtension);
            assertThat(with).isNotEqualTo(without);
        }
    }

    /// Tests for pre-defined slot presence (issue #3377). Under the always-feed-16 shape,
    /// positions 2-7 may be absent (empty subtree hashers or absent state root) but still
    /// contribute {@code EMPTY_TREE_HASH} at their fixed positions in the Mountain Top tree.
    /// The tree shape is fully stable; presence patterns produce distinct hashes that match
    /// the reference.
    @Nested
    @DisplayName("Pre-Defined Slot Presence Tests")
    class PreDefinedSlotPresenceTests {
        /// Presence bitmask over positions 2-7 (bit 0 = position 2, ..., bit 5 = position 7).
        /// Covers: none present (WRB with only prev-block and all-blocks), only position 2,
        /// only position 7, positions 2 and 7, interior gap with rightmost present, all present.
        @ParameterizedTest
        @ValueSource(ints = {0b000000, 0b000001, 0b100000, 0b100001, 0b100010, 0b111111})
        @DisplayName("computeFinalBlockHash() pre-defined presence patterns match reference")
        void testPreDefinedPresencePatternsMatchReference(final int presenceMask) {
            final FixedTreeInputs inputs = randomInputsWithPreDefinedPresence(presenceMask);
            final StreamingTreeHasher[] noExtensions = emptyExtensionHashers();
            final Bytes actual = computeFinalHash(inputs, noExtensions);
            assertThat(actual).isEqualTo(referenceRootHash(inputs, noExtensions));
        }

        /// Presence at any pre-defined position must affect the root hash — an absent trace
        /// subtree (position 7) must produce a different root than a present one, otherwise
        /// the position would be indistinguishable and dropping it would be safe (it isn't).
        @Test
        @DisplayName("computeFinalBlockHash() position 7 presence changes the root hash")
        void testPositionSevenPresenceChangesRootHash() {
            final FixedTreeInputs traceEmptyInputs = new FixedTreeInputs(
                    fixedTimestamp(),
                    fixedHash(),
                    fixedHash(),
                    fixedHash(),
                    hasherWithLeaves(1),
                    hasherWithLeaves(1),
                    hasherWithLeaves(1),
                    hasherWithLeaves(1),
                    hasherWithLeaves(0));
            final FixedTreeInputs tracePresentInputs = new FixedTreeInputs(
                    fixedTimestamp(),
                    fixedHash(),
                    fixedHash(),
                    fixedHash(),
                    hasherWithLeaves(1),
                    hasherWithLeaves(1),
                    hasherWithLeaves(1),
                    hasherWithLeaves(1),
                    hasherWithLeaves(1));
            final StreamingTreeHasher[] noExtensions = emptyExtensionHashers();
            assertThat(computeFinalHash(traceEmptyInputs, noExtensions))
                    .isNotEqualTo(computeFinalHash(tracePresentInputs, noExtensions));
        }

        /// Minimum-content block (only positions 0-1 populated, state root absent, all subtree
        /// hashers empty — e.g. an empty WRB) hashes correctly against the reference. All 16
        /// slots still contribute; positions 2-15 are EMPTY_TREE_HASH.
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
            final StreamingTreeHasher[] noExtensions = emptyExtensionHashers();
            assertThat(computeFinalHash(inputs, noExtensions)).isEqualTo(referenceRootHash(inputs, noExtensions));
        }

        /// Positions 0 and 1 (previousBlockHash, rootHashOfAllPreviousBlockHashes) must be a
        /// full SHA-384 digest. A short value would silently be padded by the streaming
        /// hasher's underlying buffer, producing an incorrect root — the length guard rejects
        /// it up front.
        @Test
        @DisplayName("computeFinalBlockHash() short previousBlockHash throws")
        void testShortPreviousBlockHashRejected() {
            final FixedTreeInputs inputs = new FixedTreeInputs(
                    fixedTimestamp(),
                    Bytes.wrap(new byte[16]),
                    fixedHash(),
                    fixedHash(),
                    hasherWithLeaves(0),
                    hasherWithLeaves(0),
                    hasherWithLeaves(0),
                    hasherWithLeaves(0),
                    hasherWithLeaves(0));
            org.junit.jupiter.api.Assertions.assertThrows(
                    IllegalArgumentException.class, () -> computeFinalHash(inputs, emptyExtensionHashers()));
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

    /// Calls the [HashingUtilities#computeFinalBlockHash] overload, expanding the given hashers
    /// into the individual extension subtree hasher parameters. Absent slots in {@code
    /// extensionHashers} must be a fresh empty {@link NaiveStreamingTreeHasher} via
    /// {@link #emptyExtensionHashers()}.
    private static Bytes computeFinalHash(final FixedTreeInputs inputs, final StreamingTreeHasher[] extensionHashers) {
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
                extensionHashers[0],
                extensionHashers[1],
                extensionHashers[2],
                extensionHashers[3],
                extensionHashers[4],
                extensionHashers[5],
                extensionHashers[6],
                extensionHashers[7]);
    }

    /// Returns a fresh {@code StreamingTreeHasher[8]} of empty {@link NaiveStreamingTreeHasher}
    /// instances, representing "no extension subtrees present". Callers can replace slots with
    /// populated hashers to inject presence.
    private static StreamingTreeHasher[] emptyExtensionHashers() {
        final StreamingTreeHasher[] hashers = new StreamingTreeHasher[8];
        for (int i = 0; i < hashers.length; i++) {
            hashers[i] = new NaiveStreamingTreeHasher();
        }
        return hashers;
    }

    /// Builds a fresh {@link NaiveStreamingTreeHasher} with a single random leaf, so its
    /// {@code rootHash()} is non-empty and deterministically depends on the seeded content.
    private static StreamingTreeHasher hasherWithRandomLeaf() {
        final NaiveStreamingTreeHasher hasher = new NaiveStreamingTreeHasher();
        hasher.addLeaf(ByteBuffer.wrap(randomHash()));
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

    /// Reference implementation of the block root hash per HIP-1424 and issue #3377: a single
    /// streaming-hasher fold over all 16 leaves. Positions 0-7 are the pre-defined leaves
    /// (previousBlockHash, rootHashOfAllPreviousBlockHashes, state root, then the five subtree
    /// hasher roots); positions 8-15 are the extension subtree hasher roots. Absent state
    /// root contributes {@link #REF_EMPTY_TREE_HASH}; empty subtree hashers naturally return
    /// {@link #REF_EMPTY_TREE_HASH}. The 16-leaf tree shape is fully stable so Merkle proof
    /// paths are independent of presence patterns. The root combines the consensus timestamp
    /// leaf with the Mountain Top root.
    private static Bytes referenceRootHash(final FixedTreeInputs inputs, final StreamingTreeHasher[] extensionHashers) {
        final byte[] stateRoot = inputs.startOfBlockStateRootHash().length() == 0
                ? REF_EMPTY_TREE_HASH
                : inputs.startOfBlockStateRootHash().toByteArray();
        final List<byte[]> mountainTopLeaves = new ArrayList<>();
        mountainTopLeaves.add(inputs.previousBlockHash().toByteArray());
        mountainTopLeaves.add(inputs.rootOfAllPreviousBlockHashes().toByteArray());
        mountainTopLeaves.add(stateRoot);
        mountainTopLeaves.add(inputs.consensusHeaderHasher().rootHash().toByteArray());
        mountainTopLeaves.add(inputs.inputTreeHasher().rootHash().toByteArray());
        mountainTopLeaves.add(inputs.outputTreeHasher().rootHash().toByteArray());
        mountainTopLeaves.add(inputs.stateChangesHasher().rootHash().toByteArray());
        mountainTopLeaves.add(inputs.traceDataHasher().rootHash().toByteArray());
        for (int i = 0; i < 8; i++) {
            mountainTopLeaves.add(extensionHashers[i].rootHash().toByteArray());
        }
        final byte[] mountainTop = refFoldUp(mountainTopLeaves);
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

    private static byte[] refLeaf(final byte[] data) {
        return refSha384(new byte[] {0x00}, data);
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
