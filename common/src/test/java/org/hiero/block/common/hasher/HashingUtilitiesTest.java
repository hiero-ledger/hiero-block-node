// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.common.hasher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hedera.hapi.node.base.Timestamp;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;
import org.hiero.block.internal.BlockItemUnparsed;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/// Tests for the [HashingUtilities] class. Expected values are produced by a local reference
/// implementation of the HIP-1424 block root tree ("Merkle Mountain Top"), independent of the
/// production code, so the two implementations must agree. Every block hashing test runs for
/// every [HashAlgorithm]: the utilities take the algorithm as an argument and must favour none.
@DisplayName("Hashing Utilities Tests")
class HashingUtilitiesTest {
    private static final String EXTENSION_PRESENCE_PATTERNS =
            "org.hiero.block.common.hasher.HashingUtilitiesTest#extensionPresencePatterns";
    private static final String PRE_DEFINED_PRESENCE_PATTERNS =
            "org.hiero.block.common.hasher.HashingUtilitiesTest#preDefinedPresencePatterns";
    /// Deterministic seed so failures are reproducible.
    private static final Random RANDOM = new Random(6431582L);

    /// Tests for [HashingUtilities#computeFinalBlockHash] with extension subtree roots.
    @Nested
    @DisplayName("Final Block Hash Extension Subtree Tests")
    class FinalBlockHashExtensionSubtreeTests {
        /// This test aims to assert that every presence pattern of the eight extension
        /// subtree leaves produces the block root hash computed by the reference
        /// implementation of the HIP-1424 tree, for every algorithm. Patterns are supplied as
        /// bitmasks where bit N marks Extension N as present; absent slots contribute the empty
        /// tree hash of the algorithm.
        @ParameterizedTest
        @MethodSource(EXTENSION_PRESENCE_PATTERNS)
        @DisplayName("computeFinalBlockHash() extension presence patterns match reference")
        void testExtensionPresencePatternsMatchReference(final HashAlgorithm algorithm, final int presenceMask) {
            final FixedTreeInputs inputs = randomInputs(algorithm);
            final StreamingTreeHasher[] extensionHashers = emptyExtensionHashers(algorithm);
            for (int i = 0; i < extensionHashers.length; i++) {
                if ((presenceMask & (1 << i)) != 0) {
                    extensionHashers[i] = hasherWithRandomLeaf(algorithm);
                }
            }
            final Bytes actual = computeFinalHash(inputs, extensionHashers);
            assertThat(actual).isEqualTo(referenceRootHash(inputs, extensionHashers));
        }

        /// This test aims to assert that a presence pattern with extension items produces a
        /// different root hash than the same inputs with no extension items, so extension
        /// items can never be dropped without changing the block hash.
        @ParameterizedTest
        @EnumSource(HashAlgorithm.class)
        @DisplayName("computeFinalBlockHash() extension presence changes the root hash")
        void testExtensionPresenceChangesRootHash(final HashAlgorithm algorithm) {
            final FixedTreeInputs inputs = randomInputs(algorithm);
            final StreamingTreeHasher[] noExtensions = emptyExtensionHashers(algorithm);
            final StreamingTreeHasher[] withExtension = emptyExtensionHashers(algorithm);
            withExtension[0] = hasherWithRandomLeaf(algorithm);
            final Bytes without = computeFinalHash(inputs, noExtensions);
            final Bytes with = computeFinalHash(inputs, withExtension);
            assertThat(with).isNotEqualTo(without);
        }
    }

    /// Tests for pre-defined slot presence (issue #3377). Under the always-feed-16 shape,
    /// positions 2-7 may be absent (empty subtree hashers or absent state root) but still
    /// contribute the empty tree hash at their fixed positions in the Mountain Top tree.
    /// The tree shape is fully stable; presence patterns produce distinct hashes that match
    /// the reference.
    @Nested
    @DisplayName("Pre-Defined Slot Presence Tests")
    class PreDefinedSlotPresenceTests {
        /// This test aims to assert that every presence pattern of the pre-defined positions
        /// 2-7 (bit 0 = position 2, ..., bit 5 = position 7) matches the reference for every
        /// algorithm. Covers: none present (WRB with only prev-block and all-blocks), only
        /// position 2, only position 7, positions 2 and 7, interior gap with rightmost present,
        /// all present.
        @ParameterizedTest
        @MethodSource(PRE_DEFINED_PRESENCE_PATTERNS)
        @DisplayName("computeFinalBlockHash() pre-defined presence patterns match reference")
        void testPreDefinedPresencePatternsMatchReference(final HashAlgorithm algorithm, final int presenceMask) {
            final FixedTreeInputs inputs = randomInputsWithPreDefinedPresence(algorithm, presenceMask);
            final StreamingTreeHasher[] noExtensions = emptyExtensionHashers(algorithm);
            final Bytes actual = computeFinalHash(inputs, noExtensions);
            assertThat(actual).isEqualTo(referenceRootHash(inputs, noExtensions));
        }

        /// This test aims to assert that presence at any pre-defined position affects the root
        /// hash: an absent trace subtree (position 7) must produce a different root than a
        /// present one, otherwise the position would be indistinguishable and dropping it would
        /// go unnoticed.
        @ParameterizedTest
        @EnumSource(HashAlgorithm.class)
        @DisplayName("computeFinalBlockHash() position 7 presence changes the root hash")
        void testPositionSevenPresenceChangesRootHash(final HashAlgorithm algorithm) {
            final FixedTreeInputs traceEmptyInputs = new FixedTreeInputs(
                    algorithm,
                    fixedTimestamp(),
                    fixedHash(algorithm),
                    fixedHash(algorithm),
                    fixedHash(algorithm),
                    hasherWithLeaves(algorithm, 1),
                    hasherWithLeaves(algorithm, 1),
                    hasherWithLeaves(algorithm, 1),
                    hasherWithLeaves(algorithm, 1),
                    hasherWithLeaves(algorithm, 0));
            final FixedTreeInputs tracePresentInputs = new FixedTreeInputs(
                    algorithm,
                    fixedTimestamp(),
                    fixedHash(algorithm),
                    fixedHash(algorithm),
                    fixedHash(algorithm),
                    hasherWithLeaves(algorithm, 1),
                    hasherWithLeaves(algorithm, 1),
                    hasherWithLeaves(algorithm, 1),
                    hasherWithLeaves(algorithm, 1),
                    hasherWithLeaves(algorithm, 1));
            assertThat(computeFinalHash(traceEmptyInputs, emptyExtensionHashers(algorithm)))
                    .isNotEqualTo(computeFinalHash(tracePresentInputs, emptyExtensionHashers(algorithm)));
        }

        /// This test aims to assert that a minimum-content block (only positions 0-1 populated,
        /// state root absent, all subtree hashers empty, e.g. an empty WRB) hashes correctly
        /// against the reference. All 16 slots still contribute; positions 2-15 are the empty
        /// tree hash of the algorithm.
        @ParameterizedTest
        @EnumSource(HashAlgorithm.class)
        @DisplayName("computeFinalBlockHash() only positions 0-1 populated matches reference")
        void testOnlyRequiredPositionsMatchesReference(final HashAlgorithm algorithm) {
            final FixedTreeInputs inputs = new FixedTreeInputs(
                    algorithm,
                    new Timestamp(1234567890L, 0),
                    Bytes.wrap(randomHash(algorithm)),
                    Bytes.wrap(randomHash(algorithm)),
                    Bytes.EMPTY,
                    hasherWithLeaves(algorithm, 0),
                    hasherWithLeaves(algorithm, 0),
                    hasherWithLeaves(algorithm, 0),
                    hasherWithLeaves(algorithm, 0),
                    hasherWithLeaves(algorithm, 0));
            final StreamingTreeHasher[] noExtensions = emptyExtensionHashers(algorithm);
            assertThat(computeFinalHash(inputs, noExtensions)).isEqualTo(referenceRootHash(inputs, noExtensions));
        }
    }

    /// Tests for the guards of [HashingUtilities#computeFinalBlockHash]: every leaf of the block
    /// root tree is one digest of the algorithm, so a value of another size or a subtree hasher
    /// of another algorithm is refused instead of being hashed into a root that belongs to no
    /// block.
    @Nested
    @DisplayName("Final Block Hash Guard Tests")
    class FinalBlockHashGuardTests {
        /// This test aims to assert that the root hash has the digest size of the algorithm the
        /// tree is computed with.
        @ParameterizedTest
        @EnumSource(HashAlgorithm.class)
        @DisplayName("computeFinalBlockHash() root hash has the digest size of the algorithm")
        void testRootHashHasDigestSize(final HashAlgorithm algorithm) {
            final Bytes actual = computeFinalHash(randomInputs(algorithm), emptyExtensionHashers(algorithm));
            assertThat(actual.length()).isEqualTo(algorithm.hashSize());
        }

        /// This test aims to assert that a previous block hash shorter than one digest is
        /// refused up front. A short value would otherwise be refused by the streaming hasher
        /// with a less specific message, and could never be the root of the previous block.
        @ParameterizedTest
        @EnumSource(HashAlgorithm.class)
        @DisplayName("computeFinalBlockHash() short previousBlockHash throws")
        void testShortPreviousBlockHashRejected(final HashAlgorithm algorithm) {
            final FixedTreeInputs inputs = inputsWithPreviousBlockHash(algorithm, Bytes.wrap(new byte[16]));
            assertThatThrownBy(() -> computeFinalHash(inputs, emptyExtensionHashers(algorithm)))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        /// This test aims to assert that a previous block hash with the digest size of the
        /// other algorithm is refused: such a block was hashed by a network computing with
        /// another algorithm, and folding it into this tree would silently truncate it.
        @ParameterizedTest
        @EnumSource(HashAlgorithm.class)
        @DisplayName("computeFinalBlockHash() previousBlockHash of the other digest size throws")
        void testPreviousBlockHashOfOtherDigestSizeRejected(final HashAlgorithm algorithm) {
            final Bytes otherSize = Bytes.wrap(randomHash(otherAlgorithm(algorithm)));
            final FixedTreeInputs inputs = inputsWithPreviousBlockHash(algorithm, otherSize);
            assertThatThrownBy(() -> computeFinalHash(inputs, emptyExtensionHashers(algorithm)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("previousBlockHash");
        }

        /// This test aims to assert that a root of all previous block hashes with the digest
        /// size of the other algorithm is refused, for the same reason as the previous block
        /// hash.
        @ParameterizedTest
        @EnumSource(HashAlgorithm.class)
        @DisplayName("computeFinalBlockHash() rootHashOfAllPreviousBlockHashes of the other digest size throws")
        void testRootOfAllPreviousBlockHashesOfOtherDigestSizeRejected(final HashAlgorithm algorithm) {
            final FixedTreeInputs valid = randomInputs(algorithm);
            final FixedTreeInputs inputs = new FixedTreeInputs(
                    algorithm,
                    valid.timestamp(),
                    valid.previousBlockHash(),
                    Bytes.wrap(randomHash(otherAlgorithm(algorithm))),
                    valid.startOfBlockStateRootHash(),
                    valid.inputTreeHasher(),
                    valid.outputTreeHasher(),
                    valid.consensusHeaderHasher(),
                    valid.stateChangesHasher(),
                    valid.traceDataHasher());
            assertThatThrownBy(() -> computeFinalHash(inputs, emptyExtensionHashers(algorithm)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("rootHashOfAllPreviousBlockHashes");
        }

        /// This test aims to assert that a non-empty start of block state root with the digest
        /// size of the other algorithm is refused: the streaming hasher reads exactly one digest
        /// per leaf, so a longer value would otherwise be truncated without any error. An empty
        /// state root stays allowed and contributes the empty tree hash.
        @ParameterizedTest
        @EnumSource(HashAlgorithm.class)
        @DisplayName("computeFinalBlockHash() non-empty startOfBlockStateRootHash of the other digest size throws")
        void testNonEmptyStateRootOfOtherDigestSizeRejected(final HashAlgorithm algorithm) {
            final FixedTreeInputs valid = randomInputs(algorithm);
            final FixedTreeInputs inputs = new FixedTreeInputs(
                    algorithm,
                    valid.timestamp(),
                    valid.previousBlockHash(),
                    valid.rootOfAllPreviousBlockHashes(),
                    Bytes.wrap(randomHash(otherAlgorithm(algorithm))),
                    valid.inputTreeHasher(),
                    valid.outputTreeHasher(),
                    valid.consensusHeaderHasher(),
                    valid.stateChangesHasher(),
                    valid.traceDataHasher());
            assertThatThrownBy(() -> computeFinalHash(inputs, emptyExtensionHashers(algorithm)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("startOfBlockStateRootHash");
        }

        /// This test aims to assert that a subtree hasher computing with another algorithm is
        /// refused: its root is not one digest of the Mountain Top's algorithm, so the two
        /// algorithms can never be mixed within one block root tree.
        @ParameterizedTest
        @EnumSource(HashAlgorithm.class)
        @DisplayName("computeFinalBlockHash() subtree hasher of another algorithm throws")
        void testSubtreeHasherOfOtherAlgorithmRejected(final HashAlgorithm algorithm) {
            final FixedTreeInputs inputs = randomInputs(algorithm);
            final StreamingTreeHasher[] extensionHashers = emptyExtensionHashers(algorithm);
            extensionHashers[3] = new NaiveStreamingTreeHasher(otherAlgorithm(algorithm));
            assertThatThrownBy(() -> computeFinalHash(inputs, extensionHashers))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(otherAlgorithm(algorithm).name());
        }
    }

    /// Tests for the leaf and internal node hash helpers.
    @Nested
    @DisplayName("Leaf And Node Hash Tests")
    class LeafAndNodeHashTests {
        /// This test aims to assert that a leaf is hashed with the 0x00 domain separation prefix
        /// under the given algorithm and that the result is one digest of that algorithm.
        @ParameterizedTest
        @EnumSource(HashAlgorithm.class)
        @DisplayName("hashLeaf() is the prefixed digest of the leaf data")
        void testHashLeafMatchesReference(final HashAlgorithm algorithm) {
            final byte[] data = randomBytes(77);
            final byte[] actual = HashingUtilities.hashLeaf(algorithm, data);
            assertThat(actual).isEqualTo(refLeaf(algorithm, data));
            assertThat(actual).hasSize(algorithm.hashSize());
        }

        /// This test aims to assert that two children are combined with the 0x02 domain
        /// separation prefix in left to right order under the given algorithm.
        @ParameterizedTest
        @EnumSource(HashAlgorithm.class)
        @DisplayName("hashInternalNode() is the prefixed digest of left then right")
        void testHashInternalNodeMatchesReference(final HashAlgorithm algorithm) {
            final byte[] left = randomHash(algorithm);
            final byte[] right = randomHash(algorithm);
            final byte[] actual = HashingUtilities.hashInternalNode(algorithm, left, right);
            assertThat(actual).isEqualTo(refNode(algorithm, left, right));
            assertThat(actual).isNotEqualTo(refNode(algorithm, right, left));
            assertThat(actual).hasSize(algorithm.hashSize());
        }

        /// This test aims to assert that a single child is hashed with the 0x01 domain
        /// separation prefix under the given algorithm, distinct from the leaf and the two
        /// children prefixes.
        @ParameterizedTest
        @EnumSource(HashAlgorithm.class)
        @DisplayName("hashInternalNodeSingleChild() is the prefixed digest of the child")
        void testHashInternalNodeSingleChildMatchesReference(final HashAlgorithm algorithm) {
            final byte[] child = randomHash(algorithm);
            final byte[] actual = HashingUtilities.hashInternalNodeSingleChild(algorithm, child);
            assertThat(actual).isEqualTo(refHash(algorithm, new byte[] {0x01}, child));
            assertThat(actual).isNotEqualTo(refLeaf(algorithm, child));
            assertThat(actual).hasSize(algorithm.hashSize());
        }
    }

    /// Tests for the block item hashing helpers.
    @Nested
    @DisplayName("Block Item Hash Tests")
    class BlockItemHashTests {
        /// This test aims to assert that the hash of a block item is the prefixed leaf hash of
        /// its serialized bytes, returned in a buffer holding exactly one digest of the
        /// algorithm.
        @ParameterizedTest
        @EnumSource(HashAlgorithm.class)
        @DisplayName("getBlockItemHash() is the leaf hash of the serialized item")
        void testGetBlockItemHashIsLeafHash(final HashAlgorithm algorithm) {
            final BlockItemUnparsed item = BlockItemUnparsed.newBuilder()
                    .transactionResult(Bytes.wrap("transaction result"))
                    .build();
            final ByteBuffer actual = HashingUtilities.getBlockItemHash(algorithm, item);
            assertThat(actual.remaining()).isEqualTo(algorithm.hashSize());
            assertThat(remainingBytes(actual)).isEqualTo(refItemLeaf(algorithm, item));
        }

        /// This test aims to assert that the hashes of a list of block items are routed into
        /// the buffer of their category, in item order, one digest of the algorithm per item:
        /// headers and transaction results to the output buffer, round and event headers to the
        /// consensus header buffer, signed transactions to the input buffer, state changes and
        /// trace data to their own buffers, while a block proof is not hashed at all.
        @ParameterizedTest
        @EnumSource(HashAlgorithm.class)
        @DisplayName("getBlockHashes() routes the item hashes by category")
        void testGetBlockHashesRoutesItemsByCategory(final HashAlgorithm algorithm) {
            final BlockItemUnparsed header = BlockItemUnparsed.newBuilder()
                    .blockHeader(Bytes.wrap("header"))
                    .build();
            final BlockItemUnparsed roundHeader = BlockItemUnparsed.newBuilder()
                    .roundHeader(Bytes.wrap("round"))
                    .build();
            final BlockItemUnparsed eventHeader = BlockItemUnparsed.newBuilder()
                    .eventHeader(Bytes.wrap("event"))
                    .build();
            final BlockItemUnparsed firstTransaction = BlockItemUnparsed.newBuilder()
                    .signedTransaction(Bytes.wrap("first transaction"))
                    .build();
            final BlockItemUnparsed secondTransaction = BlockItemUnparsed.newBuilder()
                    .signedTransaction(Bytes.wrap("second transaction"))
                    .build();
            final BlockItemUnparsed transactionResult = BlockItemUnparsed.newBuilder()
                    .transactionResult(Bytes.wrap("result"))
                    .build();
            final BlockItemUnparsed stateChanges = BlockItemUnparsed.newBuilder()
                    .stateChanges(Bytes.wrap("state changes"))
                    .build();
            final BlockItemUnparsed traceData = BlockItemUnparsed.newBuilder()
                    .traceData(Bytes.wrap("trace"))
                    .build();
            final BlockItemUnparsed blockProof = BlockItemUnparsed.newBuilder()
                    .blockProof(Bytes.wrap("proof"))
                    .build();
            final List<BlockItemUnparsed> items = List.of(
                    header,
                    roundHeader,
                    eventHeader,
                    firstTransaction,
                    secondTransaction,
                    transactionResult,
                    stateChanges,
                    traceData,
                    blockProof);
            final Hashes hashes = HashingUtilities.getBlockHashes(algorithm, items);
            assertThat(remainingBytes(hashes.inputHashes()))
                    .isEqualTo(concat(
                            refItemLeaf(algorithm, firstTransaction), refItemLeaf(algorithm, secondTransaction)));
            assertThat(remainingBytes(hashes.outputHashes()))
                    .isEqualTo(concat(refItemLeaf(algorithm, header), refItemLeaf(algorithm, transactionResult)));
            assertThat(remainingBytes(hashes.consensusHeaderHashes()))
                    .isEqualTo(concat(refItemLeaf(algorithm, roundHeader), refItemLeaf(algorithm, eventHeader)));
            assertThat(remainingBytes(hashes.stateChangesHashes())).isEqualTo(refItemLeaf(algorithm, stateChanges));
            assertThat(remainingBytes(hashes.traceDataHashes())).isEqualTo(refItemLeaf(algorithm, traceData));
        }
    }

    /// Tests for the record file signed payload, the one SHA-384 computation that is independent
    /// of the algorithm the block root tree is computed with.
    @Nested
    @DisplayName("Record File Signed Payload Tests")
    class RecordFileSignedPayloadTests {
        /// This test aims to assert that the version 6 record file signed payload is the SHA-384
        /// digest of the big endian int32 value 6 followed by the record file contents, 48
        /// bytes long: the record file format defines this payload, so it does not follow the
        /// block root algorithm.
        @Test
        @DisplayName("computeV6SignedPayload() is SHA-384 of the version prefix and the contents")
        void testComputeV6SignedPayloadIsSha384OfVersionAndContents() {
            final byte[] contents = randomBytes(300);
            final byte[] actual = HashingUtilities.computeV6SignedPayload(Bytes.wrap(contents));
            final byte[] expected = refHash(HashAlgorithm.SHA2_384, new byte[] {0, 0, 0, 6}, contents);
            assertThat(actual).isEqualTo(expected);
            assertThat(actual).hasSize(48);
        }
    }

    /// The inputs to a final block hash computation.
    private record FixedTreeInputs(
            HashAlgorithm algorithm,
            Timestamp timestamp,
            Bytes previousBlockHash,
            Bytes rootOfAllPreviousBlockHashes,
            Bytes startOfBlockStateRootHash,
            StreamingTreeHasher inputTreeHasher,
            StreamingTreeHasher outputTreeHasher,
            StreamingTreeHasher consensusHeaderHasher,
            StreamingTreeHasher stateChangesHasher,
            StreamingTreeHasher traceDataHasher) {}

    /// Supplies every extension presence bitmask for every algorithm.
    private static Stream<Arguments> extensionPresencePatterns() {
        return presencePatterns(new int[] {0b00000001, 0b00000010, 0b10000000, 0b00001001, 0b01100110, 0b11111111});
    }

    /// Supplies every pre-defined slot presence bitmask for every algorithm.
    private static Stream<Arguments> preDefinedPresencePatterns() {
        return presencePatterns(new int[] {0b000000, 0b000001, 0b100000, 0b100001, 0b100010, 0b111111});
    }

    private static Stream<Arguments> presencePatterns(final int[] masks) {
        final List<Arguments> arguments = new ArrayList<>();
        for (final HashAlgorithm algorithm : HashAlgorithm.values()) {
            for (final int mask : masks) {
                arguments.add(Arguments.of(algorithm, mask));
            }
        }
        return arguments.stream();
    }

    /// Builds deterministic pseudo random inputs: the block level values and the five category
    /// subtree hashers with varying leaf counts, including an empty one at position 3.
    private static FixedTreeInputs randomInputs(final HashAlgorithm algorithm) {
        return new FixedTreeInputs(
                algorithm,
                new Timestamp(RANDOM.nextLong(0, Long.MAX_VALUE), RANDOM.nextInt(0, 1_000_000_000)),
                Bytes.wrap(randomHash(algorithm)),
                Bytes.wrap(randomHash(algorithm)),
                Bytes.wrap(randomHash(algorithm)),
                hasherWithLeaves(algorithm, 1),
                hasherWithLeaves(algorithm, 3),
                hasherWithLeaves(algorithm, 0),
                hasherWithLeaves(algorithm, 4),
                hasherWithLeaves(algorithm, 2));
    }

    /// Builds inputs where pre-defined positions 2-7 are either empty or populated according to
    /// the given bitmask (bit 0 = position 2 state root, bit 1 = position 3 consensus headers,
    /// bit 2 = position 4 inputs, bit 3 = position 5 outputs, bit 4 = position 6 state changes,
    /// bit 5 = position 7 trace). Positions 0-1 are always populated with random hashes.
    private static FixedTreeInputs randomInputsWithPreDefinedPresence(
            final HashAlgorithm algorithm, final int presenceMask) {
        return new FixedTreeInputs(
                algorithm,
                new Timestamp(RANDOM.nextLong(0, Long.MAX_VALUE), RANDOM.nextInt(0, 1_000_000_000)),
                Bytes.wrap(randomHash(algorithm)),
                Bytes.wrap(randomHash(algorithm)),
                (presenceMask & 0b000001) != 0 ? Bytes.wrap(randomHash(algorithm)) : Bytes.EMPTY,
                hasherWithLeaves(algorithm, (presenceMask & 0b000100) != 0 ? 1 : 0),
                hasherWithLeaves(algorithm, (presenceMask & 0b001000) != 0 ? 1 : 0),
                hasherWithLeaves(algorithm, (presenceMask & 0b000010) != 0 ? 1 : 0),
                hasherWithLeaves(algorithm, (presenceMask & 0b010000) != 0 ? 1 : 0),
                hasherWithLeaves(algorithm, (presenceMask & 0b100000) != 0 ? 1 : 0));
    }

    /// Builds valid random inputs with the given previous block hash in place of the random one.
    private static FixedTreeInputs inputsWithPreviousBlockHash(
            final HashAlgorithm algorithm, final Bytes previousBlockHash) {
        final FixedTreeInputs valid = randomInputs(algorithm);
        return new FixedTreeInputs(
                algorithm,
                valid.timestamp(),
                previousBlockHash,
                valid.rootOfAllPreviousBlockHashes(),
                valid.startOfBlockStateRootHash(),
                valid.inputTreeHasher(),
                valid.outputTreeHasher(),
                valid.consensusHeaderHasher(),
                valid.stateChangesHasher(),
                valid.traceDataHasher());
    }

    private static StreamingTreeHasher hasherWithLeaves(final HashAlgorithm algorithm, final int leafCount) {
        final NaiveStreamingTreeHasher hasher = new NaiveStreamingTreeHasher(algorithm);
        for (int i = 0; i < leafCount; i++) {
            hasher.addLeaf(ByteBuffer.wrap(randomHash(algorithm)));
        }
        return hasher;
    }

    /// Calls [HashingUtilities#computeFinalBlockHash], expanding the given hashers into the
    /// individual extension subtree hasher parameters. Absent slots in {@code extensionHashers}
    /// must be a fresh empty {@link NaiveStreamingTreeHasher} via {@link #emptyExtensionHashers}.
    private static Bytes computeFinalHash(final FixedTreeInputs inputs, final StreamingTreeHasher[] extensionHashers) {
        return HashingUtilities.computeFinalBlockHash(
                inputs.algorithm(),
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
    /// instances of the algorithm, representing "no extension subtrees present". Callers can
    /// replace slots with populated hashers to inject presence.
    private static StreamingTreeHasher[] emptyExtensionHashers(final HashAlgorithm algorithm) {
        final StreamingTreeHasher[] hashers = new StreamingTreeHasher[8];
        for (int i = 0; i < hashers.length; i++) {
            hashers[i] = new NaiveStreamingTreeHasher(algorithm);
        }
        return hashers;
    }

    /// Builds a fresh {@link NaiveStreamingTreeHasher} with a single random leaf, so its
    /// {@code rootHash()} is non-empty and deterministically depends on the seeded content.
    private static StreamingTreeHasher hasherWithRandomLeaf(final HashAlgorithm algorithm) {
        final NaiveStreamingTreeHasher hasher = new NaiveStreamingTreeHasher(algorithm);
        hasher.addLeaf(ByteBuffer.wrap(randomHash(algorithm)));
        return hasher;
    }

    private static byte[] randomHash(final HashAlgorithm algorithm) {
        return randomBytes(algorithm.hashSize());
    }

    private static byte[] randomBytes(final int length) {
        final byte[] bytes = new byte[length];
        RANDOM.nextBytes(bytes);
        return bytes;
    }

    private static Timestamp fixedTimestamp() {
        return new Timestamp(1234567890L, 0);
    }

    private static Bytes fixedHash(final HashAlgorithm algorithm) {
        final byte[] hash = new byte[algorithm.hashSize()];
        for (int i = 0; i < hash.length; i++) {
            hash[i] = (byte) i;
        }
        return Bytes.wrap(hash);
    }

    /// The other constant of the enum, the algorithm a value "of the other digest size" belongs to.
    private static HashAlgorithm otherAlgorithm(final HashAlgorithm algorithm) {
        return switch (algorithm) {
            case SHA2_256 -> HashAlgorithm.SHA2_384;
            case SHA2_384 -> HashAlgorithm.SHA2_256;
        };
    }

    /// Returns the remaining bytes of the buffer without consuming them.
    private static byte[] remainingBytes(final ByteBuffer buffer) {
        final byte[] bytes = new byte[buffer.remaining()];
        buffer.duplicate().get(bytes);
        return bytes;
    }

    private static byte[] concat(final byte[] first, final byte[] second) {
        final byte[] result = new byte[first.length + second.length];
        System.arraycopy(first, 0, result, 0, first.length);
        System.arraycopy(second, 0, result, first.length, second.length);
        return result;
    }

    /// Reference implementation of the block root hash per HIP-1424 and issue #3377: a single
    /// streaming-hasher fold over all 16 leaves. Positions 0-7 are the pre-defined leaves
    /// (previousBlockHash, rootHashOfAllPreviousBlockHashes, state root, then the five subtree
    /// hasher roots); positions 8-15 are the extension subtree hasher roots. Absent state
    /// root contributes the reference empty tree hash; empty subtree hashers naturally return
    /// it. The 16-leaf tree shape is fully stable so Merkle proof paths are independent of
    /// presence patterns. The root combines the consensus timestamp leaf with the Mountain Top
    /// root.
    private static Bytes referenceRootHash(final FixedTreeInputs inputs, final StreamingTreeHasher[] extensionHashers) {
        final HashAlgorithm algorithm = inputs.algorithm();
        final byte[] stateRoot = inputs.startOfBlockStateRootHash().length() == 0
                ? refEmptyTreeHash(algorithm)
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
        final byte[] mountainTop = refFoldUp(algorithm, mountainTopLeaves);
        final byte[] timestampLeaf = refLeaf(
                algorithm, Timestamp.PROTOBUF.toBytes(inputs.timestamp()).toByteArray());
        return Bytes.wrap(refNode(algorithm, timestampLeaf, mountainTop));
    }

    /// Mirrors [NaiveStreamingTreeHasher]: pair-combine leaves at 0x02, promoting odd survivors
    /// to the next round until a single hash remains.
    private static byte[] refFoldUp(final HashAlgorithm algorithm, final List<byte[]> leaves) {
        List<byte[]> current = new ArrayList<>(leaves);
        while (current.size() > 1) {
            final List<byte[]> next = new ArrayList<>();
            for (int i = 0; i < current.size(); i += 2) {
                if (i + 1 < current.size()) {
                    next.add(refNode(algorithm, current.get(i), current.get(i + 1)));
                } else {
                    next.add(current.get(i));
                }
            }
            current = next;
        }
        return current.get(0);
    }

    /// The reference empty tree hash: the leaf hash of empty data equals hash(0x00).
    private static byte[] refEmptyTreeHash(final HashAlgorithm algorithm) {
        return refLeaf(algorithm, new byte[0]);
    }

    /// Reference leaf hash of a serialized block item.
    private static byte[] refItemLeaf(final HashAlgorithm algorithm, final BlockItemUnparsed item) {
        return refLeaf(algorithm, BlockItemUnparsed.PROTOBUF.toBytes(item).toByteArray());
    }

    private static byte[] refLeaf(final HashAlgorithm algorithm, final byte[] data) {
        return refHash(algorithm, new byte[] {0x00}, data);
    }

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
