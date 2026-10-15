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
import org.hiero.block.internal.BlockItemUnparsed;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/// Tests for the [HashingUtilities] class. Expected values are produced by a local reference
/// implementation of the HIP-1424 block root tree ("Merkle Mountain Top"), independent of the
/// production code, so the two implementations must agree. The utilities compute with SHA-256, and
/// the reference spells out the JDK SHA-256 digest.
@DisplayName("Hashing Utilities Tests")
class HashingUtilitiesTest {
    /// Deterministic seed so failures are reproducible.
    private static final Random RANDOM = new Random(6431582L);
    /// The SHA-256 digest size, spelled out so the reference stays independent of the production enum.
    private static final int HASH_SIZE = 32;
    /// The SHA-384 digest size, the size of a hash computed by a network hashing with SHA-384.
    private static final int SHA384_HASH_SIZE = 48;

    /// Tests for [HashingUtilities#computeFinalBlockHash] with extension subtree roots.
    @Nested
    @DisplayName("Final Block Hash Extension Subtree Tests")
    class FinalBlockHashExtensionSubtreeTests {
        /// This test aims to assert that every presence pattern of the eight extension
        /// subtree leaves produces the block root hash computed by the reference
        /// implementation of the HIP-1424 tree. Patterns are supplied as bitmasks where bit N
        /// marks Extension N as present; absent slots contribute the empty tree hash.
        @ParameterizedTest
        @ValueSource(ints = {0b00000001, 0b00000010, 0b10000000, 0b00001001, 0b01100110, 0b11111111})
        @DisplayName("computeFinalBlockHash() extension presence patterns match reference")
        void testExtensionPresencePatternsMatchReference(final int presenceMask) throws NoSuchAlgorithmException {
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
        void testExtensionPresenceChangesRootHash() throws NoSuchAlgorithmException {
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
    /// contribute the empty tree hash at their fixed positions in the Mountain Top tree.
    /// The tree shape is fully stable; presence patterns produce distinct hashes that match
    /// the reference.
    @Nested
    @DisplayName("Pre-Defined Slot Presence Tests")
    class PreDefinedSlotPresenceTests {
        /// This test aims to assert that every presence pattern of the pre-defined positions
        /// 2-7 (bit 0 = position 2, ..., bit 5 = position 7) matches the reference. Covers: none
        /// present (WRB with only prev-block and all-blocks), only position 2, only position 7,
        /// positions 2 and 7, interior gap with rightmost present, all present.
        @ParameterizedTest
        @ValueSource(ints = {0b000000, 0b000001, 0b100000, 0b100001, 0b100010, 0b111111})
        @DisplayName("computeFinalBlockHash() pre-defined presence patterns match reference")
        void testPreDefinedPresencePatternsMatchReference(final int presenceMask) throws NoSuchAlgorithmException {
            final FixedTreeInputs inputs = randomInputsWithPreDefinedPresence(presenceMask);
            final StreamingTreeHasher[] noExtensions = emptyExtensionHashers();
            final Bytes actual = computeFinalHash(inputs, noExtensions);
            assertThat(actual).isEqualTo(referenceRootHash(inputs, noExtensions));
        }

        /// This test aims to assert that presence at any pre-defined position affects the root
        /// hash: an absent trace subtree (position 7) must produce a different root than a
        /// present one, otherwise the position would be indistinguishable and dropping it would
        /// go unnoticed.
        @Test
        @DisplayName("computeFinalBlockHash() position 7 presence changes the root hash")
        void testPositionSevenPresenceChangesRootHash() throws NoSuchAlgorithmException {
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
            assertThat(computeFinalHash(traceEmptyInputs, emptyExtensionHashers()))
                    .isNotEqualTo(computeFinalHash(tracePresentInputs, emptyExtensionHashers()));
        }

        /// This test aims to assert that a minimum-content block (only positions 0-1 populated,
        /// state root absent, all subtree hashers empty, e.g. an empty WRB) hashes correctly
        /// against the reference. All 16 slots still contribute; positions 2-15 are the empty
        /// tree hash.
        @Test
        @DisplayName("computeFinalBlockHash() only positions 0-1 populated matches reference")
        void testOnlyRequiredPositionsMatchesReference() throws NoSuchAlgorithmException {
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
    }

    /// Tests for the guards of [HashingUtilities#computeFinalBlockHash]: every leaf of the block
    /// root tree is one SHA-256 digest, so a value of another size is refused instead of being
    /// hashed into a root that belongs to no block.
    @Nested
    @DisplayName("Final Block Hash Guard Tests")
    class FinalBlockHashGuardTests {
        /// This test aims to assert that the root hash is one SHA-256 digest.
        @Test
        @DisplayName("computeFinalBlockHash() root hash is one SHA-256 digest")
        void testRootHashHasDigestSize() throws NoSuchAlgorithmException {
            final Bytes actual = computeFinalHash(randomInputs(), emptyExtensionHashers());
            assertThat(actual.length()).isEqualTo(HASH_SIZE);
        }

        /// This test aims to assert that a previous block hash shorter than one digest is
        /// refused up front. A short value would otherwise be refused by the streaming hasher
        /// with a less specific message, and could never be the root of the previous block.
        @Test
        @DisplayName("computeFinalBlockHash() short previousBlockHash throws")
        void testShortPreviousBlockHashRejected() throws NoSuchAlgorithmException {
            final FixedTreeInputs inputs = inputsWithPreviousBlockHash(Bytes.wrap(new byte[16]));
            assertThatThrownBy(() -> computeFinalHash(inputs, emptyExtensionHashers()))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        /// This test aims to assert that a previous block hash with the SHA-384 digest size is
        /// refused: such a block was hashed by a network computing with SHA-384, and folding it
        /// into this tree would silently truncate it.
        @Test
        @DisplayName("computeFinalBlockHash() previousBlockHash of the SHA-384 digest size throws")
        void testPreviousBlockHashOfOtherDigestSizeRejected() throws NoSuchAlgorithmException {
            final Bytes otherSize = Bytes.wrap(randomBytes(SHA384_HASH_SIZE));
            final FixedTreeInputs inputs = inputsWithPreviousBlockHash(otherSize);
            assertThatThrownBy(() -> computeFinalHash(inputs, emptyExtensionHashers()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("previousBlockHash");
        }

        /// This test aims to assert that a root of all previous block hashes with the SHA-384
        /// digest size is refused, for the same reason as the previous block hash.
        @Test
        @DisplayName("computeFinalBlockHash() rootHashOfAllPreviousBlockHashes of the SHA-384 digest size throws")
        void testRootOfAllPreviousBlockHashesOfOtherDigestSizeRejected() throws NoSuchAlgorithmException {
            final FixedTreeInputs valid = randomInputs();
            final FixedTreeInputs inputs = new FixedTreeInputs(
                    valid.timestamp(),
                    valid.previousBlockHash(),
                    Bytes.wrap(randomBytes(SHA384_HASH_SIZE)),
                    valid.startOfBlockStateRootHash(),
                    valid.inputTreeHasher(),
                    valid.outputTreeHasher(),
                    valid.consensusHeaderHasher(),
                    valid.stateChangesHasher(),
                    valid.traceDataHasher());
            assertThatThrownBy(() -> computeFinalHash(inputs, emptyExtensionHashers()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("rootHashOfAllPreviousBlockHashes");
        }

        /// This test aims to assert that a non-empty start of block state root with the SHA-384
        /// digest size is refused: the streaming hasher reads exactly one digest
        /// per leaf, so a longer value would otherwise be truncated without any error. An empty
        /// state root stays allowed and contributes the empty tree hash.
        @Test
        @DisplayName("computeFinalBlockHash() non-empty startOfBlockStateRootHash of the SHA-384 digest size throws")
        void testNonEmptyStateRootOfOtherDigestSizeRejected() throws NoSuchAlgorithmException {
            final FixedTreeInputs valid = randomInputs();
            final FixedTreeInputs inputs = new FixedTreeInputs(
                    valid.timestamp(),
                    valid.previousBlockHash(),
                    valid.rootOfAllPreviousBlockHashes(),
                    Bytes.wrap(randomBytes(SHA384_HASH_SIZE)),
                    valid.inputTreeHasher(),
                    valid.outputTreeHasher(),
                    valid.consensusHeaderHasher(),
                    valid.stateChangesHasher(),
                    valid.traceDataHasher());
            assertThatThrownBy(() -> computeFinalHash(inputs, emptyExtensionHashers()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("startOfBlockStateRootHash");
        }
    }

    /// Tests for the leaf and internal node hash helpers.
    @Nested
    @DisplayName("Leaf And Node Hash Tests")
    class LeafAndNodeHashTests {
        /// This test aims to assert that a leaf is hashed with the 0x00 domain separation prefix
        /// under SHA-256 and that the result is one SHA-256 digest.
        @Test
        @DisplayName("hashLeaf() is the prefixed digest of the leaf data")
        void testHashLeafMatchesReference() throws NoSuchAlgorithmException {
            final byte[] data = randomBytes(77);
            final byte[] actual = HashingUtilities.hashLeaf(data);
            assertThat(actual).isEqualTo(refLeaf(data));
            assertThat(actual).hasSize(HASH_SIZE);
        }

        /// This test aims to assert that two children are combined with the 0x02 domain
        /// separation prefix in left to right order under SHA-256.
        @Test
        @DisplayName("hashInternalNode() is the prefixed digest of left then right")
        void testHashInternalNodeMatchesReference() throws NoSuchAlgorithmException {
            final byte[] left = randomHash();
            final byte[] right = randomHash();
            final byte[] actual = HashingUtilities.hashInternalNode(left, right);
            assertThat(actual).isEqualTo(refNode(left, right));
            assertThat(actual).isNotEqualTo(refNode(right, left));
            assertThat(actual).hasSize(HASH_SIZE);
        }

        /// This test aims to assert that a single child is hashed with the 0x01 domain
        /// separation prefix under SHA-256, distinct from the leaf and the two children prefixes.
        @Test
        @DisplayName("hashInternalNodeSingleChild() is the prefixed digest of the child")
        void testHashInternalNodeSingleChildMatchesReference() throws NoSuchAlgorithmException {
            final byte[] child = randomHash();
            final byte[] actual = HashingUtilities.hashInternalNodeSingleChild(child);
            assertThat(actual).isEqualTo(refHash(new byte[] {0x01}, child));
            assertThat(actual).isNotEqualTo(refLeaf(child));
            assertThat(actual).hasSize(HASH_SIZE);
        }
    }

    /// Tests for the block item hashing helpers.
    @Nested
    @DisplayName("Block Item Hash Tests")
    class BlockItemHashTests {
        /// This test aims to assert that the hash of a block item is the prefixed leaf hash of
        /// its serialized bytes, returned in a buffer holding exactly one SHA-256 digest.
        @Test
        @DisplayName("getBlockItemHash() is the leaf hash of the serialized item")
        void testGetBlockItemHashIsLeafHash() throws NoSuchAlgorithmException {
            final BlockItemUnparsed item = BlockItemUnparsed.newBuilder()
                    .transactionResult(Bytes.wrap("transaction result"))
                    .build();
            final ByteBuffer actual = HashingUtilities.getBlockItemHash(item);
            assertThat(actual.remaining()).isEqualTo(HASH_SIZE);
            assertThat(remainingBytes(actual)).isEqualTo(refItemLeaf(item));
        }

        /// This test aims to assert that the hashes of a list of block items are routed into
        /// the buffer of their category, in item order, one SHA-256 digest per item:
        /// headers and transaction results to the output buffer, round and event headers to the
        /// consensus header buffer, signed transactions to the input buffer, state changes and
        /// trace data to their own buffers, while a block proof is not hashed at all.
        @Test
        @DisplayName("getBlockHashes() routes the item hashes by category")
        void testGetBlockHashesRoutesItemsByCategory() throws NoSuchAlgorithmException {
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
            final Hashes hashes = HashingUtilities.getBlockHashes(items);
            assertThat(remainingBytes(hashes.inputHashes()))
                    .isEqualTo(concat(refItemLeaf(firstTransaction), refItemLeaf(secondTransaction)));
            assertThat(remainingBytes(hashes.outputHashes()))
                    .isEqualTo(concat(refItemLeaf(header), refItemLeaf(transactionResult)));
            assertThat(remainingBytes(hashes.consensusHeaderHashes()))
                    .isEqualTo(concat(refItemLeaf(roundHeader), refItemLeaf(eventHeader)));
            assertThat(remainingBytes(hashes.stateChangesHashes())).isEqualTo(refItemLeaf(stateChanges));
            assertThat(remainingBytes(hashes.traceDataHashes())).isEqualTo(refItemLeaf(traceData));
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
        void testComputeV6SignedPayloadIsSha384OfVersionAndContents() throws NoSuchAlgorithmException {
            final byte[] contents = randomBytes(300);
            final byte[] actual = HashingUtilities.computeV6SignedPayload(Bytes.wrap(contents));
            final byte[] expected = refDigest("SHA-384", new byte[] {0, 0, 0, 6}, contents);
            assertThat(actual).isEqualTo(expected);
            assertThat(actual).hasSize(48);
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
    private static FixedTreeInputs randomInputs() throws NoSuchAlgorithmException {
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
    private static FixedTreeInputs randomInputsWithPreDefinedPresence(final int presenceMask)
            throws NoSuchAlgorithmException {
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

    /// Builds valid random inputs with the given previous block hash in place of the random one.
    private static FixedTreeInputs inputsWithPreviousBlockHash(final Bytes previousBlockHash)
            throws NoSuchAlgorithmException {
        final FixedTreeInputs valid = randomInputs();
        return new FixedTreeInputs(
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

    private static StreamingTreeHasher hasherWithLeaves(final int leafCount) throws NoSuchAlgorithmException {
        final NaiveStreamingTreeHasher hasher = new NaiveStreamingTreeHasher();
        for (int i = 0; i < leafCount; i++) {
            hasher.addLeaf(ByteBuffer.wrap(randomHash()));
        }
        return hasher;
    }

    /// Calls [HashingUtilities#computeFinalBlockHash], expanding the given hashers into the
    /// individual extension subtree hasher parameters. Absent slots in {@code extensionHashers}
    /// must be a fresh empty {@link NaiveStreamingTreeHasher} via {@link #emptyExtensionHashers}.
    private static Bytes computeFinalHash(final FixedTreeInputs inputs, final StreamingTreeHasher[] extensionHashers)
            throws NoSuchAlgorithmException {
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
    /// instances, representing "no extension subtrees present". Callers can
    /// replace slots with populated hashers to inject presence.
    private static StreamingTreeHasher[] emptyExtensionHashers() {
        final StreamingTreeHasher[] hashers = new StreamingTreeHasher[8];
        for (int i = 0; i < hashers.length; i++) {
            hashers[i] = new NaiveStreamingTreeHasher();
        }
        return hashers;
    }

    /// Builds a fresh {@link NaiveStreamingTreeHasher} with a single random leaf, so its
    /// {@code rootHash()} is non-empty and deterministically depends on the seeded content.
    private static StreamingTreeHasher hasherWithRandomLeaf() throws NoSuchAlgorithmException {
        final NaiveStreamingTreeHasher hasher = new NaiveStreamingTreeHasher();
        hasher.addLeaf(ByteBuffer.wrap(randomHash()));
        return hasher;
    }

    private static byte[] randomHash() {
        return randomBytes(HASH_SIZE);
    }

    private static byte[] randomBytes(final int length) {
        final byte[] bytes = new byte[length];
        RANDOM.nextBytes(bytes);
        return bytes;
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
    private static Bytes referenceRootHash(final FixedTreeInputs inputs, final StreamingTreeHasher[] extensionHashers)
            throws NoSuchAlgorithmException {
        final byte[] stateRoot = inputs.startOfBlockStateRootHash().length() == 0
                ? refEmptyTreeHash()
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
    private static byte[] refFoldUp(final List<byte[]> leaves) throws NoSuchAlgorithmException {
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

    /// The reference empty tree hash: the leaf hash of empty data equals hash(0x00).
    private static byte[] refEmptyTreeHash() throws NoSuchAlgorithmException {
        return refLeaf(new byte[0]);
    }

    /// Reference leaf hash of a serialized block item.
    private static byte[] refItemLeaf(final BlockItemUnparsed item) throws NoSuchAlgorithmException {
        return refLeaf(BlockItemUnparsed.PROTOBUF.toBytes(item).toByteArray());
    }

    private static byte[] refLeaf(final byte[] data) throws NoSuchAlgorithmException {
        return refHash(new byte[] {0x00}, data);
    }

    private static byte[] refNode(final byte[] left, final byte[] right) throws NoSuchAlgorithmException {
        return refHash(new byte[] {0x02}, left, right);
    }

    /// SHA-256 over the concatenation of the given parts, the JDK digest spelled out here so the
    /// reference stays independent of the production code.
    private static byte[] refHash(final byte[]... parts) throws NoSuchAlgorithmException {
        return refDigest("SHA-256", parts);
    }

    /// The named JDK digest over the concatenation of the given parts.
    private static byte[] refDigest(final String jcaName, final byte[]... parts) throws NoSuchAlgorithmException {
        final MessageDigest digest = MessageDigest.getInstance(jcaName);
        for (final byte[] part : parts) {
            digest.update(part);
        }
        return digest.digest();
    }
}
