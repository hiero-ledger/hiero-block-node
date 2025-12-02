// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.model.hashing;

import static org.hiero.block.tools.blocks.model.hashing.Hasher.INTERNAL_NODE_PREFIX;
import static org.hiero.block.tools.blocks.model.hashing.Hasher.LEAF_PREFIX;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import org.hiero.block.tools.utils.Sha384;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for {@link StreamingHasher}.
 *
 * <p>These tests verify:
 * <ul>
 *   <li>Correct Merkle tree root hash computation using SHA-384</li>
 *   <li>Proper leaf counting and intermediate state management</li>
 *   <li>State persistence (save/load) functionality</li>
 *   <li>Deterministic hashing behavior</li>
 *   <li>Edge cases like empty data and large leaves</li>
 * </ul>
 */
@DisplayName("StreamingHasher Tests")
class StreamingHasherTest {

    /** Temporary directory for save/load tests. */
    @TempDir
    Path tempDir;

    /**
     * Verifies that the StreamingHasher produces the correct Merkle root hash for a 7-leaf tree by manually computing
     * the expected hash and comparing it to the StreamingHasher result.
     *
     * <p>Tree structure for 7 leaves:
     * <pre>
     *                           Root
     *                    hash(C, E)
     *                   /          \
     *                  C            E
     *             hash(A,B)    hash(D,L6)
     *             /      \       /     \
     *            A        B     D      L6
     *        hash(L0,L1) hash(L2,L3) hash(L4,L5)
     *         /   \       /   \       /   \
     *        L0   L1    L2   L3     L4   L5
     * </pre>
     */
    @Test
    @DisplayName("Manual tree computation for 7 leaves should match StreamingHasher result")
    void sampleTreeTest() {
        MessageDigest d = Sha384.sha384Digest();
        List<byte[]> leaves = new ArrayList<>();
        for (int i = 0; i < 7; i++) {
            leaves.add(("leaf_" + i).getBytes(StandardCharsets.UTF_8));
        }

        // Manually compute tree root
        final byte[][] leaveHashes = leaves.stream().map(l -> hashLeaf(d, l)).toArray(byte[][]::new);
        final byte[] nodeA = hashInternalNode(d, leaveHashes[0], leaveHashes[1]);
        final byte[] nodeB = hashInternalNode(d, leaveHashes[2], leaveHashes[3]);
        final byte[] nodeC = hashInternalNode(d, nodeA, nodeB);
        final byte[] nodeD = hashInternalNode(d, leaveHashes[4], leaveHashes[5]);
        final byte[] nodeE = hashInternalNode(d, nodeD, leaveHashes[6]);
        final byte[] manualRoot = hashInternalNode(d, nodeC, nodeE);
        final String manualRootHex = HexFormat.of().formatHex(manualRoot);

        // Compute using StreamingHasher
        StreamingHasher streamingHasher = new StreamingHasher();
        for (byte[] leaf : leaves) {
            streamingHasher.addLeaf(leaf);
        }
        byte[] rootHash = streamingHasher.computeRootHash();
        final String rootHashHex = HexFormat.of().formatHex(rootHash);

        assertEquals(
                manualRootHex,
                rootHashHex,
                "StreamingHasher root hash should match manually computed hash for 7-leaf tree");
    }

    /**
     * Verifies that a single leaf produces a valid SHA-384 hash (48 bytes) as the root.
     */
    @Test
    @DisplayName("Single leaf should produce valid 48-byte SHA-384 root hash")
    void testSingleLeaf() {
        StreamingHasher hasher = new StreamingHasher();
        byte[] data = "single leaf".getBytes(StandardCharsets.UTF_8);
        hasher.addLeaf(data);

        assertEquals(1, hasher.leafCount(), "Leaf count should be 1 after adding one leaf");

        byte[] root = hasher.computeRootHash();
        assertNotNull(root, "Root hash should not be null");
        assertEquals(48, root.length, "SHA-384 root hash should be 48 bytes");
    }

    /**
     * Verifies that leafCount() correctly tracks the number of leaves added.
     */
    @Test
    @DisplayName("Leaf count should increment correctly as leaves are added")
    void testLeafCount() {
        StreamingHasher hasher = new StreamingHasher();
        assertEquals(0, hasher.leafCount(), "New hasher should have zero leaves");

        for (int i = 1; i <= 10; i++) {
            hasher.addLeaf(("leaf " + i).getBytes(StandardCharsets.UTF_8));
            assertEquals(i, hasher.leafCount(), "Leaf count should be " + i + " after adding " + i + " leaves");
        }
    }

    /**
     * Verifies that the intermediate hashing state follows the expected pattern:
     * <ul>
     *   <li>1 leaf → 1 hash in state</li>
     *   <li>2 leaves → 1 hash (combined)</li>
     *   <li>3 leaves → 2 hashes</li>
     *   <li>4 leaves → 1 hash (all combined)</li>
     *   <li>5 leaves → 2 hashes</li>
     * </ul>
     */
    @Test
    @DisplayName("Intermediate hashing state should follow binary combination pattern")
    void testIntermediateHashingState() {
        StreamingHasher hasher = new StreamingHasher();

        hasher.addLeaf("leaf 0".getBytes(StandardCharsets.UTF_8));
        assertEquals(
                1, hasher.intermediateHashingState().size(), "After 1 leaf, intermediate state should have 1 hash");

        hasher.addLeaf("leaf 1".getBytes(StandardCharsets.UTF_8));
        assertEquals(
                1,
                hasher.intermediateHashingState().size(),
                "After 2 leaves, intermediate state should have 1 combined hash");

        hasher.addLeaf("leaf 2".getBytes(StandardCharsets.UTF_8));
        assertEquals(
                2, hasher.intermediateHashingState().size(), "After 3 leaves, intermediate state should have 2 hashes");

        hasher.addLeaf("leaf 3".getBytes(StandardCharsets.UTF_8));
        assertEquals(
                1,
                hasher.intermediateHashingState().size(),
                "After 4 leaves (power of 2), intermediate state should have 1 hash");

        hasher.addLeaf("leaf 4".getBytes(StandardCharsets.UTF_8));
        assertEquals(
                2, hasher.intermediateHashingState().size(), "After 5 leaves, intermediate state should have 2 hashes");
    }

    /**
     * Verifies that creating a new StreamingHasher from intermediate state produces the same root hash.
     */
    @Test
    @DisplayName("Hasher created from intermediate state should produce same root hash")
    void testConstructorWithIntermediateState() {
        StreamingHasher original = new StreamingHasher();
        for (int i = 0; i < 5; i++) {
            original.addLeaf(("leaf " + i).getBytes(StandardCharsets.UTF_8));
        }
        List<byte[]> intermediateState = new ArrayList<>(original.intermediateHashingState());

        StreamingHasher resumed = new StreamingHasher(intermediateState);

        assertArrayEquals(
                original.computeRootHash(),
                resumed.computeRootHash(),
                "Hasher created from intermediate state should produce identical root hash");
    }

    /**
     * Verifies that the intermediate state hash list has the expected size for different leaf counts.
     */
    @Test
    @DisplayName("Intermediate state size should equal popcount of leaf count")
    void testIntermediateStateHashListContents() {
        StreamingHasher hasher = new StreamingHasher();

        for (int i = 0; i < 4; i++) {
            hasher.addLeaf(("leaf " + i).getBytes(StandardCharsets.UTF_8));
        }
        assertEquals(
                1,
                hasher.intermediateHashingState().size(),
                "After 4 leaves (2^2), intermediate state should have 1 hash");

        hasher.addLeaf("leaf 4".getBytes(StandardCharsets.UTF_8));
        assertEquals(
                2,
                hasher.intermediateHashingState().size(),
                "After 5 leaves (binary 101), intermediate state should have 2 hashes");
    }

    /**
     * Verifies that state can be saved to a file and loaded back, producing the same root hash.
     */
    @Test
    @DisplayName("Saved and loaded hasher should produce identical root hash")
    void testSaveAndLoad() throws Exception {
        StreamingHasher original = new StreamingHasher();
        for (int i = 0; i < 15; i++) {
            original.addLeaf(("leaf " + i).getBytes(StandardCharsets.UTF_8));
        }
        byte[] originalRoot = original.computeRootHash();
        long originalLeafCount = original.leafCount();

        Path saveFile = tempDir.resolve("streaming_hasher.bin");
        original.save(saveFile);

        StreamingHasher loaded = new StreamingHasher();
        loaded.load(saveFile);

        assertEquals(originalLeafCount, loaded.leafCount(), "Loaded hasher should have same leaf count as original");
        assertArrayEquals(
                originalRoot, loaded.computeRootHash(), "Loaded hasher should produce same root hash as original");
    }

    /**
     * Verifies that a hasher can be saved, loaded, and then continue adding leaves correctly.
     */
    @Test
    @DisplayName("Loaded hasher should continue building tree correctly with additional leaves")
    void testSaveLoadAndContinue() throws Exception {
        StreamingHasher original = new StreamingHasher();
        for (int i = 0; i < 10; i++) {
            original.addLeaf(("leaf " + i).getBytes(StandardCharsets.UTF_8));
        }

        Path saveFile = tempDir.resolve("streaming_hasher_continue.bin");
        original.save(saveFile);

        // Continue adding to original
        for (int i = 10; i < 20; i++) {
            original.addLeaf(("leaf " + i).getBytes(StandardCharsets.UTF_8));
        }
        byte[] originalFinalRoot = original.computeRootHash();

        // Load and continue adding same leaves
        StreamingHasher loaded = new StreamingHasher();
        loaded.load(saveFile);
        for (int i = 10; i < 20; i++) {
            loaded.addLeaf(("leaf " + i).getBytes(StandardCharsets.UTF_8));
        }

        assertArrayEquals(
                originalFinalRoot,
                loaded.computeRootHash(),
                "Loaded hasher should produce same final root hash after adding same leaves");
    }

    /**
     * Verifies that calling computeRootHash() multiple times produces the same result and does not modify internal
     * state.
     */
    @Test
    @DisplayName("computeRootHash() should be idempotent and not modify state")
    void testComputeRootHashDoesNotModifyState() {
        StreamingHasher hasher = new StreamingHasher();
        for (int i = 0; i < 5; i++) {
            hasher.addLeaf(("leaf " + i).getBytes(StandardCharsets.UTF_8));
        }

        byte[] root1 = hasher.computeRootHash();
        byte[] root2 = hasher.computeRootHash();
        byte[] root3 = hasher.computeRootHash();

        assertArrayEquals(root1, root2, "Multiple calls to computeRootHash() should return identical results");
        assertArrayEquals(root2, root3, "Multiple calls to computeRootHash() should return identical results");
        assertEquals(5, hasher.leafCount(), "Leaf count should remain unchanged after computeRootHash() calls");

        // Adding another leaf should change the root
        hasher.addLeaf("leaf 5".getBytes(StandardCharsets.UTF_8));
        byte[] newRoot = hasher.computeRootHash();
        assertFalse(Arrays.equals(root1, newRoot), "Root hash should change after adding a new leaf");
    }

    /**
     * Verifies that power-of-2 leaf counts result in exactly 1 hash in the intermediate state.
     */
    @Test
    @DisplayName("Power-of-2 leaf counts should result in single hash in intermediate state")
    void testPowerOfTwoLeaves() {
        for (int numLeaves : new int[] {2, 4, 8, 16, 32}) {
            StreamingHasher hasher = new StreamingHasher();
            for (int i = 0; i < numLeaves; i++) {
                hasher.addLeaf(("leaf " + i).getBytes(StandardCharsets.UTF_8));
            }

            assertEquals(numLeaves, hasher.leafCount(), "Leaf count should be " + numLeaves);
            assertEquals(
                    1,
                    hasher.intermediateHashingState().size(),
                    "For " + numLeaves + " leaves (power of 2), intermediate state should have exactly 1 hash");
            assertNotNull(hasher.computeRootHash(), "Root hash should not be null for " + numLeaves + " leaves");
        }
    }

    /**
     * Verifies that identical input always produces identical output (deterministic hashing).
     */
    @Test
    @DisplayName("Identical input should always produce identical root hash (deterministic)")
    void testDeterministicHashing() {
        StreamingHasher hasher1 = new StreamingHasher();
        for (int i = 0; i < 10; i++) {
            hasher1.addLeaf(("deterministic leaf " + i).getBytes(StandardCharsets.UTF_8));
        }
        byte[] root1 = hasher1.computeRootHash();

        StreamingHasher hasher2 = new StreamingHasher();
        for (int i = 0; i < 10; i++) {
            hasher2.addLeaf(("deterministic leaf " + i).getBytes(StandardCharsets.UTF_8));
        }
        byte[] root2 = hasher2.computeRootHash();

        assertArrayEquals(root1, root2, "Two hashers with identical input should produce identical root hashes");
    }

    /**
     * Verifies that different input produces different root hashes.
     */
    @Test
    @DisplayName("Different input should produce different root hashes")
    void testDifferentInputProducesDifferentHash() {
        StreamingHasher hasher1 = new StreamingHasher();
        hasher1.addLeaf("data A".getBytes(StandardCharsets.UTF_8));

        StreamingHasher hasher2 = new StreamingHasher();
        hasher2.addLeaf("data B".getBytes(StandardCharsets.UTF_8));

        assertFalse(
                Arrays.equals(hasher1.computeRootHash(), hasher2.computeRootHash()),
                "Different input data should produce different root hashes");
    }

    /**
     * Verifies that empty byte array as leaf data is handled correctly.
     */
    @Test
    @DisplayName("Empty leaf data should be handled correctly")
    void testEmptyLeafData() {
        StreamingHasher hasher = new StreamingHasher();
        hasher.addLeaf(new byte[0]);

        assertEquals(1, hasher.leafCount(), "Leaf count should be 1 after adding empty leaf");

        byte[] root = hasher.computeRootHash();
        assertNotNull(root, "Root hash should not be null for empty leaf");
        assertEquals(48, root.length, "Root hash should be 48 bytes (SHA-384) for empty leaf");
    }

    /**
     * Verifies that large leaf data (1 MB) is handled correctly.
     */
    @Test
    @DisplayName("Large leaf data (1 MB) should be handled correctly")
    void testLargeLeafData() {
        StreamingHasher hasher = new StreamingHasher();
        byte[] largeData = new byte[1024 * 1024]; // 1 MB
        Arrays.fill(largeData, (byte) 0x42);
        hasher.addLeaf(largeData);

        assertEquals(1, hasher.leafCount(), "Leaf count should be 1 after adding large leaf");

        byte[] root = hasher.computeRootHash();
        assertNotNull(root, "Root hash should not be null for large leaf");
        assertEquals(48, root.length, "Root hash should be 48 bytes (SHA-384) for large leaf");
    }

    /**
     * Computes the hash of a leaf node with the leaf prefix.
     *
     * @param digest the message digest to use
     * @param leafData the leaf data to hash
     * @return the hash of the leaf node
     */
    private static byte[] hashLeaf(final MessageDigest digest, final byte[] leafData) {
        digest.update(LEAF_PREFIX);
        return digest.digest(leafData);
    }

    /**
     * Computes the hash of an internal node by combining two child hashes with the internal node prefix.
     *
     * @param digest the message digest to use
     * @param firstChild the hash of the first (left) child
     * @param secondChild the hash of the second (right) child
     * @return the hash of the internal node
     */
    private static byte[] hashInternalNode(
            final MessageDigest digest, final byte[] firstChild, final byte[] secondChild) {
        digest.update(INTERNAL_NODE_PREFIX);
        digest.update(firstChild);
        return digest.digest(secondChild);
    }
}
