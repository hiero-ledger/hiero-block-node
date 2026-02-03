// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.model.hashing;

import static org.hiero.block.tools.blocks.model.hashing.HashingUtils.EMPTY_TREE_HASH;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for {@link InMemoryTreeHasher}.
 *
 * <p>These tests verify:
 * <ul>
 *   <li>Root hash equivalence with {@link StreamingHasher} for various tree sizes</li>
 *   <li>Correct tree structure (levels, hash counts)</li>
 *   <li>Merkle path generation for proof verification</li>
 *   <li>State persistence (save/load) functionality</li>
 *   <li>Edge cases and error handling</li>
 * </ul>
 *
 * <p>The InMemoryTreeHasher maintains the full tree structure in memory, enabling Merkle path
 * generation while producing identical root hashes to the more memory-efficient StreamingHasher.
 */
@DisplayName("InMemoryTreeHasher Tests")
class InMemoryTreeHasherTest {

    /** Temporary directory for save/load tests. */
    @TempDir
    Path tempDir;

    // ========== Root Hash Equivalence Tests ==========

    /**
     * Verifies that a single leaf produces the same root hash in both hasher implementations.
     */
    @Test
    @DisplayName("Single leaf should produce same root hash as StreamingHasher")
    void testSingleLeafMatchesStreamingHasher() {
        StreamingHasher streaming = new StreamingHasher();
        InMemoryTreeHasher inMemory = new InMemoryTreeHasher();

        byte[] data = "Hello, World!".getBytes();
        streaming.addLeaf(data);
        inMemory.addLeaf(data);

        assertArrayEquals(
                streaming.computeRootHash(),
                inMemory.computeRootHash(),
                "Single leaf root hash should match StreamingHasher");
        assertEquals(1, inMemory.leafCount(), "Leaf count should be 1 after adding one leaf");
    }

    /**
     * Verifies that two leaves produce the same root hash in both hasher implementations.
     */
    @Test
    @DisplayName("Two leaves should produce same root hash as StreamingHasher")
    void testTwoLeavesMatchesStreamingHasher() {
        StreamingHasher streaming = new StreamingHasher();
        InMemoryTreeHasher inMemory = new InMemoryTreeHasher();

        streaming.addLeaf("Leaf 1".getBytes());
        streaming.addLeaf("Leaf 2".getBytes());
        inMemory.addLeaf("Leaf 1".getBytes());
        inMemory.addLeaf("Leaf 2".getBytes());

        assertArrayEquals(
                streaming.computeRootHash(),
                inMemory.computeRootHash(),
                "Two-leaf tree root hash should match StreamingHasher");
        assertEquals(2, inMemory.leafCount(), "Leaf count should be 2 after adding two leaves");
    }

    /**
     * Verifies that power-of-2 leaf counts (2, 4, 8, 16, 32, 64) produce matching root hashes.
     * Power-of-2 trees form perfect binary trees.
     */
    @Test
    @DisplayName("Power-of-2 leaf counts should produce same root hash as StreamingHasher")
    void testPowerOfTwoLeavesMatchesStreamingHasher() {
        for (int numLeaves : new int[] {2, 4, 8, 16, 32, 64}) {
            StreamingHasher streaming = new StreamingHasher();
            InMemoryTreeHasher inMemory = new InMemoryTreeHasher();

            for (int i = 0; i < numLeaves; i++) {
                byte[] data = ("Leaf " + i).getBytes();
                streaming.addLeaf(data);
                inMemory.addLeaf(data);
            }

            assertArrayEquals(
                    streaming.computeRootHash(),
                    inMemory.computeRootHash(),
                    "Power-of-2 tree (" + numLeaves + " leaves) root hash should match StreamingHasher");
            assertEquals(numLeaves, inMemory.leafCount(), "Leaf count should be " + numLeaves + " for power-of-2 tree");
        }
    }

    /**
     * Verifies that non-power-of-2 leaf counts produce matching root hashes.
     * These trees require the right-to-left folding algorithm to handle incomplete levels.
     */
    @Test
    @DisplayName("Non-power-of-2 leaf counts should produce same root hash as StreamingHasher")
    void testNonPowerOfTwoLeavesMatchesStreamingHasher() {
        for (int numLeaves : new int[] {3, 5, 7, 9, 10, 15, 17, 31, 33, 63, 65, 100}) {
            StreamingHasher streaming = new StreamingHasher();
            InMemoryTreeHasher inMemory = new InMemoryTreeHasher();

            for (int i = 0; i < numLeaves; i++) {
                byte[] data = ("Leaf " + i).getBytes();
                streaming.addLeaf(data);
                inMemory.addLeaf(data);
            }

            assertArrayEquals(
                    streaming.computeRootHash(),
                    inMemory.computeRootHash(),
                    "Non-power-of-2 tree (" + numLeaves + " leaves) root hash should match StreamingHasher");
            assertEquals(
                    numLeaves, inMemory.leafCount(), "Leaf count should be " + numLeaves + " for non-power-of-2 tree");
        }
    }

    /**
     * Verifies that random data with varying leaf counts produces matching root hashes.
     * Uses a fixed seed for reproducibility.
     */
    @Test
    @DisplayName("Random data with varying leaf counts should produce same root hash as StreamingHasher")
    void testRandomDataMatchesStreamingHasher() {
        Random random = new Random(42);
        for (int trial = 0; trial < 10; trial++) {
            int numLeaves = random.nextInt(100) + 1;
            StreamingHasher streaming = new StreamingHasher();
            InMemoryTreeHasher inMemory = new InMemoryTreeHasher();

            for (int i = 0; i < numLeaves; i++) {
                byte[] data = new byte[random.nextInt(1000) + 1];
                random.nextBytes(data);
                streaming.addLeaf(data);
                inMemory.addLeaf(data);
            }

            assertArrayEquals(
                    streaming.computeRootHash(),
                    inMemory.computeRootHash(),
                    "Random data trial " + trial + " (" + numLeaves
                            + " leaves) root hash should match StreamingHasher");
        }
    }

    // ========== Persistence Tests ==========

    /**
     * Verifies that tree state can be saved to a file and loaded back correctly.
     */
    @Test
    @DisplayName("Saved and loaded tree should preserve root hash, leaf count, and level count")
    void testSaveAndLoad() throws Exception {
        InMemoryTreeHasher original = new InMemoryTreeHasher();
        for (int i = 0; i < 10; i++) {
            original.addLeaf(("Leaf " + i).getBytes());
        }
        byte[] originalRoot = original.computeRootHash();

        Path saveFile = tempDir.resolve("hasher.bin");
        original.save(saveFile);

        InMemoryTreeHasher loaded = new InMemoryTreeHasher();
        loaded.load(saveFile);

        assertArrayEquals(
                originalRoot, loaded.computeRootHash(), "Loaded tree should produce same root hash as original");
        assertEquals(original.leafCount(), loaded.leafCount(), "Loaded tree should have same leaf count as original");
        assertEquals(
                original.levelCount(), loaded.levelCount(), "Loaded tree should have same level count as original");
    }

    // ========== Empty Tree Tests ==========

    /**
     * Verifies that computing root hash on an empty tree returns EMPTY_TREE_HASH.
     */
    @Test
    @DisplayName("Empty tree should return EMPTY_TREE_HASH when computing root hash")
    void testEmptyTreeReturnsEmptyTreeHash() {
        InMemoryTreeHasher hasher = new InMemoryTreeHasher();
        byte[] rootHash = hasher.computeRootHash();

        assertArrayEquals(
                EMPTY_TREE_HASH,
                rootHash,
                "Empty tree root hash should equal EMPTY_TREE_HASH (sha384Hash(new byte[]{0x00}))");
        assertEquals(48, rootHash.length, "Empty tree hash should be 48 bytes (SHA-384)");
    }

    /**
     * Verifies that the empty tree hash matches between InMemoryTreeHasher and StreamingHasher.
     */
    @Test
    @DisplayName("Empty tree hash should match between InMemoryTreeHasher and StreamingHasher")
    void testEmptyTreeHashMatchesStreamingHasher() {
        InMemoryTreeHasher inMemory = new InMemoryTreeHasher();
        StreamingHasher streaming = new StreamingHasher();

        assertArrayEquals(
                streaming.computeRootHash(),
                inMemory.computeRootHash(),
                "Empty tree root hash should match between implementations");
    }

    /**
     * Verifies that empty tree hash does not modify internal state and can continue adding leaves.
     */
    @Test
    @DisplayName("Empty tree hash computation should not prevent adding leaves afterward")
    void testEmptyTreeHashThenAddLeaves() {
        InMemoryTreeHasher inMemory = new InMemoryTreeHasher();
        StreamingHasher streaming = new StreamingHasher();

        // Compute empty tree hash first
        byte[] emptyHash = inMemory.computeRootHash();
        assertArrayEquals(EMPTY_TREE_HASH, emptyHash, "Initial empty tree hash should be EMPTY_TREE_HASH");

        // Add leaves afterward
        inMemory.addLeaf("Leaf 1".getBytes());
        streaming.addLeaf("Leaf 1".getBytes());

        assertArrayEquals(
                streaming.computeRootHash(),
                inMemory.computeRootHash(),
                "After adding leaf, root hash should match StreamingHasher");

        // Verify no longer returns empty tree hash
        assertFalse(
                java.util.Arrays.equals(EMPTY_TREE_HASH, inMemory.computeRootHash()),
                "After adding leaf, root hash should differ from EMPTY_TREE_HASH");
    }

    // ========== Tree Structure Tests ==========

    /**
     * Verifies that a 4-leaf tree has the correct structure:
     * <ul>
     *   <li>Level 0: 4 leaf hashes</li>
     *   <li>Level 1: 2 internal node hashes</li>
     *   <li>Level 2: 1 root hash</li>
     * </ul>
     */
    @Test
    @DisplayName("4-leaf tree should have correct level structure (4, 2, 1 hashes)")
    void testGetHashAtLevel() {
        InMemoryTreeHasher hasher = new InMemoryTreeHasher();
        hasher.addLeaf("Leaf 0".getBytes());
        hasher.addLeaf("Leaf 1".getBytes());
        hasher.addLeaf("Leaf 2".getBytes());
        hasher.addLeaf("Leaf 3".getBytes());

        hasher.computeRootHash();

        assertEquals(4, hasher.hashCountAtLevel(0), "Level 0 (leaves) should have 4 hashes for 4-leaf tree");
        assertEquals(
                2, hasher.hashCountAtLevel(1), "Level 1 (first internal level) should have 2 hashes for 4-leaf tree");
        assertEquals(1, hasher.hashCountAtLevel(2), "Level 2 (root level) should have 1 hash for 4-leaf tree");
    }

    // ========== Merkle Path Tests ==========

    /**
     * Verifies that getMerklePath throws for invalid leaf indices.
     */
    @Test
    @DisplayName("getMerklePath should throw IllegalArgumentException for out-of-range indices")
    void testGetMerklePathOutOfRange() {
        InMemoryTreeHasher hasher = new InMemoryTreeHasher();
        hasher.addLeaf("Leaf 0".getBytes());

        assertThrows(
                IllegalArgumentException.class,
                () -> hasher.getMerklePath(-1),
                "getMerklePath(-1) should throw IllegalArgumentException");
        assertThrows(
                IllegalArgumentException.class,
                () -> hasher.getMerklePath(1),
                "getMerklePath(1) should throw IllegalArgumentException when only 1 leaf exists");
    }

    /**
     * Verifies Merkle path structure for a 2-leaf tree.
     *
     * <p>For a 2-leaf tree:
     * <ul>
     *   <li>Leaf 0's path: sibling is Leaf 1 (on the right)</li>
     *   <li>Leaf 1's path: sibling is Leaf 0 (on the left)</li>
     * </ul>
     */
    @Test
    @DisplayName("2-leaf tree Merkle paths should have correct sibling positions")
    void testMerklePathForTwoLeaves() {
        InMemoryTreeHasher hasher = new InMemoryTreeHasher();
        hasher.addLeaf("Leaf 0".getBytes());
        hasher.addLeaf("Leaf 1".getBytes());

        hasher.computeRootHash();

        // Path for leaf 0
        List<InMemoryTreeHasher.MerklePathEntry> path0 = hasher.getMerklePath(0);
        assertEquals(1, path0.size(), "Leaf 0 path should have 1 entry in 2-leaf tree");
        assertFalse(path0.getFirst().siblingIsLeft(), "Leaf 0's sibling (Leaf 1) should be on the right");

        // Path for leaf 1
        List<InMemoryTreeHasher.MerklePathEntry> path1 = hasher.getMerklePath(1);
        assertEquals(1, path1.size(), "Leaf 1 path should have 1 entry in 2-leaf tree");
        assertTrue(path1.getFirst().siblingIsLeft(), "Leaf 1's sibling (Leaf 0) should be on the left");

        // Verify sibling hashes match the stored leaf hashes
        assertArrayEquals(
                hasher.getHash(0, 1),
                path0.getFirst().siblingHash(),
                "Leaf 0's sibling hash should equal Leaf 1's hash");
        assertArrayEquals(
                hasher.getHash(0, 0),
                path1.getFirst().siblingHash(),
                "Leaf 1's sibling hash should equal Leaf 0's hash");
    }

    /**
     * Verifies Merkle path structure for a 4-leaf tree (perfect binary tree).
     *
     * <p>Each leaf should have a path of length 2 (log2(4) = 2).
     */
    @Test
    @DisplayName("4-leaf tree Merkle paths should have length 2 with correct structure")
    void testMerklePathForFourLeaves() {
        InMemoryTreeHasher hasher = new InMemoryTreeHasher();
        hasher.addLeaf("Leaf 0".getBytes());
        hasher.addLeaf("Leaf 1".getBytes());
        hasher.addLeaf("Leaf 2".getBytes());
        hasher.addLeaf("Leaf 3".getBytes());

        hasher.computeRootHash();

        // Each leaf should have a path of length 2 (log2(4) = 2)
        for (int i = 0; i < 4; i++) {
            List<InMemoryTreeHasher.MerklePathEntry> path = hasher.getMerklePath(i);
            assertEquals(2, path.size(), "Leaf " + i + " path should have length 2 in 4-leaf tree (log2(4) = 2)");
        }

        // Verify Leaf 0's path structure:
        // - First entry: sibling is Leaf 1 (right)
        // - Second entry: sibling is internal node [2,3] (right)
        List<InMemoryTreeHasher.MerklePathEntry> path0 = hasher.getMerklePath(0);
        assertArrayEquals(
                hasher.getHash(0, 1), path0.get(0).siblingHash(), "Leaf 0's first sibling should be Leaf 1's hash");
        assertFalse(path0.get(0).siblingIsLeft(), "Leaf 0's first sibling (Leaf 1) should be on the right");
        assertArrayEquals(
                hasher.getHash(1, 1),
                path0.get(1).siblingHash(),
                "Leaf 0's second sibling should be internal node [2,3]");
        assertFalse(path0.get(1).siblingIsLeft(), "Leaf 0's second sibling (internal [2,3]) should be on the right");
    }

    // ========== State Continuity Tests ==========

    /**
     * Verifies that computing an intermediate root hash does not prevent adding more leaves,
     * and that the final tree still matches the StreamingHasher result.
     */
    @Test
    @DisplayName("Computing intermediate root hash should not break subsequent leaf additions")
    void testIntermediateRootHashDoesNotBreakTree() {
        StreamingHasher streaming = new StreamingHasher();
        InMemoryTreeHasher inMemory = new InMemoryTreeHasher();

        // Add some leaves, compute root, add more leaves
        for (int i = 0; i < 5; i++) {
            byte[] data = ("Leaf " + i).getBytes();
            streaming.addLeaf(data);
            inMemory.addLeaf(data);
        }

        // Compute intermediate root
        assertArrayEquals(
                streaming.computeRootHash(),
                inMemory.computeRootHash(),
                "Intermediate root hash (5 leaves) should match StreamingHasher");

        // Add more leaves
        for (int i = 5; i < 10; i++) {
            byte[] data = ("Leaf " + i).getBytes();
            streaming.addLeaf(data);
            inMemory.addLeaf(data);
        }

        // Final root should still match
        assertArrayEquals(
                streaming.computeRootHash(),
                inMemory.computeRootHash(),
                "Final root hash (10 leaves) should match StreamingHasher after intermediate computation");
    }
}
