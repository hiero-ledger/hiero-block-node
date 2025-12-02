// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.model.hashing;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link InMemoryTreeHasher} verifying it produces identical root hashes to {@link StreamingHasher} and
 * correctly supports Merkle path generation.
 */
class InMemoryTreeHasherTest {

    @TempDir
    Path tempDir;

    @Test
    void testSingleLeafMatchesStreamingHasher() {
        StreamingHasher streaming = new StreamingHasher();
        InMemoryTreeHasher inMemory = new InMemoryTreeHasher();

        byte[] data = "Hello, World!".getBytes();
        streaming.addLeaf(data);
        inMemory.addLeaf(data);

        assertArrayEquals(streaming.computeRootHash(), inMemory.computeRootHash());
        assertEquals(1, inMemory.leafCount());
    }

    @Test
    void testTwoLeavesMatchesStreamingHasher() {
        StreamingHasher streaming = new StreamingHasher();
        InMemoryTreeHasher inMemory = new InMemoryTreeHasher();

        streaming.addLeaf("Leaf 1".getBytes());
        streaming.addLeaf("Leaf 2".getBytes());
        inMemory.addLeaf("Leaf 1".getBytes());
        inMemory.addLeaf("Leaf 2".getBytes());

        assertArrayEquals(streaming.computeRootHash(), inMemory.computeRootHash());
        assertEquals(2, inMemory.leafCount());
    }

    @Test
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
                    streaming.computeRootHash(), inMemory.computeRootHash(), "Mismatch for " + numLeaves + " leaves");
            assertEquals(numLeaves, inMemory.leafCount());
        }
    }

    @Test
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
                    streaming.computeRootHash(), inMemory.computeRootHash(), "Mismatch for " + numLeaves + " leaves");
            assertEquals(numLeaves, inMemory.leafCount());
        }
    }

    @Test
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
                    "Mismatch for trial " + trial + " with " + numLeaves + " leaves");
        }
    }

    @Test
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

        assertArrayEquals(originalRoot, loaded.computeRootHash());
        assertEquals(original.leafCount(), loaded.leafCount());
        assertEquals(original.levelCount(), loaded.levelCount());
    }

    @Test
    void testEmptyTreeThrows() {
        InMemoryTreeHasher hasher = new InMemoryTreeHasher();
        assertThrows(IllegalStateException.class, hasher::computeRootHash);
    }

    @Test
    void testGetHashAtLevel() {
        InMemoryTreeHasher hasher = new InMemoryTreeHasher();
        hasher.addLeaf("Leaf 0".getBytes());
        hasher.addLeaf("Leaf 1".getBytes());
        hasher.addLeaf("Leaf 2".getBytes());
        hasher.addLeaf("Leaf 3".getBytes());

        // Force rebuild
        hasher.computeRootHash();

        // Level 0 should have 4 leaves
        assertEquals(4, hasher.hashCountAtLevel(0));

        // Level 1 should have 2 internal nodes
        assertEquals(2, hasher.hashCountAtLevel(1));

        // Level 2 (root level) should have 1 node
        assertEquals(1, hasher.hashCountAtLevel(2));
    }

    @Test
    void testGetMerklePathOutOfRange() {
        InMemoryTreeHasher hasher = new InMemoryTreeHasher();
        hasher.addLeaf("Leaf 0".getBytes());

        assertThrows(IllegalArgumentException.class, () -> hasher.getMerklePath(-1));
        assertThrows(IllegalArgumentException.class, () -> hasher.getMerklePath(1));
    }

    @Test
    void testMerklePathForTwoLeaves() {
        InMemoryTreeHasher hasher = new InMemoryTreeHasher();
        hasher.addLeaf("Leaf 0".getBytes());
        hasher.addLeaf("Leaf 1".getBytes());

        byte[] root = hasher.computeRootHash();

        // Path for leaf 0
        List<InMemoryTreeHasher.MerklePathEntry> path0 = hasher.getMerklePath(0);
        assertEquals(1, path0.size());
        assertFalse(path0.get(0).siblingIsLeft()); // Sibling is on the right

        // Path for leaf 1
        List<InMemoryTreeHasher.MerklePathEntry> path1 = hasher.getMerklePath(1);
        assertEquals(1, path1.size());
        assertTrue(path1.get(0).siblingIsLeft()); // Sibling is on the left

        // Verify paths have sibling hashes
        assertArrayEquals(hasher.getHash(0, 1), path0.get(0).siblingHash());
        assertArrayEquals(hasher.getHash(0, 0), path1.get(0).siblingHash());
    }

    @Test
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
            assertEquals(2, path.size(), "Leaf " + i + " should have path length 2");
        }

        // Leaf 0's path should be: sibling leaf 1 (right), sibling internal [2,3] (right)
        List<InMemoryTreeHasher.MerklePathEntry> path0 = hasher.getMerklePath(0);
        assertArrayEquals(hasher.getHash(0, 1), path0.get(0).siblingHash());
        assertFalse(path0.get(0).siblingIsLeft());
        assertArrayEquals(hasher.getHash(1, 1), path0.get(1).siblingHash());
        assertFalse(path0.get(1).siblingIsLeft());
    }

    @Test
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
        assertArrayEquals(streaming.computeRootHash(), inMemory.computeRootHash());

        // Add more leaves
        for (int i = 5; i < 10; i++) {
            byte[] data = ("Leaf " + i).getBytes();
            streaming.addLeaf(data);
            inMemory.addLeaf(data);
        }

        // Final root should still match
        assertArrayEquals(streaming.computeRootHash(), inMemory.computeRootHash());
    }
}
