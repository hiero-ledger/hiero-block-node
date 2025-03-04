package com.hedera.block.common.hasher;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import org.junit.jupiter.api.Test;

public class MerkleProofCalculatorTest {

    @Test
    public void testCalculateMerkleProof() {
        // Build a simple Merkle tree:
        // Level 0 (leaves): [L0, L1, L2, L3]
        // Level 1 (internal nodes): [H0_1, H2_3]
        // Level 2 (root): [H0_3]
        //
        // In this precomputed tree, assume:
        // - H0_1 is the hash of L0 and L1.
        // - H2_3 is the hash of L2 and L3.
        // - H0_3 is the hash of H0_1 and H2_3.
        // For our test, we use literal byte arrays with the given names.

        Bytes L0 = Bytes.wrap("L0".getBytes(StandardCharsets.UTF_8));
        Bytes L1 = Bytes.wrap("L1".getBytes(StandardCharsets.UTF_8));
        Bytes L2 = Bytes.wrap("L2".getBytes(StandardCharsets.UTF_8));
        Bytes L3 = Bytes.wrap("L3".getBytes(StandardCharsets.UTF_8));
        Bytes H0_1 = Bytes.wrap("H0_1".getBytes(StandardCharsets.UTF_8));
        Bytes H2_3 = Bytes.wrap("H2_3".getBytes(StandardCharsets.UTF_8));
        Bytes H0_3 = Bytes.wrap("H0_3".getBytes(StandardCharsets.UTF_8));

        List<List<Bytes>> completeMerkleTree = new ArrayList<>();

        // Level 0: Leaves
        completeMerkleTree.add(Arrays.asList(L0, L1, L2, L3));
        // Level 1: Intermediate nodes
        completeMerkleTree.add(Arrays.asList(H0_1, H2_3));
        // Level 2: Root
        completeMerkleTree.add(Arrays.asList(H0_3));

        MerkleProofCalculator calculator = new MerkleProofCalculator();

        // For this test, we choose leaf index 2 (which corresponds to L2).
        // Expected proof:
        //   - At Level 0: sibling of index 2 is at index 3 (L3).
        //   - At Level 1: then parent's index becomes 2/2 = 1; its sibling is at index 0 (H0_1).
        // So the expected proof is [L3, H0_1].
        int leafIndex = 2;
        List<MerkleProofElement> proof = calculator.calculateMerkleProof(completeMerkleTree, leafIndex);

        List<MerkleProofElement> expectedProof = new ArrayList<>();
        //expectedProof.add(L3);
        expectedProof.add(new MerkleProofElement(L3, false));

        //expectedProof.add(H0_1);
        expectedProof.add(new MerkleProofElement(H0_1, true));

        // Check that the proof has the expected size.
        assertEquals(expectedProof.size(), proof.size(), "Proof size does not match expected size.");

        // Verify that each element in the proof matches the expected value.
        for (int i = 0; i < expectedProof.size(); i++) {
            MerkleProofElement expectedElement = expectedProof.get(i);
            MerkleProofElement actualElement = proof.get(i);

            assertArrayEquals(expectedElement.hash().toByteArray(), actualElement.hash().toByteArray(), "Hashes do not match.");
            assertEquals(expectedElement.isLeft(), actualElement.isLeft(), "isLeft does not match.");
        }
    }

    @Test
    public void testIndexIfExist() {
        // Prepare some sample leaf hashes.
        byte[] leaf1 = "leaf1".getBytes(StandardCharsets.UTF_8);
        byte[] leaf2 = "leaf2".getBytes(StandardCharsets.UTF_8);
        byte[] leaf3 = "leaf3".getBytes(StandardCharsets.UTF_8);

        List<byte[]> leafHashes = new ArrayList<>();
        leafHashes.add(leaf1);
        leafHashes.add(leaf2);
        leafHashes.add(leaf3);

        MerkleProofCalculator calculator = new MerkleProofCalculator();

        // Test finding an existing element.
        int foundIndex = calculator.indexIfExist(leafHashes, leaf2);
        assertEquals(1, foundIndex, "The index of leaf2 should be 1.");

        // Test with an element that does not exist.
        byte[] nonExistingLeaf = "nonExisting".getBytes(StandardCharsets.UTF_8);
        int notFoundIndex = calculator.indexIfExist(leafHashes, nonExistingLeaf);
        assertEquals(-1, notFoundIndex, "A non-existing leaf should return an index of -1.");
    }
}
