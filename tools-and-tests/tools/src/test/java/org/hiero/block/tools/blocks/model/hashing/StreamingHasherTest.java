// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.model.hashing;

import static org.hiero.block.tools.blocks.model.hashing.Hasher.INTERNAL_NODE_PREFIX;
import static org.hiero.block.tools.blocks.model.hashing.Hasher.LEAF_PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.stream.Collectors;
import org.hiero.block.tools.utils.Sha384;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link StreamingHasher} verifying correct computation of Merkle tree root hashes.
 */
public class StreamingHasherTest {

    /**
     * Test computing the Merkle tree root hash for a sample tree with 7 leaves.
     * The test manually computes the expected root hash and compares it to the result from StreamingHasher.
     */
    @Test
    void sampleTreeTest() {
        MessageDigest d = Sha384.sha384Digest();
        // create sample test leaves
        List<byte[]> leaves = new ArrayList<>();
        for (int i = 0; i < 7; i++) {
            leaves.add(("leaf_" + i).getBytes(StandardCharsets.UTF_8));
        }
        // manually compute tree root for 7 node tree based on:
        //                                                                Streaming Tree Root
        //                                                       R_hash = hash(0x2 + C_hash + L4_hash)
        //                                        /-------------------/                      \-----------------------\
        //                                       /                                                                    \
        //                               Internal Node C                                                        Internal
        // Node E
        //                    A_hash = hash(0x2 + A_hash + B_hash)                                    A_hash = hash(0x2
        // + D_hash + L6_hash)
        //                  /                                     \                                       /
        //           \
        //           Internal Node A                        Internal Node B                        Internal Node D
        //            \
        //   A_hash = hash(0x2 + L0_hash+L1_hash)  A_hash = hash(0x2 + L2_hash+L3_hash)   A_hash = hash(0x2 +
        // L4_hash+L5_hash)       \
        //           /              \                       /             \                       /              \
        //              \
        //       Leaf 0           Leaf 1                Leaf 2          Leaf 3                 Leaf 4          Leaf 5
        //            Leaf 6
        //       L0_hash=        L1_hash=              L2_hash=        L3_hash=               L4_hash=        L5_hash=
        //           L6_hash=
        //   hash(0x0+bytes) hash(0x0+bytes)        hash(0x0+bytes) hash(0x0+bytes)        hash(0x0+bytes)
        // hash(0x0+bytes)      hash(0x0+bytes)
        final byte[][] leaveHashes = leaves.stream().map(l -> hashLeaf(d, l)).toArray(byte[][]::new);
        final byte[] nodeA = hashInternalNode(d, leaveHashes[0], leaveHashes[1]);
        final byte[] nodeB = hashInternalNode(d, leaveHashes[2], leaveHashes[3]);
        final byte[] nodeC = hashInternalNode(d, nodeA, nodeB);
        final byte[] nodeD = hashInternalNode(d, leaveHashes[4], leaveHashes[5]);
        final byte[] nodeE = hashInternalNode(d, nodeD, leaveHashes[6]);
        final byte[] manualRoot = hashInternalNode(d, nodeC, nodeE);
        final String manualRootHex = HexFormat.of().formatHex(manualRoot);
        System.out.println("manualRoot = " + manualRootHex + "\n");

        // feed in the leaves one at a timeThe exa
        StreamingHasher streamingHasherOrig = new StreamingHasher();
        for (int i = 0; i < leaves.size(); i++) {
            final byte[] leaf = leaves.get(i);
            streamingHasherOrig.addLeaf(leaf);
            System.out.println("leaf[" + i + "] = " + HexFormat.of().formatHex(leaf) + " hash(leaf) = "
                    + HexFormat.of().formatHex(hashLeaf(d, leaf)));
        }
        List<byte[]> intermediateHashingState = streamingHasherOrig.intermediateHashingState();
        System.out.println("\nintermediateHashingState = "
                + intermediateHashingState.stream()
                        .map(h -> HexFormat.of().formatHex(h))
                        .collect(Collectors.joining(",\n                           ")));
        byte[] rootHash = streamingHasherOrig.computeRootHash();
        final String rootHashHex = HexFormat.of().formatHex(rootHash);
        System.out.println("rootHash = " + rootHashHex);
        assertEquals(manualRootHex, rootHashHex);
    }

    private static byte[] hashLeaf(final MessageDigest digest, final byte[] leafData) {
        digest.update(LEAF_PREFIX);
        return digest.digest(leafData);
    }

    private static byte[] hashInternalNode(
            final MessageDigest digest, final byte[] firstChild, final byte[] secondChild) {
        digest.update(INTERNAL_NODE_PREFIX);
        digest.update(firstChild);
        return digest.digest(secondChild);
    }
}
