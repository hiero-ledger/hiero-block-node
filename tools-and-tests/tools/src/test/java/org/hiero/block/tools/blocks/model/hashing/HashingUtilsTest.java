// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.model.hashing;

import static org.hiero.block.tools.blocks.model.hashing.HashingUtils.*;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;
import org.hiero.block.tools.utils.Sha384;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link HashingUtils} validating compliance with the Block & State Merkle Tree Design.
 *
 * <p>These tests verify:
 * <ul>
 *   <li>Correct prefix bytes as specified in the design doc (0x00, 0x01, 0x02)</li>
 *   <li>Leaf hashing: {@code hash(0x00 || leafData)}</li>
 *   <li>Single-child internal node: {@code hash(0x01 || childHash)}</li>
 *   <li>Two-child internal node: {@code hash(0x02 || leftHash || rightHash)}</li>
 *   <li>Domain separation between node types to prevent collision attacks</li>
 * </ul>
 *
 * @see <a href="data/block_stream.md">Block & State Merkle Tree Design</a>
 */
@DisplayName("HashingUtils Tests - Design Doc Compliance")
class HashingUtilsTest {

    private MessageDigest digest;

    @BeforeEach
    void setUp() {
        digest = Sha384.sha384Digest();
    }

    // ========== Prefix Constant Tests ==========

    @Nested
    @DisplayName("Prefix Constants (Design Doc Section: Tree Node Design)")
    class PrefixConstantTests {

        /**
         * Verifies the leaf prefix is 0x00 as specified in the design doc.
         *
         * <p>From design doc: "leaf node hash=hash(0x00 || MerkleLeaf(..).toProtobuf())"
         */
        @Test
        @DisplayName("LEAF_PREFIX should be 0x00 as specified in design doc")
        void testLeafPrefixValue() {
            assertArrayEquals(
                    new byte[] {0x00},
                    LEAF_PREFIX,
                    "LEAF_PREFIX must be 0x00 per design doc: 'leaf node hash=hash(0x00 || ...)'");
        }

        /**
         * Verifies the single-child internal node prefix is 0x01 as specified in the design doc.
         *
         * <p>From design doc: "Internal node with 1 child hash=hash(0x01 || hash(firstChild))"
         */
        @Test
        @DisplayName("SINGLE_CHILD_PREFIX should be 0x01 as specified in design doc")
        void testSingleChildPrefixValue() {
            assertArrayEquals(
                    new byte[] {0x01},
                    SINGLE_CHILD_PREFIX,
                    "SINGLE_CHILD_PREFIX must be 0x01 per design doc: 'Internal node with 1 child hash=hash(0x01 || ...)'");
        }

        /**
         * Verifies the two-child internal node prefix is 0x02 as specified in the design doc.
         *
         * <p>From design doc: "Internal node with 2 child hash=hash(0x02 || hash(firstChild) || hash(secondChild))"
         */
        @Test
        @DisplayName("TWO_CHILDREN_NODE_PREFIX should be 0x02 as specified in design doc")
        void testTwoChildrenPrefixValue() {
            assertArrayEquals(
                    new byte[] {0x02},
                    TWO_CHILDREN_NODE_PREFIX,
                    "TWO_CHILDREN_NODE_PREFIX must be 0x02 per design doc: 'Internal node with 2 child hash=hash(0x02 || ...)'");
        }

        /**
         * Verifies all prefix bytes are distinct for domain separation.
         *
         * <p>From design doc: "Each node kind in tree has a one byte prefix so it puts their hashes
         * in a separate hash space and they can not collide."
         */
        @Test
        @DisplayName("All prefix bytes should be distinct for domain separation")
        void testPrefixesAreDistinct() {
            assertNotEquals(
                    LEAF_PREFIX[0],
                    SINGLE_CHILD_PREFIX[0],
                    "LEAF_PREFIX and SINGLE_CHILD_PREFIX must be distinct for domain separation");
            assertNotEquals(
                    LEAF_PREFIX[0],
                    TWO_CHILDREN_NODE_PREFIX[0],
                    "LEAF_PREFIX and TWO_CHILDREN_NODE_PREFIX must be distinct for domain separation");
            assertNotEquals(
                    SINGLE_CHILD_PREFIX[0],
                    TWO_CHILDREN_NODE_PREFIX[0],
                    "SINGLE_CHILD_PREFIX and TWO_CHILDREN_NODE_PREFIX must be distinct for domain separation");
        }

        /**
         * Verifies the EMPTY_TREE_HASH constant is correctly computed as sha384(0x00).
         *
         * <p>The empty tree hash is defined as the SHA-384 hash of a single zero byte,
         * which is the LEAF_PREFIX. This provides a well-defined hash for empty merkle trees.
         */
        @Test
        @DisplayName("EMPTY_TREE_HASH should be sha384(0x00)")
        void testEmptyTreeHashValue() {
            // Manually compute sha384(0x00)
            byte[] expectedHash = digest.digest(new byte[] {0x00});

            assertArrayEquals(EMPTY_TREE_HASH, expectedHash, "EMPTY_TREE_HASH must be sha384(new byte[]{0x00})");
            assertEquals(48, EMPTY_TREE_HASH.length, "EMPTY_TREE_HASH must be 48 bytes (SHA-384)");
        }
    }

    // ========== Leaf Hashing Tests ==========

    @Nested
    @DisplayName("Leaf Hashing (Design Doc: hash(0x00 || leafData))")
    class LeafHashingTests {

        /**
         * Verifies leaf hash computation matches the design doc formula.
         *
         * <p>Design doc: "leaf node hash=hash(0x00 || MerkleLeaf(..).toProtobuf())"
         */
        @Test
        @DisplayName("hashLeaf should compute hash(0x00 || leafData)")
        void testHashLeafFormula() {
            byte[] leafData = "test leaf data".getBytes(StandardCharsets.UTF_8);

            // Compute using HashingUtils
            byte[] actualHash = hashLeaf(digest, leafData);

            // Manually compute expected hash per design doc
            digest.update(new byte[] {0x00});
            byte[] expectedHash = digest.digest(leafData);

            assertArrayEquals(
                    expectedHash,
                    actualHash,
                    "hashLeaf should compute hash(0x00 || leafData) as specified in design doc");
        }

        /**
         * Verifies that leaf hashes are 48 bytes (SHA-384 output).
         */
        @Test
        @DisplayName("Leaf hash should be 48 bytes (SHA-384)")
        void testLeafHashLength() {
            byte[] leafData = "any data".getBytes(StandardCharsets.UTF_8);
            byte[] hash = hashLeaf(digest, leafData);

            assertEquals(48, hash.length, "SHA-384 hash must be 48 bytes");
        }

        /**
         * Verifies that empty leaf data produces a valid hash.
         *
         * <p>From design doc regarding empty leaves: "Empty leaves for future use will be treated as
         * MerkleLeaf message with no oneof value... So the hash of an empty leaf will be hash(0x0)"
         */
        @Test
        @DisplayName("Empty leaf data should produce valid hash(0x00 || empty)")
        void testEmptyLeafData() {
            byte[] emptyData = new byte[0];
            byte[] hash = hashLeaf(digest, emptyData);

            // Manually compute: hash(0x00 || [empty])
            digest.update(new byte[] {0x00});
            byte[] expectedHash = digest.digest(new byte[0]);

            assertArrayEquals(expectedHash, hash, "Empty leaf should hash as hash(0x00 || [empty])");
        }

        /**
         * Verifies that different leaf data produces different hashes.
         */
        @Test
        @DisplayName("Different leaf data should produce different hashes")
        void testDifferentLeafDataProducesDifferentHashes() {
            byte[] hash1 = hashLeaf(digest, "data A".getBytes(StandardCharsets.UTF_8));
            byte[] hash2 = hashLeaf(digest, "data B".getBytes(StandardCharsets.UTF_8));

            assertFalse(Arrays.equals(hash1, hash2), "Different leaf data must produce different hashes");
        }

        /**
         * Verifies that identical leaf data produces identical hashes (deterministic).
         */
        @Test
        @DisplayName("Identical leaf data should produce identical hashes (deterministic)")
        void testDeterministicLeafHashing() {
            byte[] data = "deterministic data".getBytes(StandardCharsets.UTF_8);

            byte[] hash1 = hashLeaf(digest, data);
            byte[] hash2 = hashLeaf(digest, data);

            assertArrayEquals(hash1, hash2, "Identical leaf data must produce identical hashes");
        }
    }

    // ========== Internal Node Hashing Tests ==========

    @Nested
    @DisplayName("Internal Node Hashing (Design Doc: 0x01/0x02 prefixes)")
    class InternalNodeHashingTests {

        /**
         * Verifies two-child internal node hash computation matches the design doc formula.
         *
         * <p>Design doc: "Internal node with 2 child hash=hash(0x02 || hash(firstChild) || hash(secondChild))"
         */
        @Test
        @DisplayName("hashInternalNode with both children should compute hash(0x02 || left || right)")
        void testTwoChildInternalNodeFormula() {
            byte[] leftChild = new byte[48];
            byte[] rightChild = new byte[48];
            Arrays.fill(leftChild, (byte) 0xAA);
            Arrays.fill(rightChild, (byte) 0xBB);

            // Compute using HashingUtils
            byte[] actualHash = hashInternalNode(digest, leftChild, rightChild);

            // Manually compute expected hash per design doc
            digest.update(new byte[] {0x02});
            digest.update(leftChild);
            byte[] expectedHash = digest.digest(rightChild);

            assertArrayEquals(
                    expectedHash,
                    actualHash,
                    "Two-child internal node should compute hash(0x02 || left || right) as specified in design doc");
        }

        /**
         * Verifies single-child internal node hash computation when only first child is present.
         *
         * <p>Design doc: "Internal node with 1 child hash=hash(0x01 || hash(firstChild))"
         */
        @Test
        @DisplayName("hashInternalNode with only first child should compute hash(0x01 || firstChild)")
        void testSingleChildInternalNodeFormula() {
            byte[] firstChild = new byte[48];
            Arrays.fill(firstChild, (byte) 0xCC);

            // Compute using HashingUtils with null second child
            byte[] actualHash = hashInternalNode(digest, firstChild, null);

            // Manually compute expected hash per design doc
            digest.update(new byte[] {0x01});
            byte[] expectedHash = digest.digest(firstChild);

            assertArrayEquals(
                    expectedHash,
                    actualHash,
                    "Single-child internal node should compute hash(0x01 || firstChild) as specified in design doc");
        }

        /**
         * Verifies that internal node hashes are 48 bytes (SHA-384 output).
         */
        @Test
        @DisplayName("Internal node hash should be 48 bytes (SHA-384)")
        void testInternalNodeHashLength() {
            byte[] leftChild = new byte[48];
            byte[] rightChild = new byte[48];

            byte[] hash = hashInternalNode(digest, leftChild, rightChild);
            assertEquals(48, hash.length, "SHA-384 hash must be 48 bytes");
        }

        /**
         * Verifies that null firstChild throws NullPointerException.
         */
        @Test
        @DisplayName("hashInternalNode should throw NullPointerException when firstChild is null")
        void testNullFirstChildThrows() {
            byte[] rightChild = new byte[48];

            //noinspection DataFlowIssue
            assertThrows(
                    NullPointerException.class,
                    () -> hashInternalNode(digest, null, rightChild),
                    "firstChild cannot be null per design - internal nodes always have at least one child");
        }

        /**
         * Verifies that swapping children produces different hashes (order matters).
         */
        @Test
        @DisplayName("Swapping children should produce different hashes (order matters)")
        void testChildOrderMatters() {
            byte[] child1 = new byte[48];
            byte[] child2 = new byte[48];
            Arrays.fill(child1, (byte) 0x11);
            Arrays.fill(child2, (byte) 0x22);

            byte[] hash1 = hashInternalNode(digest, child1, child2);
            byte[] hash2 = hashInternalNode(digest, child2, child1);

            assertFalse(Arrays.equals(hash1, hash2), "Child order must affect hash result");
        }
    }

    // ========== Domain Separation Tests ==========

    @Nested
    @DisplayName("Domain Separation (Design Doc: Preventing collision attacks)")
    class DomainSeparationTests {

        /**
         * Verifies that a leaf hash differs from an internal node hash with the same content.
         *
         * <p>From design doc: "This separates the hash space for internal nodes and leaf nodes
         * so they can not collide. Those collisions would allow for potential attacks."
         */
        @Test
        @DisplayName("Leaf hash should differ from internal node hash of same data (domain separation)")
        void testLeafVsInternalNodeDomainSeparation() {
            byte[] data = new byte[48];
            Arrays.fill(data, (byte) 0xFF);

            // Hash as leaf
            byte[] leafHash = hashLeaf(digest, data);

            // Hash as single-child internal node
            byte[] internalHash = hashInternalNode(digest, data, null);

            assertFalse(
                    Arrays.equals(leafHash, internalHash),
                    "Leaf hash must differ from internal node hash of same content (prevents collision attacks)");
        }

        /**
         * Verifies that single-child internal node hash differs from two-child internal node hash
         * when the concatenated children could appear as a single child.
         */
        @Test
        @DisplayName("Single-child hash should differ from two-child hash (different prefixes)")
        void testSingleVsTwoChildDomainSeparation() {
            byte[] child = new byte[48];
            Arrays.fill(child, (byte) 0xAB);

            // Single child: hash(0x01 || child)
            byte[] singleChildHash = hashInternalNode(digest, child, null);

            // Two children: hash(0x02 || child || child) - same child twice
            byte[] twoChildHash = hashInternalNode(digest, child, child);

            assertFalse(
                    Arrays.equals(singleChildHash, twoChildHash),
                    "Single-child and two-child hashes must differ even with same children (0x01 vs 0x02 prefix)");
        }

        /**
         * Verifies the prefix scheme prevents length extension attacks.
         *
         * <p>From design doc: "This is the reason we have added 0x00, 0x01, and 0x02 prefixes
         * in hashes to provide super simple and clear domain separation between leaves and
         * internal nodes."
         */
        @Test
        @DisplayName("Prefix scheme should prevent creating collisions between node types")
        void testPrefixPreventsCollisions() {
            // Create leaf data that starts with 0x01 (internal node prefix)
            byte[] maliciousData = new byte[49];
            maliciousData[0] = 0x01;
            Arrays.fill(maliciousData, 1, 49, (byte) 0xDD);

            // Hash as leaf: hash(0x00 || 0x01 || DD...)
            byte[] leafHash = hashLeaf(digest, maliciousData);

            // Create internal node hash with same bytes after prefix
            byte[] childHash = new byte[48];
            System.arraycopy(maliciousData, 1, childHash, 0, 48);
            // Internal node: hash(0x01 || childHash) where childHash = maliciousData[1:49]
            byte[] internalHash = hashInternalNode(digest, childHash, null);

            assertFalse(
                    Arrays.equals(leafHash, internalHash),
                    "Prefix scheme must prevent constructing leaf data that hashes same as internal node");
        }
    }

    // ========== Example from Design Doc ==========

    @Nested
    @DisplayName("Design Doc Example Verification")
    class DesignDocExampleTests {

        /**
         * Reproduces the 5-leaf example from the design doc to verify our implementation.
         *
         * <p>From design doc example with 5 leaves:
         * <pre>
         * Step 1: Add L0     → hashList: [L0]
         * Step 2: Add L1     → L0+L1 pair → NodeA = hash(0x02 || L0 || L1)
         * Step 3: Add L2     → hashList: [NodeA, L2]
         * Step 4: Add L3     → L2+L3 pair → NodeB, NodeA+NodeB → NodeC
         * Step 5: Add L4     → hashList: [NodeC, L4]
         * Root: hash(0x02 || NodeC || L4)
         * </pre>
         */
        @Test
        @DisplayName("5-leaf tree should match design doc example structure")
        void testFiveLeafExampleFromDesignDoc() {
            // Create 5 leaves
            byte[][] leafData = new byte[5][];
            for (int i = 0; i < 5; i++) {
                leafData[i] = ("Leaf " + i).getBytes(StandardCharsets.UTF_8);
            }

            // Compute leaf hashes: L0-L4
            byte[][] L = new byte[5][];
            for (int i = 0; i < 5; i++) {
                L[i] = hashLeaf(digest, leafData[i]);
            }

            // Build tree manually per design doc
            // NodeA = hash(0x02 || L0 || L1)
            byte[] nodeA = hashInternalNode(digest, L[0], L[1]);
            // NodeB = hash(0x02 || L2 || L3)
            byte[] nodeB = hashInternalNode(digest, L[2], L[3]);
            // NodeC = hash(0x02 || NodeA || NodeB)
            byte[] nodeC = hashInternalNode(digest, nodeA, nodeB);
            // Root = hash(0x02 || NodeC || L4)
            byte[] expectedRoot = hashInternalNode(digest, nodeC, L[4]);

            // Verify with StreamingHasher
            StreamingHasher hasher = new StreamingHasher();
            for (byte[] data : leafData) {
                hasher.addLeaf(data);
            }
            byte[] actualRoot = hasher.computeRootHash();

            assertArrayEquals(
                    expectedRoot,
                    actualRoot,
                    "5-leaf tree root hash should match design doc example: hash(0x02 || NodeC || L4)");
        }
    }

    // ========== Hash Output Format Tests ==========

    @Nested
    @DisplayName("Hash Output Format")
    class HashOutputFormatTests {

        /**
         * Verifies that hashes can be formatted as hex for debugging/logging.
         */
        @Test
        @DisplayName("Hash output should be convertible to 96-character hex string")
        void testHashToHexFormat() {
            byte[] hash = hashLeaf(digest, "test".getBytes(StandardCharsets.UTF_8));
            String hex = HexFormat.of().formatHex(hash);

            assertEquals(96, hex.length(), "SHA-384 hash as hex should be 96 characters (48 bytes * 2)");
            assertTrue(hex.matches("[0-9a-f]+"), "Hex string should only contain hex characters");
        }
    }
}
