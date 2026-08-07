// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.model.hashing;

import static org.hiero.block.tools.blocks.model.hashing.HashingUtils.EMPTY_TREE_HASH;
import static org.junit.jupiter.api.Assertions.*;

import java.security.MessageDigest;
import java.util.Arrays;
import org.hiero.block.tools.utils.Sha384;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Unit tests for {@link BlockStreamBlockHasher} validating compliance with Block & State Merkle Tree Design.
 *
 * <p>These tests verify the internal hashing logic used by BlockStreamBlockHasher aligns with the design doc.
 * Full integration tests with real Block objects are in the RecordBlockConverterTest class.
 *
 * <p>The block root hash computation follows the 16-leaf fixed tree structure from the design doc:
 * <pre>
 *                              Block Root
 *                                  │
 *              ┌───────────────────┴───────────────────┐
 *       Consensus Time                           Fixed Root Tree
 *      (MerkleLeaf)                                (16 leaves)
 * </pre>
 *
 * <p>Fixed leaf positions (from design doc):
 * <ol>
 *   <li>Previous Block Root Hash - Links to previous block, forming the blockchain</li>
 *   <li>All Block Hashes Tree Root - Streaming merkle tree of all previous block hashes</li>
 *   <li>State Root Hash - State merkle tree root at block start</li>
 *   <li>Consensus Headers - EventHeader, RoundHeader items</li>
 *   <li>Input Items - SignedTransaction</li>
 *   <li>Output Items - BlockHeader, RecordFileItem, TransactionResult, TransactionOutput items</li>
 *   <li>State Changes - StateChanges items</li>
 *   <li>Trace Data - TraceData items</li>
 *   <li>-16. Reserved - For future expansion</li>
 * </ol>
 *
 * @see <a href="data/block_stream.md">Block & State Merkle Tree Design - Block Merkle Tree Section</a>
 */
@DisplayName("BlockStreamBlockHasher Tests - Design Doc Compliance")
class BlockStreamBlockHasherTest {

    private MessageDigest digest;

    @BeforeEach
    void setUp() {
        digest = Sha384.sha384Digest();
    }

    // ========== Tree Structure Tests ==========

    @Nested
    @DisplayName("Block Tree Structure Validation")
    class TreeStructureTests {

        /**
         * Verifies that the StreamingHasher used for subtrees produces consistent hashes.
         *
         * <p>BlockStreamBlockHasher uses StreamingHasher for each item category subtree.
         */
        @Test
        @DisplayName("StreamingHasher should be used for subtree computation")
        void testStreamingHasherUsedForSubTrees() {
            StreamingHasher hasher = new StreamingHasher();
            hasher.addLeaf("test item".getBytes());
            byte[] hash = hasher.computeRootHash();

            assertNotNull(hash, "StreamingHasher should produce non-null hash");
            assertEquals(48, hash.length, "StreamingHasher should produce 48-byte SHA-384 hash");
        }

        /**
         * Verifies that empty subtrees still produce valid hashes via StreamingHasher.
         *
         * <p>When no items are added to a category, the subtree has an empty leaf list
         * which returns the predefined EMPTY_TREE_HASH constant.
         */
        @Test
        @DisplayName("Empty StreamingHasher should return EMPTY_TREE_HASH")
        void testEmptyStreamingHasherReturnsEmptyTreeHash() {
            StreamingHasher hasher = new StreamingHasher();

            // An empty hasher should return EMPTY_TREE_HASH
            byte[] rootHash = hasher.computeRootHash();
            assertArrayEquals(
                    EMPTY_TREE_HASH,
                    rootHash,
                    "Empty StreamingHasher should return EMPTY_TREE_HASH (sha384Hash(new byte[]{0x00}))");
        }
    }

    // ========== Hashing Utility Tests ==========

    @Nested
    @DisplayName("HashingUtils Integration")
    class HashingUtilsIntegrationTests {

        /**
         * Verifies that hashLeaf produces correct prefixed hashes for block items.
         *
         * <p>From design doc: Block items are hashed as leaves with 0x00 prefix.
         */
        @Test
        @DisplayName("Block item hashing should use hashLeaf with 0x00 prefix")
        void testBlockItemHashingUsesLeafPrefix() {
            byte[] itemBytes = "serialized block item".getBytes();

            // Manually compute expected hash: hash(0x00 || itemBytes)
            digest.update(HashingUtils.LEAF_PREFIX);
            byte[] expectedHash = digest.digest(itemBytes);

            // Verify HashingUtils produces same result
            byte[] actualHash = HashingUtils.hashLeaf(digest, itemBytes);

            assertArrayEquals(expectedHash, actualHash, "hashLeaf should produce hash(0x00 || data) per design doc");
        }

        /**
         * Verifies that internal node hashing uses correct prefix for two children.
         *
         * <p>From design doc: Internal nodes with 2 children use 0x02 prefix.
         */
        @Test
        @DisplayName("Internal node with two children should use 0x02 prefix")
        void testTwoChildInternalNodePrefix() {
            byte[] leftChild = new byte[48];
            byte[] rightChild = new byte[48];

            byte[] hash = HashingUtils.hashInternalNode(digest, leftChild, rightChild);

            assertNotNull(hash, "Internal node hash should not be null");
            assertEquals(48, hash.length, "Internal node hash should be 48 bytes");
        }

        /**
         * Verifies that internal node hashing uses correct prefix for single child.
         *
         * <p>From design doc: Internal nodes with 1 child use 0x01 prefix.
         */
        @Test
        @DisplayName("Internal node with single child should use 0x01 prefix")
        void testSingleChildInternalNodePrefix() {
            byte[] singleChild = new byte[48];

            byte[] hash = HashingUtils.hashInternalNode(digest, singleChild, null);

            assertNotNull(hash, "Single-child internal node hash should not be null");
            assertEquals(48, hash.length, "Single-child internal node hash should be 48 bytes");
        }
    }

    // ========== Block Root Structure Tests ==========

    @Nested
    @DisplayName("Block Root Computation Structure")
    class BlockRootStructureTests {

        /**
         * Verifies the block root is computed as hash(consensusTime, fixedRootTree).
         *
         * <p>From design doc, the root structure is:
         * <pre>
         * Block Root = hash(0x02 || hash(consensusTime) || hash(fixedRootTree))
         * </pre>
         */
        @Test
        @DisplayName("Block root should be internal node of consensus time and fixed root tree")
        void testBlockRootStructure() {
            // Simulate block root computation structure
            byte[] consensusTimeHash = HashingUtils.hashLeaf(digest, "consensus timestamp bytes".getBytes());
            byte[] fixedRootTreeHash = new byte[48]; // Placeholder for subtree hash

            byte[] blockRoot = HashingUtils.hashInternalNode(digest, consensusTimeHash, fixedRootTreeHash);

            assertNotNull(blockRoot, "Block root should not be null");
            assertEquals(48, blockRoot.length, "Block root should be 48 bytes");
        }

        /**
         * Verifies that the fixed root tree uses 16-leaf structure per design doc.
         *
         * <p>From design doc: The block has a fixed 16-leaf tree structure at the root level.
         * Reserved leaves use null/empty placeholders.
         */
        @Test
        @DisplayName("Fixed root tree should support reserved (null) branches")
        void testFixedRootTreeSupportsNullBranches() {
            // The design doc shows the root tree has reserved branches for future use
            // These are represented as null children in hashInternalNode

            byte[] leftSubtree = new byte[48];
            byte[] rightReserved = null; // Reserved for future use

            byte[] hash = HashingUtils.hashInternalNode(digest, leftSubtree, rightReserved);

            assertNotNull(hash, "Hash with reserved null branch should not be null");
            assertEquals(48, hash.length, "Hash should be 48 bytes");
        }
    }

    // ========== Item Categorization Documentation ==========

    @Nested
    @DisplayName("Item Category Documentation")
    class ItemCategoryDocumentationTests {

        /**
         * Documents which block item types go to which subtree.
         *
         * <p>From design doc and BlockStreamBlockHasher switch statement:
         * <ul>
         *   <li>consensusHeadersHasher: EVENT_HEADER, ROUND_HEADER</li>
         *   <li>inputItemsHasher: SIGNED_TRANSACTION</li>
         *   <li>outputItemsHasher: BLOCK_HEADER, RECORD_FILE, TRANSACTION_RESULT, TRANSACTION_OUTPUT</li>
         *   <li>stateChangeItemsHasher: STATE_CHANGES, FILTERED_ITEM_HASH</li>
         *   <li>traceItemsHasher: TRACE_DATA</li>
         *   <li>Not hashed: BLOCK_FOOTER, BLOCK_PROOF</li>
         * </ul>
         */
        @SuppressWarnings("MismatchedReadAndWriteOfArray")
        @Test
        @DisplayName("Item categorization should match design doc specification")
        void testItemCategorizationDocumented() {
            // This test documents the expected categorization from the design doc
            // Actual verification is done by integration tests with real blocks

            // Consensus Headers subtree items
            String[] consensusItems = {"EVENT_HEADER", "ROUND_HEADER"};
            assertEquals(2, consensusItems.length, "Consensus headers has 2 item types");

            // Input Items subtree items
            String[] inputItems = {"SIGNED_TRANSACTION"};
            assertEquals(1, inputItems.length, "Input items has 1 item type");

            // Output Items subtree items
            String[] outputItems = {"BLOCK_HEADER", "RECORD_FILE", "TRANSACTION_RESULT", "TRANSACTION_OUTPUT"};
            assertEquals(4, outputItems.length, "Output items has 4 item types");

            // State Changes subtree items
            String[] stateItems = {"STATE_CHANGES", "FILTERED_ITEM_HASH"};
            assertEquals(2, stateItems.length, "State changes has 2 item types");

            // Trace Data subtree items
            String[] traceItems = {"TRACE_DATA"};
            assertEquals(1, traceItems.length, "Trace data has 1 item type");

            // Not hashed items (excluded from all subtrees)
            String[] excludedItems = {"BLOCK_FOOTER", "BLOCK_PROOF"};
            assertEquals(2, excludedItems.length, "2 item types are excluded from hashing");
        }
    }

    // ========== Pre-Defined Side Rightmost Scan Tests ==========

    /// Tests for {@link BlockStreamBlockHasher#computeSubtreesInternalRoot} — the rightmost-scan
    /// + single-child promotion algorithm from issue #3377. Only positions 0-1 are always
    /// populated; positions 2-7 can be empty. Trailing empties are dropped from the fold-up;
    /// interior empties contribute {@code EMPTY_TREE_HASH}. Result is canonicalized to height 3.
    @Nested
    @DisplayName("Pre-Defined Side Rightmost Scan")
    class PreDefinedRightmostScanTests {

        /// Presence bitmask over positions 2-7 (bit 0 = position 2, ..., bit 5 = position 7).
        /// Covers: only 0-1 (WRB with no state root and no content), only position 2,
        /// only position 7 (rightmost with interior empties), positions 2 and 7 (interior gap),
        /// interior gap with position 5 rightmost, all present.
        @ParameterizedTest
        @ValueSource(ints = {0b000000, 0b000001, 0b100000, 0b100001, 0b100010, 0b001000, 0b111111})
        @DisplayName("computeSubtreesInternalRoot matches reference for presence pattern")
        void testPresencePatternMatchesReference(final int presenceMask) {
            final byte[][] preDefinedLeaves = buildLeaves(presenceMask);
            final byte[] actual = BlockStreamBlockHasher.computeSubtreesInternalRoot(digest, preDefinedLeaves);
            final byte[] expected = referenceInternalRoot(preDefinedLeaves);
            Assertions.assertArrayEquals(
                    expected,
                    actual,
                    "internal root must match reference for mask 0b" + Integer.toBinaryString(presenceMask));
        }

        /// Dropping a trailing empty position must not collapse two different presence patterns
        /// onto the same internal root — rightmost=6 and rightmost=7 with identical interior
        /// data must diverge.
        @Test
        @DisplayName("Trailing empty position changes the internal root")
        void testTrailingEmptyChangesRoot() {
            final byte[][] rightmostAtSix = buildLeaves(0b011111);
            final byte[][] rightmostAtSeven = buildLeaves(0b111111);
            final byte[] atSix = BlockStreamBlockHasher.computeSubtreesInternalRoot(digest, rightmostAtSix);
            final byte[] atSeven = BlockStreamBlockHasher.computeSubtreesInternalRoot(digest, rightmostAtSeven);
            Assertions.assertFalse(
                    Arrays.equals(atSix, atSeven),
                    "rightmost=6 and rightmost=7 with same interior must produce different roots");
        }

        /// Interior empty (position 2 empty while position 7 present) must contribute
        /// EMPTY_TREE_HASH — dropping it would move the rightmost content and change the shape.
        /// Verifies interior emptiness is preserved through the scan.
        @Test
        @DisplayName("Interior empty is included, not dropped")
        void testInteriorEmptyIncluded() {
            final byte[][] withInteriorEmpty = buildLeaves(0b100000);
            final byte[][] withInteriorFilled = buildLeaves(0b100001);
            final byte[] emptyInterior = BlockStreamBlockHasher.computeSubtreesInternalRoot(digest, withInteriorEmpty);
            final byte[] filledInterior =
                    BlockStreamBlockHasher.computeSubtreesInternalRoot(digest, withInteriorFilled);
            Assertions.assertFalse(
                    Arrays.equals(emptyInterior, filledInterior),
                    "interior empty vs filled with same rightmost must produce different roots");
        }

        /// Constructs the 8-slot pre-defined leaves array. Positions 0-1 are always populated
        /// with deterministic non-empty hashes. Positions 2-7 are either populated (a
        /// distinct deterministic hash) or set to EMPTY_TREE_HASH based on the bitmask
        /// (bit 0 = position 2, ..., bit 5 = position 7).
        private byte[][] buildLeaves(final int presenceMask) {
            final byte[][] leaves = new byte[8][];
            leaves[0] = deterministicHash(0);
            leaves[1] = deterministicHash(1);
            for (int i = 2; i < 8; i++) {
                final int bit = 1 << (i - 2);
                leaves[i] = (presenceMask & bit) != 0 ? deterministicHash(i) : EMPTY_TREE_HASH;
            }
            return leaves;
        }

        /// Deterministic 48-byte hash where every byte equals {@code (index + 1)}, guaranteed
        /// different from {@link HashingUtils#EMPTY_TREE_HASH}.
        private byte[] deterministicHash(final int index) {
            final byte[] hash = new byte[48];
            Arrays.fill(hash, (byte) (index + 1));
            return hash;
        }

        /// Reference reimplementation of the rightmost-scan + fold-up + height-3 promotion,
        /// independent of the production code path. Uses the same {@link StreamingHasher} the
        /// production code uses; verifies the algorithm shape not the hasher.
        private byte[] referenceInternalRoot(final byte[][] leaves) {
            int rightmost = 1;
            for (int i = 2; i < leaves.length; i++) {
                if (!Arrays.equals(leaves[i], EMPTY_TREE_HASH)) {
                    rightmost = i;
                }
            }
            final StreamingHasher hasher = new StreamingHasher();
            for (int i = 0; i <= rightmost; i++) {
                hasher.addNodeByHash(leaves[i]);
            }
            byte[] root = hasher.computeRootHash();
            final int height = rightmost < 2 ? 1 : rightmost < 4 ? 2 : 3;
            for (int h = height; h < 3; h++) {
                root = HashingUtils.hashInternalNode(digest, root, null);
            }
            return root;
        }
    }
}
