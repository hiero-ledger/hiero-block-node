// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.model.hashing;

import static org.hiero.block.tools.utils.Sha384.SHA_384_HASH_SIZE;
import static org.hiero.block.tools.utils.Sha384.sha384Digest;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

/**
 * A binary Merkle tree hasher that maintains the full tree in memory. Unlike {@link StreamingHasher}, this
 * implementation stores all leaf hashes and internal node hashes, allowing Merkle paths to be generated for any leaf.
 *
 * <p>The tree is built incrementally as each leaf is added, maintaining all internal nodes. This makes
 * {@link #computeRootHash()} very efficient (O(k) where k is the number of pending subtree roots, typically log(n))
 * since the tree structure is already computed.
 *
 * <p>The tree structure follows the StreamingHasher algorithm exactly:
 * <ul>
 *   <li>Leaves are added left-to-right</li>
 *   <li>When two sibling nodes are available, they are immediately combined into a parent</li>
 *   <li>At root computation time, remaining unpaired subtrees are folded right-to-left</li>
 * </ul>
 *
 * <p>This produces the exact same root hash as {@link StreamingHasher}.
 *
 * <p>This is not thread safe; it is assumed to be used by a single thread.
 */
public class InMemoryTreeHasher implements Hasher {
    /** The hashing algorithm used for computing the hashes. */
    private final MessageDigest digest;

    /**
     * The tree stored as levels. Level 0 = leaf hashes, level 1+ = internal nodes.
     * Each level stores all nodes at that height in the tree.
     */
    private final List<List<byte[]>> levels;

    /**
     * Pending subtree roots that haven't been combined yet.
     * This mirrors StreamingHasher's hashList - contains roots of complete binary subtrees
     * that will be folded right-to-left when computing the final root.
     * Each entry is a pair: [level, index] pointing into the levels structure.
     */
    private final List<int[]> pendingSubtreeRoots;

    /** The count of leaves in the tree. */
    private long leafCount;

    /** Create a new InMemoryTreeHasher with an empty state. */
    public InMemoryTreeHasher() {
        digest = sha384Digest();
        levels = new ArrayList<>();
        levels.add(new ArrayList<>()); // Level 0 for leaves
        pendingSubtreeRoots = new ArrayList<>();
        leafCount = 0;
    }

    /**
     * Add a new leaf to the Merkle tree. The tree structure is updated incrementally,
     * combining nodes bottom-up as complete pairs become available.
     *
     * @param data the data for the new leaf
     */
    @Override
    public void addLeaf(byte[] data) {
        // Hash the leaf data
        byte[] leafHash = hashLeaf(data);

        // Add to level 0 (leaves)
        int leafIndex = levels.getFirst().size();
        levels.getFirst().add(leafHash);

        // Track this as a pending subtree root (at level 0)
        pendingSubtreeRoots.add(new int[] {0, leafIndex});

        // Combine nodes bottom-up as long as we have pairs
        // This mirrors StreamingHasher's logic: combine when (leafCount & 1) == 1
        long n = leafCount;
        while ((n & 1L) == 1) {
            // Pop the last two pending roots and combine them
            int[] right = pendingSubtreeRoots.removeLast();
            int[] left = pendingSubtreeRoots.removeLast();

            // Get the hashes
            byte[] rightHash = levels.get(right[0]).get(right[1]);
            byte[] leftHash = levels.get(left[0]).get(left[1]);

            // Compute parent hash
            byte[] parentHash = hashInternalNode(leftHash, rightHash);

            // Add to the next level
            int parentLevel = right[0] + 1;
            ensureLevelExists(parentLevel);
            int parentIndex = levels.get(parentLevel).size();
            levels.get(parentLevel).add(parentHash);

            // Track the new parent as a pending subtree root
            pendingSubtreeRoots.add(new int[] {parentLevel, parentIndex});

            n >>= 1;
        }

        leafCount++;
    }

    /**
     * Get the number of leaves added to the tree so far.
     *
     * @return the number of leaves
     */
    @Override
    public long leafCount() {
        return leafCount;
    }

    /**
     * Compute the Merkle tree root hash from the current state. This is efficient because
     * the tree structure is already built - only the final fold of pending subtree roots
     * is needed (O(log n) operations).
     *
     * <p>This does not modify the internal state, so more leaves can be added afterward.
     *
     * @return the SHA-384 Merkle tree root hash
     */
    @Override
    public byte[] computeRootHash() {
        if (leafCount == 0) {
            throw new IllegalStateException("Cannot compute root hash of empty tree");
        }

        if (pendingSubtreeRoots.size() == 1) {
            // Only one subtree - its root is the tree root
            int[] root = pendingSubtreeRoots.getFirst();
            return levels.get(root[0]).get(root[1]).clone();
        }

        // Fold pending subtree roots from right to left (matches StreamingHasher)
        int[] rightmost = pendingSubtreeRoots.getLast();
        byte[] accumulated = levels.get(rightmost[0]).get(rightmost[1]);

        for (int i = pendingSubtreeRoots.size() - 2; i >= 0; i--) {
            int[] left = pendingSubtreeRoots.get(i);
            byte[] leftHash = levels.get(left[0]).get(left[1]);
            accumulated = hashInternalNode(leftHash, accumulated);
        }

        return accumulated;
    }

    /**
     * Get the Merkle path (proof) for a leaf at the given index. The path consists of sibling hashes
     * from the leaf up to the root, along with position indicators (left/right).
     *
     * @param leafIndex the index of the leaf (0-based)
     * @return the Merkle path as a list of {@link MerklePathEntry} objects
     * @throws IllegalArgumentException if the leaf index is out of range
     */
    public List<MerklePathEntry> getMerklePath(long leafIndex) {
        if (leafIndex < 0 || leafIndex >= leafCount) {
            throw new IllegalArgumentException("Leaf index " + leafIndex + " out of range [0, " + leafCount + ")");
        }

        List<MerklePathEntry> path = new ArrayList<>();

        // Find which pending subtree this leaf belongs to
        // and walk up through the stored tree structure

        long idx = leafIndex;
        int currentLevel = 0;

        // Walk up through the complete binary subtree this leaf is part of
        while (currentLevel < levels.size() - 1 || hasSiblingAtLevel(currentLevel, (int) idx)) {
            if (currentLevel >= levels.size()) {
                break;
            }

            List<byte[]> level = levels.get(currentLevel);
            boolean isLeftChild = (idx % 2 == 0);
            long siblingIdx = isLeftChild ? idx + 1 : idx - 1;

            if (siblingIdx < level.size()) {
                // Sibling exists in the stored tree
                path.add(new MerklePathEntry(level.get((int) siblingIdx), !isLeftChild));
            } else if (isLeftChild) {
                // No sibling on the right - this node is promoted, check pending roots
                break;
            }

            idx = idx / 2;
            currentLevel++;
        }

        // Handle the fold portion - find siblings from pending subtree roots
        // This is more complex and depends on which pending subtrees exist
        addFoldPathEntries(path, leafIndex);

        return path;
    }

    /**
     * Add path entries for the fold portion of the tree (where pending subtree roots are combined).
     */
    private void addFoldPathEntries(List<MerklePathEntry> path, long leafIndex) {
        if (pendingSubtreeRoots.size() <= 1) {
            return;
        }

        // Determine which pending subtree this leaf belongs to
        int subtreeIndex = findPendingSubtreeForLeaf(leafIndex);
        if (subtreeIndex < 0) {
            return;
        }

        // The fold combines subtrees right-to-left
        // For a leaf in subtree i, the siblings are the accumulated hashes of subtrees to its right
        // combined with the hashes of subtrees to its left

        // Compute accumulated hash from the right up to (but not including) this subtree
        byte[] rightAccumulated = null;
        for (int i = pendingSubtreeRoots.size() - 1; i > subtreeIndex; i--) {
            int[] root = pendingSubtreeRoots.get(i);
            byte[] rootHash = levels.get(root[0]).get(root[1]);
            if (rightAccumulated == null) {
                rightAccumulated = rootHash;
            } else {
                rightAccumulated = hashInternalNode(rootHash, rightAccumulated);
            }
        }

        // Add entries for combining with subtrees to the left
        if (rightAccumulated != null) {
            path.add(new MerklePathEntry(rightAccumulated, false)); // right sibling
        }

        // Add entries for left siblings
        for (int i = subtreeIndex - 1; i >= 0; i--) {
            int[] leftRoot = pendingSubtreeRoots.get(i);
            byte[] leftHash = levels.get(leftRoot[0]).get(leftRoot[1]);
            path.add(new MerklePathEntry(leftHash, true)); // left sibling
        }
    }

    /**
     * Find which pending subtree contains the given leaf index.
     */
    private int findPendingSubtreeForLeaf(long leafIndex) {
        // Each pending subtree root covers a range of leaves
        // The subtree at pendingSubtreeRoots[i] covers 2^(level) leaves starting at some offset

        long leafOffset = 0;
        for (int i = 0; i < pendingSubtreeRoots.size(); i++) {
            int[] root = pendingSubtreeRoots.get(i);
            int level = root[0];
            long subtreeSize = 1L << level; // 2^level leaves in this subtree

            if (leafIndex < leafOffset + subtreeSize) {
                return i;
            }
            leafOffset += subtreeSize;
        }
        return -1;
    }

    /**
     * Check if there's a sibling at the given level and index.
     */
    private boolean hasSiblingAtLevel(int level, int index) {
        if (level >= levels.size()) {
            return false;
        }
        boolean isLeft = (index % 2 == 0);
        int siblingIndex = isLeft ? index + 1 : index - 1;
        return siblingIndex >= 0 && siblingIndex < levels.get(level).size();
    }

    /**
     * Ensure the level exists in the levels list.
     */
    private void ensureLevelExists(int level) {
        while (levels.size() <= level) {
            levels.add(new ArrayList<>());
        }
    }

    /**
     * Get the hash at a specific position in the tree.
     *
     * @param level the level (0 = leaves, higher = internal nodes)
     * @param index the index within the level
     * @return the hash at that position
     * @throws IllegalArgumentException if the position is out of range
     */
    public byte[] getHash(int level, int index) {
        if (level < 0 || level >= levels.size()) {
            throw new IllegalArgumentException("Level " + level + " out of range [0, " + levels.size() + ")");
        }
        List<byte[]> levelHashes = levels.get(level);
        if (index < 0 || index >= levelHashes.size()) {
            throw new IllegalArgumentException(
                    "Index " + index + " out of range [0, " + levelHashes.size() + ") at level " + level);
        }
        return levelHashes.get(index);
    }

    /**
     * Get the number of levels in the tree (including the leaf level).
     *
     * @return the number of levels
     */
    public int levelCount() {
        return levels.size();
    }

    /**
     * Get the number of hashes at a specific level.
     *
     * @param level the level (0 = leaves)
     * @return the number of hashes at that level
     */
    public int hashCountAtLevel(int level) {
        if (level < 0 || level >= levels.size()) {
            throw new IllegalArgumentException("Level " + level + " out of range [0, " + levels.size() + ")");
        }
        return levels.get(level).size();
    }

    /**
     * Get the number of pending subtree roots. For a power-of-2 leaf count, this will be 1.
     * For other counts, this equals the number of 1-bits in the binary representation of the leaf count.
     *
     * @return the number of pending subtree roots
     */
    public int pendingSubtreeCount() {
        return pendingSubtreeRoots.size();
    }

    /**
     * Save the current tree state to a binary file. All hashes at all levels are saved,
     * along with the pending subtree root pointers.
     *
     * @param filePath the path to the file where the state will be saved
     * @throws Exception if an I/O error occurs
     */
    @Override
    public void save(Path filePath) throws Exception {
        try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(filePath))) {
            // Write leaf count
            out.writeLong(leafCount);

            // Write number of levels
            out.writeInt(levels.size());

            // Write each level
            for (List<byte[]> level : levels) {
                out.writeInt(level.size());
                for (byte[] hash : level) {
                    out.write(hash);
                }
            }

            // Write pending subtree roots
            out.writeInt(pendingSubtreeRoots.size());
            for (int[] root : pendingSubtreeRoots) {
                out.writeInt(root[0]); // level
                out.writeInt(root[1]); // index
            }
        }
    }

    /**
     * Load the tree state from a binary file.
     *
     * @param filePath the path to the file from which the state will be loaded
     * @throws Exception if an I/O error occurs
     */
    @Override
    public void load(Path filePath) throws Exception {
        try (DataInputStream din = new DataInputStream(Files.newInputStream(filePath))) {
            // Read leaf count
            leafCount = din.readLong();

            // Read levels
            int levelCount = din.readInt();
            levels.clear();
            for (int l = 0; l < levelCount; l++) {
                int hashCount = din.readInt();
                List<byte[]> level = new ArrayList<>(hashCount);
                for (int i = 0; i < hashCount; i++) {
                    byte[] hash = new byte[SHA_384_HASH_SIZE];
                    din.readFully(hash);
                    level.add(hash);
                }
                levels.add(level);
            }

            // Read pending subtree roots
            int pendingCount = din.readInt();
            pendingSubtreeRoots.clear();
            for (int i = 0; i < pendingCount; i++) {
                int level = din.readInt();
                int index = din.readInt();
                pendingSubtreeRoots.add(new int[] {level, index});
            }
        }
    }

    /**
     * Hash a leaf node with the appropriate prefix.
     *
     * @param leafData the data of the leaf
     * @return the hash of the leaf node
     */
    private byte[] hashLeaf(final byte[] leafData) {
        digest.update(LEAF_PREFIX);
        return digest.digest(leafData);
    }

    /**
     * Hash an internal node by combining the hashes of its two children with the appropriate prefix.
     *
     * @param firstChild the hash of the first child
     * @param secondChild the hash of the second child
     * @return the hash of the internal node
     */
    private byte[] hashInternalNode(final byte[] firstChild, final byte[] secondChild) {
        digest.update(INTERNAL_NODE_PREFIX);
        digest.update(firstChild);
        return digest.digest(secondChild);
    }

    /**
     * A Merkle path entry representing a sibling hash and its position relative to the path node.
     *
     * @param siblingHash the hash of the sibling node
     * @param siblingIsLeft true if the sibling is on the left side (path node is right child)
     */
    public record MerklePathEntry(byte[] siblingHash, boolean siblingIsLeft) {}
}
