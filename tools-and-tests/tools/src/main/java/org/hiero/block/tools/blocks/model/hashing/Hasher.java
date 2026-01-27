// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.model.hashing;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.nio.file.Path;

/**
 * Interface for a binary Merkle tree hasher using SHA-384 hashes.
 *
 * <p>Implementations of this interface compute Merkle tree root hashes following the
 * Block &amp; State Merkle Tree Design specification. The design uses domain-separated
 * hashing with the following prefix scheme:
 *
 * <ul>
 *   <li>{@code 0x00} - Leaf node: {@code hash(0x00 || leafData)}</li>
 *   <li>{@code 0x01} - Single-child internal node: {@code hash(0x01 || childHash)}</li>
 *   <li>{@code 0x02} - Two-child internal node: {@code hash(0x02 || leftHash || rightHash)}</li>
 * </ul>
 *
 * <p>This interface supports the Streaming Binary Merkle Tree algorithm, which allows
 * incremental tree construction where the root hash can be computed at any point
 * and additional leaves can be added afterward. Implementations must maintain only
 * O(log n) intermediate state for n leaves.
 *
 * <h2>Implementations</h2>
 * <ul>
 *   <li>{@link StreamingHasher} - Memory-efficient streaming implementation that
 *       maintains only the minimal intermediate hashes needed to continue building.</li>
 *   <li>{@link InMemoryTreeHasher} - Full tree implementation that stores all nodes,
 *       supporting merkle path generation for any leaf.</li>
 * </ul>
 *
 * <h2>Thread Safety</h2>
 * <p>Implementations are NOT thread-safe and are designed for single-threaded use.
 *
 * @see StreamingHasher
 * @see InMemoryTreeHasher
 * @see HashingUtils
 */
public interface Hasher {

    /**
     * Add a new leaf to the Merkle tree.
     *
     * <p>The leaf data is hashed using the leaf prefix scheme: {@code hash(0x00 || data)}.
     * This method may trigger internal node hash computations as the tree grows.
     *
     * @param data the raw data for the new leaf (will be prefixed and hashed)
     */
    void addLeaf(byte[] data);

    /**
     * Add a new leaf to the Merkle tree.
     *
     * <p>The leaf data is hashed using the leaf prefix scheme: {@code hash(0x00 || data)}.
     * This method may trigger internal node hash computations as the tree grows.
     *
     * @param data the raw data for the new leaf (will be prefixed and hashed)
     */
    void addLeaf(Bytes data);

    /**
     * Compute the Merkle tree root hash from the current state.
     *
     * <p>This method does not modify the internal state, so it can be called at any time
     * and more leaves can be added afterward. For a tree with n leaves, this operation
     * combines at most O(log n) pending subtree roots using right-to-left folding.
     *
     * <p>The returned hash is a 48-byte SHA-384 hash following the domain-separated
     * prefixing scheme from the design specification.
     *
     * @return the 48-byte SHA-384 Merkle tree root hash
     * @throws java.util.NoSuchElementException if no leaves have been added
     */
    byte[] computeRootHash();

    /**
     * Get the number of leaves added to the tree so far.
     *
     * <p>Note: The number of pending subtree roots in a streaming implementation
     * equals {@code Integer.bitCount(leafCount)}.
     *
     * @return the number of leaves (0 or more)
     */
    long leafCount();

    /**
     * Save the current hashing state to a binary file.
     *
     * <p>The saved state includes the leaf count and all intermediate hashes needed
     * to resume tree construction. The exact format depends on the implementation.
     *
     * @param filePath the path to the file where the state will be saved
     * @throws Exception if an I/O error occurs during saving
     */
    void save(Path filePath) throws Exception;

    /**
     * Load the hashing state from a binary file.
     *
     * <p>Restores the hasher to a previously saved state, allowing tree construction
     * to resume. Any existing state in this hasher is replaced.
     *
     * @param filePath the path to the file from which the state will be loaded
     * @throws Exception if an I/O error occurs or the file format is invalid
     */
    void load(Path filePath) throws Exception;
}
