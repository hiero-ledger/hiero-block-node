// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.model.hashing;

import java.nio.file.Path;

/**
 * Interface for a binary Merkle tree hasher using SHA-384 hashes.
 */
public interface Hasher {
    /**
     * Add a new leaf to the Merkle tree.
     *
     * @param data the data for the new leaf
     */
    void addLeaf(byte[] data);

    /**
     * Compute the Merkle tree root hash from the current state. This does not modify the internal state, so can be
     * called at any time and more leaves can be added afterward.
     *
     * @return the SHA-384 Merkle tree root hash
     */
    byte[] computeRootHash();

    /**
     * Get the number of leaves added to the tree so far.
     *
     * @return the number of leaves
     */
    long leafCount();

    /**
     * Save the current hashing state to a binary file.
     *
     * @param filePath the path to the file where the state will be saved
     * @throws Exception if an I/O error occurs
     */
    void save(Path filePath) throws Exception;

    /**
     * Load the hashing state from a binary file.
     *
     * @param filePath the path to the file from which the state will be loaded
     * @throws Exception if an I/O error occurs
     */
    void load(Path filePath) throws Exception;
}
