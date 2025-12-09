// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.model.hashing;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.hiero.block.tools.utils.Sha384;

/**
 * A registry for the history of block stream block root hashes. It is designed to store on disk all hashes for block
 * from block 0 on. To allow random read by block number. Blocks are stored in a binary file of 48 byte hashes at index
 * by block number. As if it was a giant array of hash byte[]s, like byte[][].
 */
public class BlockStreamBlockHashRegistry implements AutoCloseable {
    /** random access file for reading and storing block hashes */
    private final RandomAccessFile randomAccessFile;
    /** The highest block number stored in the file */
    private long highestBlockNumberStored = -1;
    /** The hash for the most recent block added, if none have been added it's the highest stored block number's hash */
    private byte[] mostRecentBlockHash = null;

    /**
     * Construct a new BlockStreamBlockHashRegistry which uses the given file path to store block hashes. If the file
     * doesn't exist, a new one will be created.
     *
     * @param blockHashesFilePath path to a binary file to use for storing block hashes, e.g. "blockstream-block-hashes.bin"
     */
    public BlockStreamBlockHashRegistry(Path blockHashesFilePath) {
        try {
            randomAccessFile = new RandomAccessFile(blockHashesFilePath.toFile(), "rw");
            if (Files.exists(blockHashesFilePath)) {
                // compute the highestBlockNumberStored based on file size
                highestBlockNumberStored = (randomAccessFile.length() / Sha384.SHA_384_HASH_SIZE) - 1;
                // read mostRecentBlockHash
                mostRecentBlockHash = getBlockHash(highestBlockNumberStored);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Add a new block to the blocks stored in this register. If the block number is not the next block after the
     * current highest block stored, then an IllegalArgumentException is thrown.
     *
     * @param blockNumber the block number to add
     * @param blockHash the block hash to add
     */
    public void addBlock(long blockNumber, byte[] blockHash) {
        // check block is the next block or throw exception
        if (blockNumber != highestBlockNumberStored + 1) {
            throw new IllegalArgumentException(
                    "Block number " + blockNumber + " is not the next block after " + highestBlockNumberStored);
        }
        try {
            randomAccessFile.seek(blockNumber * Sha384.SHA_384_HASH_SIZE);
            randomAccessFile.write(blockHash);
            highestBlockNumberStored = blockNumber;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Get the block hash for the given block number.
     *
     * @param blockNumber The block number for the block to get the hash for
     * @return the block stream block root hash for that block number
     */
    public byte[] getBlockHash(long blockNumber) {
        if (blockNumber < 0 || blockNumber > highestBlockNumberStored) {
            throw new IllegalArgumentException("Block number " + blockNumber
                    + " is out of range. Highest block stored is " + highestBlockNumberStored);
        }
        try {
            randomAccessFile.seek(blockNumber * Sha384.SHA_384_HASH_SIZE);
            byte[] hash = new byte[Sha384.SHA_384_HASH_SIZE];
            randomAccessFile.readFully(hash);
            return hash;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Get the highest block number stored in this registry.
     *
     * @return the highest block number stored, -1 if the store is empty
     */
    public long highestBlockNumberStored() {
        return highestBlockNumberStored;
    }

    /**
     * Get the most recent block hash added to this registry.
     *
     * @return The hash of the most recent block added to this registry
     */
    public byte[] mostRecentBlockHash() {
        return mostRecentBlockHash;
    }

    /**
     * Closes this resource, relinquishing any underlying resources. This method is invoked automatically on objects
     * managed by the {@code try}-with-resources statement.
     */
    @Override
    public void close() throws Exception {
        randomAccessFile.close();
    }
}
