// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.model.hashing;

import static org.hiero.block.tools.blocks.model.hashing.HashingUtils.EMPTY_TREE_HASH;

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
 *
 * <p>Writes are batched in an in-memory buffer (10,000 entries = 480 KB) and flushed to disk in one
 * large write when full or on {@link #close()}. This reduces 90 million individual 48-byte syscalls
 * to ~9,000 large writes without affecting crash-safety (the existing monthly checkpoint and
 * shutdown hook already handle recovery from the registry).
 */
public class BlockStreamBlockHashRegistry implements AutoCloseable {
    /** Number of hashes to accumulate before flushing to disk (10,000 × 48 bytes = 480 KB per flush). */
    private static final int WRITE_BUFFER_ENTRIES = 10_000;
    /** random access file for reading and storing block hashes */
    private final RandomAccessFile randomAccessFile;
    /** In-memory write buffer: accumulates hashes before flushing to disk. */
    private final byte[] writeBuffer = new byte[WRITE_BUFFER_ENTRIES * Sha384.SHA_384_HASH_SIZE];
    /** Number of hashes currently held in {@link #writeBuffer} that have not yet been flushed. */
    private int writeBufferCount = 0;
    /** The highest block number stored in the file */
    private long highestBlockNumberStored = -1;
    /** The hash for the most recent block added, if none have been added it's the highest stored block number's hash */
    // Convert a record file block to a wrapped block.
    // For block 0, use EMPTY_TREE_HASH for the previous block hash since there's no
    // previous block. The streamingHasher.computeRootHash() already returns
    // EMPTY_TREE_HASH when empty, so allBlocksMerkleTreeRootHash is handled.
    private byte[] mostRecentBlockHash = EMPTY_TREE_HASH;

    /**
     * Construct a new BlockStreamBlockHashRegistry which uses the given file path to store block hashes. If the file
     * doesn't exist, a new one will be created.
     *
     * @param blockHashesFilePath path to a binary file to use for storing block hashes, e.g. "blockstream-block-hashes.bin"
     */
    public BlockStreamBlockHashRegistry(Path blockHashesFilePath) {
        try {
            randomAccessFile = new RandomAccessFile(blockHashesFilePath.toFile(), "rw");
            if (Files.exists(blockHashesFilePath) && randomAccessFile.length() > 0) {
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
     * <p>Hashes are accumulated in an in-memory buffer and flushed to disk in batches of
     * {@value #WRITE_BUFFER_ENTRIES} entries to minimise syscall overhead.
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
        System.arraycopy(
                blockHash, 0, writeBuffer, writeBufferCount * Sha384.SHA_384_HASH_SIZE, Sha384.SHA_384_HASH_SIZE);
        writeBufferCount++;
        highestBlockNumberStored = blockNumber;
        mostRecentBlockHash = blockHash;
        if (writeBufferCount == WRITE_BUFFER_ENTRIES) {
            flushWriteBuffer();
        }
    }

    /**
     * Flush any buffered hashes to disk as a single large write. Called automatically when the
     * buffer is full and explicitly on {@link #close()}.
     */
    private void flushWriteBuffer() {
        if (writeBufferCount == 0) return;
        try {
            final long firstBufferedBlock = highestBlockNumberStored - writeBufferCount + 1;
            randomAccessFile.seek(firstBufferedBlock * Sha384.SHA_384_HASH_SIZE);
            randomAccessFile.write(writeBuffer, 0, writeBufferCount * Sha384.SHA_384_HASH_SIZE);
            writeBufferCount = 0;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Get the block hash for the given block number.
     *
     * <p>Any hashes still held in the write buffer are flushed to disk before reading so that
     * recently added blocks are always readable.
     *
     * @param blockNumber The block number for the block to get the hash for
     * @return the block stream block root hash for that block number
     */
    public byte[] getBlockHash(long blockNumber) {
        if (blockNumber < 0 || blockNumber > highestBlockNumberStored) {
            throw new IllegalArgumentException("Block number " + blockNumber
                    + " is out of range. Highest block stored is " + highestBlockNumberStored);
        }
        flushWriteBuffer();
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
     *
     * <p>Any hashes still held in the in-memory write buffer are flushed to disk before the file is closed.
     */
    @Override
    public void close() throws Exception {
        flushWriteBuffer();
        randomAccessFile.close();
    }
}
