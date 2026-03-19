// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.model.hashing;

import static org.hiero.block.tools.blocks.model.hashing.HashingUtils.EMPTY_TREE_HASH;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Consumer;
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
    // Convert record file block to wrapped block.
    // For block 0, use EMPTY_TREE_HASH for previous block hash since there's no
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
                long fileLength = randomAccessFile.length();
                long remainder = fileLength % Sha384.SHA_384_HASH_SIZE;
                if (remainder != 0) {
                    // Partial write detected — truncate to last complete record
                    long truncatedLength = fileLength - remainder;
                    System.err.println("Warning: blockStreamBlockHashes.bin has partial write ("
                            + fileLength + " bytes, remainder " + remainder
                            + "). Truncating to " + truncatedLength + " bytes.");
                    randomAccessFile.setLength(truncatedLength);
                    fileLength = truncatedLength;
                }
                if (fileLength > 0) {
                    // compute the highestBlockNumberStored based on file size
                    highestBlockNumberStored = (fileLength / Sha384.SHA_384_HASH_SIZE) - 1;
                    // read mostRecentBlockHash
                    mostRecentBlockHash = getBlockHash(highestBlockNumberStored);
                }
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
            mostRecentBlockHash = blockHash;
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
     * Truncates the registry so that only hashes for blocks 0 through {@code blockNumber}
     * (inclusive) are retained. Passing {@code -1} clears the entire file.
     *
     * <p>After truncation, {@link #highestBlockNumberStored()} returns {@code blockNumber} and
     * {@link #mostRecentBlockHash()} returns the hash of that block (or {@code EMPTY_TREE_HASH}
     * if the file is now empty).
     *
     * @param blockNumber the last block number to keep, or {@code -1} to clear all
     * @throws IllegalArgumentException if {@code blockNumber} is less than {@code -1} or greater
     *     than the current highest stored block number
     */
    public void truncateTo(long blockNumber) {
        if (blockNumber < -1 || blockNumber > highestBlockNumberStored) {
            throw new IllegalArgumentException(
                    "Cannot truncate to block " + blockNumber + "; highest stored is " + highestBlockNumberStored);
        }
        try {
            final long newLength = (blockNumber + 1) * Sha384.SHA_384_HASH_SIZE;
            randomAccessFile.setLength(newLength);
            highestBlockNumberStored = blockNumber;
            mostRecentBlockHash = blockNumber >= 0 ? getBlockHash(blockNumber) : EMPTY_TREE_HASH;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Flushes any buffered writes to the underlying storage device, ensuring durability.
     */
    public void sync() {
        try {
            randomAccessFile.getFD().sync();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Reads block hashes sequentially from {@code fromBlock} to {@code toBlock} (inclusive),
     * passing each 48-byte hash to the given consumer. This is much faster than per-hash
     * {@link #getBlockHash(long)} calls for bulk replay, especially on spinning disks,
     * because it performs a single seek followed by sequential reads with a large buffer.
     *
     * @param fromBlock the first block number to read (inclusive)
     * @param toBlock the last block number to read (inclusive)
     * @param hashConsumer consumer that receives each 48-byte block hash
     */
    @SuppressWarnings("unused")
    public void readSequential(long fromBlock, long toBlock, Consumer<byte[]> hashConsumer) {
        if (fromBlock < 0 || toBlock > highestBlockNumberStored || fromBlock > toBlock) {
            throw new IllegalArgumentException("Invalid range [" + fromBlock + ", " + toBlock + "]; highest stored is "
                    + highestBlockNumberStored);
        }
        try {
            randomAccessFile.seek(fromBlock * Sha384.SHA_384_HASH_SIZE);
            // Use a 1 MiB read buffer (holds ~21,845 hashes) for efficient sequential I/O
            final int bufferSize = 1 << 20;
            final byte[] buffer = new byte[bufferSize];
            long remaining = (toBlock - fromBlock + 1) * (long) Sha384.SHA_384_HASH_SIZE;
            int leftover = 0;

            while (remaining > 0) {
                int toRead = (int) Math.min(bufferSize - leftover, remaining);
                int bytesRead = randomAccessFile.read(buffer, leftover, toRead);
                if (bytesRead <= 0) break;
                int available = leftover + bytesRead;
                int offset = 0;
                while (offset + Sha384.SHA_384_HASH_SIZE <= available) {
                    byte[] hash = new byte[Sha384.SHA_384_HASH_SIZE];
                    System.arraycopy(buffer, offset, hash, 0, Sha384.SHA_384_HASH_SIZE);
                    hashConsumer.accept(hash);
                    offset += Sha384.SHA_384_HASH_SIZE;
                }
                leftover = available - offset;
                if (leftover > 0) {
                    System.arraycopy(buffer, offset, buffer, 0, leftover);
                }
                remaining -= bytesRead;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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
