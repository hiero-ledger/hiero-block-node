// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.model.hashing;

import static org.hiero.block.tools.blocks.model.hashing.HashingUtils.EMPTY_TREE_HASH;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.function.Consumer;
import org.hiero.block.tools.utils.Sha256;

/**
 * A registry for the history of block stream block root hashes. It is designed to store on disk all hashes for block
 * from block 0 on. To allow random read by block number. Blocks are stored in a binary file of 32 byte hashes at index
 * by block number. As if it was a giant array of hash byte[]s, like byte[][].
 *
 * <p>The file starts with a 4 byte {@link #MAGIC} header that marks it as a SHA-256 registry. Older CLI versions wrote
 * a headerless file of 48 byte SHA-384 hashes, which cannot be told apart from a SHA-256 file by its length alone. A
 * non-empty file without the header is rejected rather than misparsed or truncated.
 */
public class BlockStreamBlockHashRegistry implements AutoCloseable {
    /** Header at the start of the file, identifying a registry of SHA-256 hashes */
    static final byte[] MAGIC = {'S', '2', '5', '6'};
    /** Size of the header in bytes */
    private static final int HEADER_SIZE = MAGIC.length;
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
            long fileLength = randomAccessFile.length();
            if (fileLength < HEADER_SIZE) {
                // New file, or a crash before the header was fully written. A legacy SHA-384 file is never this
                // short, so it is safe to start over with a fresh header.
                randomAccessFile.setLength(0);
                randomAccessFile.write(MAGIC);
            } else {
                final byte[] header = new byte[HEADER_SIZE];
                randomAccessFile.readFully(header);
                if (!Arrays.equals(header, MAGIC)) {
                    randomAccessFile.close();
                    throw new IllegalStateException(blockHashesFilePath
                            + " is not a SHA-256 block hash registry (missing header). It is probably SHA-384 output"
                            + " from an older CLI version. Wrap into a fresh output directory.");
                }
                long remainder = (fileLength - HEADER_SIZE) % Sha256.SHA_256_HASH_SIZE;
                if (remainder != 0) {
                    // Partial write detected — truncate to last complete record
                    long truncatedLength = fileLength - remainder;
                    System.err.println("Warning: blockStreamBlockHashes.bin has partial write ("
                            + fileLength + " bytes, remainder " + remainder
                            + "). Truncating to " + truncatedLength + " bytes.");
                    randomAccessFile.setLength(truncatedLength);
                    fileLength = truncatedLength;
                }
                if (fileLength > HEADER_SIZE) {
                    // compute the highestBlockNumberStored based on file size
                    highestBlockNumberStored = ((fileLength - HEADER_SIZE) / Sha256.SHA_256_HASH_SIZE) - 1;
                    // read mostRecentBlockHash
                    mostRecentBlockHash = getBlockHash(highestBlockNumberStored);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Check, without opening the registry for writing, whether an existing registry file is a SHA-256 one. Only the
     * header is read, so the file is never created, truncated or modified, and a read-only file can be checked. A file
     * shorter than the header holds no hashes yet and is accepted, as the constructor does.
     *
     * @param blockHashesFilePath path to an existing registry file
     * @return false if the file is long enough to have a header but does not start with {@link #MAGIC}
     * @throws IOException if the file cannot be read
     */
    public static boolean hasSha256Header(Path blockHashesFilePath) throws IOException {
        final byte[] header;
        try (InputStream in = Files.newInputStream(blockHashesFilePath)) {
            header = in.readNBytes(HEADER_SIZE);
        }
        return header.length < HEADER_SIZE || Arrays.equals(header, MAGIC);
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
            randomAccessFile.seek(offsetOf(blockNumber));
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
            randomAccessFile.seek(offsetOf(blockNumber));
            byte[] hash = new byte[Sha256.SHA_256_HASH_SIZE];
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
            final long newLength = offsetOf(blockNumber + 1);
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
     * passing each 32-byte hash to the given consumer. This is much faster than per-hash
     * {@link #getBlockHash(long)} calls for bulk replay, especially on spinning disks,
     * because it performs a single seek followed by sequential reads with a large buffer.
     *
     * @param fromBlock the first block number to read (inclusive)
     * @param toBlock the last block number to read (inclusive)
     * @param hashConsumer consumer that receives each 32-byte block hash
     */
    @SuppressWarnings("unused")
    public void readSequential(long fromBlock, long toBlock, Consumer<byte[]> hashConsumer) {
        if (fromBlock < 0 || toBlock > highestBlockNumberStored || fromBlock > toBlock) {
            throw new IllegalArgumentException("Invalid range [" + fromBlock + ", " + toBlock + "]; highest stored is "
                    + highestBlockNumberStored);
        }
        try {
            randomAccessFile.seek(offsetOf(fromBlock));
            // Use a 1 MiB read buffer (holds 32,768 hashes) for efficient sequential I/O
            final int bufferSize = 1 << 20;
            final byte[] buffer = new byte[bufferSize];
            long remaining = (toBlock - fromBlock + 1) * (long) Sha256.SHA_256_HASH_SIZE;
            int leftover = 0;

            while (remaining > 0) {
                int toRead = (int) Math.min(bufferSize - leftover, remaining);
                int bytesRead = randomAccessFile.read(buffer, leftover, toRead);
                if (bytesRead <= 0) break;
                int available = leftover + bytesRead;
                int offset = 0;
                while (offset + Sha256.SHA_256_HASH_SIZE <= available) {
                    byte[] hash = new byte[Sha256.SHA_256_HASH_SIZE];
                    System.arraycopy(buffer, offset, hash, 0, Sha256.SHA_256_HASH_SIZE);
                    hashConsumer.accept(hash);
                    offset += Sha256.SHA_256_HASH_SIZE;
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
     * Returns the file offset of the hash slot for the given block number.
     *
     * @param blockNumber the block number
     * @return the offset of that block's hash, after the header
     */
    private static long offsetOf(long blockNumber) {
        return HEADER_SIZE + blockNumber * Sha256.SHA_256_HASH_SIZE;
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
