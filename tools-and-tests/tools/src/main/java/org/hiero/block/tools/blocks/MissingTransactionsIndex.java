// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import com.hedera.hapi.streams.RecordStreamItem;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.hiero.block.tools.mirrornode.BlockTimeReader;

/**
 * Indexes missing transactions by block number using block time data.
 *
 * <p>This class uses the {@link BlockTimeReader} to determine which block each
 * missing transaction belongs to based on its consensus timestamp. Transactions
 * are grouped by block number for efficient lookup during block conversion.
 *
 * <p>A transaction belongs to block N if its consensus timestamp is:
 * <ul>
 *   <li>Greater than or equal to the block time of block N</li>
 *   <li>Less than the block time of block N+1</li>
 * </ul>
 */
public class MissingTransactionsIndex {

    /** Map from block number to list of missing transactions for that block */
    private final Map<Long, List<RecordStreamItem>> transactionsByBlock;

    /** Total count of indexed transactions */
    private final int totalTransactions;

    /**
     * Creates a MissingTransactionsIndex using the provided loader and block time reader.
     *
     * @param loader the missing transactions loader
     * @param blockTimeReader the block time reader for timestamp-to-block mapping
     */
    public MissingTransactionsIndex(MissingTransactionsLoader loader, BlockTimeReader blockTimeReader) {
        this.transactionsByBlock = buildIndex(loader, blockTimeReader);
        this.totalTransactions = loader.getTransactionCount();
    }

    /**
     * Creates a MissingTransactionsIndex using default paths.
     *
     * @return the index, or null if required files don't exist
     */
    public static MissingTransactionsIndex createDefault() {
        if (!MissingTransactionsLoader.defaultFileExists()) {
            return null;
        }
        try {
            MissingTransactionsLoader loader = MissingTransactionsLoader.getInstance();
            BlockTimeReader blockTimeReader = new BlockTimeReader();
            return new MissingTransactionsIndex(loader, blockTimeReader);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create missing transactions index", e);
        }
    }

    /**
     * Creates a MissingTransactionsIndex using the specified file paths.
     *
     * @param missingTransactionsFile path to the missing_transactions.gz file
     * @param blockTimesFile path to the block_times.bin file
     * @return the index
     * @throws IOException if files cannot be read
     */
    public static MissingTransactionsIndex create(Path missingTransactionsFile, Path blockTimesFile)
            throws IOException {
        MissingTransactionsLoader loader = MissingTransactionsLoader.getInstance(missingTransactionsFile);
        BlockTimeReader blockTimeReader = new BlockTimeReader(blockTimesFile);
        return new MissingTransactionsIndex(loader, blockTimeReader);
    }

    /**
     * Checks if there are missing transactions for the specified block.
     *
     * @param blockNumber the block number to check
     * @return true if there are missing transactions for this block
     */
    public boolean hasTransactionsForBlock(long blockNumber) {
        return transactionsByBlock.containsKey(blockNumber);
    }

    /**
     * Gets the missing transactions for the specified block.
     *
     * @param blockNumber the block number
     * @return list of missing transactions for this block, or empty list if none
     */
    public List<RecordStreamItem> getTransactionsForBlock(long blockNumber) {
        return transactionsByBlock.getOrDefault(blockNumber, Collections.emptyList());
    }

    /**
     * Gets the set of block numbers that have missing transactions.
     *
     * @return unmodifiable set of block numbers
     */
    public java.util.Set<Long> getBlocksWithTransactions() {
        return Collections.unmodifiableSet(transactionsByBlock.keySet());
    }

    /**
     * Gets the total number of indexed transactions.
     *
     * @return the total transaction count
     */
    public int getTotalTransactionCount() {
        return totalTransactions;
    }

    /**
     * Gets the number of blocks that have missing transactions.
     *
     * @return the count of blocks with missing transactions
     */
    public int getBlockCount() {
        return transactionsByBlock.size();
    }

    /**
     * Builds the index mapping block numbers to their missing transactions.
     *
     * @param loader the missing transactions loader
     * @param blockTimeReader the block time reader
     * @return map from block number to list of transactions
     */
    private Map<Long, List<RecordStreamItem>> buildIndex(
            MissingTransactionsLoader loader, BlockTimeReader blockTimeReader) {

        Map<Long, List<RecordStreamItem>> index = new HashMap<>();

        for (RecordStreamItem item : loader.getAllTransactions()) {
            if (item.record() == null || item.record().consensusTimestamp() == null) {
                System.out.println("Warning: Skipping missing transaction with no consensus timestamp");
                continue;
            }

            // Convert consensus timestamp to nanos for comparison with block times
            var ts = item.record().consensusTimestamp();
            long timestampNanos = ts.seconds() * 1_000_000_000L + ts.nanos();

            // Find the block number for this timestamp
            // Block N contains transactions with timestamps >= block_time[N] and < block_time[N+1]
            long blockNumber = findBlockForTimestamp(blockTimeReader, timestampNanos);

            index.computeIfAbsent(blockNumber, k -> new ArrayList<>()).add(item);
        }

        // Sort transactions within each block by consensus timestamp
        for (List<RecordStreamItem> items : index.values()) {
            items.sort((a, b) -> {
                var tsA = a.record().consensusTimestamp();
                var tsB = b.record().consensusTimestamp();
                int cmp = Long.compare(tsA.seconds(), tsB.seconds());
                if (cmp != 0) return cmp;
                return Integer.compare(tsA.nanos(), tsB.nanos());
            });
        }

        System.out.println(
                "Indexed " + loader.getTransactionCount() + " missing transactions across " + index.size() + " blocks");
        return Collections.unmodifiableMap(index);
    }

    /**
     * Finds the block number that contains the given timestamp.
     *
     * <p>Uses binary search via BlockTimeReader to find the block whose time
     * is at or before the target timestamp, with the next block's time being
     * after the target timestamp.
     *
     * @param blockTimeReader the block time reader
     * @param timestampNanos the target timestamp in nanoseconds
     * @return the block number containing this timestamp
     */
    private long findBlockForTimestamp(BlockTimeReader blockTimeReader, long timestampNanos) {
        // getNearestBlockAfterTime returns the first block with time >= target
        // So if we get block N, the transaction either belongs to:
        // - Block N if timestamp == block_time[N]
        // - Block N-1 if timestamp < block_time[N]
        long nearestBlock = blockTimeReader.getNearestBlockAfterTime(timestampNanos);

        if (nearestBlock == 0) {
            // Transaction is at or before block 0
            return 0;
        }

        long blockTime = blockTimeReader.getBlockTime(nearestBlock);
        if (timestampNanos < blockTime) {
            // Transaction is before this block's start time, so it belongs to the previous block
            return nearestBlock - 1;
        }

        return nearestBlock;
    }
}
