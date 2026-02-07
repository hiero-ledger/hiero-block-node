// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import static org.junit.jupiter.api.Assertions.*;

import com.hedera.hapi.node.base.Timestamp;
import com.hedera.hapi.node.base.Transaction;
import com.hedera.hapi.node.transaction.TransactionRecord;
import com.hedera.hapi.streams.RecordStreamItem;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPOutputStream;
import org.hiero.block.tools.mirrornode.BlockTimeReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link MissingTransactionsIndex}.
 */
class MissingTransactionsIndexTest {

    @TempDir
    Path tempDir;

    private Path missingTransactionsFile;
    private Path blockTimesFile;

    @BeforeEach
    void setUp() {
        missingTransactionsFile = tempDir.resolve("missing_transactions.gz");
        blockTimesFile = tempDir.resolve("block_times.bin");
    }

    @Nested
    @DisplayName("Index building")
    class IndexBuildingTests {

        @Test
        @DisplayName("Indexes single transaction to correct block")
        void testIndexSingleTransaction() throws IOException {
            // Block times: block 0 at 1000s, block 1 at 2000s, block 2 at 3000s
            writeBlockTimes(1000_000_000_000L, 2000_000_000_000L, 3000_000_000_000L);

            // Transaction at 1500s should be in block 0 (between block 0 and block 1)
            writeTransactions(createTestRecordStreamItem(1500L, 0));

            MissingTransactionsIndex index = createIndex();

            assertTrue(index.hasTransactionsForBlock(0), "Block 0 should have transactions");
            assertFalse(index.hasTransactionsForBlock(1), "Block 1 should not have transactions");
            assertFalse(index.hasTransactionsForBlock(2), "Block 2 should not have transactions");

            List<RecordStreamItem> block0Items = index.getTransactionsForBlock(0);
            assertEquals(1, block0Items.size(), "Block 0 should have 1 transaction");
            assertEquals(1500L, block0Items.get(0).record().consensusTimestamp().seconds());
        }

        @Test
        @DisplayName("Indexes transaction at exact block boundary to that block")
        void testTransactionAtExactBlockTime() throws IOException {
            // Block times: block 0 at 1000s, block 1 at 2000s
            writeBlockTimes(1000_000_000_000L, 2000_000_000_000L);

            // Transaction at exactly 2000s should be in block 1
            writeTransactions(createTestRecordStreamItem(2000L, 0));

            MissingTransactionsIndex index = createIndex();

            assertFalse(index.hasTransactionsForBlock(0), "Block 0 should not have transactions");
            assertTrue(index.hasTransactionsForBlock(1), "Block 1 should have transactions");
        }

        @Test
        @DisplayName("Indexes multiple transactions to different blocks")
        void testMultipleTransactionsToMultipleBlocks() throws IOException {
            // Block times: block 0 at 1000s, block 1 at 2000s, block 2 at 3000s
            writeBlockTimes(1000_000_000_000L, 2000_000_000_000L, 3000_000_000_000L);

            // Transactions in different blocks
            writeTransactions(
                    createTestRecordStreamItem(1100L, 0), // Block 0
                    createTestRecordStreamItem(1200L, 0), // Block 0
                    createTestRecordStreamItem(2500L, 0) // Block 1
                    );

            MissingTransactionsIndex index = createIndex();

            assertEquals(2, index.getTransactionsForBlock(0).size(), "Block 0 should have 2 transactions");
            assertEquals(1, index.getTransactionsForBlock(1).size(), "Block 1 should have 1 transaction");
            assertTrue(index.getTransactionsForBlock(2).isEmpty(), "Block 2 should have no transactions");
        }

        @Test
        @DisplayName("Returns sorted transactions within each block")
        void testTransactionsSortedWithinBlock() throws IOException {
            // Block times: block 0 at 1000s, block 1 at 2000s
            writeBlockTimes(1000_000_000_000L, 2000_000_000_000L);

            // Transactions in block 0, written out of order
            writeTransactions(
                    createTestRecordStreamItem(1300L, 0),
                    createTestRecordStreamItem(1100L, 0),
                    createTestRecordStreamItem(1200L, 0));

            MissingTransactionsIndex index = createIndex();

            List<RecordStreamItem> block0Items = index.getTransactionsForBlock(0);
            assertEquals(3, block0Items.size());
            assertEquals(1100L, block0Items.get(0).record().consensusTimestamp().seconds(), "First should be earliest");
            assertEquals(1200L, block0Items.get(1).record().consensusTimestamp().seconds(), "Second should be middle");
            assertEquals(1300L, block0Items.get(2).record().consensusTimestamp().seconds(), "Third should be latest");
        }
    }

    @Nested
    @DisplayName("Query methods")
    class QueryMethodsTests {

        @Test
        @DisplayName("getTransactionsForBlock returns empty list for blocks without transactions")
        void testGetTransactionsForBlockReturnsEmptyList() throws IOException {
            writeBlockTimes(1000_000_000_000L, 2000_000_000_000L);
            writeTransactions(createTestRecordStreamItem(1500L, 0));

            MissingTransactionsIndex index = createIndex();

            List<RecordStreamItem> result = index.getTransactionsForBlock(1);
            assertNotNull(result, "Should return non-null list");
            assertTrue(result.isEmpty(), "Should return empty list for block without transactions");
        }

        @Test
        @DisplayName("getBlocksWithTransactions returns correct set")
        void testGetBlocksWithTransactions() throws IOException {
            writeBlockTimes(1000_000_000_000L, 2000_000_000_000L, 3000_000_000_000L, 4000_000_000_000L);
            writeTransactions(
                    createTestRecordStreamItem(1500L, 0), // Block 0
                    createTestRecordStreamItem(3500L, 0) // Block 2
                    );

            MissingTransactionsIndex index = createIndex();

            Set<Long> blocksWithTx = index.getBlocksWithTransactions();
            assertEquals(2, blocksWithTx.size(), "Should have 2 blocks with transactions");
            assertTrue(blocksWithTx.contains(0L), "Should contain block 0");
            assertTrue(blocksWithTx.contains(2L), "Should contain block 2");
            assertFalse(blocksWithTx.contains(1L), "Should not contain block 1");
        }

        @Test
        @DisplayName("getTotalTransactionCount returns correct count")
        void testGetTotalTransactionCount() throws IOException {
            writeBlockTimes(1000_000_000_000L, 2000_000_000_000L);
            writeTransactions(
                    createTestRecordStreamItem(1100L, 0),
                    createTestRecordStreamItem(1200L, 0),
                    createTestRecordStreamItem(1300L, 0));

            MissingTransactionsIndex index = createIndex();

            assertEquals(3, index.getTotalTransactionCount());
        }

        @Test
        @DisplayName("getBlockCount returns number of blocks with transactions")
        void testGetBlockCount() throws IOException {
            writeBlockTimes(1000_000_000_000L, 2000_000_000_000L, 3000_000_000_000L);
            writeTransactions(
                    createTestRecordStreamItem(1500L, 0), // Block 0
                    createTestRecordStreamItem(2500L, 0) // Block 1
                    );

            MissingTransactionsIndex index = createIndex();

            assertEquals(2, index.getBlockCount(), "Should have 2 blocks with transactions");
        }
    }

    @Nested
    @DisplayName("Edge cases")
    class EdgeCaseTests {

        @Test
        @DisplayName("Handles transaction in first block (block 0)")
        void testTransactionInFirstBlock() throws IOException {
            writeBlockTimes(1000_000_000_000L, 2000_000_000_000L);
            writeTransactions(createTestRecordStreamItem(1000L, 500)); // At block 0 start + 500 nanos

            MissingTransactionsIndex index = createIndex();

            assertTrue(index.hasTransactionsForBlock(0));
            assertEquals(1, index.getTransactionsForBlock(0).size());
        }

        @Test
        @DisplayName("Handles empty transactions file")
        void testEmptyTransactionsFile() throws IOException {
            writeBlockTimes(1000_000_000_000L, 2000_000_000_000L);

            // Write empty gzipped file
            try (DataOutputStream out =
                    new DataOutputStream(new GZIPOutputStream(Files.newOutputStream(missingTransactionsFile)))) {
                // Write nothing
            }

            MissingTransactionsIndex index = createIndex();

            assertEquals(0, index.getTotalTransactionCount());
            assertEquals(0, index.getBlockCount());
        }
    }

    // ========== Helper Methods ==========

    private MissingTransactionsIndex createIndex() throws IOException {
        MissingTransactionsLoader loader = new MissingTransactionsLoader(missingTransactionsFile);
        BlockTimeReader blockTimeReader = new BlockTimeReader(blockTimesFile);
        return new MissingTransactionsIndex(loader, blockTimeReader);
    }

    private RecordStreamItem createTestRecordStreamItem(long seconds, int nanos) {
        Timestamp timestamp = new Timestamp(seconds, nanos);
        TransactionRecord record =
                TransactionRecord.newBuilder().consensusTimestamp(timestamp).build();
        Transaction transaction = Transaction.newBuilder().build();
        return new RecordStreamItem(transaction, record);
    }

    private void writeTransactions(RecordStreamItem... items) throws IOException {
        try (DataOutputStream out =
                new DataOutputStream(new GZIPOutputStream(Files.newOutputStream(missingTransactionsFile)))) {
            for (RecordStreamItem item : items) {
                byte[] itemBytes = RecordStreamItem.PROTOBUF.toBytes(item).toByteArray();
                out.writeInt(itemBytes.length);
                out.write(itemBytes);
            }
        }
    }

    /**
     * Writes block times to the block_times.bin file.
     * Each time is stored as a long (8 bytes) representing nanoseconds since epoch.
     */
    private void writeBlockTimes(long... timesInNanos) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(timesInNanos.length * Long.BYTES);
        for (long time : timesInNanos) {
            buffer.putLong(time);
        }
        buffer.flip();

        try (FileChannel channel =
                FileChannel.open(blockTimesFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            channel.write(buffer);
        }
    }
}
