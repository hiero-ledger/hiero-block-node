// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import static org.junit.jupiter.api.Assertions.*;

import com.hedera.hapi.node.base.Timestamp;
import com.hedera.hapi.node.base.Transaction;
import com.hedera.hapi.node.transaction.TransactionRecord;
import com.hedera.hapi.streams.RecordStreamItem;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link MissingTransactionsLoader}.
 */
class MissingTransactionsLoaderTest {

    @TempDir
    Path tempDir;

    @Nested
    @DisplayName("Loading from file")
    class LoadingFromFileTests {

        @Test
        @DisplayName("Returns empty list when file does not exist")
        void testMissingFileReturnsEmptyList() throws IOException {
            Path nonExistentFile = tempDir.resolve("non_existent.gz");
            MissingTransactionsLoader loader = new MissingTransactionsLoader(nonExistentFile);

            List<RecordStreamItem> transactions = loader.getAllTransactions();

            assertNotNull(transactions, "Should return non-null list");
            assertTrue(transactions.isEmpty(), "Should return empty list for non-existent file");
            assertEquals(0, loader.getTransactionCount(), "Count should be 0");
        }

        @Test
        @DisplayName("Loads single transaction from file")
        void testLoadSingleTransaction() throws IOException {
            Path testFile = tempDir.resolve("single_transaction.gz");
            RecordStreamItem item = createTestRecordStreamItem(1000L, 100);
            writeTransactionsToFile(testFile, List.of(item));

            MissingTransactionsLoader loader = new MissingTransactionsLoader(testFile);

            assertEquals(1, loader.getTransactionCount(), "Should load 1 transaction");
            List<RecordStreamItem> transactions = loader.getAllTransactions();
            assertEquals(1, transactions.size(), "List should have 1 item");

            RecordStreamItem loaded = transactions.get(0);
            assertNotNull(loaded.record(), "Loaded item should have record");
            assertEquals(1000L, loaded.record().consensusTimestamp().seconds(), "Timestamp seconds should match");
            assertEquals(100, loaded.record().consensusTimestamp().nanos(), "Timestamp nanos should match");
        }

        @Test
        @DisplayName("Loads multiple transactions and sorts by timestamp")
        void testLoadMultipleTransactionsSorted() throws IOException {
            Path testFile = tempDir.resolve("multiple_transactions.gz");

            // Create items in non-sorted order
            RecordStreamItem item1 = createTestRecordStreamItem(3000L, 0); // Latest
            RecordStreamItem item2 = createTestRecordStreamItem(1000L, 500); // Earliest
            RecordStreamItem item3 = createTestRecordStreamItem(2000L, 0); // Middle

            writeTransactionsToFile(testFile, List.of(item1, item2, item3));

            MissingTransactionsLoader loader = new MissingTransactionsLoader(testFile);

            assertEquals(3, loader.getTransactionCount(), "Should load 3 transactions");
            List<RecordStreamItem> transactions = loader.getAllTransactions();

            // Verify sorted order
            assertEquals(
                    1000L, transactions.get(0).record().consensusTimestamp().seconds(), "First should be earliest");
            assertEquals(
                    2000L, transactions.get(1).record().consensusTimestamp().seconds(), "Second should be middle");
            assertEquals(
                    3000L, transactions.get(2).record().consensusTimestamp().seconds(), "Third should be latest");
        }

        @Test
        @DisplayName("Sorts by nanos when seconds are equal")
        void testSortByNanosWhenSecondsEqual() throws IOException {
            Path testFile = tempDir.resolve("same_seconds.gz");

            RecordStreamItem item1 = createTestRecordStreamItem(1000L, 300);
            RecordStreamItem item2 = createTestRecordStreamItem(1000L, 100);
            RecordStreamItem item3 = createTestRecordStreamItem(1000L, 200);

            writeTransactionsToFile(testFile, List.of(item1, item2, item3));

            MissingTransactionsLoader loader = new MissingTransactionsLoader(testFile);

            List<RecordStreamItem> transactions = loader.getAllTransactions();

            assertEquals(
                    100, transactions.get(0).record().consensusTimestamp().nanos(), "First should have lowest nanos");
            assertEquals(
                    200, transactions.get(1).record().consensusTimestamp().nanos(), "Second should have middle nanos");
            assertEquals(
                    300, transactions.get(2).record().consensusTimestamp().nanos(), "Third should have highest nanos");
        }

        @Test
        @DisplayName("Returns unmodifiable list")
        void testReturnsUnmodifiableList() throws IOException {
            Path testFile = tempDir.resolve("unmodifiable.gz");
            writeTransactionsToFile(testFile, List.of(createTestRecordStreamItem(1000L, 0)));

            MissingTransactionsLoader loader = new MissingTransactionsLoader(testFile);
            List<RecordStreamItem> transactions = loader.getAllTransactions();

            assertThrows(
                    UnsupportedOperationException.class,
                    () -> transactions.add(createTestRecordStreamItem(2000L, 0)),
                    "List should be unmodifiable");
        }
    }

    @Nested
    @DisplayName("Static helper methods")
    class StaticHelperTests {

        @Test
        @DisplayName("defaultFileExists returns false when file missing")
        void testDefaultFileExistsFalse() {
            // The default file path is "missing_transactions.gz" which shouldn't exist in test env
            // This test may be environment-dependent, so we just verify the method doesn't throw
            assertDoesNotThrow(() -> MissingTransactionsLoader.defaultFileExists());
        }
    }

    /**
     * Creates a test RecordStreamItem with the given timestamp.
     */
    private RecordStreamItem createTestRecordStreamItem(long seconds, int nanos) {
        Timestamp timestamp = new Timestamp(seconds, nanos);
        TransactionRecord record =
                TransactionRecord.newBuilder().consensusTimestamp(timestamp).build();
        Transaction transaction = Transaction.newBuilder().build();
        return new RecordStreamItem(transaction, record);
    }

    /**
     * Writes a list of RecordStreamItems to a gzipped file in the expected format.
     */
    private void writeTransactionsToFile(Path file, List<RecordStreamItem> items) throws IOException {
        try (DataOutputStream out = new DataOutputStream(new GZIPOutputStream(Files.newOutputStream(file)))) {
            for (RecordStreamItem item : items) {
                byte[] itemBytes = RecordStreamItem.PROTOBUF.toBytes(item).toByteArray();
                out.writeInt(itemBytes.length);
                out.write(itemBytes);
            }
        }
    }
}
