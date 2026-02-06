// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import com.hedera.hapi.streams.RecordStreamItem;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * Loads and caches missing transactions from the missing_transactions.gz file.
 *
 * <p>The file format is a sequence of length-prefixed RecordStreamItem protobufs,
 * where each entry consists of a 4-byte big-endian length followed by the protobuf bytes.
 * This file is produced by the {@code fetchMissingTransactions} command.
 *
 * <p>This class provides a singleton pattern for efficient reuse, as the missing
 * transactions data is static and only needs to be loaded once per JVM session.
 */
public class MissingTransactionsLoader {

    /** Default path to the missing transactions file */
    public static final Path DEFAULT_MISSING_TRANSACTIONS_FILE = Path.of("missing_transactions.gz");

    /** Singleton instance */
    private static MissingTransactionsLoader instance;

    /** List of all missing transactions, sorted by consensus timestamp */
    private final List<RecordStreamItem> missingTransactions;

    /**
     * Creates a MissingTransactionsLoader by loading transactions from the specified file.
     *
     * @param missingTransactionsFile the path to the missing_transactions.gz file
     * @throws IOException if the file cannot be read or parsed
     */
    public MissingTransactionsLoader(Path missingTransactionsFile) throws IOException {
        this.missingTransactions = loadTransactions(missingTransactionsFile);
    }

    /**
     * Gets the singleton instance, loading from the default file location if needed.
     *
     * @return the singleton instance
     * @throws UncheckedIOException if the file cannot be loaded
     */
    public static synchronized MissingTransactionsLoader getInstance() {
        if (instance == null) {
            try {
                instance = new MissingTransactionsLoader(DEFAULT_MISSING_TRANSACTIONS_FILE);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to load missing transactions from default location", e);
            }
        }
        return instance;
    }

    /**
     * Gets the singleton instance, loading from the specified file if not already loaded.
     *
     * @param missingTransactionsFile the path to the missing_transactions.gz file
     * @return the singleton instance
     * @throws UncheckedIOException if the file cannot be loaded
     */
    public static synchronized MissingTransactionsLoader getInstance(Path missingTransactionsFile) {
        if (instance == null) {
            try {
                instance = new MissingTransactionsLoader(missingTransactionsFile);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to load missing transactions", e);
            }
        }
        return instance;
    }

    /**
     * Checks if the missing transactions file exists at the default location.
     *
     * @return true if the file exists
     */
    public static boolean defaultFileExists() {
        return Files.exists(DEFAULT_MISSING_TRANSACTIONS_FILE);
    }

    /**
     * Gets all missing transactions.
     *
     * @return unmodifiable list of all missing transactions
     */
    public List<RecordStreamItem> getAllTransactions() {
        return missingTransactions;
    }

    /**
     * Gets the number of missing transactions.
     *
     * @return the count of missing transactions
     */
    public int getTransactionCount() {
        return missingTransactions.size();
    }

    /**
     * Loads transactions from the gzipped file.
     *
     * <p>Each entry in the file is a 4-byte big-endian length prefix followed by
     * RecordStreamItem protobuf bytes.
     *
     * @param file the path to the missing_transactions.gz file
     * @return list of loaded RecordStreamItem objects
     * @throws IOException if the file cannot be read or parsed
     */
    private List<RecordStreamItem> loadTransactions(Path file) throws IOException {
        if (!Files.exists(file)) {
            System.out.println("Missing transactions file not found: " + file.toAbsolutePath());
            return Collections.emptyList();
        }

        List<RecordStreamItem> items = new ArrayList<>();
        try (InputStream fis = Files.newInputStream(file);
                GZIPInputStream gzis = new GZIPInputStream(fis);
                DataInputStream dis = new DataInputStream(gzis)) {

            while (true) {
                // Read the 4-byte length prefix
                int length;
                try {
                    length = dis.readInt();
                } catch (java.io.EOFException e) {
                    // Normal end of file
                    break;
                }

                if (length <= 0) {
                    throw new IOException("Invalid record length: " + length);
                }

                // Read the protobuf bytes
                byte[] protoBytes = new byte[length];
                dis.readFully(protoBytes);

                // Parse the RecordStreamItem
                try {
                    RecordStreamItem item = RecordStreamItem.PROTOBUF.parse(Bytes.wrap(protoBytes));
                    items.add(item);
                } catch (ParseException e) {
                    throw new IOException("Failed to parse RecordStreamItem", e);
                }
            }
        }

        // Sort by consensus timestamp (seconds then nanos)
        items.sort((a, b) -> {
            if (a.record() == null || a.record().consensusTimestamp() == null) return -1;
            if (b.record() == null || b.record().consensusTimestamp() == null) return 1;
            var tsA = a.record().consensusTimestamp();
            var tsB = b.record().consensusTimestamp();
            int cmp = Long.compare(tsA.seconds(), tsB.seconds());
            if (cmp != 0) return cmp;
            return Integer.compare(tsA.nanos(), tsB.nanos());
        });

        System.out.println("Loaded " + items.size() + " missing transactions from " + file);
        return Collections.unmodifiableList(items);
    }
}
