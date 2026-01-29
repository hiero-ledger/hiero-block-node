// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.mirrornode;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.hedera.hapi.node.base.Transaction;
import com.hedera.hapi.node.transaction.TransactionRecord;
import com.hedera.hapi.streams.RecordStreamItem;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Fetch missing transactions errata data from the hiero-mirror-node repository
 * and produce a single gzipped file of length-prefixed RecordStreamItem protobufs.
 *
 * <p>Each .bin file from the mirror-node errata contains two length-prefixed
 * protobuf records: a TransactionRecord followed by a Transaction. This command
 * downloads all .bin files, parses each pair, combines them into a
 * RecordStreamItem, and writes all items sequentially into a single gzipped
 * file.
 *
 * <p>The output file contains sequentially written records, each preceded by a
 * 4-byte big-endian length prefix. The amendment provider can read this file
 * by opening a GZIPInputStream and reading length-prefixed RecordStreamItem
 * protobufs in a loop until EOF.
 *
 * <p>The consensus timestamp for each record is embedded in the
 * TransactionRecord inside the RecordStreamItem itself, so no external index
 * is needed.
 */
@SuppressWarnings("CallToPrintStackTrace")
@Command(
        name = "fetchMissingTransactions",
        description = "Fetch missing transactions errata from hiero-mirror-node and produce a single"
                + " gzipped file of RecordStreamItem protobufs",
        mixinStandardHelpOptions = true)
public class FetchMissingTransactions implements Runnable {

    /** GitHub API URL for the missing transactions directory */
    private static final String GITHUB_API_URL =
            "https://api.github.com/repos/hiero-ledger/hiero-mirror-node/contents/importer/src/main/resources/errata/mainnet/missingtransactions";

    /** Base URL for raw file downloads */
    private static final String RAW_CONTENT_BASE_URL =
            "https://raw.githubusercontent.com/hiero-ledger/hiero-mirror-node/main/importer/src/main/resources/errata/mainnet/missingtransactions/";

    @Option(
            names = {"-o", "--output"},
            description = "Output gzipped file path (default: missing_transactions.gz)")
    private Path outputFile = Path.of("missing_transactions.gz");

    @Option(
            names = {"--json"},
            description = "Also generate a JSON debug file with parsed transaction details")
    private Path jsonFile;

    private final HttpClient httpClient =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();

    private final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    @Override
    public void run() {
        try {
            System.out.println("Fetching missing transactions errata from hiero-mirror-node repository...");

            // Get list of files from GitHub API
            final List<String> fileNames = fetchFileList();
            System.out.println("Found " + fileNames.size() + " missing transaction files");

            // Download all files, parse into RecordStreamItems, and write to a single gzipped file
            final JsonArray jsonArray = jsonFile != null ? new JsonArray() : null;
            int totalItems = 0;
            int fileCount = 0;
            int errorCount = 0;

            try (DataOutputStream out = new DataOutputStream(new GZIPOutputStream(Files.newOutputStream(outputFile)))) {

                for (int i = 0; i < fileNames.size(); i++) {
                    final String fileName = fileNames.get(i);
                    System.out.printf("[%d/%d] Processing %s... ", i + 1, fileNames.size(), fileName);

                    try {
                        final byte[] data = downloadFile(fileName);
                        final RecordStreamItem item = parseBinFile(data);

                        // Write RecordStreamItem as length-prefixed protobuf
                        final byte[] itemBytes =
                                RecordStreamItem.PROTOBUF.toBytes(item).toByteArray();
                        out.writeInt(itemBytes.length);
                        out.write(itemBytes);
                        totalItems++;

                        final String timestamp = item.record() != null
                                        && item.record().consensusTimestamp() != null
                                ? item.record().consensusTimestamp().seconds() + "."
                                        + item.record().consensusTimestamp().nanos()
                                : "unknown";
                        System.out.println("OK (timestamp=" + timestamp + ", " + data.length + " bytes)");
                        fileCount++;

                        // If JSON debug output requested, add entry
                        if (jsonArray != null) {
                            jsonArray.add(buildJsonEntry(fileName, item));
                        }
                    } catch (Exception e) {
                        errorCount++;
                        System.out.println("ERROR: " + e.getMessage());
                    }
                }
            }

            // Write JSON debug file if requested
            if (jsonFile != null && jsonArray != null) {
                Files.writeString(jsonFile, gson.toJson(jsonArray));
                System.out.println("\nJSON debug file: " + jsonFile.toAbsolutePath());
            }

            System.out.println("\n" + "=".repeat(60));
            System.out.println("Summary:");
            System.out.println("  Source files:    " + fileNames.size());
            System.out.println("  Successful:      " + fileCount);
            System.out.println("  Errors:          " + errorCount);
            System.out.println("  Total items:     " + totalItems);
            System.out.println("  Output file:     " + outputFile.toAbsolutePath());
            System.out.println("  Output size:     " + Files.size(outputFile) + " bytes");
            System.out.println("=".repeat(60));

        } catch (Exception e) {
            System.err.println("Fatal error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Parse a .bin errata file into a RecordStreamItem.
     *
     * <p>Each .bin file contains two length-prefixed protobuf records:
     * <ol>
     *   <li>A TransactionRecord (4-byte big-endian length + protobuf bytes)</li>
     *   <li>A Transaction (4-byte big-endian length + protobuf bytes)</li>
     * </ol>
     * These are combined into a single RecordStreamItem.
     *
     * @param data the raw binary data from a .bin file
     * @return the combined RecordStreamItem
     * @throws IOException if the file cannot be parsed
     */
    private RecordStreamItem parseBinFile(final byte[] data) throws IOException, ParseException {
        int offset = 0;

        // Read first record: TransactionRecord
        if (offset + 4 > data.length) {
            throw new IOException("File too short for first length prefix");
        }
        final int recordLength = readInt(data, offset);
        offset += 4;
        if (recordLength <= 0 || offset + recordLength > data.length) {
            throw new IOException("Invalid TransactionRecord length: " + recordLength);
        }
        final byte[] recordBytes = new byte[recordLength];
        System.arraycopy(data, offset, recordBytes, 0, recordLength);
        final TransactionRecord transactionRecord = TransactionRecord.PROTOBUF.parse(Bytes.wrap(recordBytes));
        offset += recordLength;

        // Read second record: Transaction
        if (offset + 4 > data.length) {
            throw new IOException("File too short for second length prefix");
        }
        final int txLength = readInt(data, offset);
        offset += 4;
        if (txLength <= 0 || offset + txLength > data.length) {
            throw new IOException("Invalid Transaction length: " + txLength);
        }
        final byte[] txBytes = new byte[txLength];
        System.arraycopy(data, offset, txBytes, 0, txLength);
        final Transaction transaction = Transaction.PROTOBUF.parse(Bytes.wrap(txBytes));

        return new RecordStreamItem(transaction, transactionRecord);
    }

    /**
     * Read a 4-byte big-endian integer from the given byte array at the specified offset.
     */
    private static int readInt(final byte[] data, final int offset) {
        return ((data[offset] & 0xFF) << 24)
                | ((data[offset + 1] & 0xFF) << 16)
                | ((data[offset + 2] & 0xFF) << 8)
                | (data[offset + 3] & 0xFF);
    }

    /**
     * Fetch the list of .bin file names from the GitHub API.
     */
    private List<String> fetchFileList() throws IOException, InterruptedException {
        final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(GITHUB_API_URL))
                .header("Accept", "application/vnd.github.v3+json")
                .header("User-Agent", "hiero-block-tools")
                .GET()
                .build();

        final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            throw new IOException("GitHub API returned status " + response.statusCode() + ": " + response.body());
        }

        final JsonArray files = gson.fromJson(response.body(), JsonArray.class);
        final List<String> fileNames = new ArrayList<>();

        for (JsonElement element : files) {
            final JsonObject file = element.getAsJsonObject();
            final String name = file.get("name").getAsString();
            if (name.endsWith(".bin")) {
                fileNames.add(name);
            }
        }

        return fileNames;
    }

    /**
     * Download a file from the raw GitHub content URL.
     */
    private byte[] downloadFile(final String fileName) throws IOException, InterruptedException {
        final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(RAW_CONTENT_BASE_URL + fileName))
                .header("User-Agent", "hiero-block-tools")
                .GET()
                .build();

        final HttpResponse<InputStream> response = httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());

        if (response.statusCode() != 200) {
            throw new IOException("Failed to download " + fileName + ": HTTP " + response.statusCode());
        }

        try (InputStream is = response.body()) {
            return is.readAllBytes();
        }
    }

    /**
     * Build a JSON debug entry for a single source file and its parsed RecordStreamItem.
     */
    private JsonObject buildJsonEntry(final String fileName, final RecordStreamItem item) {
        final JsonObject entry = new JsonObject();
        entry.addProperty("sourceFile", fileName);
        if (item.record() != null && item.record().consensusTimestamp() != null) {
            entry.addProperty(
                    "consensusTimestamp",
                    item.record().consensusTimestamp().seconds() + "."
                            + item.record().consensusTimestamp().nanos());
        }
        entry.addProperty("hasTransaction", item.transaction() != null);
        entry.addProperty("hasRecord", item.record() != null);
        return entry;
    }
}
