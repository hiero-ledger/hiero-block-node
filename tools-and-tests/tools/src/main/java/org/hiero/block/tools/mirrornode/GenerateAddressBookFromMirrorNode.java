// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.mirrornode;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.pbj.runtime.io.stream.ReadableStreamingData;
import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;
import org.hiero.block.internal.AddressBookHistory;
import org.hiero.block.internal.DatedNodeAddressBook;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Generate address book history JSON from Mirror Node CSV export.
 */
@Command(
        name = "generateAddressBook",
        description = "Generate address book history JSON from Mirror Node CSV export")
public class GenerateAddressBookFromMirrorNode implements Runnable {
    private static final String BUCKET_NAME = "mirrornode-db-export";
    private static final String ADDRESS_BOOK_PATH = "/address_book/address_book.csv.gz";
    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");
    private static final HexFormat HEX_FORMAT = HexFormat.of();

    @Option(
            names = {"--output", "-o"},
            description = "Output path for address book history JSON file",
            defaultValue = "data/addressBookHistory.json")
    private Path outputFile;

    @Option(
            names = {"--temp-dir"},
            description = "Temporary directory for downloading CSV file",
            defaultValue = "data/temp")
    private Path tempDir;

    @Option(
            names = {"--keep-duplicates"},
            description = "Keep all entries even if same timestamp appears multiple times (default: false, keeps latest)")
    private boolean keepDuplicates = false;

    @Override
    public void run() {
        try {
            GoogleCredentials.getApplicationDefault();
            String projectId = ServiceOptions.getDefaultProjectId();
            if (projectId == null) {
                throw new RuntimeException("Project ID not found. Please configure Google Cloud credentials.");
            }

            Files.createDirectories(tempDir);

            Storage storage = createStorageClient(projectId);
            String latestVersion = findLatestVersion(storage, projectId);
            System.out.println("Latest version: " + latestVersion);

            Path csvFile = downloadAddressBookCsv(storage, projectId, latestVersion);
            List<DatedNodeAddressBook> addressBooks = parseAddressBookCsv(csvFile);
            System.out.println("Parsed " + addressBooks.size() + " address book entries");

            writeAddressBookHistory(addressBooks);
            System.out.println("Address book history written to: " + outputFile);

            Files.deleteIfExists(csvFile);
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private Storage createStorageClient(String projectId) {
        return StorageOptions.grpc()
                .setAttemptDirectPath(false)
                .setProjectId(projectId)
                .build()
                .getService();
    }

    private String findLatestVersion(Storage storage, String projectId) {
        return storage.list(
                        BUCKET_NAME,
                        Storage.BlobListOption.currentDirectory(),
                        Storage.BlobListOption.userProject(projectId))
                .streamAll()
                .map(Blob::getName)
                .map(name -> name.replaceAll("/$", ""))
                .filter(name -> MirrorNodeUtils.SYMANTIC_VERSION_PATTERN.matcher(name).matches())
                .max(Comparator.comparingLong(MirrorNodeUtils::parseSymantecVersion))
                .orElseThrow(() -> new RuntimeException("No version directories found"));
    }

    private Path downloadAddressBookCsv(Storage storage, String projectId, String version) throws IOException {
        String blobName = version + ADDRESS_BOOK_PATH;
        System.out.println("Downloading: gs://" + BUCKET_NAME + "/" + blobName);

        byte[] content = storage.readAllBytes(BUCKET_NAME, blobName, Storage.BlobSourceOption.userProject(projectId));
        if (content.length == 0) {
            throw new IOException("Address book CSV is empty: " + blobName);
        }

        Path csvFile = tempDir.resolve("address_book.csv.gz");
        Files.write(csvFile, content);
        System.out.println("Downloaded " + content.length + " bytes");
        return csvFile;
    }

    private List<DatedNodeAddressBook> parseAddressBookCsv(Path csvFile) throws IOException {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new GZIPInputStream(Files.newInputStream(csvFile))))) {

            String[] headers = parseHeader(reader);
            int timestampIdx = findColumnIndex(headers, "start_consensus_timestamp");
            int fileDataIdx = findColumnIndex(headers, "file_data");
            int fileIdIdx = findColumnIndex(headers, "file_id");

            return parseRows(reader, timestampIdx, fileDataIdx, fileIdIdx);
        }
    }

    private String[] parseHeader(BufferedReader reader) throws IOException {
        return java.util.Optional.ofNullable(reader.readLine())
                .map(line -> line.split(","))
                .orElseThrow(() -> new IOException("CSV file is empty"));
    }

    private int findColumnIndex(String[] headers, String columnName) throws IOException {
        return java.util.stream.IntStream.range(0, headers.length)
                .filter(i -> headers[i].equals(columnName))
                .findFirst()
                .orElseThrow(() -> new IOException("Required column not found: " + columnName));
    }

    private List<DatedNodeAddressBook> parseRows(BufferedReader reader, int timestampIdx, int fileDataIdx, int fileIdIdx)
            throws IOException {
        System.out.println("Processing CSV rows...");

        final java.util.concurrent.atomic.AtomicInteger rowCount = new java.util.concurrent.atomic.AtomicInteger(0);
        final java.util.concurrent.atomic.AtomicInteger duplicateCount = new java.util.concurrent.atomic.AtomicInteger(0);
        final java.util.concurrent.atomic.AtomicInteger filteredCount = new java.util.concurrent.atomic.AtomicInteger(0);

        List<DatedNodeAddressBook> addressBooks = reader.lines()
                .peek(line -> {
                    int count = rowCount.incrementAndGet();
                    if (count % 100 == 0) {
                        System.out.print("\rProcessed " + count + " rows");
                    }
                })
                .filter(line -> {
                    // Filter by file_id = 102 (address book file 0.0.102)
                    String[] columns = line.split(",", -1);
                    if (fileIdIdx >= columns.length) return false;
                    String fileId = columns[fileIdIdx];
                    if (!"102".equals(fileId)) {
                        filteredCount.incrementAndGet();
                        return false;
                    }
                    return true;
                })
                .map(line -> {
                    try {
                        String[] columns = line.split(",", -1);
                        return parseRow(columns, timestampIdx, fileDataIdx);
                    } catch (Exception e) {
                        System.err.println("\nWarning: Failed to parse row " + rowCount.get() + ": " + e.getMessage());
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .sorted(Comparator.comparing(this::extractInstant))
                .toList();

        System.out.println("\nCompleted processing " + rowCount.get() + " rows");
        if (filteredCount.get() > 0) {
            System.out.println("Filtered out " + filteredCount.get() + " rows (non-address-book files, keeping only file_id=102)");
        }

        // Handle duplicates
        if (!keepDuplicates) {
            List<DatedNodeAddressBook> deduplicated = deduplicateByTimestamp(addressBooks, duplicateCount);
            if (duplicateCount.get() > 0) {
                System.out.println("Removed " + duplicateCount.get() + " duplicate entries (kept latest for each timestamp)");
            }
            return deduplicated;
        }

        return addressBooks;
    }

    private List<DatedNodeAddressBook> deduplicateByTimestamp(
            List<DatedNodeAddressBook> addressBooks, AtomicInteger duplicateCount) {
        Map<Instant, DatedNodeAddressBook> byTimestamp = new LinkedHashMap<>();

        for (DatedNodeAddressBook dab : addressBooks) {
            Instant instant = extractInstant(dab);
            if (byTimestamp.containsKey(instant)) {
                duplicateCount.incrementAndGet();
            }
            byTimestamp.put(instant, dab); // Keeps latest entry for duplicate timestamps
        }

        return new ArrayList<>(byTimestamp.values());
    }

    private Instant extractInstant(DatedNodeAddressBook dab) {
        Timestamp ts = dab.blockTimestampOrThrow();
        return Instant.ofEpochSecond(ts.seconds(), ts.nanos());
    }

    private DatedNodeAddressBook parseRow(String[] columns, int timestampIdx, int fileDataIdx) throws Exception {
        // Parse timestamp - can be either nanoseconds (e.g., "1758733200632122897") or formatted string
        String timestampStr = columns[timestampIdx];

        // Skip empty or null timestamps
        if (timestampStr == null || timestampStr.trim().isEmpty()) {
            throw new IllegalArgumentException("Empty timestamp");
        }

        Timestamp timestamp;

        if (timestampStr.matches("\\d+")) {
            // Numeric timestamp in nanoseconds since epoch
            long nanos = Long.parseLong(timestampStr);

            // Validate timestamp is reasonable (after 2019-01-01 and before 2100-01-01)
            long seconds = nanos / 1_000_000_000L;
            if (seconds < 1546300800L || seconds > 4102444800L) {
                throw new IllegalArgumentException("Timestamp out of valid range: " + seconds + " (" + timestampStr + ")");
            }

            int remainingNanos = (int) (nanos % 1_000_000_000L);
            timestamp = Timestamp.newBuilder()
                    .seconds(seconds)
                    .nanos(remainingNanos)
                    .build();
        } else {
            // Formatted string: "2019-09-13 21:53:51.396440003"
            LocalDateTime ldt = LocalDateTime.parse(timestampStr, TIMESTAMP_FORMATTER);
            Instant instant = ldt.toInstant(ZoneOffset.UTC);
            timestamp = Timestamp.newBuilder()
                    .seconds(instant.getEpochSecond())
                    .nanos(instant.getNano())
                    .build();
        }

        // Decode hex file_data
        String hexData = columns[fileDataIdx];
        if (hexData.startsWith("\\x")) {
            hexData = hexData.substring(2);
        }
        byte[] fileDataBytes = HEX_FORMAT.parseHex(hexData);

        // Parse protobuf
        NodeAddressBook addressBook = NodeAddressBook.PROTOBUF.parse(
                new ReadableStreamingData(new ByteArrayInputStream(fileDataBytes)));

        return new DatedNodeAddressBook(timestamp, addressBook);
    }

    private void writeAddressBookHistory(List<DatedNodeAddressBook> addressBooks) throws IOException {
        Path parentDir = outputFile.getParent();
        if (parentDir != null) {
            Files.createDirectories(parentDir);
        }

        try (WritableStreamingData out = new WritableStreamingData(Files.newOutputStream(outputFile))) {
            AddressBookHistory history = new AddressBookHistory(addressBooks);
            AddressBookHistory.JSON.write(history, out);
        }
    }
}
