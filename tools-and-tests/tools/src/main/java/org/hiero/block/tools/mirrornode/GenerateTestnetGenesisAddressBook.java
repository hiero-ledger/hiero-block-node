// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.mirrornode;

import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.stream.ReadableStreamingData;
import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HexFormat;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Generates the testnet genesis address book protobuf binary file from a mirror node database
 * CSV export.
 *
 * <p>The CSV file is an export of the {@code address_book} table from the mirror node database.
 * It must have columns {@code start_consensus_timestamp} and {@code file_data}. The genesis
 * entry has {@code start_consensus_timestamp = 1} and {@code file_data} contains the
 * hex-encoded {@link NodeAddressBook} protobuf (prefixed with {@code \x}).
 *
 * <p>Usage example:
 * <pre>
 *   subcommands mirror generateTestnetAddressBook --csv addressbook.csv -o testnet-genesis-address-book.proto.bin
 * </pre>
 */
@Command(
        name = "generateTestnetAddressBook",
        description = "Generate testnet genesis address book proto binary from mirror node CSV export")
public class GenerateTestnetGenesisAddressBook implements Runnable {

    private static final HexFormat HEX_FORMAT = HexFormat.of();

    @Option(
            names = {"-o", "--output"},
            description = "Output file path for the proto binary",
            defaultValue = "testnet-genesis-address-book.proto.bin")
    private Path outputFile;

    @Option(
            names = {"--csv"},
            required = true,
            description = "Path to mirror node address book CSV export (columns: start_consensus_timestamp,file_data)")
    private Path csvFile;

    @Override
    @SuppressWarnings("DataFlowIssue")
    public void run() {
        try {
            NodeAddressBook addressBook = loadFromCsv(csvFile);

            System.out.println(
                    "Loaded address book with " + addressBook.nodeAddress().size() + " nodes:");
            for (NodeAddress node : addressBook.nodeAddress()) {
                System.out.printf(
                        "  Node %d  account 0.0.%d  key=%s...%n",
                        node.nodeId(),
                        node.nodeAccountId().accountNum(),
                        node.rsaPubKey().substring(0, 20));
            }

            Path parent = outputFile.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }

            try (WritableStreamingData out = new WritableStreamingData(Files.newOutputStream(outputFile))) {
                NodeAddressBook.PROTOBUF.write(addressBook, out);
            }

            long fileSize = Files.size(outputFile);
            System.out.println("Written address book (" + fileSize + " bytes) to: " + outputFile);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to generate testnet genesis address book", e);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to generate testnet genesis address book", e);
        }
    }

    /**
     * Load the genesis address book from a mirror node database CSV export.
     *
     * <p>The CSV is expected to have columns {@code start_consensus_timestamp,file_data}.
     * The genesis entry has {@code start_consensus_timestamp = 1} and {@code file_data} is
     * hex-encoded protobuf prefixed with {@code \x}.
     *
     * @param csvPath path to the CSV file
     * @return the parsed genesis {@link NodeAddressBook}
     */
    static NodeAddressBook loadFromCsv(Path csvPath) throws IOException, ParseException {
        System.out.println("Reading genesis address book from CSV: " + csvPath);

        try (BufferedReader reader = Files.newBufferedReader(csvPath)) {
            String header = reader.readLine();
            if (header == null) {
                throw new IOException("CSV file is empty");
            }

            // Find column indices
            String[] columns = header.split(",", -1);
            int timestampIdx = -1;
            int fileDataIdx = -1;
            for (int i = 0; i < columns.length; i++) {
                if ("start_consensus_timestamp".equals(columns[i])) {
                    timestampIdx = i;
                } else if ("file_data".equals(columns[i])) {
                    fileDataIdx = i;
                }
            }
            if (timestampIdx < 0 || fileDataIdx < 0) {
                throw new IOException(
                        "CSV must have 'start_consensus_timestamp' and 'file_data' columns, found: " + header);
            }

            // Find the genesis row (timestamp = 1)
            String line;
            while ((line = reader.readLine()) != null) {
                String[] fields = line.split(",", 2);
                if (fields.length <= Math.max(timestampIdx, fileDataIdx)) {
                    continue;
                }

                String timestamp = fields[timestampIdx].trim();
                if ("1".equals(timestamp)) {
                    String hexData = fields[fileDataIdx];
                    // Strip \x prefix if present
                    if (hexData.startsWith("\\x")) {
                        hexData = hexData.substring(2);
                    }
                    byte[] protoBytes = HEX_FORMAT.parseHex(hexData);

                    System.out.println(
                            "Found genesis entry (timestamp=1), " + protoBytes.length + " bytes of protobuf");
                    return NodeAddressBook.PROTOBUF.parse(
                            new ReadableStreamingData(new ByteArrayInputStream(protoBytes)));
                }
            }

            throw new IOException("No genesis entry (start_consensus_timestamp=1) found in CSV");
        }
    }
}
