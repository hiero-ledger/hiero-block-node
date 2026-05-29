// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.mirrornode;

import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.pbj.runtime.io.stream.ReadableStreamingData;
import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import org.hiero.block.internal.AddressBookHistory;
import org.hiero.block.internal.DatedNodeAddressBook;
import org.hiero.block.tools.utils.TimeUtils;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Generate address book history JSON from a protobuf binary file.
 *
 * <p>This command is useful for networks like previewnet where Mirror Node CSV exports
 * are not available. It reads a raw address book protobuf binary file (e.g., file 0.0.102
 * from the database) and generates the addressBookHistory.json format expected by WRB CLI tools.
 */
@Command(
        name = "generateAddressBookFromBin",
        description = "Generate address book history JSON from a protobuf binary file (file 0.0.102)")
public class GenerateAddressBookFromBinFile implements Runnable {

    @picocli.CommandLine.Parameters(
            index = "0",
            description = "Input path to address book binary file (e.g., address_book.bin)")
    private Path inputFile;

    @Option(
            names = {"--output", "-o"},
            description = "Output path for address book history JSON file",
            defaultValue = "data/addressBookHistory.json")
    private Path outputFile;

    @Option(
            names = {"--timestamp", "-t"},
            description =
                    "Consensus timestamp for this address book in format 'seconds.nanos' (e.g., '1234567890.123456789'). "
                            + "If not provided, uses genesis timestamp (1970-01-01T00:00:00Z)")
    private String timestamp;

    @Option(
            names = {"--use-current-time"},
            description = "Use current system time as the consensus timestamp (default: false, uses genesis timestamp)")
    private boolean useCurrentTime = false;

    @Override
    public void run() {
        try {
            if (!Files.exists(inputFile)) {
                System.err.println("Error: Input file not found: " + inputFile);
                System.exit(1);
            }

            System.out.println("Reading address book from: " + inputFile);
            byte[] binData = Files.readAllBytes(inputFile);
            System.out.println("Read " + binData.length + " bytes");

            // Parse protobuf
            NodeAddressBook addressBook = NodeAddressBook.PROTOBUF.parse(
                    new ReadableStreamingData(new java.io.ByteArrayInputStream(binData)));

            System.out.println(
                    "Parsed address book with " + addressBook.nodeAddress().size() + " nodes");

            // Create timestamp
            Timestamp ts = createTimestamp();
            Instant instant = Instant.ofEpochSecond(ts.seconds(), ts.nanos());
            System.out.println("Using consensus timestamp: " + instant);

            // Create dated address book
            DatedNodeAddressBook datedAddressBook = new DatedNodeAddressBook(ts, addressBook);

            // Write to JSON
            writeAddressBookHistory(List.of(datedAddressBook));
            System.out.println("Address book history written to: " + outputFile);

            // Print node summary
            printNodeSummary(addressBook);

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private Timestamp createTimestamp() {
        if (timestamp != null) {
            // Parse custom timestamp format: "seconds.nanos"
            String[] parts = timestamp.split("\\.");
            if (parts.length != 2) {
                throw new IllegalArgumentException(
                        "Invalid timestamp format. Expected 'seconds.nanos', got: " + timestamp);
            }
            long seconds = Long.parseLong(parts[0]);
            int nanos = Integer.parseInt(parts[1]);
            return Timestamp.newBuilder().seconds(seconds).nanos(nanos).build();
        } else if (useCurrentTime) {
            // Use current system time
            Instant now = Instant.now();
            return Timestamp.newBuilder()
                    .seconds(now.getEpochSecond())
                    .nanos(now.getNano())
                    .build();
        } else {
            // Default: use genesis timestamp
            return TimeUtils.GENESIS_TIMESTAMP;
        }
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

    private void printNodeSummary(NodeAddressBook addressBook) {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("NODE SUMMARY");
        System.out.println("=".repeat(80));

        for (var node : addressBook.nodeAddress()) {
            long nodeId = getNodeAccountId(node);
            System.out.printf("Node 0.0.%d: %s:%d - %s%n", nodeId, node.ipAddress(), node.portno(), node.description());
        }

        System.out.println("=".repeat(80));
    }

    private long getNodeAccountId(com.hedera.hapi.node.base.NodeAddress nodeAddress) {
        if (nodeAddress.hasNodeAccountId() && nodeAddress.nodeAccountId().hasAccountNum()) {
            return nodeAddress.nodeAccountId().accountNum();
        } else if (nodeAddress.memo().length() > 0) {
            final String memoStr = nodeAddress.memo().asUtf8String();
            return Long.parseLong(memoStr.substring(memoStr.lastIndexOf('.') + 1));
        } else {
            throw new IllegalArgumentException("NodeAddress has no nodeAccountId or memo: " + nodeAddress);
        }
    }
}
