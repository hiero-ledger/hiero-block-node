// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.mirrornode;

import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.pbj.runtime.io.stream.ReadableStreamingData;
import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import org.hiero.block.internal.AddressBookHistory;
import org.hiero.block.internal.DatedNodeAddressBook;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Generates a testnet {@code addressBookHistory.json} from the bundled genesis address book.
 *
 * <p>Testnet has no mirror node CSV export bucket (unlike mainnet's {@code mirrornode-db-export}),
 * so the existing {@code generateAddressBook} command cannot produce the history JSON for testnet.
 * Since testnet has only had one address book (7 nodes, unchanged since the February 2024 reset),
 * this command generates the history directly from the bundled
 * {@code testnet-genesis-address-book.proto.bin} resource.
 *
 * <p>Usage example:
 * <pre>
 *   subcommands mirror generateTestnetAddressBookHistory -o testnet-addressBookHistory.json
 * </pre>
 */
@Command(
        name = "generateTestnetAddressBookHistory",
        description = "Generate testnet address book history JSON from bundled genesis address book")
public class GenerateTestnetAddressBookHistory implements Runnable {

    private static final String GENESIS_ADDRESS_BOOK_RESOURCE = "/testnet-genesis-address-book.proto.bin";
    private static final String TESTNET_GENESIS_TIMESTAMP = "2024-02-01T18:35:20.644859297Z";

    @Option(
            names = {"-o", "--output"},
            description = "Output file path for the address book history JSON",
            defaultValue = "testnet-addressBookHistory.json")
    private Path outputFile;

    @Override
    public void run() {
        try {
            // Load the bundled genesis address book
            NodeAddressBook addressBook;
            try (InputStream in = getClass().getResourceAsStream(GENESIS_ADDRESS_BOOK_RESOURCE)) {
                if (in == null) {
                    throw new IllegalStateException(
                            "Bundled resource not found on classpath: " + GENESIS_ADDRESS_BOOK_RESOURCE);
                }
                addressBook = NodeAddressBook.PROTOBUF.parse(new ReadableStreamingData(in));
            }

            System.out.println("Loaded genesis address book with "
                    + addressBook.nodeAddress().size() + " nodes");

            // Create the genesis timestamp
            Instant genesis = Instant.parse(TESTNET_GENESIS_TIMESTAMP);
            Timestamp timestamp = Timestamp.newBuilder()
                    .seconds(genesis.getEpochSecond())
                    .nanos(genesis.getNano())
                    .build();

            // Build the single-entry history
            DatedNodeAddressBook datedBook = new DatedNodeAddressBook(timestamp, addressBook);
            AddressBookHistory history = new AddressBookHistory(List.of(datedBook));

            // Write JSON output
            Path parent = outputFile.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }

            try (WritableStreamingData out = new WritableStreamingData(Files.newOutputStream(outputFile))) {
                AddressBookHistory.JSON.write(history, out);
            }

            long fileSize = Files.size(outputFile);
            System.out.println("Written address book history (" + fileSize + " bytes) to: " + outputFile);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to generate testnet address book history", e);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to generate testnet address book history", e);
        }
    }
}
