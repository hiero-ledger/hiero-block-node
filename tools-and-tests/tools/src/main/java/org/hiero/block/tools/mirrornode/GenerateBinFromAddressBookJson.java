// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.mirrornode;

import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.pbj.runtime.io.stream.ReadableStreamingData;
import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
import java.nio.file.Files;
import java.nio.file.Path;
import org.hiero.block.internal.AddressBookHistory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Generate a binary NodeAddressBook protobuf file from addressBookHistory.json.
 *
 * <p>This is the inverse of {@link GenerateAddressBookFromBinFile}. It reads the JSON
 * addressBookHistory.json file and writes a binary NodeAddressBook protobuf file
 * (file 0.0.102 format) which can be mounted into a Mirror Node importer pod as
 * initialAddressBook.
 */
@Command(
        name = "generateBinFromAddressBookJson",
        description =
                "Generate a binary NodeAddressBook protobuf file (file 0.0.102 format) from addressBookHistory.json")
public class GenerateBinFromAddressBookJson implements Runnable {

    @Parameters(index = "0", description = "Input path to addressBookHistory.json")
    private Path inputFile;

    @Option(
            names = {"--output", "-o"},
            description = "Output path for binary NodeAddressBook protobuf file",
            defaultValue = "addressbook.bin")
    private Path outputFile;

    @Option(
            names = {"--index", "-i"},
            description = "Which address book to use from the history (default: 0, the first one)",
            defaultValue = "0")
    private int index;

    @Override
    public void run() {
        try {
            if (!Files.exists(inputFile)) {
                System.err.println("Error: Input file not found: " + inputFile);
                System.exit(1);
            }

            System.out.println("Reading address book history from: " + inputFile);
            AddressBookHistory history;
            try (ReadableStreamingData in = new ReadableStreamingData(Files.newInputStream(inputFile))) {
                history = AddressBookHistory.JSON.parse(in);
            }

            if (history.addressBooks().isEmpty()) {
                System.err.println("Error: No address books in input file");
                System.exit(1);
            }

            if (index >= history.addressBooks().size()) {
                System.err.println("Error: Index " + index + " out of range, only "
                        + history.addressBooks().size() + " address books available");
                System.exit(1);
            }

            NodeAddressBook addressBook = history.addressBooks().get(index).addressBook();
            System.out.println(
                    "Selected address book with " + addressBook.nodeAddress().size() + " nodes");

            Path parentDir = outputFile.getParent();
            if (parentDir != null) {
                Files.createDirectories(parentDir);
            }

            try (WritableStreamingData out = new WritableStreamingData(Files.newOutputStream(outputFile))) {
                NodeAddressBook.PROTOBUF.write(addressBook, out);
            }

            long size = Files.size(outputFile);
            System.out.println("Wrote " + size + " bytes to: " + outputFile);

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
