// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.mirrornode;

import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Generates the testnet genesis address book protobuf binary file from the testnet mirror node API.
 *
 * <p>This command fetches the <b>current</b> node information from the testnet mirror node and
 * serializes it as a {@link NodeAddressBook} protobuf binary file. This is valid for the genesis
 * address book as long as testnet nodes have not changed since the February 2024 reset
 * (7 nodes: 0.0.3 through 0.0.9).
 *
 * <p><b>Note:</b> The mirror node {@code /api/v1/network/nodes} endpoint does not support a
 * {@code timestamp} parameter, so historical node data cannot be queried. If testnet undergoes
 * a node change in the future, this command's output will no longer match genesis and the
 * bundled {@code testnet-genesis-address-book.proto.bin} should not be regenerated.
 *
 * <p>Usage example:
 * <pre>
 *   subcommands mirror generateTestnetAddressBook -o testnet-genesis-address-book.proto.bin
 * </pre>
 */
@Command(
        name = "generateTestnetAddressBook",
        description = "Generate testnet genesis address book proto binary from mirror node API")
public class GenerateTestnetGenesisAddressBook implements Runnable {

    private static final String TESTNET_NODES_URL =
            "https://testnet.mirrornode.hedera.com/api/v1/network/nodes?limit=25&order=asc";

    @Option(
            names = {"-o", "--output"},
            description = "Output file path for the proto binary",
            defaultValue = "testnet-genesis-address-book.proto.bin")
    private Path outputFile;

    @Override
    @SuppressWarnings("DataFlowIssue")
    public void run() {
        try {
            URL url = URI.create(TESTNET_NODES_URL).toURL();
            NodeAddressBook addressBook = MirrorNodeAddressBook.loadJsonAddressBook(url);

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
}
